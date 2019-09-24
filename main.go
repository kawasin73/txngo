package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
)

const (
	LInsert = 1 + iota
	LDelete
	LUpdate
	LRead
	LCommit
	LAbort
)

var (
	ErrExist       = errors.New("record already exists")
	ErrNotExist    = errors.New("record not exists")
	ErrBufferShort = errors.New("buffer size is not enough to deserialize")
)

type Record struct {
	Key   string
	Value []byte
}

func (r *Record) Serialize(buf []byte) (int, error) {
	key := []byte(r.Key)
	value := r.Value
	total := 5 + len(key) + len(value)

	// check buffer size
	if len(buf) < total {
		return 0, ErrBufferShort
	}

	// serialize
	// TODO: support NULL value
	buf[0] = uint8(len(key))
	binary.BigEndian.PutUint32(buf[1:], uint32(len(r.Value)))
	copy(buf[5:], key)
	copy(buf[5+len(key):], r.Value)
	return total, nil
}

func (r *Record) Deserialize(buf []byte) (int, error) {
	if len(buf) < 5 {
		return 0, ErrBufferShort
	}

	// parse length
	keyLen := buf[0]
	valueLen := binary.BigEndian.Uint32(buf[1:])
	total := 5 + int(keyLen) + int(valueLen)
	if len(buf) < total {
		return 0, ErrBufferShort
	}

	// copy key and value from buffer
	r.Key = string(buf[5 : 5+keyLen])
	// TODO: support NULL value
	r.Value = make([]byte, valueLen)
	copy(r.Value, buf[5+keyLen:total])

	return total, nil
}

type RecordLog struct {
	Action uint8
	Record
}

func (r *RecordLog) Serialize(buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, ErrBufferShort
	}

	if r.Action > LRead {
		// LCommit or LAbort
		buf[0] = r.Action
		return 1, nil
	}

	// serialize record content first (check buffer size)
	n, err := r.Record.Serialize(buf[1:])
	if err != nil {
		return 0, err
	}

	buf[0] = r.Action
	return 1 + n, nil
}

func (r *RecordLog) Deserialize(buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, ErrBufferShort
	}
	r.Action = buf[0]

	switch r.Action {
	case LCommit:
		return 1, nil

	case LInsert, LUpdate, LDelete:
		n, err := r.Record.Deserialize(buf[1:])
		if err != nil {
			return 0, err
		}
		return 1 + n, nil

	default:
		return 0, fmt.Errorf("action is not supported : %v", r.Action)
	}
}

type RecordCache struct {
	Record
	deleted bool
}

type Txn struct {
	dbPath   string
	tmpPath  string
	wal      *os.File
	db       map[string]Record
	logs     []RecordLog
	writeSet map[string]RecordCache
}

func NewTxn(wal *os.File, dbPath, tmpPath string) *Txn {
	return &Txn{
		dbPath:   dbPath,
		tmpPath:  tmpPath,
		wal:      wal,
		db:       make(map[string]Record),
		writeSet: make(map[string]RecordCache),
	}
}

func (txn *Txn) LoadWAL() (int, error) {
	if _, err := txn.wal.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var (
		logs  []RecordLog
		buf   [4096]byte
		size  int
		nlogs int
	)

	// redo all record logs in WAL file
	for {
		n, err := txn.wal.Read(buf[size:])
		size += n
		if err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		}

		head := 0
		for {
			var rlog RecordLog
			n, err = rlog.Deserialize(buf[head:size])
			if err == ErrBufferShort {
				// move data to head
				copy(buf[:], buf[head:size])
				size -= head

				if size == 4096 {
					// buffer size (4096) is too short for this log
					// TODO: allocate and read directly to db buffer
					return 0, err
				}
				// read more log data to buffer
				break
			} else if err != nil {
				return 0, err
			}
			head += n
			nlogs++

			switch rlog.Action {
			case LInsert, LUpdate, LDelete:
				// append log
				logs = append(logs, rlog)

			case LCommit:
				// redo record logs
				for _, rlog := range logs {
					switch rlog.Action {
					case LInsert:
						txn.db[rlog.Key] = rlog.Record

					case LUpdate:
						// reuse Key string in db and Key in rlog will be GCed.
						r, ok := txn.db[rlog.Key]
						if !ok {
							// record in db may be sometimes deleted. complete with rlog.Key for idempotency.
							r.Key = rlog.Key
						}
						r.Value = rlog.Value
						txn.db[r.Key] = r

					case LDelete:
						delete(txn.db, rlog.Key)
					}
				}
				// clear logs
				logs = nil

			case LAbort:
				// clear logs
				logs = nil

			default:
				// skip
			}
		}
	}
	return nlogs, nil
}

func (txn *Txn) ClearWAL() error {
	if _, err := txn.wal.Seek(0, io.SeekStart); err != nil {
		return err
	} else if err = txn.wal.Truncate(0); err != nil {
		return err
		// it is not obvious that ftruncate(2) sync the change to disk or not. sync explicitly for safe.
	} else if err = txn.wal.Sync(); err != nil {
		return err
	}
	return nil
}

func (txn *Txn) SaveCheckPoint() error {
	// create temporary checkout file
	f, err := os.Create(txn.tmpPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var buf [4096]byte
	// write header
	binary.BigEndian.PutUint32(buf[:4], uint32(len(txn.db)))
	_, err = f.Write(buf[:4])
	if err != nil {
		return err
	}

	// write all data
	for _, r := range txn.db {
		// FIXME: key order in map will be randomized
		n, err := r.Serialize(buf[:])
		if err == ErrBufferShort {
			// TODO: use writev
			return err
		} else if err != nil {
			return err
		}

		// TODO: delay write and combine multi log into one buffer
		_, err = f.Write(buf[:n])
		if err != nil {
			return err
		}
	}

	// swap dbfile and temporary file
	err = os.Rename(txn.tmpPath, txn.dbPath)
	if err != nil {
		return err
	}

	return nil
}

func (txn *Txn) LoadCheckPoint() error {
	f, err := os.Open(txn.dbPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var (
		buf    [4096]byte
		size   int
		loaded uint32
		total  uint32
	)

	// read all data
	for {
		n, err := f.Read(buf[size:])
		size += n
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		head := 0
		if total == 0 {
			// parse header
			if size < 4 {
				return fmt.Errorf("file header size is too short : %v", size)
			}
			total = binary.BigEndian.Uint32(buf[:4])
			if total == 0 {
				if size == 4 {
					return nil
				} else {
					return fmt.Errorf("total is 0. but db file have some data")
				}
			}
			head += 4
		} else if loaded == total {
			break
		}

		for {
			if loaded == total {
				// loaded all records
				// try read file again to check no more data in file (if there is, file is invalid)
				break
			}
			var r Record
			n, err = r.Deserialize(buf[head:size])
			if err == ErrBufferShort {
				if size-head == 4096 {
					// buffer size (4096) is too short for this log
					// TODO: allocate and read directly to db buffer
					return err
				}
				// read more log data to buffer
				break
			} else if err != nil {
				return err
			}
			head += n

			// set data
			txn.db[r.Key] = r
			loaded++
		}
		// move data to head
		copy(buf[:], buf[head:size])
		size -= head
	}

	if loaded != total {
		return fmt.Errorf("db file is broken : total %v records but actually %v records", total, loaded)
	} else if size != 0 {
		return fmt.Errorf("db file is broken : file size is larger than expected")
	}
	return nil
}

func (txn *Txn) Read(key string) ([]byte, error) {
	if r, ok := txn.writeSet[key]; ok {
		if r.deleted {
			return nil, ErrNotExist
		}
		return r.Value, nil
	}

	r, ok := txn.db[key]
	if !ok {
		return nil, ErrNotExist
	}
	return r.Value, nil
}

func clone(v []byte) []byte {
	// TODO: support NULL value
	v2 := make([]byte, len(v))
	copy(v2, v)
	return v2
}

func (txn *Txn) Insert(key string, value []byte) error {
	// check writeSet
	if r, ok := txn.writeSet[key]; ok {
		if !r.deleted {
			return ErrExist
		}
		// reuse key in writeSet
		key = r.Key
	} else {
		// check that the key not exists in db
		if _, ok := txn.db[key]; ok {
			return ErrExist
		}
		// reallocate string
		key = string(key)
	}

	// clone value to prevent injection after transaction
	value = clone(value)

	// add to or update writeSet
	txn.writeSet[key] = RecordCache{
		Record: Record{
			Key:   key,
			Value: value,
		},
	}

	// add insert log
	txn.logs = append(txn.logs, RecordLog{
		Action: LInsert,
		Record: Record{
			Key:   key,
			Value: value,
		},
	})
	return nil
}

func (txn *Txn) Update(key string, value []byte) error {
	// check writeSet
	if r, ok := txn.writeSet[key]; ok {
		if r.deleted {
			return ErrNotExist
		}
		// reuse key in writeSet
		key = r.Key
	} else {
		// check that the key exists in db
		r, ok := txn.db[key]
		if !ok {
			return ErrNotExist
		}
		// reuse key in db
		key = r.Key
	}

	// clone value to prevent injection after transaction
	value = clone(value)

	txn.writeSet[key] = RecordCache{
		Record: Record{
			Key:   key,
			Value: value,
		},
	}

	// add update log
	txn.logs = append(txn.logs, RecordLog{
		Action: LUpdate,
		Record: Record{
			Key:   key,
			Value: value,
		},
	})
	return nil
}

func (txn *Txn) Delete(key string) error {
	// check writeSet
	if r, ok := txn.writeSet[key]; ok {
		if r.deleted {
			return ErrNotExist
		}
		// reuse key in writeSet
		key = r.Key
	} else {
		// check that the key exists in db
		r, ok := txn.db[key]
		if !ok {
			return ErrNotExist
		}
		// reuse key in db
		key = r.Key
	}

	txn.writeSet[key] = RecordCache{
		deleted: true,
		Record: Record{
			Key: key,
		},
	}

	// add delete log
	txn.logs = append(txn.logs, RecordLog{
		Action: LDelete,
		Record: Record{
			Key: key,
		},
	})

	return nil
}

func (txn *Txn) Commit() error {
	//if len(txn.writeSet) == 0 {
	//	// no need to write WAL
	//	return nil
	//}
	var (
		i   int
		buf [4096]byte
	)

	for _, rlog := range txn.logs {
		n, err := rlog.Serialize(buf[i:])
		if err == ErrBufferShort {
			// TODO: use writev
			return err
		} else if err != nil {
			return err
		}

		// TODO: delay write and combine multi log into one buffer
		_, err = txn.wal.Write(buf[:n])
		if err != nil {
			return err
		}
	}

	// write commit log
	n, err := (&RecordLog{Action: LCommit}).Serialize(buf[:])
	if err != nil {
		// commit log serialization must not fail
		log.Panic(err)
	}
	_, err = txn.wal.Write(buf[:n])
	if err != nil {
		return err
	}

	// sync this transaction
	err = txn.wal.Sync()
	if err != nil {
		return err
	}

	// write back writeSet to db (in memory)
	for _, r := range txn.writeSet {
		if r.deleted {
			delete(txn.db, r.Key)
		} else {
			txn.db[r.Key] = r.Record
		}

		// remove from writeSet
		delete(txn.writeSet, r.Key)
	}

	// clear logs
	// TODO: clear all key and value pointer and reuse logs memory
	txn.logs = nil

	return nil
}

func (txn *Txn) Abort() {
	for k := range txn.writeSet {
		delete(txn.writeSet, k)
	}
	txn.logs = nil
}

func main() {
	walPath := flag.String("wal", "./txngo.log", "file path of WAL file")
	dbPath := flag.String("db", "./txngo.db", "file path of data file")
	isInit := flag.Bool("init", true, "create data file if not exist")

	flag.Parse()

	// execute on single thread
	runtime.GOMAXPROCS(1)

	wal, err := os.OpenFile(*walPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		log.Panic(err)
	}
	defer wal.Close()

	txn := NewTxn(wal, *dbPath, *dbPath+".tmp")

	log.Println("loading data file...")
	if err = txn.LoadCheckPoint(); os.IsNotExist(err) && *isInit {
		log.Println("db file is not found. this is initial start.")
	} else if err != nil {
		log.Printf("failed to load data file : %v\n", err)
		return
	}

	log.Println("loading WAL file...")
	if nlogs, err := txn.LoadWAL(); err != nil {
		log.Printf("failed to load WAL file : %v\n", err)
		return
	} else if nlogs != 0 {
		log.Println("previous shutdown is not success...")
		log.Println("update data file...")
		if err = txn.SaveCheckPoint(); err != nil {
			log.Printf("failed to save checkpoint %v\n", err)
			return
		}
		log.Println("clear WAL file...")
		if err = txn.ClearWAL(); err != nil {
			log.Printf("failed to clear WAL file %v\n", err)
			return
		}
	}

	defer func() {
		log.Println("shutdown...")

		if err = txn.SaveCheckPoint(); err != nil {
			log.Printf("failed to save data file : %v\n", err)
		} else if err = txn.ClearWAL(); err != nil {
			log.Printf("failed to clear WAL file : %v\n", err)
		} else {
			log.Println("success to save data")
		}
	}()

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf(">> ")
		txt, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("failed to read command : %v", err)
			break
		}

		txt = strings.TrimSpace(txt)
		cmd := strings.Split(txt, " ")
		if len(cmd) == 0 || len(cmd[0]) == 0 {
			continue
		}
		switch strings.ToLower(cmd[0]) {
		case "insert":
			if len(cmd) != 3 {
				fmt.Println("invalid command : insert <key> <value>")
			} else if err = txn.Insert(cmd[1], []byte(cmd[2])); err != nil {
				fmt.Printf("failed to insert : %v\n", err)
			} else {
				fmt.Printf("success to insert %q\n", cmd[1])
			}

		case "update":
			if len(cmd) != 3 {
				fmt.Println("invalid command : update <key> <value>")
			} else if err = txn.Update(cmd[1], []byte(cmd[2])); err != nil {
				fmt.Printf("failed to update : %v\n", err)
			} else {
				fmt.Printf("success to update %q\n", cmd[1])
			}

		case "delete":
			if len(cmd) != 2 {
				fmt.Println("invalid command : delete <key>")
			} else if err = txn.Delete(cmd[1]); err != nil {
				fmt.Printf("failed to delete : %v\n", err)
			} else {
				fmt.Printf("success to delete %q\n", cmd[1])
			}

		case "read":
			if len(cmd) != 2 {
				fmt.Println("invalid command : read <key>")
			} else if v, err := txn.Read(cmd[1]); err != nil {
				fmt.Printf("failed to read : %v\n", err)
			} else {
				fmt.Printf("%v\n", string(v))
			}

		case "commit":
			if len(cmd) != 1 {
				fmt.Println("invalid command : commit")
			} else if err = txn.Commit(); err != nil {
				fmt.Printf("failed to commit : %v\n", err)
			} else {
				fmt.Println("committed")
			}

		case "abort":
			if len(cmd) != 1 {
				fmt.Println("invalid command : abort")
			} else {
				txn.Abort()
				fmt.Println("aborted")
			}

		case "keys":
			if len(cmd) != 1 {
				fmt.Println("invalid command : keys")
			} else {
				fmt.Println(">>> show keys commited <<<")
				for k, _ := range txn.db {
					fmt.Println(k)
				}
			}

		case "quit", "exit", "q":
			fmt.Println("byebye")
			return

		default:
			fmt.Println("invalid command : not supported")
		}
	}

}
