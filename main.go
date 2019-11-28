package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/kawasin73/umutex"
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
	ErrChecksum    = errors.New("checksum does not match")
	ErrDeadLock    = errors.New("deadlock detected")
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
	if len(buf) < 5 {
		return 0, ErrBufferShort
	}

	buf[0] = r.Action
	var total = 1
	if r.Action > LRead {
		// LCommit or LAbort
	} else {
		// serialize record content first (check buffer size)
		n, err := r.Record.Serialize(buf[1:])
		if err != nil {
			return 0, err
		}
		total += n
	}
	if len(buf) < total+4 {
		return 0, ErrBufferShort
	}

	// generate checksum
	hash := crc32.NewIEEE()
	if _, err := hash.Write(buf[:total]); err != nil {
		return 0, err
	}
	binary.BigEndian.PutUint32(buf[total:], hash.Sum32())

	return total + 4, nil
}

func (r *RecordLog) Deserialize(buf []byte) (int, error) {
	if len(buf) < 5 {
		return 0, ErrBufferShort
	}
	r.Action = buf[0]
	var total = 1
	switch r.Action {
	case LCommit:

	case LInsert, LUpdate, LDelete:
		n, err := r.Record.Deserialize(buf[1:])
		if err != nil {
			return 0, err
		}
		total += n

	default:
		return 0, fmt.Errorf("action is not supported : %v", r.Action)
	}

	// validate checksum
	hash := crc32.NewIEEE()
	if _, err := hash.Write(buf[:total]); err != nil {
		return 0, err
	}
	if binary.BigEndian.Uint32(buf[total:]) != hash.Sum32() {
		return 0, ErrChecksum
	}

	return total + 4, nil
}

type lock struct {
	mu   umutex.UMutex
	refs int
}

type Locker struct {
	mu      sync.Mutex
	mutexes map[string]*lock
}

func NewLocker() *Locker {
	return &Locker{
		mutexes: make(map[string]*lock),
	}
}

func (l *Locker) refLock(key string) *lock {
	l.mu.Lock()
	rec, ok := l.mutexes[key]
	if !ok {
		// TODO: not create lock object each time, use Pool or preallocate for each record
		rec = new(lock)
		l.mutexes[key] = rec
	}
	rec.refs++
	l.mu.Unlock()
	return rec
}

func (l *Locker) unrefLock(key string) *lock {
	l.mu.Lock()
	rec := l.mutexes[key]
	rec.refs--
	if rec.refs == 0 {
		delete(l.mutexes, key)
	}
	l.mu.Unlock()
	return rec
}

func (l *Locker) Lock(key string) {
	rec := l.refLock(key)
	rec.mu.Lock()
}

func (l *Locker) Unlock(key string) {
	rec := l.unrefLock(key)
	rec.mu.Unlock()
}

func (l *Locker) RLock(key string) {
	rec := l.refLock(key)
	rec.mu.RLock()
}

func (l *Locker) RUnlock(key string) {
	rec := l.unrefLock(key)
	rec.mu.RUnlock()
}

func (l *Locker) Upgrade(key string) bool {
	rec := l.mutexes[key]
	return rec.mu.Upgrade()
}

type Storage struct {
	muWAL   sync.Mutex
	dbPath  string
	tmpPath string
	wal     *os.File
	db      map[string]Record
	lock    *Locker
}

func NewStorage(wal *os.File, dbPath, tmpPath string) *Storage {
	return &Storage{
		dbPath:  dbPath,
		tmpPath: tmpPath,
		wal:     wal,
		db:      make(map[string]Record),
		lock:    NewLocker(),
	}
}

func (s *Storage) ApplyLogs(logs []RecordLog) {
	// TODO: optimize when duplicate keys in logs
	for _, rlog := range logs {
		switch rlog.Action {
		case LInsert:
			s.db[rlog.Key] = rlog.Record

		case LUpdate:
			// reuse Key string in db and Key in rlog will be GCed.
			r, ok := s.db[rlog.Key]
			if !ok {
				// record in db may be sometimes deleted. complete with rlog.Key for idempotency.
				r.Key = rlog.Key
			}
			r.Value = rlog.Value
			s.db[r.Key] = r

		case LDelete:
			delete(s.db, rlog.Key)
		}
	}
}

func (s *Storage) SaveWAL(logs []RecordLog) error {
	// prevent parallel WAL writing by unexpected context switch
	s.muWAL.Lock()
	defer s.muWAL.Unlock()

	var (
		i   int
		buf [4096]byte
	)

	for _, rlog := range logs {
		n, err := rlog.Serialize(buf[i:])
		if err == ErrBufferShort {
			// TODO: use writev
			return err
		} else if err != nil {
			return err
		}

		// TODO: delay write and combine multi log into one buffer
		_, err = s.wal.Write(buf[:n])
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
	_, err = s.wal.Write(buf[:n])
	if err != nil {
		return err
	}

	// sync this transaction
	err = s.wal.Sync()
	if err != nil {
		return err
	}

	return nil
}

func (s *Storage) LoadWAL() (int, error) {
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}

	var (
		logs  []RecordLog
		buf   [4096]byte
		head  int
		size  int
		nlogs int
	)

	// redo all record logs in WAL file
	for {
		var rlog RecordLog
		n, err := rlog.Deserialize(buf[head:size])
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
			n, err = s.wal.Read(buf[size:])
			size += n
			if err == io.EOF {
				break
			} else if err != nil {
				return 0, err
			}
			continue
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
			s.ApplyLogs(logs)

			// clear logs
			logs = nil

		case LAbort:
			// clear logs
			logs = nil

		default:
			// skip
		}
	}

	return nlogs, nil
}

func (s *Storage) ClearWAL() error {
	if _, err := s.wal.Seek(0, io.SeekStart); err != nil {
		return err
	} else if err = s.wal.Truncate(0); err != nil {
		return err
		// it is not obvious that ftruncate(2) sync the change to disk or not. sync explicitly for safe.
	} else if err = s.wal.Sync(); err != nil {
		return err
	}
	return nil
}

func (s *Storage) SaveCheckPoint() error {
	// create temporary checkout file
	f, err := os.Create(s.tmpPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var buf [4096]byte
	// write header
	binary.BigEndian.PutUint32(buf[:4], uint32(len(s.db)))
	_, err = f.Write(buf[:4])
	if err != nil {
		goto ERROR
	}

	// write all data
	for _, r := range s.db {
		// FIXME: key order in map will be randomized
		n, err := r.Serialize(buf[:])
		if err == ErrBufferShort {
			// TODO: use writev
			goto ERROR
		} else if err != nil {
			goto ERROR
		}

		// TODO: delay write and combine multi log into one buffer
		_, err = f.Write(buf[:n])
		if err != nil {
			goto ERROR
		}
	}

	if err = f.Sync(); err != nil {
		goto ERROR
	}

	// swap dbfile and temporary file
	err = os.Rename(s.tmpPath, s.dbPath)
	if err != nil {
		goto ERROR
	}

	return nil

ERROR:
	if rerr := os.Remove(s.tmpPath); rerr != nil {
		log.Println("failed to remove temporary file for checkpoint :", rerr)
	}
	return err
}

func (s *Storage) LoadCheckPoint() error {
	f, err := os.Open(s.dbPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var buf [4096]byte

	// read and parse header
	n, err := f.Read(buf[:])
	if err != nil {
		return err
	} else if n < 4 {
		return fmt.Errorf("file header size is too short : %v", n)
	}
	total := binary.BigEndian.Uint32(buf[:4])
	if total == 0 {
		if n == 4 {
			return nil
		} else {
			return fmt.Errorf("total is 0. but db file have some data")
		}
	}

	var (
		head   = 4
		size   = n
		loaded uint32
	)

	// read all data
	for {
		var r Record
		n, err = r.Deserialize(buf[head:size])
		if err == ErrBufferShort {
			if size-head == 4096 {
				// buffer size (4096) is too short for this log
				// TODO: allocate and read directly to db buffer
				return err
			}

			// move data to head
			copy(buf[:], buf[head:size])
			size -= head

			// read more log data to buffer
			n, err = f.Read(buf[size:])
			size += n
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}
			continue
		} else if err != nil {
			return err
		}

		// set data
		s.db[r.Key] = r
		loaded++
		head += n

		if loaded > total {
			// records in checkpoint file is more than specified in header
			break
		}
	}

	if loaded != total {
		return fmt.Errorf("db file is broken : total %v records but actually %v records", total, loaded)
	} else if size != 0 {
		return fmt.Errorf("db file is broken : file size is larger than expected")
	}
	return nil
}

type Txn struct {
	s        *Storage
	logs     []RecordLog
	readSet  map[string]*Record
	writeSet map[string]int
}

func (s *Storage) NewTxn() *Txn {
	return &Txn{
		s:        s,
		readSet:  make(map[string]*Record),
		writeSet: make(map[string]int),
	}
}

func (txn *Txn) Read(key string) ([]byte, error) {
	if r, ok := txn.readSet[key]; ok {
		if r == nil {
			return nil, ErrNotExist
		}
		return r.Value, nil
	} else if idx, ok := txn.writeSet[key]; ok {
		rec := txn.logs[idx]
		if rec.Action == LDelete {
			return nil, ErrNotExist
		}
		return rec.Value, nil
	}

	// read lock
	txn.s.lock.RLock(key)

	r, ok := txn.s.db[key]
	if !ok {
		txn.readSet[key] = nil
		return nil, ErrNotExist
	}

	txn.readSet[r.Key] = &r
	return r.Value, nil
}

func clone(v []byte) []byte {
	// TODO: support NULL value
	v2 := make([]byte, len(v))
	copy(v2, v)
	return v2
}

// ensureNotExist check readSet and writeSet step by step that there IS NOT the record.
// This method is used by Insert.
func (txn *Txn) ensureNotExist(key string) (string, error) {
	if r, ok := txn.readSet[key]; ok {
		if r != nil {
			return "", ErrExist
		}
		// reallocate string
		key = string(key)

		if !txn.s.lock.Upgrade(key) {
			return "", ErrDeadLock
		}
		// move record from readSet to writeSet
		delete(txn.readSet, key)
	} else if idx, ok := txn.writeSet[key]; ok {
		rec := txn.logs[idx]
		if rec.Action != LDelete {
			return "", ErrExist
		}
		// reuse key in writeSet
		key = rec.Key
	} else {
		// lock record
		txn.s.lock.Lock(key)

		// check that the key not exists in db
		if _, ok := txn.s.db[key]; ok {
			txn.s.lock.Unlock(key)
			return "", ErrExist
		}
		// reallocate string
		key = string(key)
	}

	return key, nil
}

// ensureExist check readSet and writeSet step by step that there IS the record.
// This method is used by Update, Delete.
func (txn *Txn) ensureExist(key string) (newKey string, err error) {
	if r, ok := txn.readSet[key]; ok {
		if r == nil {
			return "", ErrNotExist
		}

		// reuse key in readSet
		key = r.Key
		if !txn.s.lock.Upgrade(key) {
			return "", ErrDeadLock
		}
		// move record from readSet to writeSet
		delete(txn.readSet, key)
	} else if idx, ok := txn.writeSet[key]; ok {
		rec := txn.logs[idx]
		if rec.Action == LDelete {
			return "", ErrNotExist
		}
		// reuse key in writeSet
		key = rec.Key
	} else {
		// lock record
		txn.s.lock.Lock(key)

		// check that the key exists in db
		r, ok := txn.s.db[key]
		if !ok {
			txn.s.lock.Unlock(key)
			return "", ErrNotExist
		}
		// reuse key in db
		key = r.Key
	}

	return key, nil
}

func (txn *Txn) Insert(key string, value []byte) error {
	key, err := txn.ensureNotExist(key)
	if err != nil {
		return err
	}

	// clone value to prevent injection after transaction
	value = clone(value)

	// add insert log
	txn.logs = append(txn.logs, RecordLog{
		Action: LInsert,
		Record: Record{
			Key:   key,
			Value: value,
		},
	})

	// add to or update writeSet (index of logs)
	txn.writeSet[key] = len(txn.logs) - 1
	return nil
}

func (txn *Txn) Update(key string, value []byte) error {
	key, err := txn.ensureExist(key)
	if err != nil {
		return err
	}

	// clone value to prevent injection after transaction
	value = clone(value)

	// add update log
	txn.logs = append(txn.logs, RecordLog{
		Action: LUpdate,
		Record: Record{
			Key:   key,
			Value: value,
		},
	})

	// add to or update writeSet (index of logs)
	txn.writeSet[key] = len(txn.logs) - 1
	return nil
}

func (txn *Txn) Delete(key string) error {
	key, err := txn.ensureExist(key)
	if err != nil {
		return err
	}

	// add delete log
	txn.logs = append(txn.logs, RecordLog{
		Action: LDelete,
		Record: Record{
			Key: key,
		},
	})

	// add to or update writeSet (index of logs)
	txn.writeSet[key] = len(txn.logs) - 1

	return nil
}

func (txn *Txn) Commit() error {
	// clearnup readSet before save WAL (S2PL)
	for key := range txn.readSet {
		txn.s.lock.RUnlock(key)
		delete(txn.readSet, key)
	}

	err := txn.s.SaveWAL(txn.logs)
	if err != nil {
		return err
	}

	// write back writeSet to db (in memory)
	txn.s.ApplyLogs(txn.logs)

	// cleanup writeSet
	for key := range txn.writeSet {
		txn.s.lock.Unlock(key)
		delete(txn.writeSet, key)
	}

	// clear logs
	// TODO: clear all key and value pointer and reuse logs memory
	txn.logs = nil

	return nil
}

func (txn *Txn) Abort() {
	for key := range txn.readSet {
		txn.s.lock.RUnlock(key)
		delete(txn.readSet, key)
	}
	for key := range txn.writeSet {
		txn.s.lock.Unlock(key)
		delete(txn.writeSet, key)
	}
	txn.logs = nil
}

func HandleTxn(r io.Reader, w io.WriteCloser, txn *Txn, storage *Storage, closeOnExit bool, wg *sync.WaitGroup) error {
	if closeOnExit {
		defer w.Close()
		defer wg.Done()
	}
	reader := bufio.NewReader(r)
	for {
		fmt.Fprintf(w, ">> ")
		txt, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintf(w, "failed to read command : %v\n", err)
			return err
		}

		txt = strings.TrimSpace(txt)
		cmd := strings.Split(txt, " ")
		if len(cmd) == 0 || len(cmd[0]) == 0 {
			continue
		}
		switch strings.ToLower(cmd[0]) {
		case "insert":
			if len(cmd) != 3 {
				fmt.Fprintf(w, "invalid command : insert <key> <value>\n")
			} else if err = txn.Insert(cmd[1], []byte(cmd[2])); err != nil {
				fmt.Fprintf(w, "failed to insert : %v\n", err)
			} else {
				fmt.Fprintf(w, "success to insert %q\n", cmd[1])
			}

		case "update":
			if len(cmd) != 3 {
				fmt.Fprintf(w, "invalid command : update <key> <value>\n")
			} else if err = txn.Update(cmd[1], []byte(cmd[2])); err != nil {
				fmt.Fprintf(w, "failed to update : %v\n", err)
			} else {
				fmt.Fprintf(w, "success to update %q\n", cmd[1])
			}

		case "delete":
			if len(cmd) != 2 {
				fmt.Fprintf(w, "invalid command : delete <key>\n")
			} else if err = txn.Delete(cmd[1]); err != nil {
				fmt.Fprintf(w, "failed to delete : %v\n", err)
			} else {
				fmt.Fprintf(w, "success to delete %q\n", cmd[1])
			}

		case "read":
			if len(cmd) != 2 {
				fmt.Fprintf(w, "invalid command : read <key>\n")
			} else if v, err := txn.Read(cmd[1]); err != nil {
				fmt.Fprintf(w, "failed to read : %v\n", err)
			} else {
				fmt.Fprintf(w, "%v\n", string(v))
			}

		case "commit":
			if len(cmd) != 1 {
				fmt.Fprintf(w, "invalid command : commit\n")
			} else if err = txn.Commit(); err != nil {
				fmt.Fprintf(w, "failed to commit : %v\n", err)
			} else {
				fmt.Fprintf(w, "committed\n")
			}

		case "abort":
			if len(cmd) != 1 {
				fmt.Fprintf(w, "invalid command : abort\n")
			} else {
				txn.Abort()
				fmt.Fprintf(w, "aborted\n")
			}

		case "keys":
			if len(cmd) != 1 {
				fmt.Fprintf(w, "invalid command : keys\n")
			} else {
				fmt.Fprintf(w, ">>> show keys commited <<<\n")
				for k, _ := range storage.db {
					fmt.Fprintf(w, "%s\n", k)
				}
			}

		case "quit", "exit", "q":
			fmt.Fprintf(w, "byebye\n")
			txn.Abort()
			return nil

		default:
			fmt.Fprintf(w, "invalid command : not supported\n")
		}
	}
}

func main() {
	walPath := flag.String("wal", "./txngo.log", "file path of WAL file")
	dbPath := flag.String("db", "./txngo.db", "file path of data file")
	isInit := flag.Bool("init", true, "create data file if not exist")
	tcpaddr := flag.String("tcp", "", "tcp handler address (e.g. localhost:3000)")

	flag.Parse()

	wal, err := os.OpenFile(*walPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		log.Panic(err)
	}
	defer wal.Close()

	storage := NewStorage(wal, *dbPath, *dbPath+".tmp")

	log.Println("loading data file...")
	if err = storage.LoadCheckPoint(); os.IsNotExist(err) && *isInit {
		log.Println("db file is not found. this is initial start.")
	} else if err != nil {
		log.Printf("failed to load data file : %v\n", err)
		return
	}

	log.Println("loading WAL file...")
	if nlogs, err := storage.LoadWAL(); err != nil {
		log.Printf("failed to load WAL file : %v\n", err)
		return
	} else if nlogs != 0 {
		log.Println("previous shutdown is not success...")
		log.Println("update data file...")
		if err = storage.SaveCheckPoint(); err != nil {
			log.Printf("failed to save checkpoint %v\n", err)
			return
		}
		log.Println("clear WAL file...")
		if err = storage.ClearWAL(); err != nil {
			log.Printf("failed to clear WAL file %v\n", err)
			return
		}
	}

	log.Println("start transactions")

	if *tcpaddr == "" {
		// stdio handler
		txn := storage.NewTxn()
		err = HandleTxn(os.Stdin, os.Stdout, txn, storage, false, nil)
		if err != nil {
			log.Println("failed to handle", err)
		}
		log.Println("shutdown...")
	} else {
		// tcp handler
		l, err := net.Listen("tcp", *tcpaddr)
		if err != nil {
			log.Println("failed to listen tcp :", err)
			return
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				conn, err := l.Accept()
				if err != nil {
					log.Println("failed to accept tcp :", err)
					break
				}
				log.Println("accept new conn :", conn.RemoteAddr())
				txn := storage.NewTxn()
				wg.Add(1)
				go HandleTxn(conn, conn, txn, storage, true, &wg)
			}
		}()

		signal.Reset()
		chsig := make(chan os.Signal)
		signal.Notify(chsig, os.Interrupt)
		<-chsig
		log.Println("shutdown...")
		l.Close()

		chDone := make(chan struct{})
		go func() {
			wg.Wait()
			chDone <- struct{}{}
		}()
		select {
		case <-time.After(30 * time.Second):
			log.Println("connection not quit. shutdown forcibly.")
			return
		case <-chDone:
		}
	}

	log.Println("save checkpoint")
	if err = storage.SaveCheckPoint(); err != nil {
		log.Printf("failed to save data file : %v\n", err)
	} else if err = storage.ClearWAL(); err != nil {
		log.Printf("failed to clear WAL file : %v\n", err)
	} else {
		log.Println("success to save data")
	}
}
