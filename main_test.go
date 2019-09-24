package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const (
	tmpdir = "tmp"
)

var (
	testWALPath = filepath.Join(tmpdir, "test.log")
	testDBPath  = filepath.Join(tmpdir, "test.db")
	testTmpPath = filepath.Join(tmpdir, "test.tmp")
)

func createTestTxn(t *testing.T) *Txn {
	_ = os.RemoveAll(tmpdir)
	_ = os.MkdirAll(tmpdir, 0777)
	file, err := os.OpenFile(testWALPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	return NewTxn(file, testDBPath, testTmpPath)
}

func TestTxn_Insert(t *testing.T) {
	var (
		value1 = []byte("value1")
		value2 = []byte("value2")
		value3 = []byte("value3")
	)
	t.Run("normal case", func(t *testing.T) {
		txn := createTestTxn(t)
		defer txn.wal.Close()
		if err := txn.Insert("key1", value1); err != nil {
			t.Errorf("failed to insert key1 : %v", err)
		}
		if err := txn.Insert("key1", value1); err != ErrExist {
			t.Errorf("unexpectedly success to insert duplicate key : %v", err)
		}
		if err := txn.Insert("key2", value2); err != nil {
			t.Errorf("failed to insert key2 : %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Errorf("failed to commit : %v", err)
		}

		// insert after commit
		if err := txn.Insert("key1", value3); err != ErrExist {
			t.Errorf("unexpectedly success to insert duplicate key after commit : %v", err)
		}
	})

	t.Run("insert after delete", func(t *testing.T) {
		txn := createTestTxn(t)
		defer txn.wal.Close()
		if err := txn.Insert("key1", value1); err != nil {
			t.Errorf("failed to insert key1 : %v", err)
		}
		if err := txn.Delete("key1"); err != nil {
			t.Errorf("failed to delete key1 : %v", err)
		}
		if err := txn.Insert("key1", value2); err != nil {
			t.Errorf("failed to insert key1 after delete : %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Errorf("failed to commit : %v", err)
		}
		if err := txn.Delete("key1"); err != nil {
			t.Errorf("failed to delete key1 after commit : %v", err)
		}
		if err := txn.Insert("key1", value3); err != nil {
			t.Errorf("failed to insert key1 after commit and delete : %v", err)
		}
	})
}

func TestTxn_Read(t *testing.T) {
	txn := createTestTxn(t)
	defer txn.wal.Close()
	value1 := []byte("value1")
	if _, err := txn.Read("key1"); err != ErrNotExist {
		t.Errorf("key1 is not (not exist) : %v", err)
	}
	if err := txn.Insert("key1", value1); err != nil {
		t.Errorf("failed to insert key1 : %v", err)
	}
	if v, err := txn.Read("key1"); err != nil {
		t.Errorf("failed to read key1 : %v", err)
	} else if !bytes.Equal(v, value1) {
		t.Errorf("value is not match %v : %v", v, value1)
	}
}

func TestTxn_Update(t *testing.T) {
	txn := createTestTxn(t)
	defer txn.wal.Close()
	var (
		value1 = []byte("value1")
		value2 = []byte("value2")
		value3 = []byte("value3")
	)
	if err := txn.Update("key1", value1); err != ErrNotExist {
		t.Errorf("key1 is not (not exist) : %v", err)
	}
	if err := txn.Insert("key1", value1); err != nil {
		t.Errorf("failed to insert key1 : %v", err)
	}
	if err := txn.Update("key1", value2); err != nil {
		t.Errorf("failed to update key1 : %v", err)
	}
	if v, err := txn.Read("key1"); err != nil {
		t.Errorf("failed to read key1 : %v", err)
	} else if !bytes.Equal(v, value2) {
		t.Errorf("value is not match %v : %v", v, value2)
	}
	if err := txn.Commit(); err != nil {
		t.Errorf("failed to commit : %v", err)
	}

	// update after commit
	if err := txn.Update("key1", value3); err != nil {
		t.Errorf("failed to update key1 : %v", err)
	}
	if v, err := txn.Read("key1"); err != nil {
		t.Errorf("failed to read key1 after commit : %v", err)
	} else if !bytes.Equal(v, value3) {
		t.Errorf("value is not match after commit %v : %v", v, value3)
	}
}

func TestTxn_Delete(t *testing.T) {
	var (
		value1 = []byte("value1")
	)
	t.Run("normal case", func(t *testing.T) {
		txn := createTestTxn(t)
		defer txn.wal.Close()
		if err := txn.Delete("key1"); err != ErrNotExist {
			t.Errorf("key1 is not (not exist) : %v", err)
		}
		if err := txn.Insert("key1", value1); err != nil {
			t.Errorf("failed to insert key1 : %v", err)
		}
		if err := txn.Delete("key1"); err != nil {
			t.Errorf("failed to delete key1 : %v", err)
		}
		if _, err := txn.Read("key1"); err != ErrNotExist {
			t.Errorf("key1 is not (not exist) after delete : %v", err)
		}
		if err := txn.Delete("key1"); err != ErrNotExist {
			t.Errorf("deleted key1 must not exist : %v", err)
		}
	})

	t.Run("delete after commit", func(t *testing.T) {
		txn := createTestTxn(t)
		defer txn.wal.Close()
		if err := txn.Insert("key1", value1); err != nil {
			t.Errorf("failed to insert key1 : %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Errorf("failed to commit : %v", err)
		}

		// delete after commit
		if err := txn.Delete("key1"); err != nil {
			t.Errorf("failed to delete key1 : %v", err)
		}
		if _, err := txn.Read("key1"); err != ErrNotExist {
			t.Errorf("key1 is not (not exist) after delete : %v", err)
		}
	})
}

func TestTxn_Commit(t *testing.T) {
	txn := createTestTxn(t)
	defer txn.wal.Close()
	var (
		value1 = []byte("value1")
		value2 = []byte("value2")
		value3 = []byte("value3")
	)
	// just insert
	if err := txn.Insert("key1", value1); err != nil {
		t.Errorf("failed to insert key1 : %v", err)
	}

	// updated key
	if err := txn.Insert("key2", value2); err != nil {
		t.Errorf("failed to insert key2 : %v", err)
	}
	if err := txn.Update("key2", value3); err != nil {
		t.Errorf("failed to update key2 : %v", err)
	}

	// deleted key
	if err := txn.Insert("key3", value3); err != nil {
		t.Errorf("failed to insert key3 : %v", err)
	}
	if err := txn.Delete("key3"); err != nil {
		t.Errorf("failed to delete key3 : %v", err)
	}

	// commit
	if err := txn.Commit(); err != nil {
		t.Errorf("failed to commit")
	}
	if len(txn.writeSet) != 0 {
		t.Errorf("writeSet is not cleared after commit : len == %v", len(txn.writeSet))
	}
	if v, err := txn.Read("key1"); err != nil {
		t.Errorf("failed to read key1 : %v", err)
	} else if !bytes.Equal(v, value1) {
		t.Errorf("value1 is not match %v : %v", v, value1)
	}
	if v, err := txn.Read("key2"); err != nil {
		t.Errorf("failed to read key2 : %v", err)
	} else if !bytes.Equal(v, value3) {
		t.Errorf("value2 is not match %v : %v", v, value3)
	}
	if _, err := txn.Read("key3"); err != ErrNotExist {
		t.Errorf("key3 is not (not exist) : %v", err)
	}
}

func TestTxn_Abort(t *testing.T) {
	var (
		value1 = []byte("value1")
		value2 = []byte("value2")
	)
	t.Run("abort insert", func(t *testing.T) {
		txn := createTestTxn(t)
		defer txn.wal.Close()
		if err := txn.Insert("key1", value1); err != nil {
			t.Errorf("failed to insert key1 : %v", err)
		}
		txn.Abort()
		if _, err := txn.Read("key1"); err != ErrNotExist {
			t.Errorf("key1 is not (not exist) : %v", err)
		}
	})

	t.Run("abort update", func(t *testing.T) {
		txn := createTestTxn(t)
		defer txn.wal.Close()
		if err := txn.Insert("key1", value1); err != nil {
			t.Errorf("failed to insert key1 : %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Errorf("failed to commit")
		}

		if err := txn.Update("key1", value2); err != nil {
			t.Errorf("failed to update key1 : %v", err)
		}
		txn.Abort()
		if v, err := txn.Read("key1"); err != nil {
			t.Errorf("failed to read key1 : %v", err)
		} else if !bytes.Equal(v, value1) {
			t.Errorf("value1 is not match %v : %v", v, value1)
		}
	})

	t.Run("abort delete", func(t *testing.T) {
		txn := createTestTxn(t)
		defer txn.wal.Close()
		if err := txn.Insert("key1", value1); err != nil {
			t.Errorf("failed to insert key1 : %v", err)
		}
		if err := txn.Commit(); err != nil {
			t.Errorf("failed to commit")
		}

		if err := txn.Delete("key1"); err != nil {
			t.Errorf("failed to delete key1 : %v", err)
		}
		txn.Abort()
		if v, err := txn.Read("key1"); err != nil {
			t.Errorf("failed to read key1 : %v", err)
		} else if !bytes.Equal(v, value1) {
			t.Errorf("value1 is not match %v : %v", v, value1)
		}
	})
}

func assertValue(t *testing.T, txn *Txn, key string, value []byte) {
	if v, err := txn.Read(key); err != nil {
		t.Errorf("failed to read %q : %v", key, err)
	} else if !bytes.Equal(v, value) {
		t.Errorf("read value for %q is not match %v, expected %v", key, v, value)
	}
}

func assertNotExist(t *testing.T, txn *Txn, key string) {
	if v, err := txn.Read(key); err != ErrNotExist {
		if err == nil {
			t.Errorf("unexpectedly value for %q exists : %v", key, v)
		} else {
			t.Errorf("failed to read %q expected not exist : %v", key, err)
		}
	}
}

func clearFile(t *testing.T, file *os.File) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Errorf("failed to seek : %v", err)
	} else if err = file.Truncate(0); err != nil {
		t.Errorf("failed to truncate : %v", err)
	}
}

func writeLogs(t *testing.T, file *os.File, logs []RecordLog) {
	var buf [4096]byte
	for i, rlog := range logs {
		if n, err := rlog.Serialize(buf[:]); err != nil {
			t.Errorf("failed to deserialize %v : %v", i, err)
		} else if _, err = file.Write(buf[:n]); err != nil {
			t.Errorf("failed to write log %v : %v", i, err)
		}
	}
}

func readLogs(t *testing.T, filename string) ([]byte, []RecordLog) {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Errorf("failed to read WAL file : %v", err)
	}
	var logsInFile []RecordLog
	for i := 0; ; i++ {
		var rlog RecordLog
		n, err := rlog.Deserialize(buf)
		if err == ErrBufferShort {
			break
		} else if err != nil {
			t.Fatalf("failed to deserialize log : n == %v : %v : buffer = %v", i, err, buf)
		}
		logsInFile = append(logsInFile, rlog)
		buf = buf[n:]
	}
	return buf, logsInFile
}

func applyLogs(t *testing.T, txn *Txn, logs []RecordLog) {
	for _, rlog := range logs {
		switch rlog.Action {
		case LInsert:
			if err := txn.Insert(rlog.Key, rlog.Value); err != nil {
				t.Errorf("failed to insert %v : %v", rlog, err)
			}

		case LUpdate:
			if err := txn.Update(rlog.Key, rlog.Value); err != nil {
				t.Errorf("failed to update %v : %v", rlog, err)
			}

		case LDelete:
			if err := txn.Delete(rlog.Key); err != nil {
				t.Errorf("failed to delete %v : %v", rlog, err)
			}

		case LCommit:
			if err := txn.Commit(); err != nil {
				t.Errorf("failed to commit %v : %v", rlog, err)
			}

		default:
			t.Fatalf("unexpected log %v", rlog)
		}
	}
}

func TestWAL(t *testing.T) {
	txn := createTestTxn(t)
	defer txn.wal.Close()
	logs := []RecordLog{
		{Action: LCommit},
		{Action: LInsert, Record: Record{Key: "key1", Value: []byte("value1")}},
		{Action: LInsert, Record: Record{Key: "key2", Value: []byte("value2")}},
		{Action: LInsert, Record: Record{Key: "key3", Value: []byte("value3")}},
		{Action: LInsert, Record: Record{Key: "key4", Value: []byte("value4")}},
		{Action: LUpdate, Record: Record{Key: "key2", Value: []byte("value5")}},
		{Action: LDelete, Record: Record{Key: "key3", Value: []byte("")}}, // TODO: delete log not need to have value
		{Action: LUpdate, Record: Record{Key: "key4", Value: []byte("value6")}},
		{Action: LUpdate, Record: Record{Key: "key4", Value: []byte("value7")}},
		{Action: LCommit},
		{Action: LUpdate, Record: Record{Key: "key1", Value: []byte("value8")}},
		{Action: LDelete, Record: Record{Key: "key2", Value: []byte("")}}, // TODO: delete log not need to have value
		{Action: LInsert, Record: Record{Key: "key3", Value: []byte("value8")}},
		{Action: LCommit},
	}
	applyLogs(t, txn, logs)

	rest, logsInFile := readLogs(t, txn.wal.Name())
	if len(rest) != 0 {
		t.Fatalf("log file is bigger than expected : %v", rest)
	} else if len(logsInFile) != len(logs) {
		t.Fatalf("count of log not match %v, expected %v", len(logsInFile), len(logs))
	}
	for i := 0; i < len(logs); i++ {
		if !reflect.DeepEqual(logsInFile[i], logs[i]) {
			t.Errorf("log not match : index == %v, %v, %v", i, logsInFile[i], logs[i])
		}
	}
}

func TestTxn_LoadWAL(t *testing.T) {
	t.Run("empty WAL", func(t *testing.T) {
		txn := createTestTxn(t)
		defer txn.wal.Close()
		if err := txn.LoadWAL(); err != nil {
			t.Errorf("failed to load : %v", err)
		}
	})

	t.Run("normal case", func(t *testing.T) {
		logs := []RecordLog{
			{Action: LCommit},
			{Action: LInsert, Record: Record{Key: "key1", Value: []byte("value1")}},
			{Action: LInsert, Record: Record{Key: "key2", Value: []byte("value2")}},
			{Action: LInsert, Record: Record{Key: "key3", Value: []byte("value3")}},
			{Action: LInsert, Record: Record{Key: "key4", Value: []byte("value4")}},
			{Action: LUpdate, Record: Record{Key: "key2", Value: []byte("value5")}},
			{Action: LDelete, Record: Record{Key: "key3", Value: []byte("")}}, // TODO: delete log not need to have value
			{Action: LUpdate, Record: Record{Key: "key4", Value: []byte("value6")}},
			{Action: LUpdate, Record: Record{Key: "key4", Value: []byte("value7")}},
			{Action: LCommit},
			{Action: LUpdate, Record: Record{Key: "key1", Value: []byte("value8")}},
			{Action: LDelete, Record: Record{Key: "key2", Value: []byte("")}}, // TODO: delete log not need to have value
			{Action: LInsert, Record: Record{Key: "key3", Value: []byte("value9")}},
			{Action: LCommit},
		}
		txn := createTestTxn(t)
		defer txn.wal.Close()
		// write log to WAL file
		writeLogs(t, txn.wal, logs)

		if err := txn.LoadWAL(); err != nil {
			t.Errorf("failed to load : %v", err)
		}
		assertValue(t, txn, "key1", []byte("value8"))
		assertNotExist(t, txn, "key2")
		assertValue(t, txn, "key3", []byte("value9"))
		assertValue(t, txn, "key4", []byte("value7"))

		// check idenpotency
		clearFile(t, txn.wal)
		writeLogs(t, txn.wal, logs)
		if err := txn.LoadWAL(); err != nil {
			t.Errorf("failed to load : %v", err)
		}
		assertValue(t, txn, "key1", []byte("value8"))
		assertNotExist(t, txn, "key2")
		assertValue(t, txn, "key3", []byte("value9"))
		assertValue(t, txn, "key4", []byte("value7"))
	})

	t.Run("log is not completed", func(t *testing.T) {
		logs := []RecordLog{
			{Action: LCommit},
			{Action: LInsert, Record: Record{Key: "key1", Value: []byte("value1")}},
			{Action: LInsert, Record: Record{Key: "key2", Value: []byte("value2")}},
			{Action: LInsert, Record: Record{Key: "key3", Value: []byte("value3")}},
			{Action: LInsert, Record: Record{Key: "key4", Value: []byte("value4")}},
			{Action: LUpdate, Record: Record{Key: "key2", Value: []byte("value5")}},
			{Action: LDelete, Record: Record{Key: "key3", Value: []byte("")}}, // TODO: delete log not need to have value
			{Action: LUpdate, Record: Record{Key: "key4", Value: []byte("value6")}},
			{Action: LUpdate, Record: Record{Key: "key4", Value: []byte("value7")}},
			{Action: LCommit},
			{Action: LUpdate, Record: Record{Key: "key1", Value: []byte("value8")}},
			{Action: LDelete, Record: Record{Key: "key2", Value: []byte("")}}, // TODO: delete log not need to have value
			{Action: LInsert, Record: Record{Key: "key3", Value: []byte("value9")}},
		}
		txn := createTestTxn(t)
		defer txn.wal.Close()
		// write log to WAL file
		writeLogs(t, txn.wal, logs)

		if err := txn.LoadWAL(); err != nil {
			t.Errorf("failed to load : %v", err)
		}
		assertValue(t, txn, "key1", []byte("value1"))
		assertValue(t, txn, "key2", []byte("value5"))
		assertNotExist(t, txn, "key3")
		assertValue(t, txn, "key4", []byte("value7"))

		// check idenpotency
		clearFile(t, txn.wal)
		writeLogs(t, txn.wal, logs)
		if err := txn.LoadWAL(); err != nil {
			t.Errorf("failed to load : %v", err)
		}
		assertValue(t, txn, "key1", []byte("value1"))
		assertValue(t, txn, "key2", []byte("value5"))
		assertNotExist(t, txn, "key3")
		assertValue(t, txn, "key4", []byte("value7"))
	})
}

func TestTxn_ClearWAL(t *testing.T) {
	logs := []RecordLog{
		{Action: LInsert, Record: Record{Key: "key1", Value: []byte("value1")}},
		{Action: LInsert, Record: Record{Key: "key2", Value: []byte("value2")}},
		{Action: LInsert, Record: Record{Key: "key3", Value: []byte("value3")}},
		{Action: LCommit},
	}
	txn := createTestTxn(t)
	defer txn.wal.Close()
	writeLogs(t, txn.wal, logs)

	if err := txn.ClearWAL(); err != nil {
		t.Errorf("failed to clearWAL : %v", err)
	}

	// rewrite from the offset ClearWAL set (must be 0)
	writeLogs(t, txn.wal, logs)

	rest, logsInFile := readLogs(t, txn.wal.Name())
	if len(rest) != 0 {
		t.Fatalf("log file is bigger than expected : %v", rest)
	} else if len(logsInFile) != len(logs) {
		t.Fatalf("count of log not match %v, expected %v", len(logsInFile), len(logs))
	}
	for i := 0; i < len(logs); i++ {
		if !reflect.DeepEqual(logsInFile[i], logs[i]) {
			t.Errorf("log not match : index == %v, %v, %v", i, logsInFile[i], logs[i])
		}
	}
}

func TestTxn_SaveCheckPoint(t *testing.T) {
	logs := []RecordLog{
		{Action: LCommit},
		{Action: LInsert, Record: Record{Key: "key1", Value: []byte("value1")}},
		{Action: LInsert, Record: Record{Key: "key2", Value: []byte("value2")}},
		{Action: LInsert, Record: Record{Key: "key3", Value: []byte("value3")}},
		{Action: LInsert, Record: Record{Key: "key4", Value: []byte("value4")}},
		{Action: LUpdate, Record: Record{Key: "key2", Value: []byte("value5")}},
		{Action: LDelete, Record: Record{Key: "key3", Value: []byte("")}}, // TODO: delete log not need to have value
		{Action: LUpdate, Record: Record{Key: "key4", Value: []byte("value6")}},
		{Action: LUpdate, Record: Record{Key: "key4", Value: []byte("value7")}},
		{Action: LCommit},
		{Action: LUpdate, Record: Record{Key: "key1", Value: []byte("value8")}},
		{Action: LDelete, Record: Record{Key: "key2", Value: []byte("")}}, // TODO: delete log not need to have value
		{Action: LInsert, Record: Record{Key: "key3", Value: []byte("value9")}},
		{Action: LCommit},
	}
	txn := createTestTxn(t)
	defer txn.wal.Close()
	applyLogs(t, txn, logs)

	if err := txn.SaveCheckPoint(); err != nil {
		t.Errorf("failed to save checkpoint : %v", err)
	}

	// load checkpoint file by new Txn
	wal2, err := os.Open(testWALPath)
	if err != nil {
		t.Errorf("failed to open wal file : %v", err)
	}
	txn2 := NewTxn(wal2, testDBPath, testTmpPath)
	if err = txn2.LoadCheckPoint(); err != nil {
		t.Errorf("failed to load checkpoint : %v", err)
	}
	if !reflect.DeepEqual(txn2.db, txn.db) {
		t.Errorf("loaded records not match")
	}
}
