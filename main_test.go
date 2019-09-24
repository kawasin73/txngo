package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

const (
	tmpdir = "tmp"
)

var (
	testWALPath = filepath.Join(tmpdir, "test.log")
)

func createTestTxn(t *testing.T) *Txn {
	_ = os.MkdirAll("./tmp", 0777)
	_ = os.Remove(testWALPath)
	file, err := os.OpenFile(testWALPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	return NewTxn(file)
}

func TestTxn_Insert(t *testing.T) {
	txn := createTestTxn(t)
	defer txn.wal.Close()
	if err := txn.Insert("key1", []byte("value1")); err != nil {
		t.Errorf("failed to insert key1 : %v", err)
	}
	if err := txn.Insert("key1", []byte("value2")); err != ErrExist {
		t.Errorf("unexpectedly success to insert duplicate key : %v", err)
	}
	if err := txn.Insert("key2", []byte("value3")); err != nil {
		t.Errorf("failed to insert key2 : %v", err)
	}
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

func TestTxn_Commit(t *testing.T) {
	txn := createTestTxn(t)
	defer txn.wal.Close()
	var (
		value1 = []byte("value1")
		value2 = []byte("value2")
	)
	if err := txn.Insert("key1", value1); err != nil {
		t.Errorf("failed to insert key1 : %v", err)
	}
	if err := txn.Insert("key2", value2); err != nil {
		t.Errorf("failed to insert key2 : %v", err)
	}
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
	} else if !bytes.Equal(v, value2) {
		t.Errorf("value2 is not match %v : %v", v, value1)
	}
}
