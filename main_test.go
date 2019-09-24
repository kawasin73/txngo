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
