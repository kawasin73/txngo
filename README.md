# txngo

[![Build Status](https://github.com/kawasin73/txngo/workflows/Go/badge.svg)](https://github.com/kawasin73/txngo/actions)

simple transaction implementation with Go.

txngo is now based on **S2PL (Strict Two Phase Lock) Concurrency Control** with Single Thread and is **In-memory KVS**.

Key is string (< 255 length) and Value is []byte (< unsigned 32bit interger max size). 

## Features

- Supports `INSERT` `UPDATE` `READ` `DELETE` `COMMIT` `ABORT` operations.
- WAL (Write Ahead Log)
  - Only have Redo log and write all logs at commit phase 
- Checkpoint
  - write back data only when shutdown
- Crash Recovery
  - Redo log have idempotency.
- Interactive Interface using stdin and stdout or tcp connection

## Example

```bash
$ txngo
# or
$ cd path/to/txngo && make run
GOMAXPROCS=1 go run main.go
2019/09/24 20:14:44 loading data file...
2019/09/24 20:14:44 db file is not found. this is initial start.
2019/09/24 20:14:44 loading WAL file...
>> insert key1 value1
success to insert "key1"
>> insert key2 value2
success to insert "key2"
>> insert key3 value3
success to insert "key3"
>> read key1
value1
>> commit
committed
>> read key1
value1
>> delete key2
success to delete "key2"
>> update key3 value4
success to update "key3"
>> commit
committed
>> read key3
value4
>> read key2
failed to read : record not exists
>> delete key1
success to delete "key1"
>> abort
aborted
>> keys
>>> show keys commited <<<
key3
key1
>> quit
byebye
2019/09/24 20:15:56 shutdown...
2019/09/24 20:15:56 success to save data
$ make run
GOMAXPROCS=1 go run main.go
2019/09/24 20:15:58 loading data file...
2019/09/24 20:15:58 loading WAL file...
>> keys
>>> show keys commited <<<
key1
key3
>> read key1
value1
>> read key3
value4
>> quit
byebye
2019/09/24 20:16:11 shutdown...
2019/09/24 20:16:11 success to save data
```

## Options

```bash
$ ./txngo -h
Usage of ./txngo:
  -db string
    	file path of data file (default "./txngo.db")
  -init
    	create data file if not exist (default true)
  -tcp string
    	tcp handler address (e.g. localhost:3000)
  -wal string
    	file path of WAL file (default "./txngo.log")
```

## How to

### Installation

```bash
$ go get https://github.com/kawasin73/txngo.git
```

### Test

```bash
$ make test
```

## LICENSE

MIT
