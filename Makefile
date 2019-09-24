SRCS = main.go

txngo:
	go build -o txngo $(SRCS)

.PHONY: run
run:
	GOMAXPROCS=1 go run $(SRCS)

test:
	GOMAXPROCS=1 go test -v

.PHONY: clean
clean:
	rm -f txngo
