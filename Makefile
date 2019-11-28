SRCS = main.go

txngo:
	go build -o txngo $(SRCS)

.PHONY: run
run:
	go run $(SRCS)

.PHONY: run_tcp
run_tcp:
	go run $(SRCS) -tcp localhost:3000

.PHONY: test
test:
	go test -v

.PHONY: clean
clean:
	rm -f txngo
