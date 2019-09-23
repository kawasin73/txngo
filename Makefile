SRCS = main.go

txngo:
	go build -o txngo $(SRCS)

.PHONY: run
run:
	go run $(SRCS)

.PHONY: clean
clean:
	rm -f txngo
