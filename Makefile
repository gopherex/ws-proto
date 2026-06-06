.PHONY: gen test fmt

gen:
	easyp generate

fmt:
	gofmt -w .

test:
	gofmt -l .
	go vet ./...
	go test ./...
