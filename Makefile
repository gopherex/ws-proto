.PHONY: gen test fmt build-gen gen-example

gen:
	easyp generate

build-gen:
	go build -o bin/protoc-gen-go-ws ./cmd/protoc-gen-go-ws

gen-example: build-gen
	cd example && PATH="$(CURDIR)/bin:$$PATH" easyp generate

fmt:
	gofmt -w .

test:
	gofmt -l .
	go vet ./...
	go test ./...
