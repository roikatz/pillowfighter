GO ?= go
BINARY := bin/cb-loadgen

.PHONY: build vet test fmt run clean

build:
	$(GO) build -o $(BINARY) ./cmd/cb-loadgen

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

fmt:
	gofmt -l -w .

run: build
	./$(BINARY) run --config config/config.example.yaml

clean:
	rm -rf bin results/*.json results/*.csv
