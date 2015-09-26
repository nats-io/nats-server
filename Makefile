.PHONY: build clean test

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  build                 - compile binaries into bin/"
	@echo "  clean                 - clean up bin/"
	@echo "  test                  - run tests"

build:
	@mkdir -p ./bin
	GOGC=off go build -i -o ./bin/gnatsd ./cmd/gnatsd

clean:
	@rm -rf $$GOPATH/pkg/*/github.com/nats-io/gnatsd{,.*}
	@rm -rf ./bin

test:
	@GOGC=off go test
