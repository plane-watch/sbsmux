all: sbsmux

sbsmux: mkbindir install_modules
	@go build -o ./bin/sbsmux ./cmd/sbsmux
	@./bin/sbsmux --version

install: sbsmux
	@cp -v ./bin/sbsmux /usr/local/sbin/sbsmux

install_modules:
	@go mod tidy

mkbindir:
	@mkdir -p ./bin

test:
	go test -v -failfast -count=2 -timeout=5m ./...
	bash ./test.sh

clean:
	@rm -v ./bin/sbsmux > /dev/null 2>&1 || true
	@rmdir -v ./bin > /dev/null 2>&1 || true