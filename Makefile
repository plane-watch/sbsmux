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

clean:
	@rm -v ./bin/sbsmux > /dev/null 2>&1 || true
	@rmdir -v ./bin > /dev/null 2>&1 || true