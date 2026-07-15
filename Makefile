OUT=out_clickhouse.so
VERSION?=dev
COMMIT?=$(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

all:

	CGO_ENABLED=1 \
	GOOS=linux GOARCH=amd64 \
	CGO_CFLAGS="-O2 -fstack-protector-all -D_FORTIFY_SOURCE=2" \
	CGO_LDFLAGS="-Wl,-z,relro,-z,now,-z,noexecstack -fstack-protector-all" \
	go build -buildmode=c-shared \
		-ldflags="-s -w -X main.pluginVersion=$(VERSION) -X main.pluginCommit=$(COMMIT)" \
		-o $(OUT)

build-native:

	CGO_ENABLED=1 \
	CGO_CFLAGS="-O2 -fstack-protector-all -D_FORTIFY_SOURCE=2" \
	CGO_LDFLAGS="-Wl,-z,relro,-z,now,-z,noexecstack -fstack-protector-all" \
	go build -buildmode=c-shared \
		-ldflags="-s -w -X main.pluginVersion=$(VERSION).native -X main.pluginCommit=$(COMMIT)" \
		-o $(OUT)

test:

	go test -v -race -count=1 ./...

bench:

	go test -bench=. -benchmem -count=1 ./...

vet:

	go vet ./...

clean:

	rm -rf *.so *.h *~

.PHONY: all test bench vet clean
