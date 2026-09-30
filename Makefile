VERSION ?= 0.1.2
PREFIX ?= $(HOME)/.local
LDFLAGS = -s -w -X main.version=$(VERSION)

.PHONY: build test install linux
build:
	go build -trimpath -ldflags '$(LDFLAGS)' -o mct-backup ./cmd/mct-backup

test:
	go test -race ./...
	go vet ./...

install: build
	install -d "$(PREFIX)/bin"
	install -m 0755 mct-backup "$(PREFIX)/bin/mct-backup"

linux:
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/mct-backup-linux-amd64 ./cmd/mct-backup
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags '$(LDFLAGS)' -o dist/mct-backup-linux-arm64 ./cmd/mct-backup
