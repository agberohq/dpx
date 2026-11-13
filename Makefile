BIN       := dpxd
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
DIST      := dist
LDFLAGS   := -s -w
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64

.PHONY: build test test-race vet fmt clean release

build:
	go build -o bin/$(BIN) ./cmd/dpxd

test:
	go test -short ./...

test-race:
	go test -short -race ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

clean:
	rm -rf bin $(DIST)

release:
	@rm -rf $(DIST) && mkdir -p $(DIST)
	@set -e; for p in $(PLATFORMS); do \
		os=$${p%/*}; arch=$${p#*/}; \
		name=$(BIN)_$(VERSION)_$${os}_$${arch}; \
		ext=; [ "$$os" = windows ] && ext=.exe; \
		echo "building $$os/$$arch"; \
		GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build -trimpath \
			-ldflags "$(LDFLAGS)" -o $(DIST)/$$name$$ext ./cmd/dpxd; \
		if [ "$$os" = windows ]; then \
			(cd $(DIST) && zip -q $$name.zip $$name$$ext); \
		else \
			(cd $(DIST) && tar -czf $$name.tar.gz $$name$$ext); \
		fi; \
		rm $(DIST)/$$name$$ext; \
	done
	@cd $(DIST) && (command -v sha256sum >/dev/null && sha256sum *.tar.gz *.zip > SHA256SUMS \
		|| shasum -a 256 *.tar.gz *.zip > SHA256SUMS)
	@ls -lh $(DIST)
