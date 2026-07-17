SHELL := /bin/sh

GO ?= go
BUILD_DIR ?= $(CURDIR)/bin
AG_BINARY ?= $(BUILD_DIR)/ag
AG_HOME ?= $(HOME)/.ag
AG_EXEC ?= $(HOME)/.local/bin/ag

.DEFAULT_GOAL := install

.PHONY: install build link test unlink clean

install: link

build:
	@mkdir -p "$(BUILD_DIR)"
	$(GO) build -trimpath -o "$(AG_BINARY)" ./cmd/ag

link: build
	@mkdir -p "$(dir $(AG_EXEC))"
	ln -sfn "$(abspath $(AG_BINARY))" "$(AG_EXEC)"
	@printf 'installed ag: %s -> %s\n' "$(AG_EXEC)" "$(abspath $(AG_BINARY))"

test:
	$(GO) test ./...

unlink:
	@if [ -L "$(AG_EXEC)" ] && \
		[ "$$(readlink "$(AG_EXEC)")" = "$(abspath $(AG_BINARY))" ]; then \
		rm "$(AG_EXEC)"; \
		printf 'removed link: %s\n' "$(AG_EXEC)"; \
	fi

clean: unlink
	rm -f "$(AG_BINARY)"
