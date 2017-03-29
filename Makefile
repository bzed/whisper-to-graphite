#!/usr/bin/make -f

NAME:=whisper-to-graphite

GO ?= go

all: $(NAME)

$(NAME): *.go
	$(GO) get
	$(GO) build

clean:
	rm -f $(NAME)
test: $(NAME)
	go vet *.go

.PHONY: clean test
