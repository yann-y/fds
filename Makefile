build:
	rm -rf fds
	go mod tidy
	go build -ldflags "-s -w" -o fds ./cmd/gateway