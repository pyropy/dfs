all: clean build-master build-chunkserver build-client

clean: clean-master clean-chunkserver clean-client

clean-master:
	rm -f ./master

clean-chunkserver:
	rm -f ./chunkserver

clean-client:
	rm -f ./client

run-master:
	go run cmd/master/*.go

run-client:
	go run cmd/client/*.go

run-chunkserver:
	go run cmd/chunkserver/*.go

build-master:
	go build -o master cmd/master/main.go

build-chunkserver:
	go build -o chunkserver cmd/chunkserver/main.go

build-client:
	go build -o client cmd/client/main.go

tidy:
	go mod tidy

vendor:
	go mod vendor
