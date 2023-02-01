all: clean build-master build-chunkserver build-client

clean: clean-master clean-chunkserver clean-client

# cleans builds and runs master and chunkserver
dev:
	./scripts/run.sh

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
	go build -o master cmd/master/*.go

build-chunkserver:
	go build -o chunkserver cmd/chunkserver/*.go

build-client:
	go build -o client cmd/client/*.go

tidy:
	go mod tidy

vendor:
	go mod vendor
