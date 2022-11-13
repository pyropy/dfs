all: clean build-master build-chunkserver build-client

clean: clean-master clean-chunkserver clean-client

clean-master:
	rm -f ./master

clean-chunkserver:
	rm -f ./chunkserver

clean-client:
	rm -f ./client

run-master:
	go run app/services/master/main.go

run-client:
	go run app/services/client/main.go

run-chunkserver:
	go run app/services/chunkserver/main.go

build-master:
	go build -o master app/services/master/main.go

build-chunkserver:
	go build -o chunkserver app/services/chunkserver/main.go

build-client:
	go build -o chunkserver app/services/chunkserver/main.go

tidy:
	go mod tidy

vendor:
	go mod vendor
