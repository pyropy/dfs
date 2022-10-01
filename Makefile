run-master:
	go run app/services/master/main.go

run-chunkserver:
	go run app/services/chunkserver/main.go

build-master:
	go build app/services/master/main.go

build-chunkserver:
	go build app/services/chunkserver/main.go
