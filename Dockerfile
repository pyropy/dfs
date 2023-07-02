FROM golang:1.19-alpine as builder

WORKDIR /app

COPY . ./

RUN go build -o master cmd/master/*.go
RUN go build -o chunkserver cmd/chunkserver/*.go
RUN go build -o client cmd/client/*.go

FROM alpine:3.17.0 as chunkserver

ENV PATH="$PATH:/app"
RUN mkdir /opt/chunks
WORKDIR /app
COPY --from=builder /app/chunkserver ./chunkserver

EXPOSE 50000-55000


FROM alpine:3.17.0 as client

ENV PATH="$PATH:/app"
WORKDIR /app
COPY --from=builder /app/client ./client


FROM alpine:3.17.0 as master

ENV PATH="$PATH:/app"
WORKDIR /app
COPY --from=builder /app/master ./master

EXPOSE 1234
