#!/bin/bash

make clean all

export MASTER_ADDR=localhost:1234

./master&
CHUNK_PATH=/tmp/chunks-1 SERVER_PORT=5544 ./chunkserver&
CHUNK_PATH=/tmp/chunks-2 SERVER_PORT=5545 ./chunkserver&
CHUNK_PATH=/tmp/chunks-3 SERVER_PORT=5546 ./chunkserver&
jobs
wait
