#!/bin/bash

make clean all

export MASTER_ADDR=localhost:1234

./master&
./chunkserver&
jobs
wait
