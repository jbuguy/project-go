#!/bin/bash
NUM_WORKERS=${1:-4}
go run ./master/master.go &

for ((i=1; i<=NUM_WORKERS; i++)); do
    go run ./worker/worker.go &
done
wait $MASTER_PID
