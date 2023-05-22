#!/bin/bash

set -e

for i in {1..3}; do
  go test -run 2A
  go test -run 2B
  go test -run 2C
  echo "Iteration $i"
done

echo "Loop finished."
