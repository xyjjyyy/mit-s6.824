#!/bin/bash

set -e

for i in {1..3}; do
  go test -run 2A --race
  go test -run 2B --race
  go test -run 2C --race
  go test -run 2D --race
  echo "Iteration $i"
done

echo "Loop finished."
