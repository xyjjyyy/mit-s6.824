#!/bin/bash

set -e

for i in {1..3}; do
  go test -run 2C --race
  echo "Iteration $i"
done

echo "Loop finished."
