#!/bin/sh
set -e
DATA_FILE="/data/0_0.tigerbeetle"
if [ -f "$DATA_FILE" ]; then
  echo "TigerBeetle data file already exists, skipping format."
  exit 0
fi
echo "Formatting TigerBeetle data file..."
tigerbeetle format --cluster=0 --replica=0 --replica-count=1 "$DATA_FILE"
echo "Done."
