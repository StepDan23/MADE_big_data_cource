#!/usr/bin/env python3
import sys
PRICE_COLUMN_NUM = -7


chunk_sum = 0
chunk_size = 0

for line in sys.stdin:
    values = line.split(',')
    try:
        chunk_sum += int(values[PRICE_COLUMN_NUM])
        chunk_size += 1
    except Exception:
        continue

chunk_mean = chunk_sum / chunk_size

print(chunk_size, chunk_mean)
