#!/usr/bin/env python3
import sys
PRICE_COLUMN_NUM = -7


chunk_sum = 0
square_sum = 0
chunk_size = 0

for line in sys.stdin:
    values = line.split(',')
    try:
        price = int(values[PRICE_COLUMN_NUM])
        chunk_sum += price
        square_sum += price ** 2
        chunk_size += 1
    except Exception:
        continue

chunk_mean = chunk_sum / chunk_size
chunk_var = square_sum / chunk_size - chunk_mean ** 2

print(chunk_size, chunk_mean, chunk_var)
