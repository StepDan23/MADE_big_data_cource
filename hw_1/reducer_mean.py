#!/usr/bin/env python3
import sys


mean = 0
size = 0

for line in sys.stdin:
    chunk_size, chunk_mean = map(float, line.split(' '))
    mean = (size * mean + chunk_size * chunk_mean) / (size + chunk_size)
    size += chunk_size

print(mean)
