#!/usr/bin/env python3
import sys


var = 0
mean = 0
size = 0

for line in sys.stdin:
    chunk_size, chunk_mean, chunk_var = map(float, line.split(' '))
    var = (size * var + chunk_size * chunk_var) / (size + chunk_size) \
                + size * chunk_size * ((mean - chunk_mean) / (size + chunk_size)) ** 2
    mean = (size * mean + chunk_size * chunk_mean) / (size + chunk_size)
    size += chunk_size

print(var)
