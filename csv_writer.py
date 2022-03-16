import sys
import json

lines_written = 0

def write_csv(f, line):
    global lines_written
    line = process(line)
    f.write(line[:-1] + "\n")
    lines_written += 1
    if lines_written > 100000:
		# Backup mechanism
        f.flush()
        lines_written = 0

def process(line):
    if isinstance(line, str):
        return line
    string = ""
    for value in line.values():
        string += str(value) + ","
    return string
