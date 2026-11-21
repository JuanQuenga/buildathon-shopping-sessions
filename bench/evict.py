#!/usr/bin/env python3
"""Drop the page cache for every file under the given directories (no root needed:
posix_fadvise(DONTNEED) evicts clean pages). Approximates a cold host cache."""
import os, sys

total = 0
for root in sys.argv[1:]:
    for dirpath, _, files in os.walk(root):
        for name in files:
            path = os.path.join(dirpath, name)
            try:
                fd = os.open(path, os.O_RDONLY)
            except OSError:
                continue
            try:
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                total += os.fstat(fd).st_size
            finally:
                os.close(fd)
print(f"evicted {total / 1e9:.2f} GB", file=sys.stderr)
