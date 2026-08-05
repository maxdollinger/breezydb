## S.1

sync_all is expensive ±3ms / sync. -> Group commits for engine / durability window

with barrier sync every frame and every 20 frames a full sync

```
filename: test.breezy
size: 5119.94MiB
frames: 81919
end file creation: 83803137 records in 42.214s (121.29 MiB/s, 1985208 records/s)
```

Creation scales linear:

```
size: 10239.94MiB
frames: 163839
end file creation: 167607297 records in 74.683s (137.11 MiB/s, 2244265 records/s)
```

## S.2

The 5G file from S.1 is used.
Between create and every verify run a `sudo purge` is done.

`verified 81919 frames, 0 records in 594.411ms (8613.46 MiB/s)`

`verified 81919 frames, 83803137 records in 879.711ms (5820.02 MiB/s)`

For a 10G file same throughput

`verified 163839 frames, 0 records in 1.184s (8648.36 MiB/s)`

Reads with enabled os page cache do not need batching.
A Frame hash is the complete frame (header + data).
So only a more granular check should be done if the frame check failed.
