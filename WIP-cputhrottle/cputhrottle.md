# Feature: adding CPU utilization limits

# Solution

## 1. Limiting the `DataBlockProcessor`
Measure how many milliseconds it has run for, and if that is more than what is allowed, sleep for `1000 - allowed` milliseconds.
The good thing about this approach is that it is simple and local to the `DataBlockProcessor` and flows to the subsequent processes automatically. The bad thing is that it is not very precise.
It's a simple solution, that provides some control over the CPU utilization:

### dry-run, 1 hasher, 1 compressor, 1 uploader tmp/sycl_diffusion mac->pihole-cable
1000 3.21s user 0.23s system 107% cpu  3.201 total
0900 3.23s user 0.23s system 101% cpu  3.427 total
0800 3.29s user 0.26s system  95% cpu  3.714 total
0700 3.44s user 0.23s system  87% cpu  4.183 total
0600 3.32s user 0.25s system  78% cpu  4.529 total
0500 3.42s user 0.27s system  71% cpu  5.145 total
0400 3.47s user 0.26s system  64% cpu  5.764 total
0300 3.44s user 0.22s system  53% cpu  6.813 total
0200 3.58s user 0.27s system  37% cpu 10.196 total
0100 3.44s user 0.26s system  27% cpu 13.249 total
0050 4.15s user 0.31s system  15% cpu 28.854 total

### dry-run, 2 hashers, 2 compressors, 1 uploader, tmp/sycl_diffusion mac->pihole-wifi
1000 16.27s user 2.07s system 254% cpu 7.201 total
0900 15.20s user 1.90s system 234% cpu 7.303 total
0800 15.34s user 1.85s system 226% cpu 7.571 total
0700 15.33s user 3.56s system 227% cpu 8.291 total
0600 15.09s user 3.60s system 204% cpu 9.130 total
0500 13.96s user 5.25s system 178% cpu 10.745 total
0400 14.44s user 6.48s system 179% cpu 11.641 total
0300 15.39s user 2.07s system 133% cpu 13.098 total
0200 14.88s user 3.03s system 94% cpu 18.980 total
0100 15.11s user 2.34s system 54% cpu 31.821 total
0050 16.57s user 3.78s system 27% cpu 1:14.07 total

### dry-run, 8 hashers, 8 compressors, 4 uploaders, tmp/pre-sc-ad mac->pihole-cable
1000  80.55s user 2.28s system 387% cpu   21.350 total
900   80.87s user 2.33s system 307% cpu   27.070 total
800   83.08s user 2.48s system 242% cpu   35.317 total
700   86.58s user 2.64s system 184% cpu   48.295 total
600   87.71s user 2.64s system 141% cpu 1:04.07  total
500   90.17s user 2.77s system 107% cpu 1:26.13  total
400   92.29s user 3.03s system  77% cpu 2:02.91  total
300   94.99s user 3.25s system  56% cpu 2:53.74  total
200   97.03s user 3.72s system  36% cpu 4:34.79  total
100  107.50s user 3.91s system  23% cpu 7:55.40  total

### dry-run, 8 hashers, 8 compressors, 4 uploaders, tmp/sycl_diffusion mac->t00-cable
1 5.09s user 0.30s system 55% cpu 9.664 total
2 5.02s user 0.28s system 91% cpu 5.773 total
3 4.75s user 0.27s system 157% cpu 3.184 total
4 4.67s user 0.25s system 152% cpu 3.234 total
5 4.64s user 0.26s system 153% cpu 3.206 total
6 4.69s user 0.27s system 151% cpu 3.270 total
7 4.72s user 0.25s system 150% cpu 3.292 total
8 4.77s user 0.28s system 151% cpu 3.336 total
9 4.61s user 0.27s system 153% cpu 3.193 total
10 4.75s user 0.24s system 151% cpu 3.288 total

### dry-run, 8 hashers, 8 compressors, 1 uploaders, tmp/pre-sc-ad mac->t00-cable
Level 1:  100.39s user 4.11s system  22% cpu 7:49.12 total
Level 2:   94.25s user 3.51s system  36% cpu 4:24.86 total
Level 3:   92.03s user 3.41s system  55% cpu 2:51.41 total
Level 4:   92.09s user 3.39s system  78% cpu 2:01.24 total
Level 5:   90.10s user 3.47s system 106% cpu 1:27.57 total
Level 6:   88.26s user 3.33s system 136% cpu 1:07.05 total
Level 7:   87.73s user 3.43s system 179% cpu   50.90 total
Level 8:   84.71s user 2.92s system 233% cpu   37.45 total
Level 9:   81.92s user 2.99s system 305% cpu   27.82 total
Level 10:  82.39s user 3.16s system 384% cpu   22.23 total

### dry-run, 8 hashers, 8 compressors, 4 uploaders, tmp/pre-sc-ad t00->t02-cable
Level 1:  168.17s user 4.39s system  22% cpu 12:41.96  total
Level 2:  155.08s user 4.13s system  41% cpu  6:19.68  total
Level 3:  150.96s user 4.24s system  62% cpu  4:06.74  total
Level 4:  149.22s user 4.06s system  86% cpu  2:56.57  total
Level 5:  149.16s user 3.92s system 116% cpu  2:11.86  total
Level 6:  145.29s user 4.02s system 156% cpu  1:35.37  total
Level 7:  140.12s user 3.42s system 196% cpu  1:12.95  total
Level 8:  139.08s user 3.95s system 261% cpu    54.590 total
Level 9:  134.66s user 3.04s system 338% cpu    40.700 total
Level 10: 128.57s user 3.38s system 409% cpu    32.205 total

### dry-run, 8 hashers, 8 compressors, 4 uploaders, tmp/pre-sc-ad t02->t00-cable
Level 1:  88.25s user 3.37s system  20% cpu 7:27.21  total
Level 2:  88.44s user 3.31s system  37% cpu 4:01.91  total
Level 3:  89.05s user 3.12s system  56% cpu 2:43.13  total
Level 4:  88.84s user 3.09s system  80% cpu 1:54.74  total
Level 5:  89.81s user 3.17s system 111% cpu 1:23.68  total
Level 6:  89.99s user 2.94s system 144% cpu 1:04.37  total
Level 7:  89.51s user 3.00s system 190% cpu   48.604 total
Level 8:  90.04s user 3.02s system 248% cpu   37.374 total
Level 9:  90.22s user 3.02s system 321% cpu   29.035 total
Level 10: 90.77s user 2.83s system 417% cpu   22.414 total

### dry-run, 8 hashers, 8 compressors, 4 uploaders, tmp/pre-sc-ad jrh->local
Level 1:  100.69s user 3.69s system  22% cpu 7:39.35  total
Level 2:  100.78s user 3.04s system  38% cpu 4:27.63  total
Level 3:  100.20s user 2.76s system  60% cpu 2:51.20  total
Level 4:  100.66s user 2.76s system  80% cpu 2:08.03  total
Level 5:  100.11s user 2.57s system 112% cpu 1:31.18  total
Level 6:   99.38s user 2.53s system 149% cpu 1:07.98  total
Level 7:  100.22s user 2.48s system 191% cpu   53.723 total
Level 8:  100.00s user 2.37s system 256% cpu   39.945 total
Level 9:  100.03s user 2.33s system 339% cpu   30.132 total
Level 10: 100.06s user 2.10s system 425% cpu   24.018 total


## 2. After each operation, sleep for a while
In this approach, each operation is followed by a sleep for `(1 / (1000 - allowed)) * workload_time`.
This is more precise over a longer period of time and can be applied to all operations.
However, since there is no synchronization between the processes, the CPU utilization can still be high in short bursts.
Also, having every part sleep independently will induce more communication wait times.

## 3. Global controller one-way
A global controller that in a loop sends a 'go ahead', sleep for allowed, then sends a 'stop' signal, and sleeps for the rest of the time.
The good thing is that it is a global solution, so it alone can focus on keeping time.
The bad thing is the communication overhead, and it doesn't add much more than each process keeping their own time - only reduced time keeping logic at the cost of communication.

## 4. Global controller two-way
A global controller that keeps track of the amount of work done by each process.
Every process tries to get a task from this process and responds with the amount of work done.
The global controller then calculates how much time each process should sleep.
This won't work (at least not precisely) if any process operation takes up more than the allowed time.
All of the internal channels require buffers, as they now have to run even more asynchronously, almost to the point of serialization.
If no buffers are used, the system may deadlock.

## 5. A global lock
A global lock that is held by the process that is currently working.
After acquiring the lock, it checks whether the global time is more than allowed, and if so, sleeps for the rest of the time before performing its operation.
If the global time is less than allowed, it performs its operation and releases the lock.
This serializes the process, but should be a very precise way of limiting the CPU utilization.
However, CPU utilization can never become above 100% with this approach.

## 6. An atomic counter
Almost the same as 5. but with an atomic counter instead of a lock.
When each process wants to perform an operation, it checks the counter to see if it is less than allowed.
Then it performs its operation and increments the counter by the time it took.
This is a more lightweight solution than 5., but it is also less precise, as processes run in parallel and may collectively exceed the allowed time.
There is also the problem of multiple processes trying to reset the counter at the same time. E.g. one might reset it to 0, one updates with its time, and then a third process resets it to 0 again.

## 7. A global controller that suspends all threads
A global controller that at a steady interval suspends all threads.
This is a very simple solution, but it should also circumvent internal work handling, as the threads are suspended and continued in intervals.
It should also monitor the time spent in the scheduler, as it will count towards the CPU utilization, and as such should be included in the allowed time.
I.e. it should correct accordingly as it processes, as this time most likely won't be stable.
It doesn't seem portable, since there's no standard way to suspend threads in C#


Maybe route all channels through the global controller, which witholds messages?


eco mode /
high/medium/low
Rename it to intensity levels, rather than CPU utilization limits

## Final solution
1., but on the first "workhorse" of the pipeline: the `