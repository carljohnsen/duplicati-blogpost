make -j

warmup=10000
repetitions=100000
sizes=(10000 100000 1000000 10000000)
threads=(1 2 4 8 16 32)
batches=(0 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536)

for size in "${sizes[@]}"; do
    ./bin/schema1 --num-entries $size --num-warmup $warmup --num-repetitions $repetitions
    ./bin/schema4 --num-entries $size --num-warmup $warmup --num-repetitions $repetitions
    ./bin/schema7 --num-entries $size --num-warmup $warmup --num-repetitions $repetitions
    ./bin/schema10 --num-entries $size --num-warmup $warmup --num-repetitions $repetitions
    ./bin/pragmas --num-entries $size --num-warmup $warmup --num-repetitions $repetitions
    for thread in "${threads[@]}"; do
        ./bin/parallel --num-entries $size --num-warmup $warmup --num-repetitions $repetitions --num-threads $thread
    done
    for batch in "${batches[@]}"; do
        ./bin/batching --num-entries $size --num-warmup $warmup --num-repetitions $repetitions --num-batch $batch
    done
done
