#!/bin/bash
DUPLICATI=$1
DATAGEN=$2
SIZES=$3

N_WARMUP=1
N_RUNS=1
HOST=$(hostname)

case $SIZES in
    "small")
        SIZES=("small")
        MAX_FILE_SIZE=10485760 # 10 MB
        MAX_TOTAL_SIZE=1073741824 # 1 GB
        FILE_COUNT=1000 # 1,000 files
        ;;
    "medium")
        SIZES=("medium")
        MAX_FILE_SIZE=10485760 # 10 MB
        MAX_TOTAL_SIZE=10737418240 # 10 GB
        FILE_COUNT=10000 # 10,000 files
        ;;
    "large")
        SIZES=("large")
        MAX_FILE_SIZE=104857600 # 100 MB
        MAX_TOTAL_SIZE=107374182400 # 100 GB
        FILE_COUNT=1000000 # 1,000,000 files
        ;;
    "all")
        SIZES=("small" "medium" "large")
        ;;
    *)
        echo "Invalid size specifier: $SIZES"
        echo "Usage: $0 <DUPLICATI> <DATAGEN> <small|medium|large>"
        exit 1
        ;;
esac

# Check if we need to generate data
DATA_DIR=$(pwd)/data/
TIMES_DIR=$(pwd)/times/
mkdir -p $DATA_DIR $TIMES_DIR

for SIZE in ${SIZES[@]}; do
    if [ ! -d "data/$SIZE" ]; then
        echo "Generating data..."
        $DATAGEN create $DATA_DIR/$SIZE --max-file-size $MAX_FILE_SIZE --max-total-size $MAX_TOTAL_SIZE --file-count $FILE_COUNT
    fi

    # Perform the backup
    BACKUP_DIR=$DATA_DIR/backup_$SIZE
    $DUPLICATI backup $BACKUP_DIR $DATA_DIR/$SIZE --passphrase=1234

    mkdir -p $TIMES_DIR

    for LEGACY in "true" "false"; do
        #
        # Full restore
        #
        # Perform the warmup runs
        RESTORE_DIR=$DATA_DIR/restore_$SIZE
        for i in $(seq 1 $N_WARMUP); do
            rm -rf $RESTORE_DIR/*
            $DUPLICATI restore $BACKUP_DIR --restore-path=$RESTORE_DIR --passphrase=1234 --restore-legacy=$LEGACY
        done

        # Perform the benchmark runs
        TIMESFILE=$TIMES_DIR/$HOST-$SIZE-full-$LEGACY.time
        for i in $(seq 1 $N_RUNS); do
            rm -rf $RESTORE_DIR/*
            /usr/bin/time -p -o tmp_timing $DUPLICATI restore $BACKUP_DIR --restore-path=$RESTORE_DIR --passphrase=1234 --restore-legacy=$LEGACY
            tr '\n' ' ' < tmp_timing >> $TIMESFILE
            echo "" >> $TIMESFILE
            rm tmp_timing
        done

        #
        # Partial restore
        #
        # Get a list of all of the files
        FILES=$(find $DATA_DIR/$SIZE -type f)
        # Shuffle the entries
        SHUFFLED=$(echo "$FILES" | shuf)

        FILES_TO_MODIFY=$(echo "$SHUFFLED" | head -n $((FILE_COUNT / 2)))
        FILES_TO_DELETE=$(echo "$FILES_TO_MODIFY" | xargs -n 1 | awk 'NR % 2 == 0')
        FILES_TO_TOUCH=$(echo "$FILES_TO_MODIFY" | xargs -n 1 | awk 'NR % 2 == 1')

        # Perform the warmup runs
        for i in $(seq 1 $N_WARMUP); do
            for FILE in $FILES_TO_DELETE; do
                rm $FILE
            done
            for FILE in $FILES_TO_TOUCH; do
                echo -n "a" | dd of=$FILE bs=1 seek=0 count=1 conv=notrunc
            done

            $DUPLICATI restore $BACKUP_DIR --restore-path=$RESTORE_DIR --passphrase=1234 --restore-legacy=$LEGACY --restore-target=/subdir
        done

        # Perform the benchmark runs
        TIMESFILE=$TIMES_DIR/$HOST-$SIZE-partial-$LEGACY.time
        for i in $(seq 1 $N_RUNS); do
            for FILE in $FILES_TO_DELETE; do
                rm $FILE
            done
            for FILE in $FILES_TO_TOUCH; do
                echo -n "a" | dd of=$FILE bs=1 seek=0 count=1 conv=notrunc
            done

            /usr/bin/time -p -o tmp_timing $DUPLICATI restore $BACKUP_DIR --restore-path=$RESTORE_DIR --passphrase=1234 --restore-legacy=$LEGACY --restore-target=/subdir
            tr '\n' ' ' < tmp_timing >> $TIMESFILE
            echo "" >> $TIMESFILE
            rm tmp_timing
        done

        #
        # No restore
        #
        # Start by filling the restore directory with the files
        $DUPLICATI restore $BACKUP_DIR --restore-path=$RESTORE_DIR --passphrase=1234 --restore-legacy=$LEGACY

        # Perform the warmup runs
        for i in $(seq 1 $N_WARMUP); do
            $DUPLICATI restore $BACKUP_DIR --restore-path=$RESTORE_DIR --passphrase=1234 --restore-legacy=$LEGACY
        done

        # Perform the benchmark runs
        TIMESFILE=$TIMES_DIR/$HOST-$SIZE-norestore-$LEGACY.time
        for i in $(seq 1 $N_RUNS); do
            /usr/bin/time -p -o tmp_timing $DUPLICATI restore $BACKUP_DIR --restore-path=$RESTORE_DIR --passphrase=1234 --restore-legacy=$LEGACY
            tr '\n' ' ' < tmp_timing >> $TIMESFILE
            echo "" >> $TIMESFILE
            rm tmp_timing
        done

        #
        # Metadata restore
        #
        # Perform the warmup runs
        for i in $(seq 1 $N_WARMUP); do
            find $RESTORE_DIR -type f -exec touch {} \;
            $DUPLICATI restore $BACKUP_DIR --restore-path=$RESTORE_DIR --passphrase=1234 --restore-legacy=$LEGACY
        done

        # Perform the benchmark runs
        TIMESFILE=$TIMES_DIR/$HOST-$SIZE-metadata-$LEGACY.time
        for i in $(seq 1 $N_RUNS); do
            find $RESTORE_DIR -type f -exec touch {} \;
            /usr/bin/time -p -o tmp_timing $DUPLICATI restore $BACKUP_DIR --restore-path=$RESTORE_DIR --passphrase=1234 --restore-legacy=$LEGACY
            tr '\n' ' ' < tmp_timing >> $TIMESFILE
            echo "" >> $TIMESFILE
            rm tmp_timing
        done
    done
done