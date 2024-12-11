# Check if the Duplicati.CommandLine resides in $PATH or $DUPLICATI
if [ -z "$DUPLICATI" ]; then
    if ! command -v Duplicati.CommandLine &> /dev/null; then
        echo "Duplicati.CommandLine not found in \$PATH nor \$DUPLICATI"
        exit 1
    fi
    DUPLICATI=Duplicati.CommandLine
fi

# Check if dotnet is installed
if ! command -v dotnet &> /dev/null; then
    echo "dotnet not found in \$PATH"
    exit 1
fi

# Check if the data directory exists
if [ ! -d "data_small" ] || [ ! -d "data_large" ]; then
    # Check if the $DUPLICATI_TEST command resides in $PATH or $DUPLICATI_TEST
    if [ -z "$DUPLICATI_TEST" ]; then
        if ! command -v TestDataGenerator &> /dev/null; then
            echo "TestDataGenerator not found in \$PATH nor \$DUPLICATI_TEST"
            exit 1
        fi
        DUPLICATI_TEST=TestDataGenerator
    fi

    if [ ! -d "data_small" ]; then
        # 1 GB of max 10 MB files
        $DUPLICATI_TEST create $(pwd)/data_small --max-file-size 10485760 --max-total-size 1073741824 --file-count 10000
    fi

    if [ ! -d "data_large" ]; then
        # 10 GB of max 1 GB min 10 MB files
        $DUPLICATI_TEST create $(pwd)/data_large --max-file-size 1073741824 --max-total-size 10737418240 --min-file-size 10485760 --file-count 1000
    fi
fi

WARMUP=1
RUNS=10
DUPLICATI_BACKUP_DIR="data_duplicatidir"
mkdir -p $DUPLICATI_BACKUP_DIR
mkdir -p times
for f in data_small data_large; do
    for intensity in $(seq 1 10); do
        for i in $(seq 1 $WARMUP); do
            $DUPLICATI backup $DUPLICATI_BACKUP_DIR --passphrase=a $f --cpu-intensity=$intensity
            $DUPLICATI delete $DUPLICATI_BACKUP_DIR --passphrase=a --allow-full-removal --version=0
        done
        for i in $(seq 1 $RUNS); do
            /usr/bin/time -p -o tmp_timing $DUPLICATI backup $DUPLICATI_BACKUP_DIR --passphrase=a $f --cpu-intensity=$intensity
            tr '\n' ' ' < tmp_timing >> times/$HOST$f$intensity.time
            echo "" >> times/$HOST$f$intensity.time
            rm tmp_timing
            $DUPLICATI delete $DUPLICATI_BACKUP_DIR --passphrase=a --allow-full-removal --version=0
        done
    done
done
rmdir $DUPLICATI_BACKUP_DIR