#!/bin/bash

# Ensure script runs from the directory containing the 'duplicati' folder
set -e # Exit immediately if a command exits with a non-zero status

# Produce a final hash of each of the backup directories and check if they match
get_double_hashed_directory() {
    local dir_path="$1"
    local tmp_file="tmp_file_hashes.txt"

    # Find files, sort them for consistency, calculate MD5 of content
    find "$dir_path" -type f | sort | xargs -I {} md5sum "{}" 2>/dev/null | awk '{print $1}' > $tmp_file

    # Hash the list of hashes (the "Double Hash")
    local final_hash=$(cat $tmp_file | md5sum | awk '{print $1}')

    # Cleanup temp file
    rm -f $tmp_file

    echo "$final_hash"
}

echo "Generating test data..."
mkdir -p data_raw

# Check if the data_raw exists and is not empty, to avoid regenerating test data on every run
if [ -d "data_raw" ] && [ "$(ls -A data_raw)" ]; then
    echo "data_raw already exists and is not empty. Skipping test data generation."
else
    ./data_duplicati/Tools/TestDataGenerator/bin/Debug/net10.0/TestDataGenerator create ./data_raw/ --file-count 10000 --max-file-size 104857600 --max-total-size 1073741824 --sparse-factor 30
fi

echo "Performing Backup 1..."
mkdir -p data_backup1

./data_duplicati/Executables/Duplicati.CommandLine/bin/Debug/net10.0/Duplicati.CommandLine backup file://data_backup1 ./data_raw/ --passphrase=1234

echo "Performing Restore 1..."
mkdir -p data_restore1

./data_duplicati/Executables/Duplicati.CommandLine/bin/Debug/net10.0/Duplicati.CommandLine restore file://data_backup1 --restore-path=./data_restore1/ --passphrase=1234

echo "Performing Sync..."
mkdir -p data_backup2

./data_duplicati/Executables/Duplicati.CommandLine.SyncTool/bin/Debug/net10.0/Duplicati.CommandLine.SyncTool file://data_backup1 file://data_backup2 --confirm --progress

echo "Performing Restore 2..."
mkdir -p data_restore2

./data_duplicati/Executables/Duplicati.CommandLine/bin/Debug/net10.0/Duplicati.CommandLine restore file://data_backup2 --restore-path=./data_restore2/ --passphrase=1234

echo "Verifying hashes..."

hash_backup1=$(get_double_hashed_directory "./data_backup1/")
hash_backup2=$(get_double_hashed_directory "./data_backup2/")

# Store the success flag
verification=true
if [ "$hash_backup1" = "$hash_backup2" ]; then
    echo "Backups match!"
else
    echo "Backups do NOT match!"
    verification=false
fi

hash_raw=$(get_double_hashed_directory "./data_raw/")
hash_restore1=$(get_double_hashed_directory "./data_restore1/")
hash_restore2=$(get_double_hashed_directory "./data_restore2/")

if [ "$hash_raw" = "$hash_restore1" ]; then
    echo "Restore 1 matches raw data!"
else
    echo "Restore 1 does NOT match raw data!"
    verification=false
fi

if [ "$hash_raw" = "$hash_restore2" ]; then
    echo "Restore 2 matches raw data!"
else
    echo "Restore 2 does NOT match raw data!"
    verification=false
fi
echo "Cleaning up..."

./data_duplicati/Executables/Duplicati.CommandLine/bin/Debug/net10.0/Duplicati.CommandLine delete file://data_backup1 --passphrase=1234 --version=0 --allow-full-removal=true

rm -rf data_raw/
rm -rf data_backup1/
rm -rf data_backup2/
rm -rf data_restore1/
rm -rf data_restore2/

if [ "$verification" = true ]; then
    echo "All verifications passed!"
else
    echo "Some verifications failed!"
fi


echo "Script finished."