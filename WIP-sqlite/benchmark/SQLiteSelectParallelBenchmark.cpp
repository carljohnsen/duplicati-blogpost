#include <sqlite3.h>
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <random>
#include <chrono>
#include <stdint.h>

const int kNumEntries = 1'00'000;
const int kParallelism = 1;
const std::string kDBPath = "benchmark.sqlite";
// const std::string kDBPath = ":memory:";

struct Entry
{
    uint64_t id;
    uint64_t length;
    std::string full_hash;
};

std::string random_hash(std::mt19937 &rng, int length)
{
    static const std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string result(length, ' ');
    for (char &c : result)
        c = chars[rng() % chars.size()];
    return result;
}

void setup_database(sqlite3 *db, std::vector<Entry> &entries)
{
    char *errMsg = nullptr;
    sqlite3_exec(db, "PRAGMA journal_mode=WAL;", nullptr, nullptr, &errMsg);
    sqlite3_exec(db, "DROP TABLE IF EXISTS Blockset;", nullptr, nullptr, &errMsg);
    sqlite3_exec(db, "CREATE TABLE Blockset(ID INTEGER PRIMARY KEY, Length INTEGER NOT NULL, FullHash TEXT NOT NULL);", nullptr, nullptr, &errMsg);
    sqlite3_exec(db, "CREATE UNIQUE INDEX IF NOT EXISTS BlocksetLengthHash ON Blockset(Length, FullHash);", nullptr, nullptr, &errMsg);

    sqlite3_exec(db, "BEGIN DEFERRED TRANSACTION;", nullptr, nullptr, &errMsg);
    std::mt19937 rng(42);
    std::string sql = "INSERT INTO Blockset(ID, Length, FullHash) VALUES (?, ?, ?);";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);

    for (int i = 0; i < kNumEntries; ++i)
    {
        Entry entry = {
            static_cast<uint64_t>(i + 1),
            static_cast<uint64_t>(rng() % 100),
            random_hash(rng, 44)};
        entries.push_back(entry);
        sqlite3_bind_int64(stmt, 1, entry.id);
        sqlite3_bind_int64(stmt, 2, entry.length);
        sqlite3_bind_text(stmt, 3, entry.full_hash.c_str(), entry.full_hash.size(), SQLITE_STATIC);
        sqlite3_step(stmt);
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, &errMsg);
    sqlite3_exec(db, "PRAGMA optimize;", nullptr, nullptr, nullptr);

    std::shuffle(entries.begin(), entries.end(), rng);
}

void benchmark_thread(std::vector<Entry> &entries, int thread_id, int start_idx, int end_idx)
{
    sqlite3 *db;
    sqlite3_open(kDBPath.c_str(), &db);
    sqlite3_exec(db, "PRAGMA synchronous = NORMAL", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "PRAGMA temp_store = MEMORY", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "PRAGMA journal_mode = WAL", nullptr, nullptr, nullptr);
    // Set the cache size to 512MB
    sqlite3_exec(db, "PRAGMA cache_size = -512000", nullptr, nullptr, nullptr);
    // sqlite3_exec(db, "PRAGMA query_only = true", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "PRAGMA threads = 8", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "PRAGMA read_uncommitted = 1", nullptr, nullptr, nullptr);
    // Set the mmap size to 512MB
    sqlite3_exec(db, "PRAGMA mmap_size = 4194304", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "PRAGMA shared_cache = true", nullptr, nullptr, nullptr);

    std::string sql = "SELECT ID FROM Blockset WHERE Length = ? AND FullHash = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);

    long ticks_bind = 0;
    long ticks_step = 0;
    long ticks_reset = 0;
    long ticks_transaction = 0;
    auto begin = std::chrono::high_resolution_clock::now();
    sqlite3_exec(db, "BEGIN DEFERRED TRANSACTION;", nullptr, nullptr, nullptr);
    auto end = std::chrono::high_resolution_clock::now();
    ticks_transaction += std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();

    for (int i = start_idx; i < end_idx; ++i)
    {
        begin = std::chrono::high_resolution_clock::now();
        sqlite3_bind_int64(stmt, 1, entries[i].length);
        sqlite3_bind_text(stmt, 2, entries[i].full_hash.c_str(), entries[i].full_hash.size(), SQLITE_STATIC);
        end = std::chrono::high_resolution_clock::now();
        ticks_bind += std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
        begin = std::chrono::high_resolution_clock::now();
        int rc = sqlite3_step(stmt);
        if (rc != SQLITE_ROW && rc != SQLITE_DONE)
        {
            std::cerr << "Query failed on thread " << thread_id << ": " << sqlite3_errmsg(db) << std::endl;
        }
        else
        {
            int id = sqlite3_column_int(stmt, 0);
            if (id != entries[i].id)
            {
                std::cerr << "ID mismatch on thread " << thread_id << ": expected " << i << ", got " << id << std::endl;
            }
        }
        end = std::chrono::high_resolution_clock::now();
        ticks_step += std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
        begin = std::chrono::high_resolution_clock::now();
        sqlite3_reset(stmt);
        end = std::chrono::high_resolution_clock::now();
        ticks_reset += std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
    }

    begin = std::chrono::high_resolution_clock::now();
    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
    end = std::chrono::high_resolution_clock::now();
    ticks_transaction += std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();

    double s_bind = ticks_bind / 1e9;
    double s_step = ticks_step / 1e9;
    double s_reset = ticks_reset / 1e9;
    double s_transaction = ticks_transaction / 1e9;

    std::cout << "Thread " << thread_id << " Transaction: " << s_transaction * 1000.0 << " ms (" << (kNumEntries / 1000.0) / s_transaction << " kops/sec)" << std::endl;
    std::cout << "Thread " << thread_id << " Bind: " << s_bind * 1000.0 << " ms (" << (kNumEntries / 1000.0) / s_bind << " kops/sec)" << std::endl;
    std::cout << "Thread " << thread_id << " Step: " << s_step * 1000.0 << " ms (" << (kNumEntries / 1000.0) / s_step << " kops/sec)" << std::endl;
    std::cout << "Thread " << thread_id << " Reset: " << s_reset * 1000.0 << " ms (" << (kNumEntries / 1000.0) / s_reset << " kops/sec)" << std::endl;

    sqlite3_finalize(stmt);
    sqlite3_close(db);
}

int main()
{
    // Delete the database file if it exists
    std::cout << "Deleting database file: " << kDBPath << std::endl;
    std::remove(kDBPath.c_str());

    std::cout << "Creating database file: " << kDBPath << std::endl;
    sqlite3 *db;
    sqlite3_open(kDBPath.c_str(), &db);
    std::vector<Entry> entries;

    std::cout << "Setting up database with " << kNumEntries << " entries..." << std::endl;
    setup_database(db, entries);
    sqlite3_close(db);

    std::vector<std::thread> threads;
    int per_thread = kNumEntries / kParallelism;

    std::cout << "Running benchmark with " << kParallelism << " threads..." << std::endl;
    auto begin = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < kParallelism; ++i)
    {
        int start = i * per_thread;
        int end = (i == kParallelism - 1) ? entries.size() : start + per_thread;
        threads.emplace_back(benchmark_thread, std::ref(entries), i, start, end);
    }

    for (auto &t : threads)
        t.join();
    auto end = std::chrono::high_resolution_clock::now();
    double total_elapsed_ms = std::chrono::duration<double, std::milli>(end - begin).count();
    std::cout << "Total elapsed time: " << total_elapsed_ms << " ms (" << kNumEntries / (total_elapsed_ms / 1000.0) / 1000 << " kops/sec)" << std::endl;

    return 0;
}
