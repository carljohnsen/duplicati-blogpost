#ifndef SHARED_HPP
#define SHARED_HPP

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <sqlite3.h>
#include <stdint.h>
#include <string>
#include <thread>
#include <vector>

const std::string
    CREATE_BLOCKSET_TABLE = "CREATE TABLE Blockset(ID INTEGER PRIMARY KEY, Length INTEGER NOT NULL);",
    CREATE_BLOCKSETENTRY_TABLE = "CREATE TABLE BlocksetEntry(BlocksetID INTEGER NOT NULL, BlockID INTEGER NOT NULL);",
    DBPATH = "benchmark.sqlite",
    DROPALL_TABLES = "DROP TABLE IF EXISTS Blockset; DROP TABLE IF EXISTS BlocksetEntry; DROP TABLE IF EXISTS Block;";

struct Config
{
    uint64_t num_entries = 100'000;
    uint64_t num_warmup = 1'000;
    uint64_t num_repitions = 10'000;
};

bool assert_sqlite_return_code(int rc, sqlite3 *db, const std::string &context)
{
    if (!(rc == SQLITE_OK || rc == SQLITE_DONE || rc == SQLITE_ROW))
    {
        std::cerr << "SQLite error in " << context << ": " << rc << " " << sqlite3_errmsg(db) << std::endl;
        return false;
    }
    return true;
}

template <typename T>
bool assert_value_matches(const T &expected, const T &actual, const std::string &context, bool print_error = true)
{
    if (expected != actual)
    {
        if (print_error)
            std::cerr << "Value mismatch in " << context << ": expected " << expected << ", got " << actual << std::endl;
        return false;
    }
    return true;
}

Config parse_args(int argc, char *argv[])
{
    Config config;
    for (int i = 1; i < argc; i++)
    {
        if (std::string(argv[i]) == "--num-entries" && i + 1 < argc)
            config.num_entries = std::stoi(argv[++i]);
        else if (std::string(argv[i]) == "--num-warmup" && i + 1 < argc)
            config.num_warmup = std::stoi(argv[++i]);
        else if (std::string(argv[i]) == "--num-repitions" && i + 1 < argc)
            config.num_repitions = std::stoi(argv[++i]);
        else
            std::cerr << "Unknown argument: " << argv[i] << std::endl;
    }
    return config;
}

std::string random_hash_string(std::mt19937 &rng, int length)
{
    static const std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string result(length, ' ');
    for (char &c : result)
        c = chars[rng() % chars.size()];
    return result;
}

void random_hash_bin(std::mt19937 &rng, int length, char *buffer)
{
    for (int i = 0; i < length; i++)
        buffer[i] = rng();
}

void report_stats(Config &config, std::vector<uint64_t> &times, std::string benchmark_name)
{
    if (!std::filesystem::exists("reports"))
        std::filesystem::create_directory("reports");

    bool emit_header = !std::filesystem::exists("reports/" + benchmark_name + ".csv");
    std::ofstream report_file("reports/" + benchmark_name + ".csv", std::ios::app);

    if (emit_header)
        report_file << "num_entries,num_warmup,num_repitions,min,1st,10th,25th,median,75th,90th,99th,max,avg,1-99_avg,10-90_avg,median_kops,avg_kops,1-99_avg_kops,10-90_avg_kops" << std::endl;

    std::sort(times.begin(), times.end());
    uint64_t min_time = times.front();
    uint64_t max_time = times.back();
    double avg_time = std::accumulate(times.begin(), times.end(), 0.0) / times.size();
    double avg_time_1_99 = std::accumulate(times.begin() + times.size() / 100, times.end() - times.size() / 100, 0.0) / (times.size() - 2 * times.size() / 100);
    double avg_time_10_90 = std::accumulate(times.begin() + times.size() / 10, times.end() - times.size() / 10, 0.0) / (times.size() - 2 * times.size() / 10);
    uint64_t median = times[times.size() / 2];
    uint64_t first = times[times.size() / 100];
    uint64_t tenth = times[times.size() / 10];
    uint64_t q1 = times[times.size() / 4];
    uint64_t q3 = times[times.size() * 3 / 4];
    uint64_t ninetieth = times[times.size() * 9 / 10];
    uint64_t ninety_ninth = times[times.size() * 99 / 100];
    double median_throughput = 1e9 / median / 1000; // Convert to kops/sec
    double avg_throughput = 1e9 / avg_time / 1000;
    double avg_throughput_1_99 = 1e9 / avg_time_1_99 / 1000;
    double avg_throughput_10_90 = 1e9 / avg_time_10_90 / 1000;

    report_file << config.num_entries << ","
                << config.num_warmup << ","
                << config.num_repitions << ","
                << min_time << ","
                << first << ","
                << tenth << ","
                << q1 << ","
                << median << ","
                << q3 << ","
                << ninetieth << ","
                << ninety_ninth << ","
                << max_time << ","
                << avg_time << ","
                << avg_time_1_99 << ","
                << avg_time_10_90 << ","
                << median_throughput << ","
                << avg_throughput << ","
                << avg_throughput_1_99 << ","
                << avg_throughput_10_90
                << std::endl;
}

sqlite3 *setup_database(std::vector<std::string> &table_queries)
{
    // Delete the database file if it exists
    std::cout << "Deleting database file: " << DBPATH << std::endl;
    std::remove(DBPATH.c_str());

    std::cout << "Creating database file: " << DBPATH << std::endl;
    sqlite3 *db;
    sqlite3_open(DBPATH.c_str(), &db);

    for (auto query : table_queries)
    {
        char *errMsg = nullptr;
        if (sqlite3_exec(db, query.c_str(), nullptr, nullptr, &errMsg) != SQLITE_OK)
        {
            std::cerr << "Error executing query: " << query << "\nError: " << errMsg << std::endl;
            sqlite3_free(errMsg);
        }
    }

    return db;
}

#endif