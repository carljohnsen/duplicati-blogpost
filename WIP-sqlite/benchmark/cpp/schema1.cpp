#include "shared.hpp"

const std::string CREATE_BLOCK_TABLE = "CREATE TABLE Block (ID INTEGER PRIMARY KEY, Hash TEXT NOT NULL, Size INTEGER NOT NULL);";

struct Entry
{
    uint64_t id;
    std::string hash;
    uint64_t size;
};

int fill(sqlite3 *db, std::mt19937 &rng, std::vector<Entry> &entries, uint64_t num_entries)
{
    auto begin = std::chrono::high_resolution_clock::now();
    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::string sql = "INSERT INTO Block(ID, Hash, Size) VALUES (?, ?, ?);";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    for (uint64_t i = 0; i < num_entries; i++)
    {
        Entry entry = {
            i + 1,
            random_hash_string(rng, 44),
            rng() % 1000};
        entries.push_back(entry);
        sqlite3_bind_int64(stmt, 1, entry.id);
        sqlite3_bind_text(stmt, 2, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 3, entry.size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt), db, "Insert entry " + std::to_string(i)))
            return -1;
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);
    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "PRAGMA optimize;", nullptr, nullptr, nullptr);
    auto end = std::chrono::high_resolution_clock::now();

    std::cout << "Inserted " << entries.size() << " entries in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
              << " ms." << std::endl;

    return 0;
}

int measure_insert(sqlite3 *db, Config &config, std::mt19937 &rng, const std::string &report_name)
{
    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::string sql = "INSERT INTO Block(ID, Hash, Size) VALUES (?, ?, ?);";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        Entry entry = {
            i + 1 + config.num_entries,
            random_hash_string(rng, 44),
            rng() % 1000};

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_int64(stmt, 1, entry.id);
        sqlite3_bind_text(stmt, 2, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 3, entry.size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt), db, "Warmup insert " + std::to_string(i)))
            return -1;
        sqlite3_reset(stmt);

        auto end = std::chrono::high_resolution_clock::now();
    }
    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::vector<uint64_t> times;
    for (uint64_t i = 0; i < config.num_repetitions; i++)
    {
        Entry entry = {
            i + 1 + config.num_entries,
            random_hash_string(rng, 44),
            rng() % 1000};

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_int64(stmt, 1, entry.id);
        sqlite3_bind_text(stmt, 2, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 3, entry.size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt), db, "Warmup insert " + std::to_string(i)))
            return -1;
        sqlite3_reset(stmt);

        auto end = std::chrono::high_resolution_clock::now();

        times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
    }
    sqlite3_finalize(stmt);
    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);

    report_stats(config, times, report_name);

    return 0;
}

int select_index_normal(Config &config)
{
    std::vector<std::string> table_queries = {
        CREATE_BLOCKSET_TABLE,
        CREATE_BLOCKSETENTRY_TABLE,
        CREATE_BLOCK_TABLE};

    auto db = setup_database(table_queries);

    sqlite3_exec(db, "CREATE INDEX BlockHashSize ON Block(Hash, Size);", nullptr, nullptr, nullptr);

    std::vector<Entry> entries;
    std::mt19937 rng(2025'07'08);
    if (fill(db, rng, entries, config.num_entries) != 0)
        return -1;

    measure_insert(db, config, rng, "schema1_insert_index_normal");

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::string sql = "SELECT ID FROM Block WHERE Hash = ? AND Size = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);

    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        uint64_t idx = rng() % entries.size();

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_text(stmt, 1, entries[idx].hash.c_str(), entries[idx].hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 2, entries[idx].size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt), db, "Warmup query " + std::to_string(i)))
            return -1;
        if (!assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check"))
            return -1;
        sqlite3_reset(stmt);

        auto end = std::chrono::high_resolution_clock::now();
    }
    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::vector<uint64_t> times;
    for (uint64_t i = 0; i < config.num_repetitions; i++)
    {
        uint64_t idx = rng() % entries.size();

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_text(stmt, 1, entries[idx].hash.c_str(), entries[idx].hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 2, entries[idx].size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt), db, "Query execution " + std::to_string(i)))
            return -1;
        if (!assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "ID check"))
            return -1;
        sqlite3_reset(stmt);

        auto end = std::chrono::high_resolution_clock::now();

        times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
    }

    sqlite3_finalize(stmt);
    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
    sqlite3_close(db);

    report_stats(config, times, "schema1_select_index_normal");

    return 0;
}

int select_index_hash(Config &config)
{
    std::vector<std::string> table_queries = {
        CREATE_BLOCKSET_TABLE,
        CREATE_BLOCKSETENTRY_TABLE,
        CREATE_BLOCK_TABLE};

    auto db = setup_database(table_queries);

    sqlite3_exec(db, "CREATE INDEX BlockHash ON Block(Hash);", nullptr, nullptr, nullptr);

    std::vector<Entry> entries;
    std::mt19937 rng(2025'07'08);
    if (fill(db, rng, entries, config.num_entries) != 0)
        return -1;

    measure_insert(db, config, rng, "schema1_insert_index_hash");

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::string sql = "SELECT ID, Size FROM Block WHERE Hash = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);

    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        uint64_t idx = rng() % entries.size();

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_text(stmt, 1, entries[idx].hash.c_str(), entries[idx].hash.size(), SQLITE_STATIC);
        bool found = false;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            if (entries[idx].size == sqlite3_column_int64(stmt, 1) && assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check", false))
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            std::cerr << "Warmup ID check failed for hash: " << entries[idx].hash << std::endl;
            return -1;
        }
        sqlite3_reset(stmt);

        auto end = std::chrono::high_resolution_clock::now();
    }
    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::vector<uint64_t> times;
    for (uint64_t i = 0; i < config.num_repetitions; i++)
    {
        uint64_t idx = rng() % entries.size();

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_text(stmt, 1, entries[idx].hash.c_str(), entries[idx].hash.size(), SQLITE_STATIC);
        bool found = false;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            if (entries[idx].size == sqlite3_column_int64(stmt, 1) && assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check", false))
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            std::cerr << "Warmup ID check failed for hash: " << entries[idx].hash << std::endl;
            return -1;
        }
        sqlite3_reset(stmt);

        auto end = std::chrono::high_resolution_clock::now();

        times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
    }

    sqlite3_finalize(stmt);
    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
    sqlite3_close(db);

    report_stats(config, times, "schema1_select_index_hash");

    return 0;
}

int select_index_size(Config &config)
{
    std::vector<std::string> table_queries = {
        CREATE_BLOCKSET_TABLE,
        CREATE_BLOCKSETENTRY_TABLE,
        CREATE_BLOCK_TABLE};

    auto db = setup_database(table_queries);

    sqlite3_exec(db, "CREATE INDEX BlockSize ON Block(Size);", nullptr, nullptr, nullptr);

    std::vector<Entry> entries;
    std::mt19937 rng(2025'07'08);
    if (fill(db, rng, entries, config.num_entries) != 0)
        return -1;

    measure_insert(db, config, rng, "schema1_insert_index_size");

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::string sql = "SELECT ID, Hash FROM Block WHERE Size = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        uint64_t idx = rng() % entries.size();

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_int64(stmt, 1, entries[idx].size);
        bool found = false;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            if (entries[idx].hash == std::string((const char *)sqlite3_column_text(stmt, 1)) && assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check", false))
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            std::cerr << "Warmup ID check failed for hash: " << entries[idx].hash << std::endl;
            return -1;
        }

        auto end = std::chrono::high_resolution_clock::now();

        sqlite3_reset(stmt);
    }
    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::vector<uint64_t> times;
    for (uint64_t i = 0; i < config.num_repetitions; i++)
    {
        uint64_t idx = rng() % entries.size();

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_int64(stmt, 1, entries[idx].size);
        bool found = false;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            if (entries[idx].hash == std::string((const char *)sqlite3_column_text(stmt, 1)) && assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check", false))
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            std::cerr << "Warmup ID check failed for hash: " << entries[idx].hash << std::endl;
            return -1;
        }

        auto end = std::chrono::high_resolution_clock::now();

        times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
        sqlite3_reset(stmt);
    }

    sqlite3_finalize(stmt);
    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
    sqlite3_close(db);

    report_stats(config, times, "schema1_select_index_size");

    return 0;
}

int main(int argc, char *argv[])
{
    auto config = parse_args(argc, argv);

    int ret;
    ret = select_index_normal(config);
    if (ret != 0)
        return ret;
    ret = select_index_hash(config);
    if (ret != 0)
        return ret;
    ret = select_index_size(config);
    if (ret != 0)
        return ret;

    if (std::filesystem::exists(DBPATH))
        std::filesystem::remove(DBPATH);

    return 0;
}