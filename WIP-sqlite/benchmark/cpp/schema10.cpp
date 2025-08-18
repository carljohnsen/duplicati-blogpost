#include "shared.hpp"

const std::string CREATE_BLOCK_TABLE = "CREATE TABLE Block (ID INTEGER PRIMARY KEY, h0 INTEGER NOT NULL, h1 INTEGER NOT NULL, h2 INTEGER NOT NULL, h3 INTEGER NOT NULL, Size INTEGER NOT NULL);";

struct Entry
{
    uint64_t id;
    uint64_t hash[4]; // Each part of the hash is a 64-bit integer
    uint64_t size;
};

int fill(sqlite3 *db, std::mt19937 &rng, std::vector<Entry> &entries, uint64_t num_entries)
{
    auto begin = std::chrono::high_resolution_clock::now();
    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::string sql = "INSERT INTO Block(ID, h0, h1, h2, h3, Size) VALUES (?, ?, ?, ?, ?, ?);";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    for (uint64_t i = 0; i < num_entries; i++)
    {
        Entry entry = {
            i + 1,
            {rng() % UINT64_MAX, rng() % UINT64_MAX, rng() % UINT64_MAX, rng() % UINT64_MAX},
            rng() % 1000};
        entries.push_back(entry);
        sqlite3_bind_int64(stmt, 1, entry.id);
        sqlite3_bind_int64(stmt, 2, entry.hash[0]);
        sqlite3_bind_int64(stmt, 3, entry.hash[1]);
        sqlite3_bind_int64(stmt, 4, entry.hash[2]);
        sqlite3_bind_int64(stmt, 5, entry.hash[3]);
        sqlite3_bind_int64(stmt, 6, entry.size);
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
    std::string sql = "INSERT INTO Block(ID, h0, h1, h2, h3, Size) VALUES (?, ?, ?, ?, ?, ?);";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        Entry entry = {
            i + 1 + config.num_entries,
            {rng() % UINT64_MAX, rng() % UINT64_MAX, rng() % UINT64_MAX, rng() % UINT64_MAX},
            rng() % 1000};

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_int64(stmt, 1, entry.id);
        sqlite3_bind_int64(stmt, 2, entry.hash[0]);
        sqlite3_bind_int64(stmt, 3, entry.hash[1]);
        sqlite3_bind_int64(stmt, 4, entry.hash[2]);
        sqlite3_bind_int64(stmt, 5, entry.hash[3]);
        sqlite3_bind_int64(stmt, 6, entry.size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt), db, "Warmup insert " + std::to_string(i)))
            return -1;
        sqlite3_reset(stmt);

        auto end = std::chrono::high_resolution_clock::now();
    }
    sqlite3_finalize(stmt);
    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    std::vector<uint64_t> times;
    for (uint64_t i = 0; i < config.num_repetitions; i++)
    {
        Entry entry = {
            i + 1 + config.num_entries,
            {rng() % UINT64_MAX, rng() % UINT64_MAX, rng() % UINT64_MAX, rng() % UINT64_MAX},
            rng() % 1000};

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_int64(stmt, 1, entry.id);
        sqlite3_bind_int64(stmt, 2, entry.hash[0]);
        sqlite3_bind_int64(stmt, 3, entry.hash[1]);
        sqlite3_bind_int64(stmt, 4, entry.hash[2]);
        sqlite3_bind_int64(stmt, 5, entry.hash[3]);
        sqlite3_bind_int64(stmt, 6, entry.size);
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

    sqlite3_exec(db, "CREATE INDEX BlockHashSize ON Block(h0, h1, h2, h3, Size);", nullptr, nullptr, nullptr);

    std::vector<Entry> entries;
    std::mt19937 rng(2025'07'08);
    if (fill(db, rng, entries, config.num_entries) != 0)
        return -1;

    measure_insert(db, config, rng, "schema10_insert_index_normal");

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::string sql = "SELECT ID FROM Block WHERE h0 = ? AND h1 = ? AND h2 = ? AND h3 = ? AND Size = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);

    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        uint64_t idx = rng() % entries.size();

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_int64(stmt, 1, entries[idx].hash[0]);
        sqlite3_bind_int64(stmt, 2, entries[idx].hash[1]);
        sqlite3_bind_int64(stmt, 3, entries[idx].hash[2]);
        sqlite3_bind_int64(stmt, 4, entries[idx].hash[3]);
        sqlite3_bind_int64(stmt, 5, entries[idx].size);
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

        sqlite3_bind_int64(stmt, 1, entries[idx].hash[0]);
        sqlite3_bind_int64(stmt, 2, entries[idx].hash[1]);
        sqlite3_bind_int64(stmt, 3, entries[idx].hash[2]);
        sqlite3_bind_int64(stmt, 4, entries[idx].hash[3]);
        sqlite3_bind_int64(stmt, 5, entries[idx].size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt), db, "Query execution " + std::to_string(i)))
            return -1;
        if (!assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "ID check"))
            return -1;
        sqlite3_reset(stmt);

        auto end = std::chrono::high_resolution_clock::now();

        times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
    }

    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);

    sqlite3_close(db);

    report_stats(config, times, "schema10_select_index_normal");

    return 0;
}

int select_index_h0(Config &config)
{
    std::vector<std::string> table_queries = {
        CREATE_BLOCKSET_TABLE,
        CREATE_BLOCKSETENTRY_TABLE,
        CREATE_BLOCK_TABLE};

    auto db = setup_database(table_queries);

    sqlite3_exec(db, "CREATE INDEX BlockH0 ON Block(h0);", nullptr, nullptr, nullptr);

    std::vector<Entry> entries;
    std::mt19937 rng(2025'07'08);
    if (fill(db, rng, entries, config.num_entries) != 0)
        return -1;

    measure_insert(db, config, rng, "schema10_insert_index_h0");

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::string sql = "SELECT ID, h1, h2, h3, Size FROM Block WHERE h0 = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);

    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        uint64_t idx = rng() % entries.size();

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_int64(stmt, 1, entries[idx].hash[0]);
        bool found = false;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            if (sqlite3_column_int64(stmt, 1) == entries[idx].hash[1] &&
                sqlite3_column_int64(stmt, 2) == entries[idx].hash[2] &&
                sqlite3_column_int64(stmt, 3) == entries[idx].hash[3] &&
                entries[idx].size == sqlite3_column_int64(stmt, 4) &&
                assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check", false))
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

        sqlite3_bind_int64(stmt, 1, entries[idx].hash[0]);
        bool found = false;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            if (sqlite3_column_int64(stmt, 1) == entries[idx].hash[1] &&
                sqlite3_column_int64(stmt, 2) == entries[idx].hash[2] &&
                sqlite3_column_int64(stmt, 3) == entries[idx].hash[3] &&
                entries[idx].size == sqlite3_column_int64(stmt, 4) &&
                assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check", false))
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

    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);

    sqlite3_close(db);

    report_stats(config, times, "schema10_select_index_h0");

    return 0;
}

int select_index_h0_size(Config &config)
{
    std::vector<std::string> table_queries = {
        CREATE_BLOCKSET_TABLE,
        CREATE_BLOCKSETENTRY_TABLE,
        CREATE_BLOCK_TABLE};

    auto db = setup_database(table_queries);

    sqlite3_exec(db, "CREATE INDEX BlockH0 ON Block(h0, Size);", nullptr, nullptr, nullptr);

    std::vector<Entry> entries;
    std::mt19937 rng(2025'07'08);
    if (fill(db, rng, entries, config.num_entries) != 0)
        return -1;

    measure_insert(db, config, rng, "schema10_insert_index_h0_size");

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::string sql = "SELECT ID, h1, h2, h3 FROM Block WHERE h0 = ? AND Size = ?;";
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);

    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        uint64_t idx = rng() % entries.size();

        auto begin = std::chrono::high_resolution_clock::now();

        sqlite3_bind_int64(stmt, 1, entries[idx].hash[0]);
        sqlite3_bind_int64(stmt, 2, entries[idx].size);
        bool found = false;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            if (sqlite3_column_int64(stmt, 1) == entries[idx].hash[1] &&
                sqlite3_column_int64(stmt, 2) == entries[idx].hash[2] &&
                sqlite3_column_int64(stmt, 3) == entries[idx].hash[3] &&
                assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check", false))
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

        sqlite3_bind_int64(stmt, 1, entries[idx].hash[0]);
        sqlite3_bind_int64(stmt, 2, entries[idx].size);
        bool found = false;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            if (sqlite3_column_int64(stmt, 1) == entries[idx].hash[1] &&
                sqlite3_column_int64(stmt, 2) == entries[idx].hash[2] &&
                sqlite3_column_int64(stmt, 3) == entries[idx].hash[3] &&
                assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check", false))
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

    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);

    sqlite3_close(db);

    report_stats(config, times, "schema10_select_index_h0_size");

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

    measure_insert(db, config, rng, "schema10_insert_index_size");

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::string sql = "SELECT ID, h0, h1, h2, h3 FROM Block WHERE Size = ?;";
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
            if (sqlite3_column_int64(stmt, 1) == entries[idx].hash[0] &&
                sqlite3_column_int64(stmt, 2) == entries[idx].hash[1] &&
                sqlite3_column_int64(stmt, 3) == entries[idx].hash[2] &&
                sqlite3_column_int64(stmt, 4) == entries[idx].hash[3] &&
                assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check", false))
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
            if (sqlite3_column_int64(stmt, 1) == entries[idx].hash[0] &&
                sqlite3_column_int64(stmt, 2) == entries[idx].hash[1] &&
                sqlite3_column_int64(stmt, 3) == entries[idx].hash[2] &&
                sqlite3_column_int64(stmt, 4) == entries[idx].hash[3] &&
                assert_value_matches(entries[idx].id, (uint64_t)sqlite3_column_int64(stmt, 0), "Warmup ID check", false))
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

    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);

    sqlite3_close(db);

    report_stats(config, times, "schema10_select_index_size");

    return 0;
}

int main(int argc, char *argv[])
{
    auto config = parse_args(argc, argv);

    int ret;
    ret = select_index_normal(config);
    if (ret != 0)
        return ret;
    ret = select_index_h0(config);
    if (ret != 0)
        return ret;
    ret = select_index_h0_size(config);
    if (ret != 0)
        return ret;
    ret = select_index_size(config);
    if (ret != 0)
        return ret;

    if (std::filesystem::exists(DBPATH))
        std::filesystem::remove(DBPATH);

    return 0;
}