#include "shared.hpp"

const std::string CREATE_BLOCK_TABLE = "CREATE TABLE Block (ID INTEGER PRIMARY KEY, Hash TEXT NOT NULL, Size INTEGER NOT NULL);";

struct Entry
{
    uint64_t id;
    std::string hash;
    uint64_t size;
    uint64_t blockset_id;
};

int fill(sqlite3 *db, std::mt19937 &rng, std::vector<Entry> &entries, uint64_t num_entries)
{
    auto begin = std::chrono::high_resolution_clock::now();
    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);

    std::string
        sql_block = "INSERT INTO Block(ID, Hash, Size) VALUES (?, ?, ?);",
        sql_blockset = "INSERT INTO Blockset(ID, Length) VALUES (?, ?);",
        sql_blockset_entry = "INSERT INTO BlocksetEntry(BlocksetID, BlockID) VALUES (?, ?);";
    sqlite3_stmt *stmt_block, *stmt_blockset, *stmt_blockset_entry;
    sqlite3_prepare_v2(db, sql_block.c_str(), -1, &stmt_block, nullptr);
    sqlite3_prepare_v2(db, sql_blockset.c_str(), -1, &stmt_blockset, nullptr);
    sqlite3_prepare_v2(db, sql_blockset_entry.c_str(), -1, &stmt_blockset_entry, nullptr);

    uint64_t
        blockset_id = 1,
        blockset_count = 0;

    for (uint64_t i = 0; i < num_entries; i++)
    {
        // Block
        Entry entry = {
            i,
            random_hash_string(rng, 44),
            rng() % 1000,
            blockset_id};
        entries.push_back(entry);
        sqlite3_bind_int64(stmt_block, 1, entry.id);
        sqlite3_bind_text(stmt_block, 2, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt_block, 3, entry.size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt_block), db, "Insert entry " + std::to_string(i)))
            return -1;
        sqlite3_reset(stmt_block);

        // BlocksetEntry
        sqlite3_bind_int64(stmt_blockset_entry, 1, blockset_id);
        sqlite3_bind_int64(stmt_blockset_entry, 2, entry.id);
        if (!assert_sqlite_return_code(sqlite3_step(stmt_blockset_entry), db, "Insert BlocksetEntry for entry " + std::to_string(i)))
            return -1;
        sqlite3_reset(stmt_blockset_entry);
        blockset_count++;

        // Blockset
        if (rng() % 1000 > 995) // 0.5% chance to create a new Blockset
        {
            sqlite3_bind_int64(stmt_blockset, 1, blockset_id);
            sqlite3_bind_int64(stmt_blockset, 2, blockset_count);
            if (!assert_sqlite_return_code(sqlite3_step(stmt_blockset), db, "Insert Blockset for entry " + std::to_string(i)))
                return -1;
            sqlite3_reset(stmt_blockset);
            blockset_id++;
            blockset_count = 0; // Reset count for the next Blockset
        }
    }

    // Finish the current blockset, if it has blocksetentries.
    if (blockset_count > 0)
    {
        sqlite3_bind_int64(stmt_blockset, 1, blockset_id);
        sqlite3_bind_int64(stmt_blockset, 2, blockset_count);
        if (!assert_sqlite_return_code(sqlite3_step(stmt_blockset), db, "Insert Blockset for entry " + std::to_string(blockset_id)))
            return -1;
        sqlite3_reset(stmt_blockset);
    }

    sqlite3_finalize(stmt_block);
    sqlite3_finalize(stmt_blockset);
    sqlite3_finalize(stmt_blockset_entry);

    sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "PRAGMA optimize;", nullptr, nullptr, nullptr);

    auto end = std::chrono::high_resolution_clock::now();

    std::cout << "Inserted " << entries.size() << " entries in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
              << " ms." << std::endl;

    return 0;
}

int measure(
    sqlite3 *db,
    Config &config,
    std::mt19937 &rng,
    const std::function<int(sqlite3 *, const Entry &, uint64_t, const std::string &)> &f,
    const std::string &report_name,
    const int create_entry, // Percentage probability of creating a new entry
    const std::vector<Entry> &entries)
{
    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    uint64_t next_id = config.num_entries;
    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        Entry entry;
        if ((rng() % 100) >= (100 - create_entry))
        {
            entry = {
                next_id++,
                random_hash_string(rng, 44),
                rng() % 1000};
        }
        else
        {
            entry = entries[i % entries.size()]; // Reuse existing entries for warmup
        }

        auto begin = std::chrono::high_resolution_clock::now();

        if (f(db, entry, i, "Warmup") != 0)
            return -1;

        auto end = std::chrono::high_resolution_clock::now();
    }
    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    std::vector<uint64_t> times;
    next_id = config.num_entries;
    for (uint64_t i = 0; i < config.num_repetitions; i++)
    {
        Entry entry;
        if ((rng() % 100) >= (100 - create_entry))
        {
            entry = {
                next_id++,
                random_hash_string(rng, 44),
                rng() % 1000};
        }
        else
        {
            entry = entries[i % entries.size()]; // Reuse existing entries for warmup
        }

        auto begin = std::chrono::high_resolution_clock::now();

        if (f(db, entry, i, "Warmup") != 0)
            return -1;

        auto end = std::chrono::high_resolution_clock::now();

        times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
    }
    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);

    report_stats(config, times, report_name);

    return 0;
}

int measure_insert(sqlite3 *db, Config &config, std::mt19937 &rng, const std::vector<Entry> &entries, const std::string &report_name)
{
    std::string sql = "INSERT INTO Block(ID, Hash, Size) VALUES (?, ?, ?);";
    sqlite3_stmt *stmt;
    if (!assert_sqlite_return_code(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr), db, "Prepare insert statement"))
        return -1;

    auto insert_inner = [=](sqlite3 *db, const Entry &entry, uint64_t i, const std::string &prefix) -> int
    {
        sqlite3_bind_int64(stmt, 1, entry.id);
        sqlite3_bind_text(stmt, 2, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 3, entry.size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt), db, prefix + " insert " + std::to_string(i)))
            return -1;
        sqlite3_reset(stmt);

        return 0;
    };

    if (measure(db, config, rng, insert_inner, report_name, 100, entries) != 0)
        return -1;

    sqlite3_finalize(stmt);

    return 0;
}

int measure_select(sqlite3 *db, Config &config, std::mt19937 &rng, const std::vector<Entry> &entries, const std::string &report_name)
{
    std::string sql = "SELECT ID FROM Block WHERE Hash = ? AND Size = ?;";
    sqlite3_stmt *stmt;
    if (!assert_sqlite_return_code(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr), db, "Prepare select statement"))
        return -1;

    auto select_inner = [=](sqlite3 *db, const Entry &entry, uint64_t i, const std::string &prefix) -> int
    {
        sqlite3_bind_text(stmt, 1, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 2, entry.size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt), db, prefix + " query execution " + std::to_string(i)))
            return -1;
        if (!assert_value_matches(entry.id, (uint64_t)sqlite3_column_int64(stmt, 0), prefix + " ID check"))
            return -1;
        sqlite3_reset(stmt);

        return 0;
    };

    if (measure(db, config, rng, select_inner, report_name, 0, entries) != 0)
        return -1;

    sqlite3_finalize(stmt);

    return 0;
}

int measure_xor1(sqlite3 *db, Config &config, std::mt19937 &rng, const std::vector<Entry> &entries, const std::string &report_name)
{
    std::string
        sql_select = "SELECT ID FROM Block WHERE (Hash = ? AND Size = ?);",
        sql_insert = "INSERT INTO Block(ID, Hash, Size) VALUES (?, ?, ?);";
    sqlite3_stmt *stmt_select, *stmt_insert;
    if (!assert_sqlite_return_code(sqlite3_prepare_v2(db, sql_select.c_str(), -1, &stmt_select, nullptr), db, "Prepare xor select statement"))
        return -1;
    if (!assert_sqlite_return_code(sqlite3_prepare_v2(db, sql_insert.c_str(), -1, &stmt_insert, nullptr), db, "Prepare xor insert statement"))
        return -1;

    auto xor_inner = [=](sqlite3 *db, const Entry &entry, uint64_t i, const std::string &prefix) -> int
    {
        sqlite3_bind_text(stmt_select, 1, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt_select, 2, entry.size);
        auto rc = sqlite3_step(stmt_select);
        if (!assert_sqlite_return_code(rc, db, prefix + " xor query execution " + std::to_string(i)))
            return -1;
        auto found_id = rc == SQLITE_ROW ? sqlite3_column_int64(stmt_select, 0) : -1;
        sqlite3_reset(stmt_select);

        if (found_id == -1)
        {
            // Not found, insert
            sqlite3_bind_int64(stmt_insert, 1, entry.id);
            sqlite3_bind_text(stmt_insert, 2, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
            sqlite3_bind_int64(stmt_insert, 3, entry.size);
            if (!assert_sqlite_return_code(sqlite3_step(stmt_insert), db, prefix + " xor insert " + std::to_string(i)))
                return -1;
            sqlite3_reset(stmt_insert);
        }
        else
        {
            if (!assert_value_matches(entry.id, (uint64_t)found_id, prefix + " xor ID check"))
                return -1;
        }

        return 0;
    };

    if (measure(db, config, rng, xor_inner, report_name, 50, entries) != 0)
        return -1;

    sqlite3_finalize(stmt_select);
    sqlite3_finalize(stmt_insert);

    return 0;
}

int measure_xor2(sqlite3 *db, Config &config, std::mt19937 &rng, const std::vector<Entry> &entries, const std::string &report_name)
{
    std::string
        sql_insert = "INSERT OR IGNORE INTO Block (ID, Hash, Size) VALUES (?, ?, ?);",
        sql_select = "SELECT * FROM Block WHERE Hash = ? AND Size = ?;";
    sqlite3_stmt *stmt_insert, *stmt_select;
    if (!assert_sqlite_return_code(sqlite3_prepare_v2(db, sql_insert.c_str(), -1, &stmt_insert, nullptr), db, "Prepare xor2 insert statement"))
        return -1;
    if (!assert_sqlite_return_code(sqlite3_prepare_v2(db, sql_select.c_str(), -1, &stmt_select, nullptr), db, "Prepare xor2 select statement"))
        return -1;

    auto xor_inner = [=](sqlite3 *db, const Entry &entry, uint64_t i, const std::string &prefix) -> int
    {
        sqlite3_bind_int64(stmt_insert, 1, entry.id);
        sqlite3_bind_text(stmt_insert, 2, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt_insert, 3, entry.size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt_insert), db, prefix + " xor2 insert query execution " + std::to_string(i)))
            return -1;
        sqlite3_reset(stmt_insert);
        sqlite3_bind_text(stmt_select, 1, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt_select, 2, entry.size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt_select), db, prefix + " xor2 select query execution " + std::to_string(i)))
            return -1;
        if (!assert_value_matches(entry.id, (uint64_t)sqlite3_column_int64(stmt_select, 0), prefix + " xor2 ID check"))
            return -1;
        sqlite3_reset(stmt_select);

        return 0;
    };

    if (measure(db, config, rng, xor_inner, report_name, 50, entries) != 0)
        return -1;

    sqlite3_finalize(stmt_insert);
    sqlite3_finalize(stmt_select);

    return 0;
}

uint64_t blockset_count(uint64_t blockset_id, const std::vector<Entry> &entries)
{
    uint64_t count = 0;
    for (const auto &entry : entries)
    {
        if (entry.blockset_id == blockset_id)
        {
            count++;
        }
    }
    return count;
}

int measure_join(sqlite3 *db, Config &config, std::mt19937 &rng, const std::vector<Entry> &entries, const std::string &report_name)
{
    std::string sql = "SELECT Block.ID, Block.Hash, Block.Size FROM Block JOIN BlocksetEntry ON BlocksetEntry.BlockID = Block.ID WHERE BlocksetEntry.BlocksetID = ?;";
    sqlite3_stmt *stmt;
    if (!assert_sqlite_return_code(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr), db, "Prepare join statement"))
        return -1;

    uint64_t max_blockset = 0;
    for (auto &entry : entries)
    {
        max_blockset = std::max(max_blockset, entry.blockset_id);
    }

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);

    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        uint64_t blockset_id = (rng() % max_blockset) + 1;
        sqlite3_bind_int64(stmt, 1, blockset_id);
        uint64_t count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            // Process the row
            auto found_id = sqlite3_column_int64(stmt, 0);
            auto found_hash = std::string((const char *)sqlite3_column_text(stmt, 1));
            auto found_size = (uint64_t)sqlite3_column_int64(stmt, 2);
            auto entry = entries[found_id];
            if (!assert_value_matches(entry.hash, found_hash, "Hash check"))
                return -1;
            if (!assert_value_matches(entry.size, found_size, "Size check"))
                return -1;
            if (!assert_value_matches(entry.blockset_id, blockset_id, "Blockset ID check"))
                return -1;
            count++;
        }
        sqlite3_reset(stmt);
        if (!assert_value_matches(blockset_count(blockset_id, entries), count, "Blockset count check"))
            return -1;
    }

    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);

    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);

    std::vector<uint64_t> times;
    for (uint64_t i = 0; i < config.num_repetitions; i++)
    {
        uint64_t blockset_id = (rng() % max_blockset) + 1;
        auto begin = std::chrono::high_resolution_clock::now();
        sqlite3_bind_int64(stmt, 1, blockset_id);
        uint64_t count = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW)
        {
            // Process the row
            auto found_id = sqlite3_column_int64(stmt, 0);
            auto found_hash = std::string((const char *)sqlite3_column_text(stmt, 1));
            auto found_size = (uint64_t)sqlite3_column_int64(stmt, 2);
            auto entry = entries[found_id];
            if (!assert_value_matches(entry.hash, found_hash, "Hash check"))
                return -1;
            if (!assert_value_matches(entry.size, found_size, "Size check"))
                return -1;
            if (!assert_value_matches(entry.blockset_id, blockset_id, "Blockset ID check"))
                return -1;
            count++;
        }
        sqlite3_reset(stmt);
        auto end = std::chrono::high_resolution_clock::now();
        if (!assert_value_matches(blockset_count(blockset_id, entries), count, "Blockset count check"))
            return -1;
        times.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count() / count);
    }

    sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);

    sqlite3_finalize(stmt);

    report_stats(config, times, report_name);

    return 0;
}

int measure_all(Config &config, std::string &report_name, std::vector<std::string> &pragmas)
{
    std::vector<std::string> table_queries = {
        CREATE_BLOCKSET_TABLE,
        CREATE_BLOCKSETENTRY_TABLE,
        CREATE_BLOCK_TABLE};

    auto db = setup_database(table_queries);

    sqlite3_exec(db, "CREATE INDEX BlockHashSize ON Block(Hash, Size);", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "CREATE INDEX BlocksetEntryBlocksetID ON BlocksetEntry(BlocksetID);", nullptr, nullptr, nullptr);
    sqlite3_exec(db, "CREATE INDEX BlocksetBlocksetID ON Blockset(ID);", nullptr, nullptr, nullptr);

    std::vector<Entry> entries;
    std::mt19937 rng(2025'07'08);
    if (fill(db, rng, entries, config.num_entries) != 0)
        return -1;

    for (const auto &pragma : pragmas)
    {
        sqlite3_exec(db, pragma.c_str(), nullptr, nullptr, nullptr);
    }

    if (measure_insert(db, config, rng, entries, "pragmas_insert_" + report_name) != 0)
        return -1;

    if (measure_select(db, config, rng, entries, "pragmas_select_" + report_name) != 0)
        return -1;

    if (measure_xor1(db, config, rng, entries, "pragmas_xor1_" + report_name) != 0)
        return -1;

    if (measure_xor2(db, config, rng, entries, "pragmas_xor2_" + report_name) != 0)
        return -1;

    if (measure_join(db, config, rng, entries, "pragmas_join_" + report_name) != 0)
        return -1;

    sqlite3_close(db);

    return 0;
}

int main(int argc, char *argv[])
{
    auto config = parse_args(argc, argv);

    std::vector<std::tuple<std::string, std::vector<std::string>>> pragmas_to_run = {
        //{"normal", {}},
        //{"synch_off", {"PRAGMA synchronous = OFF;"}},
        //{"synch_normal", {"PRAGMA synchronous = NORMAL;"}},
        //{"synch_full", {"PRAGMA synchronous = FULL;"}},
        //{"synch_extra", {"PRAGMA synchronous = EXTRA;"}},
        //{"temp_store_memory", {"PRAGMA temp_store = MEMORY;"}},
        //{"temp_store_default", {"PRAGMA temp_store = DEFAULT;"}},
        //{"journal_delete", {"PRAGMA journal_mode = DELETE;"}},
        //{"journal_memory", {"PRAGMA journal_mode = MEMORY;"}},
        //{"journal_wal", {"PRAGMA journal_mode = WAL;"}},
        //{"journal_off", {"PRAGMA journal_mode = OFF;"}},
        //{"cache_size_2M", {"PRAGMA cache_size = -2000;"}},
        //{"cache_size_4M", {"PRAGMA cache_size = -4000;"}},
        //{"cache_size_8M", {"PRAGMA cache_size = -8000;"}},
        //{"cache_size_16M", {"PRAGMA cache_size = -16000;"}},
        //{"cache_size_32M", {"PRAGMA cache_size = -32000;"}},
        //{"cache_size_64M", {"PRAGMA cache_size = -64000;"}},
        //{"cache_size_128M", {"PRAGMA cache_size = -128000;"}},
        //{"cache_size_256M", {"PRAGMA cache_size = -256000;"}},
        //{"cache_size_512M", {"PRAGMA cache_size = -512000;"}},
        //{"mmap_size_2M", {"PRAGMA mmap_size = 2000000;"}},
        //{"mmap_size_4M", {"PRAGMA mmap_size = 4000000;"}},
        //{"mmap_size_8M", {"PRAGMA mmap_size = 8000000;"}},
        //{"mmap_size_16M", {"PRAGMA mmap_size = 16000000;"}},
        //{"mmap_size_32M", {"PRAGMA mmap_size = 32000000;"}},
        //{"mmap_size_64M", {"PRAGMA mmap_size = 64000000;"}},
        //{"mmap_size_128M", {"PRAGMA mmap_size = 128000000;"}},
        //{"mmap_size_256M", {"PRAGMA mmap_size = 256000000;"}},
        //{"mmap_size_512M", {"PRAGMA mmap_size = 512000000;"}},
        //{"threads_0", {"PRAGMA threads = 0;"}},
        //{"threads_1", {"PRAGMA threads = 1;"}},
        //{"threads_2", {"PRAGMA threads = 2;"}},
        //{"threads_4", {"PRAGMA threads = 4;"}},
        //{"threads_8", {"PRAGMA threads = 8;"}},
        //{"threads_16", {"PRAGMA threads = 16;"}},
        //{"threads_32", {"PRAGMA threads = 32;"}},
        {"combination", {"PRAGMA synchronous = NORMAL;", "PRAGMA temp_store = MEMORY;", "PRAGMA journal_mode = WAL;", "PRAGMA cache_size = -64000;", "PRAGMA mmap_size = 64000000;", "PRAGMA threads = 8;"}}
        //
    };

    for (auto &[report_name, pragmas] : pragmas_to_run)
    {
        int ret;
        ret = measure_all(config, report_name, pragmas);
        if (ret != 0)
            return ret;
    }

    if (std::filesystem::exists(DBPATH))
        std::filesystem::remove(DBPATH);

    return 0;
}