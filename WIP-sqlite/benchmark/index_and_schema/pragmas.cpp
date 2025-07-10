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
            i + 1,
            random_hash_string(rng, 44),
            rng() % 1000};
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
            blockset_count++;
            sqlite3_bind_int64(stmt_blockset, 1, blockset_id);
            sqlite3_bind_int64(stmt_blockset, 2, blockset_count);
            if (!assert_sqlite_return_code(sqlite3_step(stmt_blockset), db, "Insert Blockset for entry " + std::to_string(i)))
                return -1;
            sqlite3_reset(stmt_blockset);
            blockset_id++;
            blockset_count = 0; // Reset count for the next Blockset
        }
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
    const bool create_entry,
    const std::vector<Entry> &entries)
{
    sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, nullptr);
    for (uint64_t i = 0; i < config.num_warmup; i++)
    {
        Entry entry;
        if (create_entry)
        {
            entry = {
                i + 1 + config.num_entries,
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
    for (uint64_t i = 0; i < config.num_repitions; i++)
    {
        Entry entry;
        if (create_entry)
        {
            entry = {
                i + 1 + config.num_entries,
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

    if (measure(db, config, rng, insert_inner, report_name, true, entries) != 0)
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

    if (measure(db, config, rng, select_inner, report_name, false, entries) != 0)
        return -1;

    sqlite3_finalize(stmt);

    return 0;
}

int measure_xor(sqlite3 *db, Config &config, std::mt19937 &rng, const std::vector<Entry> &entries, const std::string &report_name)
{
    std::string sql = "SELECT ID FROM Block WHERE (Hash = ? AND Size = ?) XOR (Hash = ? AND Size = ?);";
    sqlite3_stmt *stmt;
    if (!assert_sqlite_return_code(sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr), db, "Prepare XOR select statement"))
        return -1;

    auto xor_inner = [=](sqlite3 *db, const Entry &entry, uint64_t i, const std::string &prefix) -> int
    {
        std::cout << sql;
        sqlite3_bind_text(stmt, 1, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 2, entry.size);
        sqlite3_bind_text(stmt, 3, entry.hash.c_str(), entry.hash.size(), SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 4, entry.size);
        if (!assert_sqlite_return_code(sqlite3_step(stmt), db, prefix + " query execution " + std::to_string(i)))
            return -1;
        if (!assert_value_matches(entry.id, (uint64_t)sqlite3_column_int64(stmt, 0), prefix + " ID check"))
            return -1;
        sqlite3_reset(stmt);
        return 0;
    };

    if (measure(db, config, rng, xor_inner, report_name, false, entries) != 0)
        return -1;

    sqlite3_finalize(stmt);

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

    measure_insert(db, config, rng, entries, "pragmas_insert_" + report_name);

    measure_select(db, config, rng, entries, "pragmas_select_" + report_name);

    sqlite3_close(db);

    return 0;
}

int main(int argc, char *argv[])
{
    auto config = parse_args(argc, argv);

    std::vector<std::tuple<std::string, std::vector<std::string>>> pragmas_to_run = {
        {"normal", {}},
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
        {"threads_32", {"PRAGMA threads = 32;"}}};

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