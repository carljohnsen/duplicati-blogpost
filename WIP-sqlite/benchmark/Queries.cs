namespace sqlite_bench
{

    public static class SQLQeuriesBlob
    {
        public static readonly string CreateIndex = @"
            CREATE UNIQUE INDEX IF NOT EXISTS ""BlocksetFullHashLengthIdx"" ON ""Blockset"" (""FullHashLength"", ""ID"");";

        public static readonly string DropIndex = @"
            DROP INDEX IF EXISTS ""BlocksetFullHashLengthIdx"";";
        public static readonly string DropTable = @"
            DROP TABLE IF EXISTS ""Blockset"";";

        public static readonly string FindBlockset = @"SELECT ""ID"" FROM ""Blockset"" WHERE ""FullHashLength"" = @fullhashlength";

        public static readonly string InsertBlocksetManaged = @"INSERT INTO ""Blockset"" (""ID"", ""FullHashLength"") VALUES (@id, @fullhashlength);";

        public static readonly string[] TableQueries = [
            @"CREATE TABLE IF NOT EXISTS ""Blockset"" (""ID"" INTEGER PRIMARY KEY, ""FullHashLength"" BLOB NOT NULL) STRICT",
            "PRAGMA cache_size = 1000000",
        ];
    }

    public static class SQLQeuriesBlobInt
    {
        public static readonly string CreateIndex = @"
            CREATE UNIQUE INDEX IF NOT EXISTS ""BlocksetFullHashLengthIdx"" ON ""Blockset"" (""FirstHash"", ""FullHashLength"");";

        public static readonly string DropIndex = @"
            DROP INDEX IF EXISTS ""BlocksetFullHashLengthIdx"";";
        public static readonly string DropTable = @"
            DROP TABLE IF EXISTS ""Blockset"";";

        public static readonly string FindBlockset = @"SELECT ""ID"", ""FullHashLength"" FROM ""Blockset"" WHERE ""FirstHash"" = @firsthash";

        public static readonly string InsertBlocksetManaged = @"INSERT INTO ""Blockset"" (""ID"", ""FirstHash"", ""FullHashLength"") VALUES (@id, @firsthash, @fullhashlength);";

        public static readonly string[] TableQueries = [
            @"CREATE TABLE IF NOT EXISTS ""Blockset"" (""ID"" INTEGER PRIMARY KEY, ""FirstHash"" INTEGER NOT NULL, ""FullHashLength"" BLOB NOT NULL) STRICT",
            "PRAGMA cache_size = 1000000",
        ];
    }

    public static class SQLQeuriesOriginal
    {
        public static readonly string CreateIndex = @"
            CREATE UNIQUE INDEX IF NOT EXISTS ""BlocksetLengthHash"" ON ""Blockset"" (""Length"", ""FullHash"");
            CREATE INDEX IF NOT EXISTS ""BlocksetHash"" ON ""Blockset"" (""FullHash"");
            CREATE INDEX IF NOT EXISTS ""BlocksetLength"" ON ""Blockset"" (""Length"");";

        public static readonly string DropAllRows = @"DELETE FROM ""Blockset"" WHERE ""ID"" >= @id";
        public static readonly string DropIndex = @"
            DROP INDEX IF EXISTS ""BlocksetLengthHash"";
            DROP INDEX IF EXISTS ""BlocksetHash"";
            DROP INDEX IF EXISTS ""BlocksetLength"";";
        public static readonly string DropTable = @"
            DROP TABLE IF EXISTS ""Blockset"";
            DROP TABLE IF EXISTS ""BlocksetTmp"";";

        public static readonly string FindBlockset = @"SELECT ""ID"" FROM ""Blockset"" WHERE ""Length"" = @length AND ""FullHash"" = @fullhash";
        public static readonly string FindBlocksetHashOnly = @"SELECT ""ID"", ""Length"" FROM ""Blockset"" WHERE ""FullHash"" = @fullhash";
        public static readonly string FindBlocksetLengthOnly = @"SELECT ""ID"", ""FullHash"" FROM ""Blockset"" WHERE ""Length"" = @length";

        public static readonly string FlushTemp = @"INSERT INTO ""Blockset"" (""ID"", ""Length"", ""FullHash"") SELECT ""ID"", ""Length"", ""FullHash"" FROM ""BlocksetTmp""; DROP TABLE IF EXISTS ""BlocksetTmp""";
        public static readonly string FlushTempSorted = @"INSERT INTO ""Blockset"" (""ID"", ""Length"", ""FullHash"") SELECT ""ID"", ""Length"", ""FullHash"" FROM ""BlocksetTmp"" ORDER BY ""Length"" ASC, ""FullHash"" ASC; DROP TABLE IF EXISTS ""BlocksetTmp""";

        public static readonly string Index = @"CREATE INDEX IF NOT EXISTS ""BlocksetLengthHash"" ON ""Blockset"" (""Length"", ""FullHash"");";

        public static readonly string InsertBlocksetManaged = @"INSERT INTO ""Blockset"" (""ID"", ""Length"", ""FullHash"") VALUES (@id, @length, @fullhash);";
        public static readonly string InsertBlocksetSelect = @"INSERT INTO ""Blockset"" (""Length"", ""FullHash"") VALUES (@length, @fullhash); SELECT last_insert_rowid()";
        public static readonly string InsertBlocksetReturning = @"INSERT INTO ""Blockset"" (""Length"", ""FullHash"") VALUES (@length, @fullhash) RETURNING ""ID""";

        public static readonly string InsertTempManaged = @"INSERT INTO ""BlocksetTmp"" (""ID"", ""Length"", ""FullHash"") VALUES (@id, @length, @fullhash);";

        public static readonly string LastRowId = @"SELECT ""ID"" FROM ""Blockset"" ORDER BY ""ID"" DESC LIMIT 1";

        public static readonly string TempTable = @"CREATE TEMP TABLE IF NOT EXISTS ""BlocksetTmp"" (""ID"" INTEGER PRIMARY KEY, ""Length"" INTEGER NOT NULL, ""FullHash"" TEXT NOT NULL)";

        public static readonly string[] TableQueries = [
                @"CREATE TABLE IF NOT EXISTS ""Blockset"" (""ID"" INTEGER PRIMARY KEY, ""Length"" INTEGER NOT NULL, ""FullHash"" TEXT NOT NULL)"
            ];
        public static readonly string[] PragmaQueries = [
                "PRAGMA synchronous = OFF",
                "PRAGMA temp_store = MEMORY",
                "PRAGMA journal_mode = WAL",
                "PRAGMA cache_size = -512000",
                "PRAGMA query_only = true",
                "PRAGMA threads = 8",
                "PRAGMA read_uncommitted = true",
                "PRAGMA mmap_size = 536870912",
                "PRAGMA shared_cache = true",
                "PRAGMA optimize",
            ];
    }

    public static class SQLQeuriesHashIntColumn
    {
        //CREATE INDEX IF NOT EXISTS ""BlocksetLengthHash"" ON ""Blockset"" (""Length"", ""FullHash"");
        //CREATE INDEX IF NOT EXISTS ""BlocksetFullHash"" ON ""Blockset"" (""FullHash"");
        //CREATE INDEX IF NOT EXISTS ""BlocksetLength"" ON ""Blockset"" (""Length"");
        //CREATE INDEX IF NOT EXISTS ""BlocksetHash"" ON ""Blockset"" (""Hash"");
        public static readonly string CreateIndex = @"
            CREATE UNIQUE INDEX IF NOT EXISTS ""BlocksetAll"" ON ""Blockset"" (""Hash"", ""Length"", ""FullHash"");";

        public static readonly string DropAllRows = @"DELETE FROM ""Blockset"" WHERE ""ID"" >= @id";
        public static readonly string DropIndex = @"
            DROP INDEX IF EXISTS ""BlocksetLengthHash"";
            DROP INDEX IF EXISTS ""BlocksetFullHash"";
            DROP INDEX IF EXISTS ""BlocksetLength"";
            DROP INDEX IF EXISTS ""BlocksetHash"";";
        public static readonly string DropTable = @"
            DROP TABLE IF EXISTS ""Blockset"";
            DROP TABLE IF EXISTS ""BlocksetTmp"";";

        public static readonly string FindBlockset = @"SELECT ""ID"" FROM ""Blockset"" WHERE ""Length"" = @length AND ""FullHash"" = @fullhash";
        public static readonly string FindBlocksetHashOnly = @"SELECT ""ID"", ""Length"" FROM ""Blockset"" WHERE ""FullHash"" = @fullhash";
        public static readonly string FindBlocksetLengthOnly = @"SELECT ""ID"", ""FullHash"" FROM ""Blockset"" WHERE ""Length"" = @length";
        public static readonly string FindBlocksetHashOnlyInt = @"SELECT ""ID"", ""Length"", ""FullHash"" FROM ""Blockset"" WHERE ""Hash"" = @hash";
        public static readonly string FindBlocksetHashIntLength = @"SELECT ""ID"", ""FullHash"" FROM ""Blockset"" WHERE ""Hash"" = @hash AND ""Length"" = @length";

        public static readonly string FlushTemp = @"
            INSERT INTO ""Blockset"" (""ID"", ""Hash"", ""Length"", ""FullHash"") SELECT ""ID"", ""Hash"", ""Length"", ""FullHash"" FROM ""BlocksetTmp"";
            DROP TABLE IF EXISTS ""BlocksetTmp""";
        public static readonly string FlushTempSorted = @"
            INSERT INTO ""Blockset"" (""ID"", ""Hash"", ""Length"", ""FullHash"") SELECT ""ID"", ""Hash"", ""Length"", ""FullHash"" FROM ""BlocksetTmp"" ORDER BY ""Hash"" ASC;
            DROP TABLE IF EXISTS ""BlocksetTmp""";

        public static readonly string InsertBlocksetManaged = @"INSERT INTO ""Blockset"" (""ID"", ""Hash"", ""Length"", ""FullHash"") VALUES (@id, @hash, @length, @fullhash);";
        public static readonly string InsertBlocksetSelect = @"INSERT INTO ""Blockset"" (""Hash"", ""Length"", ""FullHash"") VALUES (@hash, @length, @fullhash); SELECT last_insert_rowid()";
        public static readonly string InsertBlocksetReturning = @"INSERT INTO ""Blockset"" (""Hash"", ""Length"", ""FullHash"") VALUES (@hash, @length, @fullhash) RETURNING ""ID""";

        public static readonly string InsertTempManaged = @"INSERT INTO ""BlocksetTmp"" (""ID"", ""Hash"", ""Length"", ""FullHash"") VALUES (@id, @hash, @length, @fullhash);";

        public static readonly string LastRowId = @"SELECT ""ID"" FROM ""Blockset"" ORDER BY ""ID"" DESC LIMIT 1";

        public static readonly string TempTable = @"CREATE TEMP TABLE IF NOT EXISTS ""BlocksetTmp"" (""ID"" INTEGER PRIMARY KEY, ""Hash"" INTEGER NOT NULL, ""Length"" INTEGER NOT NULL, ""FullHash"" TEXT NOT NULL)";

        public static readonly string[] TableQueries = [
            @"CREATE TABLE IF NOT EXISTS ""Blockset"" (""ID"" INTEGER PRIMARY KEY, ""Hash"" INTEGER NOT NULL,""Length"" INTEGER NOT NULL, ""FullHash"" TEXT NOT NULL)",
            //"PRAGMA synchronous = OFF",
            //"PRAGMA journal_mode = OFF",
            "PRAGMA cache_size = 1000000",
            //"PRAGMA threads = 8",
        ];
    }

}