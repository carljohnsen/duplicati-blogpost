using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace sqlite_bench
{
    public enum Backends
    {
        DuplicatiSQLite,
        //MicrosoftSQLite,
        //SystemSQLite,
        //Dictionary
    };

    public class BenchmarkParams
    {
        public int Count { get; set; } = 1000;
        public int CommitEveryN { get; set; } = 1;
        public bool UseIndex { get; set; } = true;
        public bool IndexAfter { get; set; }
        public bool UseTransaction { get; set; } = true;

        public override string ToString() => $"Count={Count}, CommitEveryN={CommitEveryN}, UseIndex={UseIndex}, IndexAfter={IndexAfter}";
    }

    public class BenchmarkConfig : ManualConfig
    {
        public BenchmarkConfig()
        {
            AddColumn(new ThroughputColumn());
            SummaryStyle = new SummaryStyle(null, true, Perfolizer.Metrology.SizeUnit.B, Perfolizer.Horology.TimeUnit.Nanosecond, true)
                .WithMaxParameterColumnWidth(int.MaxValue) // <-- prevents shortening
                .WithRatioStyle(RatioStyle.Trend);          // optional, for better readability
        }
    }

    public static class SQLQeuries
    {
        public static readonly string CreateIndex = @"
        CREATE UNIQUE INDEX IF NOT EXISTS ""BlocksetLengthHash"" ON ""Blockset"" (""Length"", ""FullHash"");
        CREATE INDEX IF NOT EXISTS ""BlocksetHash"" ON ""Blockset"" (""FullHash"");
        CREATE INDEX IF NOT EXISTS ""BlocksetLength"" ON ""Blockset"" (""Length"");";

        public static readonly string DropAllRows = @"DELETE FROM ""Blockset"" WHERE ""ID"" >= @id";
        public static readonly string DropIndex = @"DROP INDEX IF EXISTS ""BlocksetLengthHash""; DROP INDEX IF EXISTS ""BlocksetHash""; DROP INDEX IF EXISTS ""BlocksetLength"";";

        public static readonly string FindBlockset = @"SELECT ""ID"" FROM ""Blockset"" WHERE ""Length"" = @length AND ""FullHash"" = @hash";
        public static readonly string FindBlocksetHashOnly = @"SELECT ""ID"", ""Length"" FROM ""Blockset"" WHERE ""FullHash"" = @hash";
        public static readonly string FindBlocksetLengthOnly = @"SELECT ""ID"", ""FullHash"" FROM ""Blockset"" WHERE ""Length"" = @length";

        public static readonly string FlushTemp = @"INSERT INTO ""Blockset"" (""ID"", ""Length"", ""FullHash"") SELECT ""ID"", ""Length"", ""FullHash"" FROM ""BlocksetTmp""; DROP TABLE IF EXISTS ""BlocksetTmp""";
        public static readonly string FlushTempSorted = @"INSERT INTO ""Blockset"" (""ID"", ""Length"", ""FullHash"") SELECT ""ID"", ""Length"", ""FullHash"" FROM ""BlocksetTmp"" ORDER BY ""Length"" ASC, ""FullHash"" ASC; DROP TABLE IF EXISTS ""BlocksetTmp""";

        public static readonly string Index = @"CREATE INDEX IF NOT EXISTS ""BlocksetLengthHash"" ON ""Blockset"" (""Length"", ""FullHash"");";

        public static readonly string InsertBlocksetManaged = @"INSERT INTO ""Blockset"" (""ID"", ""Length"", ""FullHash"") VALUES (@id, @length, @hash);";
        public static readonly string InsertBlocksetSelect = @"INSERT INTO ""Blockset"" (""Length"", ""FullHash"") VALUES (@length, @hash); SELECT last_insert_rowid()";
        public static readonly string InsertBlocksetReturning = @"INSERT INTO ""Blockset"" (""Length"", ""FullHash"") VALUES (@length, @hash) RETURNING ""ID""";

        public static readonly string InsertTempManaged = @"INSERT INTO ""BlocksetTmp"" (""ID"", ""Length"", ""FullHash"") VALUES (@id, @length, @hash);";

        public static readonly string LastRowId = @"SELECT ""ID"" FROM ""Blockset"" ORDER BY ""ID"" DESC LIMIT 1";

        public static readonly string TempTable = @"CREATE TEMP TABLE IF NOT EXISTS ""BlocksetTmp"" (""ID"" INTEGER PRIMARY KEY, ""Length"" INTEGER NOT NULL, ""FullHash"" TEXT NOT NULL)";

        public static readonly string[] TableQueries = [
                @"CREATE TABLE IF NOT EXISTS ""Blockset"" (""ID"" INTEGER PRIMARY KEY, ""Length"" INTEGER NOT NULL, ""FullHash"" TEXT NOT NULL)",
                //"PRAGMA synchronous = OFF",
                //"PRAGMA journal_mode = OFF",
                "PRAGMA cache_size = 1000000",
                //"PRAGMA threads = 8",
            ];
    }

    public class ThroughputColumn : IColumn
    {
        public string Id => nameof(ThroughputColumn);
        public string ColumnName => "Throughput (stmts/sec)";

        public bool IsAvailable(Summary summary) => true;
        public bool AlwaysShow => true;
        public ColumnCategory Category => ColumnCategory.Custom;
        public int PriorityInCategory => 1;
        public bool IsNumeric => true;
        public UnitType UnitType => UnitType.Dimensionless;
        public string Legend => "Operations per second (calculated as 1e9 / (Mean ns / Count))";

        public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
        {
            var statistics = summary[benchmarkCase]?.ResultStatistics;
            if (statistics == null) return "N/A";

            // Extract the Count parameter
            var count = (benchmarkCase.Parameters.Items.Where(p => p.Value is BenchmarkParams).First().Value as BenchmarkParams)?.Count ?? 1;
            double meanTime = statistics.Mean;

            if (meanTime <= 0 || count <= 0)
                return "N/A";

            double throughput = 1e9 / (meanTime / count);
            return throughput.ToString("N2");
        }

        public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
        public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style) => GetValue(summary, benchmarkCase);

    }
}