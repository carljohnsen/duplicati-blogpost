using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;

namespace sqlite_bench_old
{
    public enum Backends
    {
        DuplicatiSQLite,
        MicrosoftSQLite,
        SystemSQLite,
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