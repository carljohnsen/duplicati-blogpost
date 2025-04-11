using BenchmarkDotNet.Running;

namespace sqlite_bench
{

    public class Program
    {
        static void Main(string[] args)
        {
#if DEBUG
            /* // Insertion benchmark
            using var b = new SQLiteInsertBenchmark();
            b.GlobalSetup();
            b.IterationSetup();
            b.FillManaged();
            //b.FillReturning();
            //b.FillSelect();
            //b.IterationCleanup();
            //b.GlobalCleanup();
            */

            // Selection benchmark
            using var b = new SQLiteSelectBenchmark();
            Console.WriteLine("GlobalSetup...");
            b.PreFilledCount = 1000;
            b.BenchmarkParams.Count = 100;
            b.GlobalSetup();
            Console.WriteLine("Running SelectBenchmark...");
            b.SelectBenchmark();
            //b.SelectFullHashOnlyBenchmark();
            //b.SelectLengthOnlyBenchmark();
            //b.SelectHashOnlyIntBenchmark();
            Console.WriteLine("Done!");
#else
            var summary = BenchmarkRunner.Run<SQLiteSelectHashIntColumnBenchmark>();
#endif
        }
    }

}