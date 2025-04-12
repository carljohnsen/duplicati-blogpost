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
            var sw = new System.Diagnostics.Stopwatch();
            using var b = new SQLiteSelectBlobIntPInvokeBenchmark();
            Console.WriteLine("GlobalSetup...");
            b.PreFilledCount = 1000;
            b.BenchmarkParams.Count = 1_000_000;
            sw.Restart();
            b.GlobalSetup();
            sw.Stop();
            Console.WriteLine($"GlobalSetup took {sw.ElapsedMilliseconds} ms ({(b.BenchmarkParams.Count / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
            Console.WriteLine("Running SelectBenchmark...");
            sw.Restart();
            b.SelectBenchmark();
            sw.Stop();
            Console.WriteLine($"SelectBenchmark took {sw.ElapsedMilliseconds} ms ({(b.BenchmarkParams.Count / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
            //b.SelectFullHashOnlyBenchmark();
            //b.SelectLengthOnlyBenchmark();
            //b.SelectHashOnlyIntBenchmark();
            Console.WriteLine("Done!");
#else
            var summary = BenchmarkRunner.Run<SQLiteSelectBlobIntPInvokeBenchmark>();
#endif
        }
    }

}