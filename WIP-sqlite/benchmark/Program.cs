using System.Threading.Tasks;
using BenchmarkDotNet.Running;

namespace sqlite_bench
{

    public class Program
    {
        static async Task Main(string[] args)
        {
#if DEBUG
            //#if true
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
            using var b = new SQLiteSelectOrInsertParallelBenchmark();
            Console.WriteLine("GlobalSetup...");
            b.PreFilledCount = 1_00_000;
            b.BenchmarkParams.Count = 1_00_000;
            sw.Restart();
            await b.GlobalSetup();
            sw.Stop();
            Console.WriteLine($"GlobalSetup took {sw.ElapsedMilliseconds} ms ({((b.BenchmarkParams.Count + b.PreFilledCount) / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
            Console.WriteLine("Running SelectBenchmark...");
            sw.Restart();
            b.IterationSetup();
            sw.Stop();
            Console.WriteLine($"IterationSetup took {sw.ElapsedMilliseconds} ms ({((b.BenchmarkParams.Count + b.PreFilledCount) / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
            sw.Restart();
            await b.SelectBenchmark();
            sw.Stop();
            Console.WriteLine($"SelectBenchmark took {sw.ElapsedMilliseconds} ms ({(b.BenchmarkParams.Count / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
            //b.SelectFullHashOnlyBenchmark();
            //b.SelectLengthOnlyBenchmark();
            //b.SelectHashOnlyIntBenchmark();
            b.IterationCleanup();
            b.IterationSetup();
            sw.Restart();
            await b.SelectBenchmark();
            sw.Stop();
            Console.WriteLine($"SelectBenchmark took {sw.ElapsedMilliseconds} ms ({(b.BenchmarkParams.Count / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
            b.IterationCleanup();
            Console.WriteLine("Done!");
#else
            //BenchmarkRunner.Run<SQLiteSelectBenchmark>();
            //BenchmarkRunner.Run<SQLiteSelectPInvokeBenchmark>();
            //BenchmarkRunner.Run<SQLiteSelectParallelBenchmark>();
            BenchmarkRunner.Run<SQLiteSelectOrInsertParallelBenchmark>();
#endif
        }
    }

}