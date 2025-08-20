using System.Diagnostics;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;

namespace sqlite_bench
{

    public class Program
    {
        static async Task Main(string[] args)
        {
#if DEBUG
            var sw = new Stopwatch();
            var bench = new SystemData();
            SystemData.NumEntries = 100_000;
            SystemData.NumRepetitions = 10_000;
            bench.GlobalSetup();

            // Insert benchmark
            // Warmup
            bench.IterationSetupInsert();
            bench.Insert();
            // Run
            bench.IterationSetupInsert();
            sw.Restart();
            bench.Insert();
            sw.Stop();
            Console.WriteLine($"Insert time: {sw.ElapsedMilliseconds} ms ({((double)SystemData.NumRepetitions) / sw.ElapsedMilliseconds:.02} kops/s)");

            // Select benchmark
            // Warmup
            bench.IterationSetupSelect();
            bench.Select();
            // Run
            bench.IterationSetupSelect();
            sw.Restart();
            bench.Select();
            sw.Stop();
            Console.WriteLine($"Select time: {sw.ElapsedMilliseconds} ms ({((double)SystemData.NumRepetitions) / sw.ElapsedMilliseconds:.02} kops/s)");

            // XOR1 benchmark
            // Warmup
            bench.IterationSetupXor1();
            bench.Xor1();
            // Run
            bench.IterationSetupXor1();
            sw.Restart();
            bench.Xor1();
            sw.Stop();
            Console.WriteLine($"XOR1 time: {sw.ElapsedMilliseconds} ms ({((double)SystemData.NumRepetitions) / sw.ElapsedMilliseconds:.02} kops/s)");

            bench.GlobalCleanup();
#else
            BenchmarkRunner.Run<SystemData>();
#endif
        }
    }

}