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

            (string, Action, Action)[] benchmarks = [
                ("Insert", bench.IterationSetupInsert, bench.Insert),
                ("Select", bench.IterationSetupSelect, bench.Select),
                ("XOR1", bench.IterationSetupXor, bench.Xor1),
                ("XOR2", bench.IterationSetupXor, bench.Xor2)
            ];
            foreach (var (name, setup, run) in benchmarks)
            {
                // Warmup
                setup();
                run();
                // Run
                setup();
                sw.Restart();
                run();
                sw.Stop();
                Console.WriteLine($"{name} time: {sw.ElapsedMilliseconds} ms ({((double)SystemData.NumRepetitions) / sw.ElapsedMilliseconds:.02} kops/s)");
            }

            bench.GlobalCleanup();
#else
            BenchmarkRunner.Run<SystemData>();
#endif
        }
    }

}