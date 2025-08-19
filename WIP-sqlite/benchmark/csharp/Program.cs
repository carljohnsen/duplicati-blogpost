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
            var sysdata = new SystemData();
            SystemData.NumEntries = 100_000;
            SystemData.NumRepetitions = 10_000;
            sysdata.GlobalSetup();
            sysdata.IterationSetup();
            sw.Start();
            sysdata.Select();
            sw.Stop();
            Console.WriteLine($"Elapsed time: {sw.ElapsedMilliseconds} ms ({((double)SystemData.NumRepetitions) / sw.ElapsedMilliseconds:.02} kops/s)");
            sysdata.GlobalCleanup();
#else
            BenchmarkRunner.Run<SystemData>();
#endif
        }
    }

}