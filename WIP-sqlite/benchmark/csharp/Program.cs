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
            Type[] backends = [
                typeof(DuplicatiSQLite),
                typeof(SystemData),
                typeof(MSSqlite),
                typeof(MSSqliteAsync),
                typeof(PInvokeCAPI),
            ];
            foreach (var backend in backends)
            {
                Console.WriteLine("************");
                Console.WriteLine($"Benchmarking {backend.Name}");
                BenchmarkBase? bench_base = (BenchmarkBase?)Activator.CreateInstance(backend);
                if (bench_base == null)
                    throw new InvalidOperationException($"Failed to create instance of {backend}");
                bench_base.NumEntries = 100_000;
                bench_base.NumRepetitions = 10_000;
                switch (bench_base)
                {
                    case BenchmarkSync bench_sync:
                        sw.Restart();
                        switch (bench_sync)
                        {
                            case DuplicatiSQLite duplicatiSQLite:
                                duplicatiSQLite.GlobalSetup();
                                break;
                            case SystemData systemData:
                                systemData.GlobalSetup();
                                break;
                            case MSSqlite mssqlite:
                                mssqlite.GlobalSetup();
                                break;
                            case PInvokeCAPI pInvokeCAPI:
                                pInvokeCAPI.GlobalSetup();
                                break;
                        }
                        sw.Stop();
                        Console.WriteLine($"GlobalSetup time: {sw.ElapsedMilliseconds} ms ({((double)bench_sync.NumEntries) / sw.ElapsedMilliseconds:.02} kops/s)");

                        (string, Action, Action)[] benchmarks_sync = [
                            ("Insert", bench_sync.IterationSetupInsert, bench_sync.Insert),
                            ("Select", bench_sync.IterationSetupSelect, bench_sync.Select),
                            ("XOR1", bench_sync.IterationSetupXor, bench_sync.Xor1),
                            ("XOR2", bench_sync.IterationSetupXor, bench_sync.Xor2),
                            ("Join", bench_sync.IterationSetupJoin, bench_sync.Join),
                            ("NewBlockset", bench_sync.IterationSetupXor, bench_sync.NewBlockset),
                        ];
                        foreach (var (name, setup, run) in benchmarks_sync)
                        {
                            // Warmup
                            setup();
                            run();
                            switch (bench_sync)
                            {
                                case DuplicatiSQLite duplicatiSQLite:
                                    duplicatiSQLite.IterationCleanup();
                                    break;
                                case SystemData systemData:
                                    systemData.IterationCleanup();
                                    break;
                                case MSSqlite mssqlite:
                                    mssqlite.IterationCleanup();
                                    break;
                                case PInvokeCAPI pInvokeCAPI:
                                    pInvokeCAPI.IterationCleanup();
                                    break;
                            }
                            // Run
                            setup();
                            sw.Restart();
                            run();
                            sw.Stop();
                            Console.WriteLine($"{name} time: {sw.ElapsedMilliseconds} ms ({((double)bench_sync.NumRepetitions) / sw.ElapsedMilliseconds:.02} kops/s)");
                            switch (bench_sync)
                            {
                                case DuplicatiSQLite duplicatiSQLite:
                                    duplicatiSQLite.IterationCleanup();
                                    break;
                                case SystemData systemData:
                                    systemData.IterationCleanup();
                                    break;
                                case MSSqlite mssqlite:
                                    mssqlite.IterationCleanup();
                                    break;
                                case PInvokeCAPI pInvokeCAPI:
                                    pInvokeCAPI.IterationCleanup();
                                    break;
                            }
                        }

                        switch (bench_sync)
                        {
                            case DuplicatiSQLite duplicatiSQLite:
                                duplicatiSQLite.GlobalCleanup();
                                break;
                            case SystemData systemData:
                                systemData.GlobalCleanup();
                                break;
                            case MSSqlite mssqlite:
                                mssqlite.GlobalCleanup();
                                break;
                            case PInvokeCAPI pInvokeCAPI:
                                pInvokeCAPI.GlobalCleanup();
                                break;
                        }
                        break;
                    case BenchmarkAsync bench_async:
                        sw.Restart();
                        switch (bench_async)
                        {
                            case MSSqliteAsync mssqliteasync:
                                mssqliteasync.GlobalSetup();
                                break;
                        }
                        sw.Stop();
                        Console.WriteLine($"GlobalSetup time: {sw.ElapsedMilliseconds} ms ({((double)bench_async.NumEntries) / sw.ElapsedMilliseconds:.02} kops/s)");

                        (string, Action, Func<Task>)[] benchmarks_async = [
                            ("Insert", bench_async.IterationSetupInsert, bench_async.Insert),
                            ("Select", bench_async.IterationSetupSelect, bench_async.Select),
                            ("XOR1", bench_async.IterationSetupXor, bench_async.Xor1),
                            ("XOR2", bench_async.IterationSetupXor, bench_async.Xor2),
                            ("Join", bench_async.IterationSetupJoin, bench_async.Join),
                            ("NewBlockset", bench_async.IterationSetupXor, bench_async.NewBlockset),
                        ];
                        foreach (var (name, setup, run) in benchmarks_async)
                        {
                            // Warmup
                            setup();
                            await run();
                            switch (bench_async)
                            {
                                case MSSqliteAsync mssqliteasync:
                                    mssqliteasync.IterationCleanup();
                                    break;
                            }
                            // Run
                            setup();
                            sw.Restart();
                            await run();
                            sw.Stop();
                            Console.WriteLine($"{name} time: {sw.ElapsedMilliseconds} ms ({((double)bench_async.NumRepetitions) / sw.ElapsedMilliseconds:.02} kops/s)");
                            switch (bench_async)
                            {
                                case MSSqliteAsync mssqliteasync:
                                    mssqliteasync.IterationCleanup();
                                    break;
                            }
                        }

                        switch (bench_async)
                        {
                            case MSSqliteAsync mssqliteasync:
                                mssqliteasync.GlobalCleanup();
                                break;
                        }
                        break;
                }
            }
#else
            BenchmarkRunner.Run<DuplicatiSQLiteNoPragmas>();
            BenchmarkRunner.Run<DuplicatiSQLite>();
            BenchmarkRunner.Run<SystemData>();
            BenchmarkRunner.Run<MSSqlite>();
            BenchmarkRunner.Run<MSSqliteAsync>();
            BenchmarkRunner.Run<PInvokeCAPI>();
#endif
        }
    }

}