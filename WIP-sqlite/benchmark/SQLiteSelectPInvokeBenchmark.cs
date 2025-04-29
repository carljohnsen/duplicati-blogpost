// A P/Invoke-based benchmark for SQLite SELECT using raw SQLite C API
// Replace or run alongside your existing SQLiteSelectBlobBenchmark to compare

using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace sqlite_bench
{
    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public partial class SQLiteSelectPInvokeBenchmark : SQLiteBenchmarkSequential
    {
        private IntPtr db;
        private IntPtr stmt;
        private List<(long, long, string)> entries = [];
        readonly string selectSql = @"SELECT ""ID"" FROM ""Blockset"" WHERE ""Length"" = ? AND ""FullHash"" = ?;";

        public SQLiteSelectPInvokeBenchmark() : base(Backends.DuplicatiSQLite)
        {

        }

        [Params(1_000_000)]
        public int PreFilledCount { get; set; } = 0;

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        [GlobalSetup]
        public void GlobalSetup()
        {
            int return_code = -1;
            var dbPath = Path.GetFullPath("pinvoke_benchmark.sqlite");
            if (File.Exists(dbPath)) File.Delete(dbPath);

            sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
            return_code = sqlite3_open(dbPath, out db);
            if (return_code != 0)
                throw new Exception($"Failed to open database: {return_code}");
            Execute("PRAGMA journal_mode = WAL;");
            Execute(@"CREATE TABLE IF NOT EXISTS ""Blockset"" (""ID"" INTEGER PRIMARY KEY, ""Length"" INTEGER NOT NULL, ""FullHash"" TEXT NOT NULL);");
            Execute(@"CREATE UNIQUE INDEX IF NOT EXISTS ""BlocksetLengthHash"" ON ""Blockset"" (""Length"", ""FullHash"");");

            var insertSql = @"INSERT INTO ""Blockset"" (""ID"", ""Length"", ""FullHash"") VALUES (?, ?, ?);";
            sqlite3_prepare_v2(db, insertSql, -1, out var insertStmt, IntPtr.Zero);

            var rng = new Random(42);

            var buffer = new byte[44];
            var alphanumericChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

            // Generate random data to insert
            Execute("BEGIN DEFERRED TRANSACTION;");
            for (long i = 0; i < BenchmarkParams.Count + PreFilledCount; i++)
            {
                rng.NextBytes(buffer);
                for (int j = 0; j < buffer.Length; j++)
                    buffer[j] = (byte)alphanumericChars[buffer[j] % alphanumericChars.Length];

                var entry = (i + 1, rng.NextInt64() % 100, new string([.. buffer.Select(x => (char)x)]));
                entries.Add(entry);

                return_code = sqlite3_bind_int64(insertStmt, 1, entry.Item1);
                if (return_code != 0) throw new Exception($"Failed to bind ID: {return_code}");
                return_code = sqlite3_bind_int64(insertStmt, 2, entry.Item2);
                if (return_code != 0) throw new Exception($"Failed to bind ID: {return_code}");
                //return_code = sqlite3_bind_text(insertStmt, 3, entry.Item3, buffer.Length, IntPtr.Zero);
                var utf8Hash = Encoding.UTF8.GetBytes(entry.Item3);
                GCHandle pinnedInsert = GCHandle.Alloc(utf8Hash, GCHandleType.Pinned);
                try
                {
                    sqlite3_bind_text_utf8(insertStmt, 3, pinnedInsert.AddrOfPinnedObject(), utf8Hash.Length, IntPtr.Zero);
                }
                finally
                {
                    pinnedInsert.Free();
                }
                //if (return_code != 0) throw new Exception($"Failed to bind ID: {return_code}");
                sqlite3_step(insertStmt);
                sqlite3_reset(insertStmt);
            }
            Execute("COMMIT;");

            entries = [.. entries.OrderBy(x => Guid.NewGuid()).Take(BenchmarkParams.Count)];


            sqlite3_finalize(insertStmt);

            Execute("PRAGMA optimize;");
            //Execute("ANALYZE;");
            //Execute("VACUUM;");

            //sqlite3_prepare_v2(db, selectSql, -1, out stmt, IntPtr.Zero);
            sqlite3_prepare_v3(db, selectSql, -1, SQLITE_PREPARE_PERSISTENT, out stmt, IntPtr.Zero);

        }

        [Benchmark]
        public void SelectBenchmark()
        {
            var sw_bind = new System.Diagnostics.Stopwatch();
            var sw_step = new System.Diagnostics.Stopwatch();
            var sw_transaction = new System.Diagnostics.Stopwatch();
            var sw_reset = new System.Diagnostics.Stopwatch();

            Execute("PRAGMA synchronous = NORMAL;");
            Execute("PRAGMA temp_store = MEMORY;");
            Execute("PRAGMA journal_mode = WAL;");
            Execute("PRAGMA cache_size = -512000;");
            Execute("PRAGMA threads = 8;");
            Execute("PRAGMA read_uncommitted = true;");
            Execute("mmap_size = 4194304;");
            Execute("PRAGMA shared_cache = true;");

            sw_transaction.Start();
            Execute("BEGIN DEFERRED TRANSACTION;");
            sw_transaction.Stop();
            foreach (var (id, length, hash) in entries)
            {
                sw_bind.Start();
                sqlite3_bind_int64(stmt, 1, length);
                //sqlite3_bind_text(stmt, 2, hash, hash.Length, IntPtr.Zero);
                var utf8Bytes = Encoding.UTF8.GetBytes(hash);
                GCHandle pinned = GCHandle.Alloc(utf8Bytes, GCHandleType.Pinned);
                try
                {
                    sqlite3_bind_text_utf8(stmt, 2, pinned.AddrOfPinnedObject(), utf8Bytes.Length, IntPtr.Zero);
                }
                finally
                {
                    pinned.Free();
                }
                sw_bind.Stop();

                sw_step.Start();
                // bool found = false;
                // var read_id = -1;
                // while (sqlite3_step(stmt) == 100)
                // {
                //     // 100 = SQLITE_ROW, 101 = SQLITE_DONE
                //     // 101 means no more rows, so we stop
                //     // 100 means we have a row to read
                //     // 0 means error

                //     // Read the row
                //     read_id = sqlite3_column_int64(stmt, 0);
                //     found = read_id == id;
                //     break;
                // }
                sqlite3_step(stmt);
                var read_id = sqlite3_column_int64(stmt, 0);
                var found = read_id == id;
                sw_step.Stop();

                if (!found)
                    throw new Exception($"Row not found {id} != {read_id}");

                sw_reset.Start();
                sqlite3_reset(stmt);
                //sqlite3_finalize(stmt);
                //sqlite3_prepare_v2(db, selectSql, -1, out stmt, IntPtr.Zero);

                sw_reset.Stop();
            }
            sw_transaction.Start();
            Execute("COMMIT;");
            sw_transaction.Stop();
#if DEBUG
            Console.WriteLine($"Transaction took {sw_transaction.ElapsedMilliseconds} ms ({(BenchmarkParams.Count / 1000) / sw_transaction.Elapsed.TotalSeconds:0.00} kops/sec)");
            Console.WriteLine($"Binding took {sw_bind.ElapsedMilliseconds} ms ({(BenchmarkParams.Count / 1000) / sw_bind.Elapsed.TotalSeconds:0.00} kops/sec)");
            Console.WriteLine($"Stepping took {sw_step.ElapsedMilliseconds} ms ({(BenchmarkParams.Count / 1000) / sw_step.Elapsed.TotalSeconds:0.00} kops/sec)");
            Console.WriteLine($"Resetting took {sw_reset.ElapsedMilliseconds} ms ({(BenchmarkParams.Count / 1000) / sw_reset.Elapsed.TotalSeconds:0.00} kops/sec)");
#endif
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }

        private static void PackEntry(long length, byte[] hash, byte[] dest)
        {
            Buffer.BlockCopy(hash, 0, dest, 0, 32);
            Buffer.BlockCopy(BitConverter.GetBytes(length), 0, dest, 32, 8);
        }

        private void Execute(string sql)
        {
            sqlite3_prepare_v2(db, sql, -1, out var stmt, IntPtr.Zero);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }

        // --- P/Invoke Declarations ---

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_open(string filename, out IntPtr db);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl, EntryPoint = "sqlite3_bind_text")]
        public static extern int sqlite3_bind_text_utf8(
            IntPtr stmt,
            int index,
            IntPtr value,
            int n,
            IntPtr destructor);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr sqlite3_column_blob(IntPtr stmt, int col);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_column_int64(IntPtr stmt, int col);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_column_text(IntPtr stmt, int col);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_config(int op);
        const int SQLITE_CONFIG_SINGLETHREAD = 1;

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_close(IntPtr db);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        public static extern int sqlite3_prepare_v2(IntPtr db, string sql, int numBytes, out IntPtr stmt, IntPtr pTail);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_prepare_v3(
            IntPtr db,
            string zSql,
            int nByte,
            uint prepFlags,
            out IntPtr ppStmt,
            IntPtr pzTail);

        const uint SQLITE_PREPARE_PERSISTENT = 0x01;


        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_step(IntPtr stmt);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_reset(IntPtr stmt);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_finalize(IntPtr stmt);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_bind_int64(IntPtr stmt, int index, long value);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_bind_blob(IntPtr stmt, int index, IntPtr val, int n, IntPtr destructor);

        [LibraryImport("sqlite3", EntryPoint = "sqlite3_bind_text", StringMarshalling = StringMarshalling.Utf16)]
        [UnmanagedCallConv(CallConvs = new[] { typeof(CallConvStdcall) })]
        [return: MarshalAs(UnmanagedType.I4)]
        internal static partial int sqlite3_bind_text(IntPtr stmt, int index, [MarshalAs(UnmanagedType.LPWStr)] string val, int n, IntPtr destructor);

        public static IEnumerable<BenchmarkParams> ValidParams()
        {
            var counts = new[] { /*100, 1_000, 10_000, 100_000,*/ 1_000_000 };

            foreach (var count in counts)
            {
                //yield return new BenchmarkParams { Count = count, CommitEveryN = 0, UseIndex = false, IndexAfter = false };
                yield return new BenchmarkParams { Count = count, CommitEveryN = 0, UseIndex = true, IndexAfter = false };
            }
        }
    }

}