// A P/Invoke-based benchmark for SQLite SELECT using raw SQLite C API
// Replace or run alongside your existing SQLiteSelectBlobBenchmark to compare

using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace sqlite_bench
{
    [Config(typeof(BenchmarkConfig))]
    [MinColumn, MaxColumn, AllStatisticsColumn]
    public class SQLiteSelectBlobIntPInvokeBenchmark : SQLiteBenchmark
    {
        private IntPtr db;
        private IntPtr stmt;
        private List<(long, long, byte[])> entries = [];
        private byte[] buffer = new byte[32];

        public SQLiteSelectBlobIntPInvokeBenchmark() : base(Backends.DuplicatiSQLite)
        {

        }

        [Params(1_000_000)]
        public int PreFilledCount { get; set; } = 0;

        [ParamsSource(nameof(ValidParams))]
        public BenchmarkParams BenchmarkParams { get; set; } = new BenchmarkParams();

        [GlobalSetup]
        public void GlobalSetup()
        {
            var dbPath = Path.GetFullPath("pinvoke_benchmark.db");
            if (File.Exists(dbPath)) File.Delete(dbPath);

            sqlite3_open(dbPath, out db);
            Execute("PRAGMA cache_size = 1000000;");
            //Execute("PRAGMA journal_mode = OFF;");
            //Execute("PRAGMA synchronous = OFF;");
            Execute("CREATE TABLE Blockset (id INTEGER PRIMARY KEY, firsthash INTEGER NOT NULL, fullhashlength BLOB NOT NULL) STRICT, WITHOUT ROWID;");

            var insertSql = "INSERT INTO Blockset (id, firsthash, fullhashlength) VALUES (?, ?, ?);";
            sqlite3_prepare_v2(db, insertSql, -1, out var insertStmt, IntPtr.Zero);

            var rng = new Random(42);

            Execute("BEGIN TRANSACTION;");
            GCHandle handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            for (long i = 0; i < BenchmarkParams.Count; i++)
            {
                var hash = new byte[32];
                rng.NextBytes(hash);
                var length = rng.NextInt64() % 100;
                entries.Add((i, length, hash));
                var firsthash = BitConverter.ToInt64(hash, 0);
                Array.Copy(hash, 8, buffer, 0, 24);
                Array.Copy(BitConverter.GetBytes(length), 0, buffer, 24, 8);

                sqlite3_bind_int64(insertStmt, 1, i);
                sqlite3_bind_int64(insertStmt, 2, firsthash);
                sqlite3_bind_blob(insertStmt, 3, handle.AddrOfPinnedObject(), buffer.Length, new IntPtr(-1));
                sqlite3_step(insertStmt);
                sqlite3_reset(insertStmt);
            }
            handle.Free();
            Execute("COMMIT;");

            entries = [.. entries.OrderBy(x => Guid.NewGuid())];

            Execute("CREATE INDEX idx_fullhashlength ON Blockset(firsthash, fullhashlength);");

            sqlite3_finalize(insertStmt);

            Execute("PRAGMA optimize;");
            Execute("ANALYZE;");
            Execute("VACUUM;");

            var selectSql = "SELECT id, fullhashlength FROM Blockset WHERE firsthash = ?;";
            sqlite3_prepare_v2(db, selectSql, -1, out stmt, IntPtr.Zero);
        }

        [Benchmark]
        public void SelectBenchmark()
        {
            var sw = new System.Diagnostics.Stopwatch();
            Execute("BEGIN TRANSACTION;");
            foreach (var (id, length, hash) in entries)
            {
                sqlite3_bind_int64(stmt, 1, BitConverter.ToInt64(hash, 0));

                bool found = false;
                var read_id = -1;
                sw.Start();
                while (sqlite3_step(stmt) == 100)
                {
                    sw.Stop();
                    // Read the row
                    unsafe
                    {
                        read_id = sqlite3_column_int64(stmt, 0);
                        byte* rawPtr = (byte*)sqlite3_column_blob(stmt, 1).ToPointer();
                        Span<byte> span = new(rawPtr, 32);
                        var length2 = BitConverter.ToInt64(span[24..]);
                        bool matches = length == length2;
                        if (matches)
                            for (int i = 0; i < 24; i++)
                                matches &= hash[i + 8] == span[i];

                        if (matches)
                        {
                            found = true;
                            break;
                        }
                    }
                }
                sw.Stop();

                if (!found || read_id != id)
                    throw new Exception($"Row not found {id} != {read_id}");

                sqlite3_reset(stmt);
            }
            Execute("COMMIT;");
#if DEBUG
            Console.WriteLine($"Stepping took {sw.ElapsedMilliseconds} ms ({(BenchmarkParams.Count / 1000) / sw.Elapsed.TotalSeconds:0.00} kops/sec)");
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

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr sqlite3_column_blob(IntPtr stmt, int col);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_column_int64(IntPtr stmt, int col);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_close(IntPtr db);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_prepare_v2(IntPtr db, string sql, int numBytes, out IntPtr stmt, IntPtr pTail);

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