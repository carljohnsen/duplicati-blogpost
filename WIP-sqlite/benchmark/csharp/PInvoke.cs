
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace sqlite_bench
{

    public partial class PInvokeCAPI : BenchmarkSync
    {
        private IntPtr m_connection;
        private IntPtr m_command_insert;
        private IntPtr m_command_select;
        private IntPtr m_command_xor2_insert;
        private IntPtr m_command_join;
        private IntPtr m_command_blockset_start;
        private IntPtr m_command_blockset_insert_block;
        private IntPtr m_command_blockset_last_row;
        private IntPtr m_command_blockset_entry_insert;
        private IntPtr m_command_blockset_update;

        public PInvokeCAPI() : base() { }

        [GlobalSetup]
        public new void GlobalSetup()
        {
            base.GlobalSetup();
            //int return_code = sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
            //if (return_code != 0)
            //    throw new Exception($"Failed to configure SQLite: {return_code} ({ExtendedErrorMessage()})");

            int return_code = sqlite3_open("benchmark.sqlite", out m_connection);
            if (return_code != SQLITE_OK)
                throw new Exception($"Failed to open database: {return_code} ({ExtendedErrorMessage()})");

            foreach (var pragma in pragmas)
                Execute(pragma);

            Prepare("INSERT INTO Block (ID, Hash, Size) VALUES (?, ?, ?);", out m_command_insert);
            Prepare("SELECT ID FROM Block WHERE Hash = ? AND Size = ?;", out m_command_select);
            Prepare("INSERT OR IGNORE INTO Block (ID, Hash, Size) VALUES (?, ?, ?);", out m_command_xor2_insert);
            Prepare("SELECT Block.ID, Block.Hash, Block.Size FROM Block JOIN BlocksetEntry ON BlocksetEntry.BlockID = Block.ID WHERE BlocksetEntry.BlocksetID = ?;", out m_command_join);
            Prepare("INSERT INTO Block (Hash, Size) VALUES (?, ?);", out m_command_blockset_insert_block);
            Prepare("INSERT INTO Blockset (Length) VALUES (0);", out m_command_blockset_start);
            Prepare("SELECT last_insert_rowid();", out m_command_blockset_last_row);
            Prepare("INSERT INTO BlocksetEntry (BlocksetID, BlockID) VALUES (?, ?);", out m_command_blockset_entry_insert);
            Prepare("UPDATE Blockset SET Length = Length + 1 WHERE ID = ?;", out m_command_blockset_update);
        }

        [GlobalCleanup]
        public new void GlobalCleanup()
        {
            Finalize(m_command_insert);
            Finalize(m_command_select);
            Finalize(m_command_xor2_insert);
            Finalize(m_command_join);
            Finalize(m_command_blockset_insert_block);
            Finalize(m_command_blockset_start);
            Finalize(m_command_blockset_last_row);
            Finalize(m_command_blockset_entry_insert);
            Finalize(m_command_blockset_update);
            CheckIsOk(sqlite3_close(m_connection));
            base.GlobalCleanup();
        }

        [IterationCleanup]
        public override void IterationCleanup()
        {
            Execute("BEGIN TRANSACTION;");
            Execute($"DELETE FROM Block WHERE ID >= {NumEntries}");
            Execute($"DELETE FROM BlocksetEntry WHERE BlocksetID >= {m_blocksets.Count}");
            Execute($"DELETE FROM Blockset WHERE ID >= {m_blocksets.Count}");
            Execute("COMMIT;");
        }

        [Benchmark]
        public override void Insert()
        {
            Execute("BEGIN TRANSACTION;");
            foreach (var entry in EntriesToTest)
            {
                BindInt64(m_command_insert, 1, entry.Id);
                BindText(m_command_insert, 2, entry.Hash);
                BindInt64(m_command_insert, 3, entry.Size);
                ExecuteNonQuery(m_command_insert);
            }
            Execute("COMMIT;");
        }

        [Benchmark]
        public override void Select()
        {
            Execute("BEGIN TRANSACTION;");
            foreach (var entry in EntriesToTest)
            {
                BindText(m_command_select, 1, entry.Hash);
                BindInt64(m_command_select, 2, entry.Size);
                var id = ExecuteScalarInt64(m_command_select);
                if (id != entry.Id)
                    throw new Exception($"Failed to select entry {entry.Id}");
            }
            Execute("COMMIT;");
        }

        [Benchmark]
        public override void Xor1()
        {
            Execute("BEGIN TRANSACTION;");
            foreach (var entry in EntriesToTest)
            {
                BindText(m_command_select, 1, entry.Hash);
                BindInt64(m_command_select, 2, entry.Size);
                int rc = sqlite3_step(m_command_select);
                if (rc == SQLITE_DONE)
                {
                    Reset(m_command_select);
                    BindInt64(m_command_insert, 1, entry.Id);
                    BindText(m_command_insert, 2, entry.Hash);
                    BindInt64(m_command_insert, 3, entry.Size);
                    ExecuteNonQuery(m_command_insert);
                }
                else
                {
                    var id = sqlite3_column_int64(m_command_select, 0);
                    Reset(m_command_select);
                    if (id != entry.Id)
                        throw new Exception($"Failed to insert entry {entry.Id}, found {id}");
                }
            }
            Execute("COMMIT;");
        }

        [Benchmark]
        public override void Xor2()
        {
            Execute("BEGIN TRANSACTION;");
            foreach (var entry in EntriesToTest)
            {
                BindInt64(m_command_xor2_insert, 1, entry.Id);
                BindText(m_command_xor2_insert, 2, entry.Hash);
                BindInt64(m_command_xor2_insert, 3, entry.Size);
                ExecuteNonQuery(m_command_xor2_insert);

                BindText(m_command_select, 1, entry.Hash);
                BindInt64(m_command_select, 2, entry.Size);
                var id = ExecuteScalarInt64(m_command_select);
                if (id != entry.Id)
                    throw new Exception($"Failed to select entry {entry.Id}");
            }
            Execute("COMMIT;");
        }

        [Benchmark]
        public override void Join()
        {
            Execute("BEGIN TRANSACTION;");
            foreach (var (blocksetId, count, size) in BlocksetToTest)
            {
                BindInt64(m_command_join, 1, blocksetId);
                int rc = sqlite3_step(m_command_join);
                long totalSize = 0;
                long totalCount = 0;
                while (rc == SQLITE_ROW)
                {
                    totalCount++;
                    totalSize += sqlite3_column_int64(m_command_join, 2);
                    rc = sqlite3_step(m_command_join);
                }
                if (rc != SQLITE_DONE)
                    throw new Exception($"Failed to execute join: {rc}");
                if (totalCount != count)
                    throw new Exception($"Blockset {blocksetId} expected {count} entries, found {totalCount}");
                if (totalSize != size)
                    throw new Exception($"Blockset {blocksetId} expected {size} total size, found {totalSize}");
                Reset(m_command_join);
            }
            Execute("COMMIT;");
        }

        [Benchmark]
        public override void NewBlockset()
        {
            Execute("BEGIN TRANSACTION;");
            ExecuteNonQuery(m_command_blockset_start);
            var newBlocksetId = ExecuteScalarInt64(m_command_blockset_last_row);
            foreach (var entry in EntriesToTest)
            {
                BindText(m_command_select, 1, entry.Hash);
                BindInt64(m_command_select, 2, entry.Size);
                int rc = sqlite3_step(m_command_select);
                long bid = -1;
                if (rc == SQLITE_DONE)
                {
                    Reset(m_command_select);
                    BindText(m_command_blockset_insert_block, 1, entry.Hash);
                    BindInt64(m_command_blockset_insert_block, 2, entry.Size);
                    ExecuteNonQuery(m_command_blockset_insert_block);
                    bid = ExecuteScalarInt64(m_command_blockset_last_row);
                }
                else
                {
                    bid = sqlite3_column_int64(m_command_select, 0);
                    Reset(m_command_select);
                    if (bid != entry.Id)
                        throw new Exception($"Failed to insert/lookup entry {entry.Id}, found {bid}");
                }

                BindInt64(m_command_blockset_entry_insert, 1, newBlocksetId);
                BindInt64(m_command_blockset_entry_insert, 2, bid);
                ExecuteNonQuery(m_command_blockset_entry_insert);

                BindInt64(m_command_blockset_update, 1, newBlocksetId);
                ExecuteNonQuery(m_command_blockset_update);

                if (m_random.NextDouble() < 0.05)
                {
                    ExecuteNonQuery(m_command_blockset_start);
                    newBlocksetId = ExecuteScalarInt64(m_command_blockset_last_row);
                }
            }

            Execute("COMMIT;");
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
        public static extern IntPtr sqlite3_column_text(IntPtr stmt, int col);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_config(int op);
        const int SQLITE_CONFIG_SINGLETHREAD = 1;

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern int sqlite3_close(IntPtr db);

        [DllImport("sqlite3", CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr sqlite3_errmsg(IntPtr db);

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

        const uint SQLITE_OK = 0;
        const uint SQLITE_PREPARE_PERSISTENT = 0x01;
        const uint SQLITE_ROW = 100;
        const uint SQLITE_DONE = 101;

        private void BindInt64(IntPtr stmt, int index, long value)
        {
            CheckIsOk(sqlite3_bind_int64(stmt, index, value));
        }

        private void BindText(IntPtr stmt, int index, string value)
        {
            var value_utf8 = Encoding.UTF8.GetBytes(value);
            GCHandle pinnedInsert = GCHandle.Alloc(value_utf8, GCHandleType.Pinned);
            try
            {
                CheckIsOk(sqlite3_bind_text_utf8(stmt, index, pinnedInsert.AddrOfPinnedObject(), value_utf8.Length, IntPtr.Zero));
            }
            finally
            {
                pinnedInsert.Free();
            }
        }

        private void CheckIsOk(int return_code)
        {
            if (return_code != SQLITE_OK)
            {
                string? errorMessage = ExtendedErrorMessage();
                throw new Exception($"SQLite error {return_code}: {errorMessage}");
            }
        }

        private void Execute(string sql)
        {
            Prepare(sql, out var stmt);
            int return_code = sqlite3_step(stmt);
            if (!(return_code == SQLITE_DONE || return_code == SQLITE_ROW))
                throw new Exception($"Failed to execute statement {return_code}: {ExtendedErrorMessage()}");
            Finalize(stmt);
        }

        private void ExecuteNonQuery(IntPtr stmt)
        {
            int return_code = sqlite3_step(stmt);
            if (!(return_code == SQLITE_DONE || return_code == SQLITE_ROW))
                throw new Exception($"Failed to execute statement: {return_code}");
            Reset(stmt);
        }

        private long ExecuteScalarInt64(IntPtr stmt)
        {
            int return_code = sqlite3_step(stmt);
            if (return_code != SQLITE_ROW)
                throw new Exception($"Failed to execute statement: {return_code}");
            long result = sqlite3_column_int64(stmt, 0);
            Reset(stmt);
            return result;
        }

        private void Finalize(IntPtr stmt)
        {
            CheckIsOk(sqlite3_finalize(stmt));
        }

        private void Prepare(string sql, out IntPtr stmt)
        {
            CheckIsOk(sqlite3_prepare_v2(m_connection, sql, -1, out stmt, IntPtr.Zero));
        }

        private string ExtendedErrorMessage()
        {
            IntPtr errorMessagePtr = sqlite3_errmsg(m_connection);
            if (errorMessagePtr != IntPtr.Zero)
            {
                string? errorMessage = Marshal.PtrToStringAnsi(errorMessagePtr);
                return errorMessage ?? "NULL error message";
            }
            else
            {
                return "Unknown error";
            }
        }

        private void Reset(IntPtr stmt)
        {
            CheckIsOk(sqlite3_reset(stmt));
        }

    }
}