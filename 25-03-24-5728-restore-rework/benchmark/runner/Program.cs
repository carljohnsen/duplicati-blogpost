using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.Diagnostics;
using Duplicati.Library.Common.IO;
using Duplicati.Library.Main;

namespace Runner
{
    public class Program
    {

        private sealed record Config(
            int AutoTuningSteps,
            bool Cleanup,
            string DataGenerator,
            string Hostname,
            int Iterations,
            Operation Operation,
            string GenerateOutput,
            string BackupOutput,
            string RestoreOutput,
            string Output,
            Size Size,
            string Tuning,
            Legacy VersionToTest
        );

        private enum Legacy
        {
            Both,
            New,
            Legacy,
            PreNewBackend
        }

        private enum Operation
        {
            DatasetOnly,
            Filesizes,
            Regular,
            Sparsity,
            Tuning
        }

        private enum Size
        {
            All,
            Small,
            Medium,
            Large,
            Huge
        }

        public static void BackupData(string source, string destination, Dictionary<string, string> duplicati_options)
        {
            Console.WriteLine($"Backing up {source} to {destination}");
#if DEBUG
            using var console_sink = new Duplicati.CommandLine.ConsoleOutput(Console.Out, duplicati_options);
#else
            IMessageSink? console_sink = null;
#endif

            if (!Directory.Exists(destination))
                Directory.CreateDirectory(destination);

            var options = new Dictionary<string, string>(duplicati_options);

            using var c = new Controller($"file://{destination}", options, console_sink);
            var results = c.Backup([source]);
            if (results.Errors.Any())
                throw new Exception($"Backup failed with errors: {string.Join(Environment.NewLine, results.Errors)}");
        }

        /// <summary>
        /// Create the default directories for the benchmark. The order is generate, backup, restore, times.
        /// </summary>
        /// <param name="config">The configuration used to determine the directories.</param>
        /// <returns>Four strings representing the directories.</returns>
        private static (string, string, string, string) DefaultDirs(Config config)
        {
            var generate_data_dir = Path.Combine(config.GenerateOutput, "data");
            var backup_data_dir = Path.Combine(config.BackupOutput, "data");
            var restore_data_dir = Path.Combine(config.RestoreOutput, "data");
            var times_dir = Path.Combine(config.Output, "times");

            if (!Directory.Exists(generate_data_dir))
                Directory.CreateDirectory(generate_data_dir);
            if (!Directory.Exists(backup_data_dir))
                Directory.CreateDirectory(backup_data_dir);
            if (!Directory.Exists(restore_data_dir))
                Directory.CreateDirectory(restore_data_dir);
            if (!Directory.Exists(times_dir))
                Directory.CreateDirectory(times_dir);
            return (generate_data_dir, backup_data_dir, restore_data_dir, times_dir);
        }

        private static Dictionary<string, string> DefaultOptions()
        {
            return new Dictionary<string, string>
            {
                ["passphrase"] = "password",
                ["overwrite"] = "true"
            };
        }

        public static void DeleteAll(string directory)
        {
            if (!Directory.Exists(directory))
                return;

            foreach (var file in Directory.GetFiles(directory, "*", SearchOption.AllDirectories))
                SystemIO.IO_OS.FileDelete(file);

            Directory.Delete(directory, true);
        }

        public static void DeleteBackup(string directory, Dictionary<string, string> duplicati_options)
        {
            Console.WriteLine($"Deleting backup {directory}");
#if DEBUG
            using var console_sink = new Duplicati.CommandLine.ConsoleOutput(Console.Out, duplicati_options);
#else
            IMessageSink? console_sink = null;
#endif
            var packed_options = duplicati_options;
            packed_options["allow-full-removal"] = "true";
            packed_options["version"] = "0";
            using var c = new Controller($"file://{directory}", duplicati_options, console_sink);
            var results = c.Delete();
            if (results.Errors.Any())
                throw new Exception($"Delete failed with errors: {string.Join(Environment.NewLine, results.Errors)}");
        }

        public static void DeleteSome(IEnumerable<string> files)
        {
            foreach (var file in files)
                SystemIO.IO_OS.FileDelete(file);
        }

        private static string GetDatagen(Config config)
        {
            var datagen = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) ? $"{config.DataGenerator}.exe" : config.DataGenerator;
            if (!SystemIO.IO_OS.FileExists(datagen))
            {
                throw new FileNotFoundException($"Data generator not found at {datagen}");
            }
            return datagen;
        }

        public static async Task<int> Main(string[] args)
        {
            var root_cmd = new RootCommand(@"Run the benchmark of the reworked restore flow.")
            {
                new Option<int>(aliases: ["--auto-tuning-steps"], description: "Number of un-improving steps to perform in the auto tuning from finding a minima. If set to 0, only one run will be performed.", getDefaultValue: () => 3),
                new Option<bool>(aliases: ["--cleanup"], description: "Delete the generated and backup data after the benchmark", getDefaultValue: () => false),
                new Option<string>(aliases: ["--data-generator"], description: "Path to the data generator executable", getDefaultValue: () => "../data_repos/duplicati/Tools/TestDataGenerator/bin/Release/net8.0/TestDataGenerator") { Arity = ArgumentArity.ExactlyOne },
                new Option<string>(aliases: ["--hostname"], description: "Hostname of the machine running the benchmark", getDefaultValue: () => System.Net.Dns.GetHostName()) { Arity = ArgumentArity.ExactlyOne },
                new Option<int>(aliases: ["--iterations", "-i"], description: "Number of iterations", getDefaultValue: () => 1),
                new Option<Operation>(aliases: ["--operation"], description: "Operation to perform. Should be one of: datasetonly, filesizes, regular, sparsity, tuning", getDefaultValue: () => Operation.Regular) { Arity = ArgumentArity.ExactlyOne },
                new Option<string>(aliases: ["--output", "-o"], description: "Output directory to hold the the results", getDefaultValue: () => "..") { Arity = ArgumentArity.ExactlyOne },
                new Option<string>(aliases: ["--backup-output", "-bo"], description: "Output directory to hold the backup data", getDefaultValue: () => "..") { Arity = ArgumentArity.ExactlyOne },
                new Option<string>(aliases: ["--generate-output", "-go"], description: "Output directory to hold the generated data", getDefaultValue: () => "..") { Arity = ArgumentArity.ExactlyOne },
                new Option<string>(aliases: ["--restore-output", "-ro"], description: "Output directory to hold the restored data", getDefaultValue: () => "..") { Arity = ArgumentArity.ExactlyOne },
                new Option<Size>(aliases: ["--size", "-s"], description: "Size of the test data. Should one of: all, small, medium, large, huge. All includes small, medium, and large.", getDefaultValue: () => Size.Small) { Arity = ArgumentArity.ExactlyOne },
                new Option<string>(aliases: ["--tuning"], description: "The concurrency parameters to test. Should be a comma separated string: <FileProcessors>,<VolumeDownloaders>,<VolumeDecryptors>,<VolumeDecompressors>", getDefaultValue: () => "") { Arity = ArgumentArity.ExactlyOne },
                new Option<Legacy>(aliases: ["--version-to-test", "-vtt"], description: "Version of the restore flow to test. Should be one of: both, new, legacy, prenewbackend. Both runs legacy first, followed by new.", getDefaultValue: () => Legacy.Both){ Arity = ArgumentArity.ExactlyOne }
            };

            root_cmd.Handler = CommandHandler.Create(Run);

            return await root_cmd.InvokeAsync(args);
        }

        private static async Task<string> GenerateData(string datagen, Size size, string output_dir, long? sparsity = null, long? file_size_mb = null, string prefix = "")
        {
            string size_str;
            long max_file_size, max_total_size, file_count, sparse_factor;
            switch (size)
            {
                case Size.Small:
                    size_str = "small";
                    max_file_size = (file_size_mb ?? 10) * 1048576;
                    max_total_size = 1073741824; // 1GB
                    file_count = 1000;
                    sparse_factor = sparsity ?? 20;
                    break;
                case Size.Medium:
                    size_str = "medium";
                    max_file_size = (file_size_mb ?? 10) * 10485760;
                    max_total_size = 10737418240; // 10GB
                    file_count = 10000;
                    sparse_factor = sparsity ?? 30;
                    break;
                case Size.Large:
                    size_str = "large";
                    max_file_size = (file_size_mb ?? 10) * 10485760;
                    max_total_size = 107374182400; // 100GB
                    file_count = 1000000;
                    sparse_factor = sparsity ?? 40;
                    break;
                case Size.Huge:
                    size_str = "huge";
                    max_file_size = 1024 * 1024 * 1024; // 1GB
                    max_total_size = 10L * 1024L * 1024L * 1024L * 1024L; // 10TB
                    file_count = 10000000;
                    sparse_factor = sparsity ?? 25;
                    break;
                default:
                    throw new ArgumentException($"Invalid size provided: {size}");
            }

            string data_dir = Path.Combine(output_dir, prefix == "" ? size_str : $"{prefix}_{size_str}");

            if (Directory.Exists(data_dir))
                return data_dir;
            else
                Directory.CreateDirectory(data_dir);

            Console.WriteLine($"Generating data for size {size_str}");

            using var process = new System.Diagnostics.Process
            {
                StartInfo = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = datagen,
                    Arguments = $"create {data_dir} --max-file-size {max_file_size} --max-total-size {max_total_size} --file-count {file_count} --sparse-factor {sparse_factor}",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };

            process.OutputDataReceived += (sender, e) => Console.WriteLine(e.Data);
            process.ErrorDataReceived += (sender, e) => Console.WriteLine(e.Data);

            process.Start();
            process.BeginOutputReadLine();
            process.BeginErrorReadLine();
            await process.WaitForExitAsync();

            if (process.ExitCode != 0)
            {
                throw new Exception($"Data generation failed with exit code {process.ExitCode}");
            }

            return data_dir;
        }

        private static void ModifySome(IEnumerable<string> files)
        {
            foreach (var file in files)
            {
                using var stream = SystemIO.IO_OS.FileOpenReadWrite(file);
                stream.Seek(0, SeekOrigin.Begin);
                byte[] buffer = new byte[1];
                stream.Read(buffer, 0, 1);
                buffer[0] = (byte)~buffer[0];
                stream.Seek(0, SeekOrigin.Begin);
                stream.Write(buffer, 0, 1);
            }
        }

        private static string[] ParseLegaciesToRun(Legacy legacy)
        {
            return legacy switch
            {
                Legacy.Both => ["false", "true"],
                Legacy.New => ["false"],
                Legacy.Legacy => ["true"],
                Legacy.PreNewBackend => ["prenewbackend"],
                _ => throw new ArgumentException($"Invalid version to test provided: {legacy}")
            };
        }

        private static void RestoreData(string source, string destination, Dictionary<string, string> duplicati_options, string use_legacy)
        {
            var packed_options = new Dictionary<string, string>(duplicati_options)
            {
                ["restore-legacy"] = use_legacy,
                ["restore-path"] = destination
            };
#if DEBUG
            using var console_sink = new Duplicati.CommandLine.ConsoleOutput(Console.Out, packed_options);
#else
            IMessageSink? console_sink = null;
#endif
            using var c = new Controller($"file://{source}", packed_options, console_sink);
            var results = c.Restore(["*"]);
            if (results.Errors.Any())
                throw new Exception($"Restore failed with errors: {string.Join(Environment.NewLine, results.Errors)}");
        }

        private static async Task<int> Run(Config config)
        {
            switch (config.Operation)
            {
                case Operation.DatasetOnly:
                    return await RunDatasetOnly(config);
                case Operation.Filesizes:
                    return await RunFilesizes(config);
                case Operation.Regular:
                    return await RunRegular(config);
                case Operation.Sparsity:
                    return await RunSparsity(config);
                case Operation.Tuning:
                    return await RunTuning(config);
                default:
                    throw new ArgumentException($"Invalid operation provided: {config.Operation}");
            }
        }

        private static async Task<int> RunDatasetOnly(Config config)
        {
            var sw = new Stopwatch();
            var (generate_data_dir, _, _, times_dir) = DefaultDirs(config);

            Size[] sizes = config.Size == Size.All ? [Size.Small, Size.Medium, Size.Large] : [config.Size];

            var datagen = GetDatagen(config);

            foreach (var size in sizes)
            {
                var size_str = config.Size.ToString().ToLower();
                Console.WriteLine($"Generating data for size {size_str}");
                sw.Restart();
                var generated = await GenerateData(datagen, size, generate_data_dir, 10);
                sw.Stop();
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_generate_sparse.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);
            }
            return 0;
        }

        private static async Task<int> RunFilesizes(Config config)
        {
            string[] legacies = ParseLegaciesToRun(config.VersionToTest);
            var sw = new Stopwatch();
            var duplicati_options = DefaultOptions();
            var (_, data_dir, _, times_dir) = DefaultDirs(config);

            var datagen = GetDatagen(config);

            var size_str = config.Size.ToString().ToLower();
            string backup_dir = Path.Combine(data_dir, $"filesize_backup_{size_str}");
            string restore_dir = Path.Combine(data_dir, $"filesize_restore_{size_str}");

            for (int j = 1; j <= 10; j++)
            {
                Console.WriteLine(@$"* Running benchmark for size {config.Size} with file sizes {j * 10} MB");
                sw.Restart();
                var generated = await GenerateData(datagen, config.Size, data_dir, file_size_mb: j * 10, prefix: "filesize");
                sw.Stop();
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_generate_size.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);

                sw.Restart();
                BackupData(generated, backup_dir, duplicati_options);
                sw.Stop();

                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_backup_size.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);

                // Delete the generated data, as it's now backed up
                DeleteAll(generated);

                foreach (var use_legacy in new string[] { "true", "false" })
                {
                    var legacy_str = use_legacy == "true" ? "Legacy" : "New";
                    using var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_size_{use_legacy}.csv"), true);

                    Console.Write($"{legacy_str} restore: 0/{config.Iterations}");
                    for (int i = 0; i < config.Iterations; i++)
                    {
                        DeleteAll(restore_dir);

                        sw.Restart();
                        RestoreData(backup_dir, restore_dir, duplicati_options, use_legacy);
                        sw.Stop();
                        if (i > 0)
                            writer.Write(";");
                        writer.Write(sw.ElapsedMilliseconds);
                        Console.Write($"\r{legacy_str} restore: {i + 1}/{config.Iterations}");
                    }
                    writer.WriteLine();
                    Console.WriteLine();
                }

                // Delete the restored data, as it's no longer needed
                DeleteAll(restore_dir);
                DeleteBackup(backup_dir, duplicati_options);
                DeleteAll(backup_dir);
            }

            DeleteAll(data_dir);

            return 0;
        }

        private static async Task<int> RunRegular(Config config)
        {
            Size[] sizes = config.Size == Size.All ? [Size.Small, Size.Medium, Size.Large] : [config.Size];
            var legacies = ParseLegaciesToRun(config.VersionToTest);
            var sw = new Stopwatch();
            var duplicati_options = DefaultOptions();
            duplicati_options["restore-cache-max"] = "64gb";
            duplicati_options["restore-channel-buffer-size"] = "4096";
            if (config.Tuning != "")
            {
                var tuning = config.Tuning.Split(',');
                duplicati_options["restore-file-processors"] = tuning[0];
                duplicati_options["restore-volume-downloaders"] = tuning[1];
                duplicati_options["restore-volume-decryptors"] = tuning[2];
                duplicati_options["restore-volume-decompressors"] = tuning[3];
            }
            var (generate_data_dir, backup_data_dir, restore_data_dir, times_dir) = DefaultDirs(config);

            var datagen = GetDatagen(config);

            foreach (var size in sizes)
            {
                Console.WriteLine(@$"*
* Running benchmark for size {size}
*");
                var size_str = size.ToString().ToLower();
                string backup_dir = Path.Combine(backup_data_dir, $"backup_{size_str}");
                string restore_dir = Path.Combine(restore_data_dir, $"restore_{size_str}");
                if (!Directory.Exists(backup_dir))
                {

                    sw.Restart();
                    var generated = await GenerateData(datagen, size, generate_data_dir);
                    sw.Stop();
                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_generate.csv"), true))
                        writer.WriteLine(sw.ElapsedMilliseconds);

                    sw.Restart();
                    if (!Directory.Exists(backup_dir))
                        BackupData(generated, backup_dir, duplicati_options);
                    sw.Stop();
                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_backup.csv"), true))
                        writer.WriteLine(sw.ElapsedMilliseconds);
                    // Delete the generated data, as it's now backed up
                    if (config.Cleanup)
                        DeleteAll(generated);
                }


                foreach (var use_legacy in legacies)
                {
                    Console.WriteLine($"Legacy restore: {use_legacy}");
                    // Warmup is handled in plotting.

                    //
                    // Perform the full restore
                    //
                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_full_{use_legacy}.csv")))
                    {
                        Console.Write($"Full restore: 0/{config.Iterations}");
                        for (int i = 0; i < config.Iterations; i++)
                        {
                            DeleteAll(restore_dir);

                            sw.Restart();
                            RestoreData(backup_dir, restore_dir, duplicati_options, use_legacy);
                            sw.Stop();
                            writer.WriteLine(sw.ElapsedMilliseconds);
                            Console.Write($"\rFull restore: {i + 1}/{config.Iterations}");
                        }
                        Console.WriteLine();
                    }

                    //
                    // Perform the partial restore
                    //
                    // Get all of the restored files
                    var files = Directory.GetFiles(restore_dir, "*", SearchOption.AllDirectories);
                    // Take 50 % random files
                    var random_files = files.OrderBy(x => Guid.NewGuid()).Take(files.Length / 2).ToArray();
                    // Half of that will be deleted
                    var to_delete = random_files.Take(random_files.Length / 2);
                    // The other half will be modified
                    var to_modify = random_files.Skip(random_files.Length / 2);

                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_partial_{use_legacy}.csv")))
                    {
                        Console.Write($"Partial restore: 0/{config.Iterations}");
                        for (int i = 0; i < config.Iterations; i++)
                        {
                            DeleteSome(to_delete);
                            ModifySome(to_modify);

                            sw.Restart();
                            RestoreData(backup_dir, restore_dir, duplicati_options, use_legacy);
                            sw.Stop();
                            writer.WriteLine(sw.ElapsedMilliseconds);
                            Console.Write($"\rPartial restore: {i + 1}/{config.Iterations}");
                        }
                        Console.WriteLine();
                    }

                    //
                    // Perform the no restore
                    //
                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_no_{use_legacy}.csv")))
                    {
                        Console.Write($"No restore: 0/{config.Iterations}");
                        duplicati_options["skip-metadata"] = "true";
                        for (int i = 0; i < config.Iterations; i++)
                        {
                            sw.Restart();
                            RestoreData(backup_dir, restore_dir, duplicati_options, use_legacy);
                            sw.Stop();
                            writer.WriteLine(sw.ElapsedMilliseconds);
                            Console.Write($"\rNo restore: {i + 1}/{config.Iterations}");
                        }
                        Console.WriteLine();
                        duplicati_options.Remove("skip-metadata");
                    }

                    //
                    // Perform the metadata only restore
                    //
                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_metadata_{use_legacy}.csv")))
                    {
                        Console.Write($"Metadata only restore: 0/{config.Iterations}");
                        for (int i = 0; i < config.Iterations; i++)
                        {
                            TouchAll(restore_dir);

                            sw.Restart();
                            RestoreData(backup_dir, restore_dir, duplicati_options, use_legacy);
                            sw.Stop();
                            writer.WriteLine(sw.ElapsedMilliseconds);
                            Console.Write($"\rMetadata only restore: {i + 1}/{config.Iterations}");
                        }
                        Console.WriteLine();
                    }
                }

                if (config.Cleanup)
                {
                    // Delete the restored data, as it's no longer needed
                    DeleteAll(restore_dir);
                    DeleteBackup(backup_dir, duplicati_options);
                    DeleteAll(backup_dir);
                }
            }

            if (config.Cleanup)
            {
                DeleteAll(generate_data_dir);
                DeleteAll(backup_data_dir);
                DeleteAll(restore_data_dir);
            }

            return 0;
        }

        private static async Task<int> RunSparsity(Config config)
        {
            var legacies = ParseLegaciesToRun(config.VersionToTest);
            var sw = new Stopwatch();
            var duplicati_options = DefaultOptions();
            var (_, data_dir, _, times_dir) = DefaultDirs(config);

            var datagen = GetDatagen(config);
            var size_str = config.Size.ToString().ToLower();
            string backup_dir = Path.Combine(data_dir, $"sparsity_backup_{size_str}");
            string restore_dir = Path.Combine(data_dir, $"sparsity_restore_{size_str}");

            for (int j = 0; j < 10; j++)
            {
                Console.WriteLine(@$"*
* Running benchmark for size {config.Size} with sparsity {j * 10}
*");
                sw.Restart();
                var generated = await GenerateData(datagen, config.Size, data_dir, sparsity: j * 10, prefix: "sparsity");
                sw.Stop();
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_generate_sparse.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);

                sw.Restart();
                BackupData(generated, backup_dir, duplicati_options);
                sw.Stop();
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_backup_sparse.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);

                // Delete the generated data, as it's now backed up
                DeleteAll(generated);

                foreach (var use_legacy in legacies)
                {
                    Console.WriteLine($"Legacy restore: {use_legacy}");
                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_sparse_{use_legacy}.csv"), true))
                    {
                        Console.Write($"Full restore: 0/{config.Iterations}");
                        for (int i = 0; i < config.Iterations; i++)
                        {
                            DeleteAll(restore_dir);

                            sw.Restart();
                            RestoreData(backup_dir, restore_dir, duplicati_options, use_legacy);
                            sw.Stop();
                            if (i > 0)
                                writer.Write(";");
                            writer.Write(sw.ElapsedMilliseconds);
                            Console.Write($"\rFull restore: {i + 1}/{config.Iterations}");
                        }
                        writer.WriteLine();
                        Console.WriteLine();
                    }
                }

                // Delete the restored data, as it's no longer needed
                DeleteAll(restore_dir);
                DeleteBackup(backup_dir, duplicati_options);
                DeleteAll(backup_dir);
            }

            return 0;
        }

        private static async Task<int> RunTuning(Config config)
        {
            var legacies = ParseLegaciesToRun(config.VersionToTest);
            if (legacies.Length != 1)
                throw new ArgumentException("Tuning only supports a single legacy version to test");
            var legacy_str = legacies[0];
            var sw = new Stopwatch();
            var duplicati_options = DefaultOptions();
            var (_, data_dir, _, times_dir) = DefaultDirs(config);
            var datagen = GetDatagen(config);
            var size_str = config.Size.ToString().ToLower();
            if (config.Size == Size.All)
                throw new ArgumentException("Tuning does not support running all sizes");
            string[] tuning = config.Tuning == "" ? ["1", "1", "1", "1"] : config.Tuning.Split(',');
            if (tuning.Length != 4)
                throw new ArgumentException("Invalid tuning parameters provided. Should be a comma separated string: <FileProcessors>,<VolumeDownloaders>,<VolumeDecryptors>,<VolumeDecompressors>");



            string backup_dir = Path.Combine(data_dir, $"backup_{size_str}");
            string restore_dir = Path.Combine(data_dir, $"restore_{size_str}");

            if (!Directory.Exists(backup_dir))
            {
                sw.Start();
                var generated = await GenerateData(datagen, config.Size, data_dir);
                sw.Stop();
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_generate.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);

                sw.Restart();
                BackupData(generated, backup_dir, duplicati_options);
                sw.Stop();
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_backup.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);

                // Delete the generated data, as it's now backed up
                if (config.Cleanup)
                    DeleteAll(generated);
            }

            int steps = 0;
            double minima = double.MaxValue;
            string best_tune = string.Join(",", tuning);

            var timing_csv = Path.Combine(times_dir, $"{config.Hostname}_{size_str}_tuning.csv");
            var write_header = !SystemIO.IO_OS.FileExists(timing_csv);

            using (var writer = new StreamWriter(timing_csv, true))
            {
                if (write_header)
                    writer.WriteLine("n_FileProcessors;n_VolumeDownloaders;n_VolumeDecryptors;n_VolumeDecompressors;read_FileProcessor;work_FileProcessor;read_VolumeDownloader;work_VolumeDownloader;read_VolumeDecryptor;work_VolumeDecryptor;read_VolumeDecompressor;work_VolumeDecompressor;wall_clock_setup;wall_clock_restore");

                do
                {
                    var log_file = Path.Combine(data_dir, $"profiling_{string.Join("_", tuning)}.log");
                    if (SystemIO.IO_OS.FileExists(log_file))
                        SystemIO.IO_OS.FileDelete(log_file);

                    duplicati_options["restore-channel-buffer-size"] = "4096";
                    duplicati_options["restore-cache-max"] = "64gb";
                    duplicati_options["internal-profiling"] = "true";
                    if (config.VersionToTest != Legacy.Legacy)
                        duplicati_options["log-file-log-filter"] = "+[.*InternalTimings.*]:+[.*RestoreNetwork.*]:-[.*]";
                    duplicati_options["log-file-log-level"] = "Profiling";
                    duplicati_options["log-file"] = log_file;
                    duplicati_options["restore-file-processors"] = tuning[0];
                    duplicati_options["restore-volume-downloaders"] = tuning[1];
                    duplicati_options["restore-volume-decryptors"] = tuning[2];
                    duplicati_options["restore-volume-decompressors"] = tuning[3];

                    Dictionary<string, (List<long>, List<long>)> timings = new()
                    {
                        ["FileProcessor"] = ([], []),
                        ["VolumeDownloader"] = ([], []),
                        ["VolumeDecryptor"] = ([], []),
                        ["VolumeDecompressor"] = ([], [])
                    };
                    List<long> wall_clock = [];

                    Console.WriteLine($"Running restore with tuning parameters: {string.Join(", ", tuning)}");



                    for (int i = 0; i < config.Iterations; i++)
                    {
                        DeleteAll(restore_dir);

                        Console.Write($"\r{i}/{config.Iterations}");
                        RestoreData(backup_dir, restore_dir, duplicati_options, legacy_str);

                        if (config.VersionToTest == Legacy.Legacy)
                        {
                            Console.WriteLine();
                            Console.WriteLine("Skipping log parsing for legacy version - no profiling data available");
                            return 0;
                        }

                        using var reader = new StreamReader(log_file);
                        string? line;
                        if (reader != null)
                            while ((line = reader.ReadLine()) != null)
                            {
                                if (line.Contains("took"))
                                {
                                    // Parse the total ms from the last token which is in 0:00:00:00.000 format
                                    var time_str = line.Split(' ')[^1];
                                    var format = @"d\:hh\:mm\:ss\.fff";
                                    var time = TimeSpan.ParseExact(time_str, format, System.Globalization.CultureInfo.InvariantCulture);
                                    wall_clock.Add((long)time.TotalMilliseconds);
                                    continue;
                                }

                                // Remove the first 78 characters, as they are the timestamp and common
                                line = line[78..];
                                int idx = line.IndexOf('-');
                                var process_name = line[..idx];
                                var tokens = line[idx..].Split(' ');
                                var times = tokens
                                    .Where(x => x.Contains("ms") || x.Contains("ms,"))
                                    .Select(x => int.Parse(new string([.. x.Where(y => char.IsDigit(y))])))
                                    .ToArray();

                                var (times_list, read_list) = timings.GetValueOrDefault(process_name, ([], []));
                                var found = true;

                                switch (process_name)
                                {
                                    case "FileProcessor":
                                        var timesum = times[1..3].Concat(times[5..8]).Concat(times[9..]).Sum();
                                        times_list.Add(timesum);
                                        read_list.Add(times[4]);
                                        break;
                                    case "VolumeDownloader":
                                        times_list.Add(times[2]);
                                        read_list.Add(times[0]);
                                        break;
                                    case "VolumeDecryptor":
                                        times_list.Add(times[1]);
                                        read_list.Add(times[0]);
                                        break;
                                    case "VolumeDecompressor":
                                        times_list.Add(times[2..].Sum());
                                        read_list.Add(times[0]);
                                        break;
                                    default:
                                        found = false; // Ignore
                                        break;
                                }

                                if (found)
                                    timings[process_name] = (times_list, read_list);
                            }
                    }

                    Console.WriteLine($"\r{config.Iterations}/{config.Iterations}");

                    foreach (string process in new string[] { "FileProcessor", "VolumeDownloader", "VolumeDecryptor", "VolumeDecompressor" })
                    {
                        var (times, reads) = timings[process];
                        var times_str = times.Average();//string.Join(";", times);
                        var reads_str = reads.Average();//string.Join(";", reads);
                        Console.WriteLine($"{process}: {reads_str:0.00} - {times_str:0.00}");
                    }

                    var total_work = timings.Select(x => x.Value.Item1.Average()).Sum();
                    var total_read = timings.Select(x => x.Value.Item2.Average()).Average();
                    Console.WriteLine($"Total work: {total_read:0.00} - {total_work:0.00}");

                    // Take every other value, as the first is setup and the second is restore
                    var wall_clock_setup = wall_clock.Where((x, i) => i % 2 == 0).Average();
                    var wall_clock_restore = wall_clock.Where((x, i) => i % 2 == 1).Average();
                    var wall_clock_total = wall_clock_setup + wall_clock_restore;
                    Console.WriteLine($"[Wall clock] - setup {wall_clock_setup:0.00}, restore {wall_clock_restore:0.00}, total {wall_clock_total:0.00}");

                    Console.WriteLine($"Approximate hidden overhead: {wall_clock_total - total_work:0.00}");

                    writer.WriteLine($"{string.Join(";", tuning)};{timings["FileProcessor"].Item2.Average()};{timings["FileProcessor"].Item1.Average()};{timings["VolumeDownloader"].Item2.Average()};{timings["VolumeDownloader"].Item1.Average()};{timings["VolumeDecryptor"].Item2.Average()};{timings["VolumeDecryptor"].Item1.Average()};{timings["VolumeDecompressor"].Item2.Average()};{timings["VolumeDecompressor"].Item1.Average()};{wall_clock_setup:0.00};{wall_clock_restore:0.00}");

                    if (wall_clock_total < minima)
                    {
                        minima = wall_clock_total;
                        best_tune = string.Join(",", tuning);
                        steps = 0;
                    }
                    else
                        steps++;

                    if (total_read > total_work)
                    {
                        tuning[0] = (int.Parse(tuning[0]) * 2).ToString();
                    }
                    else
                    {
                        var max_idx = timings.Select((x, i) => (x.Value.Item1.Average(), i)).Max().i;
                        tuning[max_idx] = (int.Parse(tuning[max_idx]) * 2).ToString();
                    }
                } while (config.AutoTuningSteps > 0 && steps < config.AutoTuningSteps && tuning.All(x => int.Parse(x) < Environment.ProcessorCount * 2));
            }

            Console.WriteLine($"Best tuning found with {minima:0.00} ms: {best_tune}");

            if (config.Cleanup)
            {
                DeleteAll(restore_dir);
                DeleteBackup(backup_dir, duplicati_options);
                DeleteAll(backup_dir);
                DeleteAll(data_dir);
            }

            return 0;
        }

        private static void TouchAll(string directory)
        {
            foreach (var file in Directory.GetFiles(directory, "*", SearchOption.AllDirectories))
                SystemIO.IO_OS.FileSetLastWriteTimeUtc(file, System.DateTime.UtcNow);
        }
    }
}
