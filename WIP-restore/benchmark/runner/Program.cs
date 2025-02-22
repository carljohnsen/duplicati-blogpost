using System;
using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.Diagnostics;
using System.Threading.Tasks;
using Duplicati;
using Duplicati.Library.Main;
using Google.Type;

namespace Runner
{
    public class Program
    {

        private sealed record Config(
            string DataGenerator,
            string Hostname,
            int Iterations,
            Operation Operation,
            string Output,
            Size Size
        );

        private enum Operation
        {
            DatasetOnly,
            Filesizes,
            Regular,
            Sparsity
        }

        private enum Size
        {
            All,
            Small,
            Medium,
            Large
        }

        public static void BackupData(string source, string destination, Dictionary<string, string> duplicati_options)
        {
            Console.WriteLine($"Backing up {source} to {destination}");
#if DEBUG
            using var console_sink = new Duplicati.CommandLine.ConsoleOutput(Console.Out, duplicati_options);
#else
            IMessageSink console_sink = null;
#endif

            if (!Directory.Exists(destination))
                Directory.CreateDirectory(destination);

            using var c = new Controller($"file://{destination}", duplicati_options, console_sink);
            var results = c.Backup([source]);
            if (results.Errors.Any())
                throw new Exception($"Backup failed with errors: {string.Join(Environment.NewLine, results.Errors)}");
        }

        public static void DeleteAll(string directory)
        {
            if (!Directory.Exists(directory))
                return;

            foreach (var file in Directory.GetFiles(directory, "*", SearchOption.AllDirectories))
                File.Delete(file);

            Directory.Delete(directory, true);
        }

        public static void DeleteBackup(string directory, Dictionary<string, string> duplicati_options)
        {
            Console.WriteLine($"Deleting backup {directory}");
#if DEBUG
            using var console_sink = new Duplicati.CommandLine.ConsoleOutput(Console.Out, duplicati_options);
#else
            IMessageSink console_sink = null;
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
                File.Delete(file);
        }

        public static async Task<int> Main(string[] args)
        {
            var root_cmd = new RootCommand(@"Run the benchmark of the reworked restore flow.")
            {
                new Option<string>(aliases: ["--data-generator"], description: "Path to the data generator executable", getDefaultValue: () => "../data_repos/duplicati/Tools/TestDataGenerator/bin/Release/net8.0/TestDataGenerator") { Arity = ArgumentArity.ExactlyOne },
                new Option<string>(aliases: ["--hostname", "-h"], description: "Hostname of the machine running the benchmark", getDefaultValue: () => System.Net.Dns.GetHostName()) { Arity = ArgumentArity.ExactlyOne },
                new Option<int>(aliases: ["--iterations", "-i"], description: "Number of iterations", getDefaultValue: () => 1),
                new Option<Operation>(aliases: ["--operation"], description: "Operation to perform. Should be one of: datasetonly, filesizes, regular, sparsity", getDefaultValue: () => Operation.Regular) { Arity = ArgumentArity.ExactlyOne },
                new Option<string>(aliases: ["--output", "-o"], description: "Output directory to hold the generated files and the results", getDefaultValue: () => "..") { Arity = ArgumentArity.ExactlyOne },
                new Option<Size> (aliases: ["--size", "-s"], description: "Size of the test data. Should one of: all, small, medium, large", getDefaultValue: () => Size.Small) { Arity = ArgumentArity.ExactlyOne },
            };

            root_cmd.Handler = CommandHandler.Create(Run);

            return await root_cmd.InvokeAsync(args);
        }

        private static async Task<string> GenerateData(string datagen, Size size, string output_dir, long? sparsity = null, long? file_size_mb = null)
        {
            string size_str;
            long max_file_size, max_total_size, file_count, sparse_factor;
            switch (size)
            {
                case Size.Small:
                    size_str = "small";
                    max_file_size = (file_size_mb ?? 10) * 1048576;
                    max_total_size = 1073741824; // 1GB
                    file_count = max_total_size / max_file_size;
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
                default:
                    throw new ArgumentException($"Invalid size provided: {size}");
            }

            string data_dir = Path.Combine(output_dir, size_str);

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
                using var stream = File.Open(file, FileMode.Open, FileAccess.ReadWrite);
                stream.Seek(0, SeekOrigin.Begin);
                byte[] buffer = new byte[1];
                stream.Read(buffer, 0, 1);
                buffer[0] = (byte)~buffer[0];
                stream.Seek(0, SeekOrigin.Begin);
                stream.Write(buffer, 0, 1);
            }
        }

        private static void RestoreData(string source, string destination, Dictionary<string, string> duplicati_options, string use_legacy)
        {
            var packed_options = duplicati_options;
            packed_options["restore-legacy"] = use_legacy;
            packed_options["restore-path"] = destination;
#if DEBUG
            using var console_sink = new Duplicati.CommandLine.ConsoleOutput(Console.Out, packed_options);
#else
            IMessageSink console_sink = null;
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
                default:
                    throw new ArgumentException($"Invalid operation provided: {config.Operation}");
            }
        }

        private static async Task<int> RunDatasetOnly(Config config)
        {
            var sw = new Stopwatch();
            string data_dir = Path.Combine(config.Output, "data");
            if (!Directory.Exists(data_dir))
                Directory.CreateDirectory(data_dir);
            string times_dir = Path.Combine(config.Output, "times");
            if (!Directory.Exists(times_dir))
                Directory.CreateDirectory(times_dir);
            Size[] sizes = config.Size == Size.All ? [Size.Small, Size.Medium, Size.Large] : [config.Size];
            var datagen = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) ? $"{config.DataGenerator}.exe" : config.DataGenerator;
            if (!File.Exists(datagen))
            {
                throw new FileNotFoundException($"Data generator not found at {datagen}");
            }
            foreach (var size in sizes)
            {
                var size_str = config.Size.ToString().ToLower();
                Console.WriteLine($"Generating data for size {size_str}");
                sw.Restart();
                var generated = await GenerateData(datagen, size, data_dir, 10);
                sw.Stop();
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_generate_sparse.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);
            }
            return 0;
        }

        private static async Task<int> RunFilesizes(Config config)
        {
            var legacies = new string[] { "false", "true" };
            var sw = new Stopwatch();
            Dictionary<string, string> duplicati_options = new()
            {
                ["passphrase"] = "password",
                ["overwrite"] = "true"
            };

            string data_dir = Path.Combine(config.Output, "data");
            string times_dir = Path.Combine(config.Output, "times");

            foreach (var dir in new string[] { data_dir, times_dir })
                if (!Directory.Exists(dir))
                    Directory.CreateDirectory(dir);

            var datagen = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) ? $"{config.DataGenerator}.exe" : config.DataGenerator;
            if (!File.Exists(datagen))
            {
                throw new FileNotFoundException($"Data generator not found at {datagen}");
            }
            var size_str = config.Size.ToString().ToLower();
            string backup_dir = Path.Combine(data_dir, $"backup_{size_str}");
            string restore_dir = Path.Combine(data_dir, $"restore_{size_str}");

            for (int j = 1; j <= 10; j++)
            {
                Console.WriteLine(@$"* Running benchmark for size {config.Size} with file sizes {j * 10} MB");
                sw.Restart();
                var generated = await GenerateData(datagen, config.Size, data_dir, file_size_mb: j * 10);
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
            var legacies = new string[] { "false", "true" };
            var sw = new Stopwatch();
            Dictionary<string, string> duplicati_options = new()
            {
                ["passphrase"] = "password",
                ["overwrite"] = "true"
            };

            string data_dir = Path.Combine(config.Output, "data");
            string times_dir = Path.Combine(config.Output, "times");
            bool should_delete_data = !Directory.Exists(data_dir);

            foreach (var dir in new string[] { data_dir, times_dir })
                if (!Directory.Exists(dir))
                    Directory.CreateDirectory(dir);

            var datagen = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) ? $"{config.DataGenerator}.exe" : config.DataGenerator;
            if (!File.Exists(datagen))
            {
                throw new FileNotFoundException($"Data generator not found at {datagen}");
            }

            foreach (var size in sizes)
            {
                Console.WriteLine(@$"*
* Running benchmark for size {size}
*");
                var size_str = size.ToString().ToLower();
                string backup_dir = Path.Combine(data_dir, $"backup_{size_str}");
                string restore_dir = Path.Combine(data_dir, $"restore_{size_str}");
                sw.Restart();
                var generated = await GenerateData(datagen, size, data_dir);
                sw.Stop();
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_generate.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);

                sw.Restart();
                BackupData(generated, backup_dir, duplicati_options);
                sw.Stop();
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{config.Hostname}_{size_str}_backup.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);

                // Delete the generated data, as it's now backed up
                DeleteAll(generated);

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

                // Delete the restored data, as it's no longer needed
                DeleteAll(restore_dir);
                DeleteBackup(backup_dir, duplicati_options);
                DeleteAll(backup_dir);
            }

            // Delete the data directory, as it's no longer needed
            if (should_delete_data)
                DeleteAll(data_dir);

            return 0;
        }

        private static async Task<int> RunSparsity(Config config)
        {
            var legacies = new string[] { "false", "true" };
            var sw = new Stopwatch();
            Dictionary<string, string> duplicati_options = new()
            {
                ["passphrase"] = "password",
                ["overwrite"] = "true"
            };

            string data_dir = Path.Combine(config.Output, "data");
            string times_dir = Path.Combine(config.Output, "times");

            foreach (var dir in new string[] { data_dir, times_dir })
                if (!Directory.Exists(dir))
                    Directory.CreateDirectory(dir);

            var datagen = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) ? $"{config.DataGenerator}.exe" : config.DataGenerator;
            if (!File.Exists(datagen))
            {
                throw new FileNotFoundException($"Data generator not found at {datagen}");
            }
            var size_str = config.Size.ToString().ToLower();
            string backup_dir = Path.Combine(data_dir, $"backup_{size_str}");
            string restore_dir = Path.Combine(data_dir, $"restore_{size_str}");

            for (int j = 0; j < 10; j++)
            {
                Console.WriteLine(@$"*
* Running benchmark for size {config.Size} with sparsity {j * 10}
*");
                sw.Restart();
                var generated = await GenerateData(datagen, config.Size, data_dir, j * 10);
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

            DeleteAll(data_dir);

            return 0;
        }

        private static void TouchAll(string directory)
        {
            foreach (var file in Directory.GetFiles(directory, "*", SearchOption.AllDirectories))
                File.SetLastWriteTimeUtc(file, System.DateTime.UtcNow);
        }
    }
}