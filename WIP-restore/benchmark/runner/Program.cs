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
        private enum Size
        {
            All,
            Small,
            Medium,
            Large
        }

        private sealed record Config(
            Size Size,
            int Iterations,
            string Output,
            string DataGenerator
        );

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
                new Option<string> (aliases: ["--size", "-s"], description: "Size of the test data. Should one of: all, small, medium, large", getDefaultValue: () => "small") { Arity = ArgumentArity.ExactlyOne },
                new Option<int>(aliases: ["--iterations", "-i"], description: "Number of iterations", getDefaultValue: () => 1),
                new Option<string>(aliases: ["--output", "-o"], description: "Output directory to hold the generated files and the results", getDefaultValue: () => "..") { Arity = ArgumentArity.ExactlyOne },
                new Option<string>(aliases: ["--data-generator"], description: "Path to the data generator executable", getDefaultValue: () => "../data_repos/duplicati_testdata/Tools/TestDataGenerator/bin/Release/net8.0/TestDataGenerator") { Arity = ArgumentArity.ExactlyOne }
            };

            root_cmd.Handler = CommandHandler.Create((string size, Config config) =>
            {
                // Parse the size to the enum
                var size_enum = size.ToLower() switch
                {
                    "all" => Size.All,
                    "small" => Size.Small,
                    "medium" => Size.Medium,
                    "large" => Size.Large,
                    _ => throw new ArgumentException($"Invalid size provided: {size}")
                };

                var new_config = config with { Size = size_enum };

                return Run(new_config);
            });

            return await root_cmd.InvokeAsync(args);
        }

        private static async Task<string> GenerateData(string datagen, Size size, string output_dir)
        {
            string size_str;
            long max_file_size, max_total_size, file_count, sparse_factor;
            switch (size)
            {
                case Size.Small:
                    size_str = "small";
                    max_file_size = 10485760; // 10MB
                    max_total_size = 1073741824; // 1GB
                    file_count = 1000;
                    sparse_factor = 20;
                    break;
                case Size.Medium:
                    size_str = "medium";
                    max_file_size = 10485760; // 10MB
                    max_total_size = 10737418240; // 10GB
                    file_count = 10000;
                    sparse_factor = 30;
                    break;
                case Size.Large:
                    size_str = "large";
                    max_file_size = 10485760; // 10MB
                    max_total_size = 107374182400; // 100GB
                    file_count = 1000000;
                    sparse_factor = 40;
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

            process.Start();
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
            string hostname = System.Net.Dns.GetHostName();
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
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{hostname}_{size_str}_generate.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);

                sw.Restart();
                BackupData(generated, backup_dir, duplicati_options);
                sw.Stop();
                using (var writer = new StreamWriter(Path.Combine(times_dir, $"{hostname}_{size_str}_backup.csv"), true))
                    writer.WriteLine(sw.ElapsedMilliseconds);

                foreach (var use_legacy in legacies)
                {
                    Console.WriteLine($"Legacy restore: {use_legacy}");
                    // Warmup is handled in plotting.

                    //
                    // Perform the full restore
                    //
                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{hostname}_{size_str}_full_{use_legacy}.csv")))
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

                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{hostname}_{size_str}_partial_{use_legacy}.csv")))
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
                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{hostname}_{size_str}_no_{use_legacy}.csv")))
                    {
                        Console.Write($"No restore: 0/{config.Iterations}");
                        for (int i = 0; i < config.Iterations; i++)
                        {
                            sw.Restart();
                            RestoreData(backup_dir, restore_dir, duplicati_options, use_legacy);
                            sw.Stop();
                            writer.WriteLine(sw.ElapsedMilliseconds);
                            Console.Write($"\rNo restore: {i + 1}/{config.Iterations}");
                        }
                        Console.WriteLine();
                    }

                    //
                    // Perform the metadata only restore
                    //
                    using (var writer = new StreamWriter(Path.Combine(times_dir, $"{hostname}_{size_str}_metadata_{use_legacy}.csv")))
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
            }

            return 0;
        }

        private static void TouchAll(string directory)
        {
            foreach (var file in Directory.GetFiles(directory, "*", SearchOption.AllDirectories))
                File.SetLastWriteTimeUtc(file, System.DateTime.UtcNow);
        }
    }
}