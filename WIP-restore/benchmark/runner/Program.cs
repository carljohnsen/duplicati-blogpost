using System;
using System.CommandLine;
using System.CommandLine.NamingConventionBinder;
using System.Threading.Tasks;
using Duplicati;

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
            int Warmup,
            int Iterations,
            string Output,
            string DataGenerator
        );

        public static async Task<int> Main(string[] args)
        {
            var root_cmd = new RootCommand(@"Run the benchmark of the reworked restore flow.")
            {
                new Option<string> (aliases: ["--size", "-s"], description: "Size of the test data. Should one of: all, small, medium, large", getDefaultValue: () => "small") { Arity = ArgumentArity.ExactlyOne },
                new Option<int>(aliases: ["--warmup", "-w"], description: "Number of warmup iterations", getDefaultValue: () => 1),
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

        private static void GenerateData(string datagen, Size size, string output_dir)
        {
            string size_str;
            long max_file_size, max_total_size, file_count;
            switch (size)
            {
                case Size.Small:
                    size_str = "small";
                    max_file_size = 10485760; // 10MB
                    max_total_size = 1073741824; // 1GB
                    file_count = 1000;
                    break;
                case Size.Medium:
                    size_str = "medium";
                    max_file_size = 10485760; // 10MB
                    max_total_size = 10737418240; // 10GB
                    file_count = 10000;
                    break;
                case Size.Large:
                    size_str = "large";
                    max_file_size = 10485760; // 10MB
                    max_total_size = 107374182400; // 100GB
                    file_count = 1000000;
                    break;
                default:
                    throw new ArgumentException($"Invalid size provided: {size}");
            }

            if (Directory.Exists(output_dir))
            {
                return;
            }
            else
            {
                Directory.CreateDirectory(output_dir);
            }

            string data_dir = Path.Combine(output_dir, size_str);

            var process = new System.Diagnostics.Process
            {
                StartInfo = new System.Diagnostics.ProcessStartInfo
                {
                    FileName = datagen,
                    Arguments = $"create {data_dir} --max-file-size {max_file_size} --max-total-size {max_total_size} --file-count {file_count}",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true
                }
            };

            process.Start();
            process.WaitForExit();
        }

        private static async Task<int> Run(Config config)
        {
            string hostname = System.Net.Dns.GetHostName();
            Size[] sizes = config.Size == Size.All ? [Size.Small, Size.Medium, Size.Large] : [config.Size];
            var legacies = new string[] { "false", "true" };

            string data_dir = Path.Combine(config.Output, "data");
            string times_dir = Path.Combine(config.Output, "times");
            string backup_dir = Path.Combine(data_dir, "backup");
            string restore_dir = Path.Combine(data_dir, "restore");

            foreach (var dir in new string[] { data_dir, times_dir, backup_dir, restore_dir })
            {
                Directory.CreateDirectory(dir);
            }

            var datagen = System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) ? $"{config.DataGenerator}.exe" : config.DataGenerator;
            if (!File.Exists(datagen))
            {
                throw new FileNotFoundException($"Data generator not found at {datagen}");
            }

            foreach (var size in sizes)
            {
                GenerateData(datagen, size, data_dir);

                foreach (var use_legacy in legacies)
                {
                    var times_file = Path.Combine(times_dir, $"{hostname}_{size}_full_{use_legacy}.csv");
                }
            }

            return 0;
        }
    }
}