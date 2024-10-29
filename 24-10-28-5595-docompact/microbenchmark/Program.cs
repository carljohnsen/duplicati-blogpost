using System.Collections.Concurrent;
using System.Collections.Frozen;

Tuple<double, double, double, double> time_func(Func<List<string>, List<string>, List<string>> f, List<string> a, List<string> b, int warmup = 10, int runs = 1000)
{
    for (int i = 0; i < warmup; i++)
    {
        f(a, b);
    }

    double[] times = new double[runs];
    var sw = new System.Diagnostics.Stopwatch();
    for (int i = 0; i < runs; i++)
    {
        sw.Restart();
        f(a, b);
        sw.Stop();
        // In microseconds
        times[i] = sw.ElapsedTicks / (double)System.Diagnostics.Stopwatch.Frequency * 1000000;
    }

    // Remove 1 % of the outliers in each end of the distribution
    Array.Sort(times);
    int start = runs / 100;
    int end = runs - start;
    times = times[start..end];

    // Compute the statistics
    double mean = times.Average();
    double std = Math.Sqrt(times.Select(x => Math.Pow(x - mean, 2)).Sum() / (runs - 1));
    double min = times.Min();
    double max = times.Max();

    return new Tuple<double, double, double, double>(mean, std, min, max);
}

List<string> generate_strings(Random rng, int n) {
    return Enumerable
        .Range(0, n)
        .Select(_ => {
            byte[] tmp = new byte[32];
            rng.NextBytes(tmp);
            return Convert.ToBase64String(tmp);
        })
        .ToList();
}

(List<string>,List<string>) generate_string_pair(Random rng, int n, int overlap) {
    var a = generate_strings(rng, n);
    var overlap_elements = n * overlap / 100;
    var b = generate_strings(rng, n - overlap_elements)
        .Concat(a.Take(overlap_elements))
        .OrderBy(_ => rng.Next())
        .ToList();
    return (a, b);
}

void print_times(string name, double mean, double std, double min, double max, double baseline)
{
    string speedup = baseline > 0 ? $" (speedup: {baseline / mean:0.00}x)" : "";
    Console.WriteLine($"{name} | Mean: {mean:0.00} ± {std:0.00} (min: {min:0.00}, max: {max:0.00}) {speedup}");
}

List<string> original_linq(List<string> a, List<string> b)
{
    return (from x in a
        where b.Contains(x)
        select x).ToList();
}

List<string> std_linq(List<string> a, List<string> b)
{
    return a.Intersect(b).ToList();
}

List<string> compose_linq(List<string> a, List<string> b)
{
    return a.Where(b.Contains).ToList();
}

List<string> hashset_linq(List<string> a, List<string> b)
{
    var b_set = new HashSet<string>(b);
    return a.Where(b_set.Contains).ToList();
}

List<string> frozenset_linq(List<string> a, List<string> b)
{
    var b_set = new HashSet<string>(b).ToFrozenSet();
    return a.Where(b_set.Contains).ToList();
}

List<string> par_std_linq(List<string> a, List<string> b)
{
    return a.AsParallel().Intersect(b.AsParallel()).ToList();
}

List<string> par_hashset_linq(List<string> a, List<string> b)
{
    var b_set = new HashSet<string>(b);
    return a.AsParallel().Where(b_set.Contains).ToList();
}

List<string> par_std_linq_partition(List<string> a, List<string> b)
{
    return Partitioner.Create(a, true).AsParallel().Intersect(b.AsParallel()).ToList();
}

List<string> par_hashset_linq_partition(List<string> a, List<string> b)
{
    var b_set = new HashSet<string>(b);
    return Partitioner.Create(a, true).AsParallel().Where(b_set.Contains).ToList();
}

void verify(Random rng) {
    int n = 5000;
    var (a,b) = generate_string_pair(rng, n, 50);

    var original = original_linq(a, b);

    var std = std_linq(a, b);
    System.Diagnostics.Debug.Assert(original.Count == std.Count && original.All(std.Contains));
    var compose = compose_linq(a, b);
    System.Diagnostics.Debug.Assert(original.Count == compose.Count && original.All(compose.Contains));
    var hashset = hashset_linq(a, b);
    System.Diagnostics.Debug.Assert(original.Count == hashset.Count && original.All(hashset.Contains));
    var frozenset = frozenset_linq(a, b);
    System.Diagnostics.Debug.Assert(original.Count == frozenset.Count && original.All(frozenset.Contains));
    var par_std = par_std_linq(a, b);
    System.Diagnostics.Debug.Assert(original.Count == par_std.Count && original.All(par_std.Contains));
    var par_hashset = par_hashset_linq(a, b);
    System.Diagnostics.Debug.Assert(original.Count == par_hashset.Count && original.All(par_hashset.Contains));
    var par_std_partition = par_std_linq_partition(a, b);
    System.Diagnostics.Debug.Assert(original.Count == par_std_partition.Count && original.All(par_std_partition.Contains));
    var par_hashset_partition = par_hashset_linq_partition(a, b);
    System.Diagnostics.Debug.Assert(original.Count == par_hashset_partition.Count && original.All(par_hashset_partition.Contains));

    Console.WriteLine("All tests passed!");
}

Random rng = new(2024_10_28);
verify(rng);

var csv_file = File.CreateText($"results.csv");
csv_file.WriteLine("n,overlap,original,original_std,original_min,original_max,compose,compose_std,compose_min,compose_max,linq,linq_std,linq_min,linq_max,hashset,hashset_std,hashset_min,hashset_max,frozenset,frozenset_std,frozenset_min,frozenset_max,par_std,par_std_std,par_std_min,par_std_max,par_hashset,par_hashset_std,par_hashset_min,par_hashset_max,par_std_partition,par_std_partition_std,par_std_partition_min,par_std_partition_max,par_hashset_partition,par_hashset_partition_std,par_hashset_partition_min,par_hashset_partition_max");
for (int i = 1; i <= 10; i++)
{
    var overlap = i * 10;
    for (int j = 1; j < 5; j++)
    {
        int k_end = j == 4 ? 4 : 10;
        for (int k = 1; k < k_end; k++)
        {
            int n = (int) Math.Pow(10, j) * k;
            var (a,b) = generate_string_pair(rng, n, overlap);

            var (original_mean, original_std, original_min, original_max) = time_func(original_linq, a, b, 5, 10);
            print_times("Original", original_mean, original_std, original_min, original_max, 0);

            var (std_mean, std_std, std_min, std_max) = time_func(std_linq, a, b);
            print_times("LINQ Intersect", std_mean, std_std, std_min, std_max, original_mean);

            var (compose_mean, compose_std, compose_min, compose_max) = time_func(compose_linq, a, b, 5, 10);
            print_times("LINQ composition", compose_mean, compose_std, compose_min, compose_max, original_mean);

            var (hashset_mean, hashset_std, hashset_min, hashset_max) = time_func(hashset_linq, a, b);
            print_times("HashSet", hashset_mean, hashset_std, hashset_min, hashset_max, original_mean);

            var (frozenset_mean, frozenset_std, frozenset_min, frozenset_max) = time_func(frozenset_linq, a, b);
            print_times("FrozenSet", frozenset_mean, frozenset_std, frozenset_min, frozenset_max, original_mean);

            var (par_std_mean, par_std_std, par_std_min, par_std_max) = time_func(par_std_linq, a, b);
            print_times("PLINQ Intersect", par_std_mean, par_std_std, par_std_min, par_std_max, original_mean);

            var (par_hashset_mean, par_hashset_std, par_hashset_min, par_hashset_max) = time_func(par_hashset_linq, a, b);
            print_times("PLINQ HashSet", par_hashset_mean, par_hashset_std, par_hashset_min, par_hashset_max, original_mean);

            var (par_std_partition_mean, par_std_partition_std, par_std_partition_min, par_std_partition_max) = time_func(par_std_linq_partition, a, b);
            print_times("PLINQ Intersect Partition", par_std_partition_mean, par_std_partition_std, par_std_partition_min, par_std_partition_max, original_mean);

            var (par_hashset_partition_mean, par_hashset_partition_std, par_hashset_partition_min, par_hashset_partition_max) = time_func(par_hashset_linq_partition, a, b);
            print_times("PLINQ HashSet Partition", par_hashset_partition_mean, par_hashset_partition_std, par_hashset_partition_min, par_hashset_partition_max, original_mean);

            Console.WriteLine("--------------------");
            csv_file.WriteLine($"{n},{overlap},{original_mean:0.00},{original_std:0.00},{original_min:0.00},{original_max:0.00},{compose_mean:0.00},{compose_std:0.00},{compose_min:0.00},{compose_max:0.00},{std_mean:0.00},{std_std:0.00},{std_min:0.00},{std_max:0.00},{hashset_mean:0.00},{hashset_std:0.00},{hashset_min:0.00},{hashset_max:0.00},{frozenset_mean:0.00},{frozenset_std:0.00},{frozenset_min:0.00},{frozenset_max:0.00},{par_std_mean:0.00},{par_std_std:0.00},{par_std_min:0.00},{par_std_max:0.00},{par_hashset_mean:0.00},{par_hashset_std:0.00},{par_hashset_min:0.00},{par_hashset_max:0.00},{par_std_partition_mean:0.00},{par_std_partition_std:0.00},{par_std_partition_min:0.00},{par_std_partition_max:0.00},{par_hashset_partition_mean:0.00},{par_hashset_partition_std:0.00},{par_hashset_partition_min:0.00},{par_hashset_partition_max:0.00}");
        }
    }
}
csv_file.Close();