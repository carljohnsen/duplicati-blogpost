using System.Diagnostics;
using System.Reflection;

ulong default_mac = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian([0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]);

ulong get_mac_1() {
    System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces();
    return 0;
}

ulong get_mac_2() {
    try {
        var interfaces = System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces();
        for (int i = 0; i < interfaces.Length; i++)
        {
            if (i != System.Net.NetworkInformation.NetworkInterface.LoopbackInterfaceIndex)
            {
                return System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(interfaces[i].GetPhysicalAddress().GetAddressBytes());
            }
        }
    }
    catch (Exception) { }
    return default_mac;
}

ulong get_mac_3() {
    try {
        var interfaces = System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces();
        for (int i = 0; i < interfaces.Length; i++)
        {
            if (i != System.Net.NetworkInformation.NetworkInterface.LoopbackInterfaceIndex)
            {
                var mac = interfaces[i].GetPhysicalAddress().GetAddressBytes();
                if (mac.Length > 0 && !mac.All(b => b == 0))
                {
                    return System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian([..mac, .. new byte[2]]);
                }
            }
        }
    }
    catch (Exception) { }
    return default_mac;
}

ulong get_mac_4() {
    return System.Net.NetworkInformation.NetworkInterface
        .GetAllNetworkInterfaces()
        .Where(ni => ni.NetworkInterfaceType != System.Net.NetworkInformation.NetworkInterfaceType.Loopback)
        .Select(ni => ni.GetPhysicalAddress().GetAddressBytes())
        .Where(mac => mac.Length > 0 && !mac.All(b => b == 0))
        .Select(mac => System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian([..mac, .. new byte[2]]))
        .FirstOrDefault(default_mac);
}

ulong get_mac_5() {
    return System.Net.NetworkInformation.NetworkInterface
        .GetAllNetworkInterfaces()
        .Where(ni => ni.OperationalStatus == System.Net.NetworkInformation.OperationalStatus.Up)
        .Where(ni => ni.NetworkInterfaceType != System.Net.NetworkInformation.NetworkInterfaceType.Loopback)
        .Select(ni => ni.GetPhysicalAddress().GetAddressBytes())
        .Where(mac => mac.Length > 0 && !mac.All(b => b == 0))
        .Select(mac => System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian([..mac, .. new byte[2]]))
        .FirstOrDefault(default_mac);
}

ulong get_mac_6() {
    return System.Net.NetworkInformation.NetworkInterface
        .GetAllNetworkInterfaces()
        .Where(ni => ni.NetworkInterfaceType != System.Net.NetworkInformation.NetworkInterfaceType.Loopback)
        .Where(ni => ni.OperationalStatus == System.Net.NetworkInformation.OperationalStatus.Up)
        .Select(ni => ni.GetPhysicalAddress().GetAddressBytes())
        .Where(mac => mac.Length > 0 && !mac.All(b => b == 0))
        .Select(mac => System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian([..mac, .. new byte[2]]))
        .FirstOrDefault(default_mac);
}

ulong get_mac_7() {
    return System.Net.NetworkInformation.NetworkInterface
        .GetAllNetworkInterfaces()
        .Where(ni => ni.NetworkInterfaceType == System.Net.NetworkInformation.NetworkInterfaceType.Ethernet || ni.NetworkInterfaceType == System.Net.NetworkInformation.NetworkInterfaceType.Wireless80211)
        .Select(ni => ni.GetPhysicalAddress().GetAddressBytes())
        .Where(mac => mac.Length > 0 && !mac.All(b => b == 0))
        .Select(mac => System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian([..mac, .. new byte[2]]))
        .FirstOrDefault(default_mac);
}

ulong get_mac_8()
{
    try
    {
        ProcessStartInfo startInfo = new()
        {
            FileName = "/sbin/ifconfig",
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using Process? process = Process.Start(startInfo);
        using var reader = process?.StandardOutput;
        string result = reader?.ReadToEnd() ?? string.Empty;
        var matches = MyRegex().Matches(result);

        foreach (System.Text.RegularExpressions.Match match in matches)
        {
            string mac_address = match.Groups[1].Value;
            if (!string.IsNullOrEmpty(mac_address))
            {
                var mac_bytes =  mac_address.Split(':').Select(b => Convert.ToByte(b, 16)).ToArray();
                return System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian([..mac_bytes, .. new byte[2]]);
            }
        }
    }
    catch (Exception) { }

    return default_mac;
}

ulong get_mac_9() {
    return System.Net.NetworkInformation.NetworkInterface
        .GetAllNetworkInterfaces()
        .Select(ni => ni.GetPhysicalAddress().GetAddressBytes())
        .Where(mac => mac.Length > 0 && !mac.All(b => b == 0))
        .Select(mac => System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian([..mac, .. new byte[2]]))
        .FirstOrDefault();
}

string get_mac_str(byte[]? mac) {
    if (mac == null) {
        return "null";
    }
    return string.Join(":", mac.Select(b => b.ToString("X2")));
}

foreach (var ni in System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces()) {
    Console.WriteLine($"Name: {ni.Name}, Type: {ni.NetworkInterfaceType}, MAC: {get_mac_str(ni.GetPhysicalAddress().GetAddressBytes())}");
}

double[] time_f(Func<ulong> f, int warmup = 5, int runs = 10) {
    var mac = f();

    for (int i = 0; i < warmup; i++) {
        f();
    }

    double[] times = new double[runs];
    Stopwatch sw = new Stopwatch();
    for (int i = 0; i < runs; i++) {
        sw.Restart();
        f();
        sw.Stop();
        times[i] = (double) sw.ElapsedTicks * 1000000 / Stopwatch.Frequency;
    }

    Console.WriteLine($"({f.GetMethodInfo().Name}) MAC: {mac:X016} | Elapsed time: {times.Sum():0.00} us ({times.Average():0.00} us per run)");

    return times;
}

var warmup = 100;
var runs = 1000;
var funcs = new Func<ulong>[] { get_mac_1, get_mac_2, get_mac_3, get_mac_4, get_mac_5, get_mac_6, get_mac_7, get_mac_8, get_mac_9 };
double[][] times = new double[funcs.Length][];
for (int i = 0; i < funcs.Length; i++) {
    var f = funcs[i];

    times[i] = time_f(f, warmup, runs);
}

using var csv_file = File.CreateText($"results.csv");
csv_file.WriteLine(string.Join(",",Enumerable.Range(1, 9).Select(i => $"mac_{i}")));
for (int i = 0; i < runs; i++) {
    csv_file.WriteLine(string.Join(",", times.Select(t => t[i].ToString("0.00"))));
}

partial class Program
{
    [System.Text.RegularExpressions.GeneratedRegex(@"ether ([0-9a-fA-F:]{17})")]
    private static partial System.Text.RegularExpressions.Regex MyRegex();
}
