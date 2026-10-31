using System;
using System.IO;
using System.Text;
using System.Diagnostics;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;

// .NET Framework / Windows 7: synchronous Win32 I/O, no SerialPort class or Node.js.
public static class SyncSerialProbe
{
    [StructLayout(LayoutKind.Sequential)]
    struct DCB {
        public uint Length, BaudRate, Flags;
        public ushort Reserved, XonLim, XoffLim;
        public byte ByteSize, Parity, StopBits, XonChar, XoffChar, ErrorChar, EofChar, EvtChar;
        public ushort Reserved1;
    }
    [StructLayout(LayoutKind.Sequential)]
    struct Timeouts { public uint Interval, ReadMultiplier, ReadConstant, WriteMultiplier, WriteConstant; }
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
    static extern SafeFileHandle CreateFile(string name, uint access, uint share, IntPtr security, uint creation, uint flags, IntPtr template);
    [DllImport("kernel32.dll", SetLastError=true)] static extern bool GetCommState(SafeFileHandle h, ref DCB dcb);
    [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetCommState(SafeFileHandle h, ref DCB dcb);
    [DllImport("kernel32.dll", SetLastError=true)] static extern bool SetCommTimeouts(SafeFileHandle h, ref Timeouts timeouts);
    [DllImport("kernel32.dll", SetLastError=true)] static extern bool ReadFile(SafeFileHandle h, [Out] byte[] data, uint count, out uint read, IntPtr overlapped);
    [DllImport("kernel32.dll", SetLastError=true)] static extern bool WriteFile(SafeFileHandle h, byte[] data, uint count, out uint written, IntPtr overlapped);
    static StreamWriter log;
    static readonly object gate = new object();
    static void Log(string text) {
        lock (gate) {
            string line = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff zzz") + " " + text;
            // Save first, so a blocked console does not lose the last native operation.
            if (log != null) log.WriteLine(line);
            Console.WriteLine(line);
        }
    }
    static void Check(bool ok, string operation) {
        if (!ok) { int code = Marshal.GetLastWin32Error(); throw new Win32Exception(code, operation + ": Win32=" + code + " " + new Win32Exception(code).Message); }
    }
    static byte[] Hex(string text) {
        byte[] b = new byte[text.Length / 2];
        for (int i=0; i<b.Length; i++) b[i] = Convert.ToByte(text.Substring(i*2, 2), 16);
        return b;
    }
    static int Probe(SafeFileHandle h, string protocol, string hex, int wait) {
        byte[] tx = Hex(hex);
        Log("TX " + protocol + " bytes=" + tx.Length + " HEX=" + BitConverter.ToString(tx).Replace('-', ' '));
        uint written;
        Check(WriteFile(h, tx, (uint)tx.Length, out written, IntPtr.Zero), "WriteFile");
        Log("WRITE OK bytes=" + written + "; FlushFileBuffers NOT called");
        if (written != tx.Length) throw new IOException("Partial write: " + written + "/" + tx.Length);
        Stopwatch clock = Stopwatch.StartNew();
        int total = 0, calls = 0;
        using (MemoryStream all = new MemoryStream()) {
            while (clock.ElapsedMilliseconds < wait) {
                byte[] rx = new byte[4096]; uint read;
                if (calls == 0) Log("READ BEGIN synchronous ReadFile; per-read timeout=200ms");
                Check(ReadFile(h, rx, (uint)rx.Length, out read, IntPtr.Zero), "ReadFile");
                calls++;
                if (read > 0) {
                    Log("RX " + protocol + " bytes=" + read + " HEX=" + BitConverter.ToString(rx, 0, (int)read).Replace('-', ' '));
                    total += (int)read;
                    if (all.Length + read > 1048576) throw new IOException("RX exceeds 1MiB diagnostic limit");
                    all.Write(rx, 0, (int)read);
                } else Thread.Sleep(5);
            }
            Log("RX SUMMARY " + protocol + " total=" + total + " readCalls=" + calls + " HEX=" + BitConverter.ToString(all.ToArray()).Replace('-', ' '));
        }
        return total;
    }
    public static int Run(string port, int baud, string parity, int wait, int delay, bool rts, bool dtr, string protocol, string output) {
        int result = 1;
        // Safety bound even when a defective driver ignores COMMTIMEOUTS.
        using (Timer watchdog = new Timer(delegate(object ignored) { Environment.Exit(124); }, null, 60000, Timeout.Infinite)) {
            try {
                log = new StreamWriter(output, false, new UTF8Encoding(true)); log.AutoFlush = true;
                Log("SYNC PROBE v1 log=" + output);
                Log("OS=" + Environment.OSVersion + " CLR=" + Environment.Version + " processBits=" + (IntPtr.Size*8));
                Log("port=" + port + " baud=" + baud + " dataBits=8 parity=" + parity + " stopBits=1 RTS=" + rts + " DTR=" + dtr + " wait=" + wait + " openDelay=" + delay + " protocol=" + protocol);
                Log("Watchdog=60s; exit 124 means stalled operation. Raw RX is NOT proof of valid meter response.");
                if (!System.Text.RegularExpressions.Regex.IsMatch(port, "^COM[0-9]+$", System.Text.RegularExpressions.RegexOptions.IgnoreCase) || baud <= 0 || wait < 1 || wait > 15000 || delay < 0 || delay > 5000) throw new ArgumentException("Invalid port/baud/wait/delay");
                if (parity != "even" && parity != "none" && parity != "odd") throw new ArgumentException("parity: even/none/odd");
                if (protocol != "both" && protocol != "698" && protocol != "645") throw new ArgumentException("protocol: both/698/645");
                Log("OPEN BEGIN flags=0 (no FILE_FLAG_OVERLAPPED), exclusive access");
                using (SafeFileHandle h = CreateFile(@"\\.\" + port, 0xC0000000, 0, IntPtr.Zero, 3, 0, IntPtr.Zero)) {
                    Check(!h.IsInvalid, "CreateFile");
                    DCB d = new DCB(); d.Length = (uint)Marshal.SizeOf(typeof(DCB));
                    if (d.Length != 28) throw new Exception("Unexpected DCB layout size=" + d.Length);
                    Check(GetCommState(h, ref d), "GetCommState before");
                    Log("DCB BEFORE baud=" + d.BaudRate + " flags=0x" + d.Flags.ToString("X8") + " parity=" + d.Parity);
                    d.BaudRate = (uint)baud; d.ByteSize = 8; d.Parity = (byte)(parity == "even" ? 2 : parity == "odd" ? 1 : 0); d.StopBits = 0;
                    // Binary, parity checking, DTR/RTS explicitly selected; no CTS/DSR or XON/XOFF gating.
                    d.Flags = 1u | (parity == "none" ? 0u : 2u) | (dtr ? 0x10u : 0u) | (rts ? 0x1000u : 0u);
                    Check(SetCommState(h, ref d), "SetCommState");
                    Check(GetCommState(h, ref d), "GetCommState after");
                    Log("DCB AFTER baud=" + d.BaudRate + " flags=0x" + d.Flags.ToString("X8") + " dataBits=" + d.ByteSize + " parity=" + d.Parity + " stopBits=" + d.StopBits);
                    Timeouts t = new Timeouts(); t.Interval = 20; t.ReadConstant = 200; t.WriteConstant = 3000;
                    Check(SetCommTimeouts(h, ref t), "SetCommTimeouts");
                    Log("OPEN OK; no PurgeComm, no FlushFileBuffers"); Thread.Sleep(delay);
                    int total = 0;
                    if (protocol == "both" || protocol == "698") total += Probe(h, "698", "FEFEFEFE6817004345AAAAAAAAAAAA11534E0501004001020000ED0316", wait);
                    if (protocol == "both" || protocol == "645") total += Probe(h, "645", "FEFEFEFE68AAAAAAAAAAAA681300DF16", wait);
                    result = total > 0 ? 0 : 2;
                    Log("RESULT " + (total > 0 ? "RAW_BYTES_RECEIVED (not protocol validated)" : "NO_RX") + " bytes=" + total);
                }
                Log("CLOSED");
            } catch (Exception e) { Log("ERROR " + e.ToString()); result = 1; }
            finally { Log("EXIT code=" + result); if (log != null) { log.Dispose(); log = null; } }
        }
        return result;
    }
}
