using System.Diagnostics;
using System.Globalization;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using StackExchange.Redis;

namespace ReshardProbe;

/// <summary>
/// Single monotonic clock for the whole run, anchored once to UTC so that CSVs
/// from this process can be correlated with server-side pollers.
/// </summary>
public static class Clock
{
    private static readonly Stopwatch Sw = Stopwatch.StartNew();

    public static DateTimeOffset T0Utc { get; private set; } = DateTimeOffset.UtcNow;

    /// <summary>Re-anchor t=0 to now. Call once, as late as possible before load starts.</summary>
    public static void Anchor()
    {
        T0Utc = DateTimeOffset.UtcNow;
        Sw.Restart();
    }

    public static double Ms => Sw.Elapsed.TotalMilliseconds;

    public static DateTimeOffset ToUtc(double ms) => T0Utc.AddMilliseconds(ms);
}

/// <summary>
/// Windows timer resolution is ~15.6ms by default, which makes a 10ms probe
/// cadence impossible via Task.Delay alone. Raising the system timer to 1ms and
/// spin-waiting the final stretch keeps the probe cadence honest without burning
/// a core for the whole run.
/// </summary>
public static class Pacer
{
    [DllImport("winmm.dll", EntryPoint = "timeBeginPeriod")]
    private static extern uint TimeBeginPeriod(uint ms);

    [DllImport("winmm.dll", EntryPoint = "timeEndPeriod")]
    private static extern uint TimeEndPeriod(uint ms);

    private static bool _raised;

    public static void RaiseResolution()
    {
        if (_raised || !RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return;
        _raised = TimeBeginPeriod(1) == 0;
    }

    public static void RestoreResolution()
    {
        if (!_raised) return;
        TimeEndPeriod(1);
        _raised = false;
    }

    /// <summary>Wait until Clock.Ms reaches <paramref name="dueMs"/>. Coarse sleep, then spin.</summary>
    public static async Task WaitUntilAsync(double dueMs, CancellationToken ct)
    {
        var remaining = dueMs - Clock.Ms;
        if (remaining > 3)
        {
            try { await Task.Delay((int)(remaining - 2), ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
        }
        var spin = new SpinWait();
        while (Clock.Ms < dueMs && !ct.IsCancellationRequested) spin.SpinOnce();
    }
}

/// <summary>How much the application actually knows about an operation's fate.</summary>
public enum Outcome
{
    /// <summary>Server acknowledged. Definitely applied.</summary>
    Ok,

    /// <summary>Provably never reached the server (no connection to send on). Safe to retry.</summary>
    FailDefinite,

    /// <summary>May or may not have been applied. NOT safe to blindly retry non-idempotent commands.</summary>
    FailAmbiguous,

    /// <summary>Server replied with an error (MOVED, LOADING, CLUSTERDOWN...). Outcome is known.</summary>
    FailServer,
}

public static class Classify
{
    /// <summary>
    /// Maps a client exception onto what the application can actually conclude.
    /// Defaults to Ambiguous: for this test, over-reporting uncertainty is safer
    /// than falsely claiming an operation never happened.
    /// </summary>
    public static Outcome Of(Exception ex) => ex switch
    {
        RedisTimeoutException => Outcome.FailAmbiguous,

        // Server spoke to us, so the command's fate is known.
        RedisServerException => Outcome.FailServer,

        RedisConnectionException rce => rce.FailureType switch
        {
            // Multiplexer had nothing to write to -> command was never sent.
            ConnectionFailureType.UnableToConnect => Outcome.FailDefinite,
            ConnectionFailureType.UnableToResolvePhysicalConnection => Outcome.FailDefinite,
            ConnectionFailureType.AuthenticationFailure => Outcome.FailDefinite,
            // Socket died; the command may already have been on the wire.
            _ => Outcome.FailAmbiguous,
        },

        // Multiplexer torn down underneath us; nothing was written.
        ObjectDisposedException => Outcome.FailDefinite,

        SocketException => Outcome.FailAmbiguous,
        IOException => Outcome.FailAmbiguous,

        _ => Outcome.FailAmbiguous,
    };

    public static string Tag(Outcome o) => o switch
    {
        Outcome.Ok => "ok",
        Outcome.FailDefinite => "fail_definite",
        Outcome.FailAmbiguous => "fail_ambiguous",
        Outcome.FailServer => "fail_server",
        _ => "unknown",
    };
}

/// <summary>
/// Append-only CSV sink. Operation threads hand lines to an unbounded channel so
/// that disk I/O never shows up in a measured latency.
/// </summary>
public sealed class CsvSink : IAsyncDisposable
{
    private readonly Channel<string> _channel =
        Channel.CreateUnbounded<string>(new UnboundedChannelOptions { SingleReader = true });

    private readonly StreamWriter _writer;
    private readonly Task _pump;

    /// <param name="autoFlush">
    /// Flush after every line. Required for files that external tooling tails while
    /// the run is in progress (the driver waits on events.csv for warmup_complete);
    /// leave off for high-volume op logs so disk I/O stays off the hot path.
    /// </param>
    public CsvSink(string path, string header, bool autoFlush = false)
    {
        _writer = new StreamWriter(
            new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read, 1 << 20))
        {
            AutoFlush = autoFlush,
        };
        _writer.WriteLine(header);
        if (autoFlush) _writer.Flush();
        _pump = Task.Run(async () =>
        {
            await foreach (var line in _channel.Reader.ReadAllAsync().ConfigureAwait(false))
                _writer.WriteLine(line);
        });
    }

    public void Write(string line) => _channel.Writer.TryWrite(line);

    public async ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        await _pump.ConfigureAwait(false);
        await _writer.FlushAsync().ConfigureAwait(false);
        await _writer.DisposeAsync().ConfigureAwait(false);
    }

    private static readonly char[] NeedsQuoting = { ',', '"', '\n', '\r' };

    /// <summary>Quote/escape a field for CSV. Error messages routinely contain commas.</summary>
    public static string Q(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        if (s.IndexOfAny(NeedsQuoting) < 0) return s;
        return "\"" + s.Replace("\"", "\"\"").Replace('\r', ' ').Replace('\n', ' ') + "\"";
    }

    public static string F(double ms) => ms.ToString("F3", CultureInfo.InvariantCulture);
}

/// <summary>All CSV outputs for one run, plus the event log helper.</summary>
public sealed class Recorder : IAsyncDisposable
{
    private readonly CsvSink _ops;
    private readonly CsvSink _probe;
    private readonly CsvSink _events;

    public Recorder(string outDir)
    {
        Directory.CreateDirectory(outDir);
        _ops = new CsvSink(Path.Combine(outDir, "ops.csv"),
            "t_issue_ms,t_done_ms,latency_ms,role,worker,op,outcome,error_type,message");
        _probe = new CsvSink(Path.Combine(outDir, "probe.csv"),
            "t_issue_ms,t_done_ms,latency_ms,outcome,error_type,message");
        // Tailed live by the driver, so it must be flushed as it is written.
        _events = new CsvSink(Path.Combine(outDir, "events.csv"),
            "t_ms,utc,source,kind,detail", autoFlush: true);
    }

    public void Op(double tIssue, double tDone, string role, int worker, string op,
                   Outcome outcome, Exception? ex)
    {
        _ops.Write(string.Join(',',
            CsvSink.F(tIssue), CsvSink.F(tDone), CsvSink.F(tDone - tIssue),
            role, worker.ToString(CultureInfo.InvariantCulture), op,
            Classify.Tag(outcome),
            ex is null ? "" : ex.GetType().Name,
            CsvSink.Q(ex?.Message)));
    }

    public void Probe(double tIssue, double tDone, Outcome outcome, Exception? ex)
    {
        _probe.Write(string.Join(',',
            CsvSink.F(tIssue), CsvSink.F(tDone), CsvSink.F(tDone - tIssue),
            Classify.Tag(outcome),
            ex is null ? "" : ex.GetType().Name,
            CsvSink.Q(ex?.Message)));
    }

    public void Event(string source, string kind, string? detail = null)
    {
        var t = Clock.Ms;
        _events.Write(string.Join(',',
            CsvSink.F(t),
            Clock.ToUtc(t).ToString("O", CultureInfo.InvariantCulture),
            source, kind, CsvSink.Q(detail)));
        Console.WriteLine($"[{t / 1000.0,9:F3}s] {source,-8} {kind} {detail}");
    }

    public async ValueTask DisposeAsync()
    {
        await _ops.DisposeAsync().ConfigureAwait(false);
        await _probe.DisposeAsync().ConfigureAwait(false);
        await _events.DisposeAsync().ConfigureAwait(false);
    }
}

/// <summary>
/// Tails a plain text file that external tooling (the reshard driver) appends to,
/// so that trigger timestamps land in the same timeline as client observations
/// with no clock-skew guesswork.
/// </summary>
public sealed class MarkerTail
{
    /// <summary>Marker line that ends the run cleanly, so reconciliation still happens.</summary>
    public const string StopMarker = "STOP";

    private readonly string _path;
    private readonly Recorder _rec;
    private readonly CancellationTokenSource? _stopSignal;
    private long _offset;

    public MarkerTail(string path, Recorder rec, CancellationTokenSource? stopSignal = null)
    {
        _path = path;
        _rec = rec;
        _stopSignal = stopSignal;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (File.Exists(_path))
                {
                    using var fs = new FileStream(_path, FileMode.Open, FileAccess.Read,
                                                  FileShare.ReadWrite | FileShare.Delete);
                    if (fs.Length > _offset)
                    {
                        fs.Seek(_offset, SeekOrigin.Begin);
                        using var sr = new StreamReader(fs);
                        string? line;
                        while ((line = await sr.ReadLineAsync(ct).ConfigureAwait(false)) is not null)
                        {
                            if (string.IsNullOrWhiteSpace(line)) continue;
                            var text = line.Trim();
                            _rec.Event("marker", "marker", text);

                            // The driver cannot know the reshard duration up front, so it
                            // ends the run this way rather than killing the process, which
                            // would skip reconciliation.
                            if (_stopSignal is not null &&
                                text.Equals(StopMarker, StringComparison.OrdinalIgnoreCase))
                            {
                                _rec.Event("marker", "stop_requested", "driver signalled end of run");
                                _stopSignal.Cancel();
                            }
                        }
                        _offset = fs.Length;
                    }
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { _rec.Event("marker", "tail_error", ex.Message); }

            try { await Task.Delay(50, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { break; }
        }
    }
}
