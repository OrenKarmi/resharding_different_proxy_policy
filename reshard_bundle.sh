#!/bin/bash
# =============================================================================
# reshard_bundle.sh - proxy-policy resharding client-impact test
#
# Self-contained. Paste onto a Redis Enterprise node, then:
#
#   bash reshard_bundle.sh setup
#   bash reshard_bundle.sh check    --user "$RL_USER" --password "$RL_PASS"
#   bash reshard_bundle.sh dryrun   --user "$RL_USER" --password "$RL_PASS"
#   bash reshard_bundle.sh matrix   --user "$RL_USER" --password "$RL_PASS" \
#                                   --db-password 'testpass'
#   bash reshard_bundle.sh collect
#
# WHAT IT DOES
#   Builds and runs a real StackExchange.Redis / NRedisStack client (pinned to
#   SE.Redis 2.8.x, what production .NET apps run), drives load against a
#   database endpoint, reshards 1 -> 2 shards, and measures what the client
#   experiences: outage duration, dropped vs ambiguous commands, phantom writes,
#   throughput hole, latency percentiles, reconnects.
#
#   Arms: control (no reshard), single, all-master-shards, all-nodes.
#   Each arm creates a fresh DB (1 master + 1 replica, dense placement,
#   proxy_policy set at create time) and deletes it afterwards, because shard
#   count cannot be reduced on a normal database.
#
# REQUIREMENTS
#   - run on a cluster node that does NOT host the test database (the script
#     detects this and refuses otherwise; override with --allow-colocated)
#   - python3 (present on Redis Enterprise nodes)
#   - outbound HTTPS during 'setup' only, to dot.net and nuget.org
#   - REST credentials for the cluster
#
#   If the node has no outbound HTTPS, skip 'setup': copy the prebuilt
#   self-contained binary (prebuilt/ReshardProbe-linux-x64) to the node instead
#   and run node_driver.py directly against it.
#
#   'setup' installs the .NET SDK under $HOME/.dotnet. Nothing is installed
#   system-wide; remove with:  rm -rf $HOME/.dotnet $HOME/reshard_probe
# =============================================================================
set -u

# Digest of the embedded payloads, stamped in at generation time. Printed by every
# subcommand so "is this actually the fixed code?" is never a guess.
BUNDLE_VERSION="a4d04d3257c5"

WORKDIR="${RESHARD_WORKDIR:-$HOME/reshard_probe}"
DOTNET_DIR="$HOME/.dotnet"
DOTNET="$DOTNET_DIR/dotnet"
DLL="$WORKDIR/ReshardProbe/bin/Release/net8.0/ReshardProbe.dll"
RESULTS="${RESHARD_RESULTS:-$WORKDIR/results}"

die() { echo "ERROR: $*" >&2; exit 1; }

_write_payloads() {
  cat > "$WORKDIR/ReshardProbe/ReshardProbe.csproj" <<'RESHARD_EOF_RESHARDPROBE_RESHARDPROBE_CSPROJ'
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net8.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <AssemblyName>ReshardProbe</AssemblyName>
    <RootNamespace>ReshardProbe</RootNamespace>
    <InvariantGlobalization>true</InvariantGlobalization>
    <ServerGarbageCollection>true</ServerGarbageCollection>
    <ConcurrentGarbageCollection>true</ConcurrentGarbageCollection>
  </PropertyGroup>

  <ItemGroup>
    <!-- Pinned to 2.8.x deliberately: that is what production .NET apps run.
         v3 is a new major with a rewritten transport, so its reconnect behaviour
         would not describe the clients we are trying to characterise. -->
    <PackageReference Include="StackExchange.Redis" Version="2.8.*" />
    <PackageReference Include="NRedisStack" Version="0.13.*" />
  </ItemGroup>

</Project>
RESHARD_EOF_RESHARDPROBE_RESHARDPROBE_CSPROJ
  cat > "$WORKDIR/ReshardProbe/Infra.cs" <<'RESHARD_EOF_RESHARDPROBE_INFRA_CS'
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
RESHARD_EOF_RESHARDPROBE_INFRA_CS
  cat > "$WORKDIR/ReshardProbe/Roles.cs" <<'RESHARD_EOF_RESHARDPROBE_ROLES_CS'
using StackExchange.Redis;

namespace ReshardProbe;

/// <summary>
/// Fixed-cadence PING probe on its own multiplexer. Open loop: a slot fires even
/// if earlier pings are still outstanding, so the outage window is resolved to the
/// probe interval rather than to syncTimeout.
/// </summary>
public static class AvailabilityProbe
{
    public static async Task RunAsync(
        IDatabase db, Recorder rec, double intervalMs, int maxInFlight, CancellationToken ct)
    {
        using var gate = new SemaphoreSlim(maxInFlight);
        var start = Clock.Ms;
        long slot = 0;
        var pending = new List<Task>();

        while (!ct.IsCancellationRequested)
        {
            await Pacer.WaitUntilAsync(start + (++slot) * intervalMs, ct).ConfigureAwait(false);
            if (ct.IsCancellationRequested) break;

            // If every slot is occupied the client is badly backed up; record the
            // saturation rather than silently dropping cadence.
            if (!await gate.WaitAsync(0, CancellationToken.None).ConfigureAwait(false))
            {
                rec.Event("probe", "inflight_saturated", $"slot={slot}");
                continue;
            }

            pending.Add(Task.Run(async () =>
            {
                var tIssue = Clock.Ms;
                try
                {
                    await db.PingAsync().ConfigureAwait(false);
                    rec.Probe(tIssue, Clock.Ms, Outcome.Ok, null);
                }
                catch (Exception ex)
                {
                    rec.Probe(tIssue, Clock.Ms, Classify.Of(ex), ex);
                }
                finally
                {
                    gate.Release();
                }
            }, CancellationToken.None));

            if (pending.Count >= 4096)
            {
                pending.RemoveAll(t => t.IsCompleted);
            }
        }

        // Let outstanding pings land so the recovery edge is captured.
        try { await Task.WhenAll(pending).WaitAsync(TimeSpan.FromSeconds(30), CancellationToken.None).ConfigureAwait(false); }
        catch { /* stragglers already recorded their own outcome */ }
    }
}

/// <summary>
/// Rate-limited SET/GET load. Open loop on purpose: a closed loop would absorb an
/// outage as latency and hide the throughput hole we are trying to measure.
/// </summary>
public static class LoadGenerator
{
    public static async Task RunAsync(
        IDatabase db, Recorder rec, int worker, string keyPrefix, int keySpace,
        int valueBytes, double opsPerSecond, int maxInFlight, CancellationToken ct)
    {
        using var gate = new SemaphoreSlim(maxInFlight);
        var rng = new Random(unchecked(worker * 7919 + 13));
        var payload = new byte[valueBytes];
        rng.NextBytes(payload);
        var value = (RedisValue)payload;

        var intervalMs = 1000.0 / opsPerSecond;
        var start = Clock.Ms;
        long slot = 0;
        var pending = new List<Task>();

        while (!ct.IsCancellationRequested)
        {
            await Pacer.WaitUntilAsync(start + (++slot) * intervalMs, ct).ConfigureAwait(false);
            if (ct.IsCancellationRequested) break;

            if (!await gate.WaitAsync(0, CancellationToken.None).ConfigureAwait(false))
            {
                rec.Op(Clock.Ms, Clock.Ms, "load", worker, "skipped_backpressure", Outcome.FailDefinite, null);
                continue;
            }

            var key = (RedisKey)$"{keyPrefix}:k:{rng.Next(keySpace)}";
            var isWrite = (slot & 1) == 0;

            pending.Add(Task.Run(async () =>
            {
                var tIssue = Clock.Ms;
                try
                {
                    if (isWrite) await db.StringSetAsync(key, value).ConfigureAwait(false);
                    else await db.StringGetAsync(key).ConfigureAwait(false);
                    rec.Op(tIssue, Clock.Ms, "load", worker, isWrite ? "SET" : "GET", Outcome.Ok, null);
                }
                catch (Exception ex)
                {
                    rec.Op(tIssue, Clock.Ms, "load", worker, isWrite ? "SET" : "GET", Classify.Of(ex), ex);
                }
                finally
                {
                    gate.Release();
                }
            }, CancellationToken.None));

            if (pending.Count >= 8192)
            {
                pending.RemoveAll(t => t.IsCompleted);
            }
        }

        try { await Task.WhenAll(pending).WaitAsync(TimeSpan.FromSeconds(30), CancellationToken.None).ConfigureAwait(false); }
        catch { /* individual outcomes already recorded */ }
    }
}

/// <summary>
/// Per-worker counters for the write-reconciliation arm. Deliberately sequential:
/// one INCR in flight at a time, so "attempted" and "acked" are unambiguous and
/// the server-side counter can be compared exactly against what the app believes.
/// </summary>
public sealed class CorrectnessWorker
{
    public required int Id { get; init; }
    public required string Key { get; init; }

    public long Attempted;
    public long Acked;
    public long FailDefinite;
    public long FailAmbiguous;
    public long FailServer;

    /// <summary>Highest counter value the server ever confirmed to us.</summary>
    public long MaxAckedValue;

    /// <summary>Server-side counter read during post-run reconciliation.</summary>
    public long ServerValue;
    public bool ServerValueRead;

    /// <summary>
    /// Counter value before this run started. Normally 0 because the key is deleted
    /// at startup, but non-zero if the reset could not be performed.
    /// </summary>
    public long BaselineValue;

    /// <summary>Increments the server actually applied during this run.</summary>
    public long AppliedThisRun => ServerValue - BaselineValue;

    public async Task RunAsync(IDatabase db, Recorder rec, double intervalMs, CancellationToken ct)
    {
        var key = (RedisKey)Key;
        var start = Clock.Ms;
        long slot = 0;

        while (!ct.IsCancellationRequested)
        {
            await Pacer.WaitUntilAsync(start + (++slot) * intervalMs, ct).ConfigureAwait(false);
            if (ct.IsCancellationRequested) break;

            var tIssue = Clock.Ms;
            Interlocked.Increment(ref Attempted);
            try
            {
                var v = await db.StringIncrementAsync(key).ConfigureAwait(false);
                Interlocked.Increment(ref Acked);
                if (v > MaxAckedValue) MaxAckedValue = v;
                rec.Op(tIssue, Clock.Ms, "correctness", Id, "INCR", Outcome.Ok, null);
            }
            catch (Exception ex)
            {
                var outcome = Classify.Of(ex);
                switch (outcome)
                {
                    case Outcome.FailDefinite: Interlocked.Increment(ref FailDefinite); break;
                    case Outcome.FailServer: Interlocked.Increment(ref FailServer); break;
                    default: Interlocked.Increment(ref FailAmbiguous); break;
                }
                rec.Op(tIssue, Clock.Ms, "correctness", Id, "INCR", outcome, ex);
            }
        }
    }

    /// <summary>
    /// Writes the server actually applied but never acknowledged. These are the
    /// dangerous ones: a naive application retry double-applies them.
    /// </summary>
    public long PhantomWrites => ServerValueRead ? Math.Max(0, AppliedThisRun - Acked) : -1;

    /// <summary>Acknowledged writes missing from the server. Should always be zero.</summary>
    public long LostWrites => ServerValueRead ? Math.Max(0, Acked - AppliedThisRun) : -1;
}

/// <summary>
/// Edge-triggered record of what the multiplexer itself believes about its
/// connection, which is often out of step with whether commands actually work.
/// </summary>
public static class ConnectivityWatcher
{
    public static async Task RunAsync(
        IReadOnlyList<(string Name, ConnectionMultiplexer Mux)> muxes, Recorder rec, CancellationToken ct)
    {
        var last = new Dictionary<string, bool>();
        foreach (var (name, mux) in muxes) last[name] = mux.IsConnected;
        foreach (var (name, mux) in muxes) rec.Event(name, "isconnected_initial", mux.IsConnected.ToString());

        while (!ct.IsCancellationRequested)
        {
            foreach (var (name, mux) in muxes)
            {
                bool now;
                try { now = mux.IsConnected; }
                catch (Exception ex) { rec.Event(name, "isconnected_error", ex.Message); continue; }

                if (now != last[name])
                {
                    last[name] = now;
                    rec.Event(name, now ? "isconnected_true" : "isconnected_false",
                              SafeStatus(mux));
                }
            }

            try { await Task.Delay(20, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { break; }
        }
    }

    private static string SafeStatus(ConnectionMultiplexer mux)
    {
        try { return mux.GetStatus().Replace("\r", " ").Replace('\n', ' '); }
        catch (Exception ex) { return "status_unavailable: " + ex.Message; }
    }
}
RESHARD_EOF_RESHARDPROBE_ROLES_CS
  cat > "$WORKDIR/ReshardProbe/Program.cs" <<'RESHARD_EOF_RESHARDPROBE_PROGRAM_CS'
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Text.Json;
using StackExchange.Redis;

namespace ReshardProbe;

public sealed class Config
{
    public string Endpoint = "";
    public string? Password;
    public string? User;
    public bool Tls;
    public bool TlsAllowUntrusted = true;

    public string Tag = "run";
    public string OutDir = "out";
    public string? MarkersPath;

    public int DurationSec = 300;
    public int WarmupSec = 60;

    /// <summary>Hard ceiling on post-run reconciliation, so a dead endpoint cannot stall the matrix.</summary>
    public int ReconcileBudgetSec = 120;

    public int LoadConnections = 4;
    public double LoadRatePerConn = 200;
    public int LoadInFlight = 256;
    public int ValueBytes = 100;
    public int KeySpace = 10000;

    public int CorrWorkers = 4;
    public double CorrIntervalMs = 20;

    public double ProbeIntervalMs = 10;
    public int ProbeInFlight = 512;

    // StackExchange.Redis knobs that dominate observed recovery behaviour.
    public int SyncTimeoutMs = 5000;
    public int ConnectTimeoutMs = 5000;
    public int ConnectRetry = 3;
    public int ConfigCheckSec = 60;
    public int KeepAliveSec = 60;
    public bool AbortConnect;

    public static Config Parse(string[] args)
    {
        var c = new Config();
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < args.Length; i++)
        {
            if (!args[i].StartsWith("--", StringComparison.Ordinal))
                throw new ArgumentException($"Unexpected argument '{args[i]}'");
            var key = args[i][2..];
            // Bare flags are treated as booleans.
            if (i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
                map[key] = args[++i];
            else
                map[key] = "true";
        }

        string? S(string k) => map.TryGetValue(k, out var v) ? v : null;
        int I(string k, int d) => map.TryGetValue(k, out var v) ? int.Parse(v, CultureInfo.InvariantCulture) : d;
        double D(string k, double d) => map.TryGetValue(k, out var v) ? double.Parse(v, CultureInfo.InvariantCulture) : d;
        bool B(string k, bool d) => map.TryGetValue(k, out var v) ? bool.Parse(v) : d;

        c.Endpoint = S("endpoint") ?? throw new ArgumentException("--endpoint host:port is required");
        c.Password = S("password");
        c.User = S("user");
        c.Tls = B("tls", false);
        c.TlsAllowUntrusted = B("tls-allow-untrusted", true);

        c.Tag = S("tag") ?? c.Tag;
        c.OutDir = S("outdir") ?? c.OutDir;
        c.MarkersPath = S("markers");

        c.DurationSec = I("duration-sec", c.DurationSec);
        c.WarmupSec = I("warmup-sec", c.WarmupSec);
        c.ReconcileBudgetSec = I("reconcile-budget-sec", c.ReconcileBudgetSec);

        c.LoadConnections = I("load-connections", c.LoadConnections);
        c.LoadRatePerConn = D("load-rate", c.LoadRatePerConn);
        c.LoadInFlight = I("load-inflight", c.LoadInFlight);
        c.ValueBytes = I("value-bytes", c.ValueBytes);
        c.KeySpace = I("keyspace", c.KeySpace);

        c.CorrWorkers = I("corr-workers", c.CorrWorkers);
        c.CorrIntervalMs = D("corr-interval-ms", c.CorrIntervalMs);

        c.ProbeIntervalMs = D("probe-interval-ms", c.ProbeIntervalMs);
        c.ProbeInFlight = I("probe-inflight", c.ProbeInFlight);

        c.SyncTimeoutMs = I("sync-timeout", c.SyncTimeoutMs);
        c.ConnectTimeoutMs = I("connect-timeout", c.ConnectTimeoutMs);
        c.ConnectRetry = I("connect-retry", c.ConnectRetry);
        c.ConfigCheckSec = I("config-check-sec", c.ConfigCheckSec);
        c.KeepAliveSec = I("keepalive-sec", c.KeepAliveSec);
        c.AbortConnect = B("abort-connect", false);

        return c;
    }

    public ConfigurationOptions ToRedisOptions(string clientName)
    {
        var o = new ConfigurationOptions
        {
            AbortOnConnectFail = AbortConnect,
            ConnectTimeout = ConnectTimeoutMs,
            SyncTimeout = SyncTimeoutMs,
            AsyncTimeout = SyncTimeoutMs,
            ConnectRetry = ConnectRetry,
            ConfigCheckSeconds = ConfigCheckSec,
            KeepAlive = KeepAliveSec,
            Ssl = Tls,
            ClientName = clientName,
            AllowAdmin = false,
        };
        o.EndPoints.Add(Endpoint);
        if (!string.IsNullOrEmpty(Password)) o.Password = Password;
        if (!string.IsNullOrEmpty(User)) o.User = User;
        o.ReconnectRetryPolicy = new LinearRetry(ConnectTimeoutMs);

        if (Tls && TlsAllowUntrusted)
        {
            // Redis Enterprise test clusters normally present a self-signed cert.
            o.CertificateValidation += (_, _, _, _) => true;
        }

        return o;
    }

    public string Host => Endpoint.Split(':')[0];
}

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        Config cfg;
        try
        {
            cfg = Config.Parse(args);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine("Config error: " + ex.Message);
            Console.Error.WriteLine(
                "Usage: ReshardProbe --endpoint host:port [--password pw] [--tls] [--tag name] " +
                "[--outdir dir] [--markers path] [--duration-sec N] [--warmup-sec N] ...");
            return 2;
        }

        Directory.CreateDirectory(cfg.OutDir);
        Pacer.RaiseResolution();

        await using var rec = new Recorder(cfg.OutDir);
        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            rec.Event("run", "sigint", "cancelling");
            cts.Cancel();
        };

        // Three separate multiplexers so that a stalled load generator cannot
        // distort the availability measurement, and vice versa.
        ConnectionMultiplexer probeMux, loadMux, corrMux;
        try
        {
            rec.Event("run", "connecting", cfg.Endpoint);
            probeMux = await ConnectAsync(cfg, "probe", rec);
            loadMux = await ConnectAsync(cfg, "load", rec);
            corrMux = await ConnectAsync(cfg, "corr", rec);
        }
        catch (Exception ex)
        {
            rec.Event("run", "initial_connect_failed", ex.GetType().Name + ": " + ex.Message);
            await WriteMetaAsync(cfg, connected: false);
            return 3;
        }

        var muxes = new List<(string, ConnectionMultiplexer)>
        {
            ("probe", probeMux), ("load", loadMux), ("corr", corrMux),
        };

        // Anchor the clock as late as possible so t=0 is "load about to start".
        Clock.Anchor();
        rec.Event("run", "start",
            $"tag={cfg.Tag} endpoint={cfg.Endpoint} duration={cfg.DurationSec}s warmup={cfg.WarmupSec}s");
        await WriteMetaAsync(cfg, connected: true);

        var keyPrefix = $"rstest:{cfg.Tag}";
        var corrWorkers = Enumerable.Range(0, cfg.CorrWorkers)
            .Select(i => new CorrectnessWorker { Id = i, Key = $"{keyPrefix}:seq:{i}" })
            .ToList();

        // Reset counters so each arm starts from a known state. Must never be fatal:
        // if the endpoint is briefly unavailable at launch we still want the run to
        // proceed and record it, rather than losing the whole measurement.
        var corrDb = corrMux.GetDatabase();
        var resetOk = await ResetCountersAsync(corrDb, corrWorkers, rec);
        if (!resetOk)
        {
            // Counters may hold values from a previous arm; reconciliation would be
            // meaningless, so establish a baseline to subtract instead.
            foreach (var w in corrWorkers)
            {
                try
                {
                    var raw = await corrDb.StringGetAsync(w.Key);
                    w.BaselineValue = raw.IsNullOrEmpty ? 0 : (long)raw;
                }
                catch { w.BaselineValue = 0; }
            }
            rec.Event("run", "counters_baselined",
                "reset failed; using pre-run values as baseline: " +
                string.Join(" ", corrWorkers.Select(w => $"{w.Id}={w.BaselineValue}")));
        }

        var tasks = new List<Task>
        {
            AvailabilityProbe.RunAsync(probeMux.GetDatabase(), rec,
                cfg.ProbeIntervalMs, cfg.ProbeInFlight, cts.Token),
            ConnectivityWatcher.RunAsync(muxes, rec, cts.Token),
            DnsWatcher.RunAsync(cfg.Host, rec, cts.Token),
        };

        for (var i = 0; i < cfg.LoadConnections; i++)
        {
            var worker = i;
            tasks.Add(LoadGenerator.RunAsync(loadMux.GetDatabase(), rec, worker, keyPrefix,
                cfg.KeySpace, cfg.ValueBytes, cfg.LoadRatePerConn, cfg.LoadInFlight, cts.Token));
        }

        foreach (var w in corrWorkers)
            tasks.Add(w.RunAsync(corrDb, rec, cfg.CorrIntervalMs, cts.Token));

        if (!string.IsNullOrEmpty(cfg.MarkersPath))
            tasks.Add(new MarkerTail(cfg.MarkersPath, rec, cts).RunAsync(cts.Token));

        // Warmup boundary is only a label; load runs continuously so the baseline
        // is measured under identical conditions to the event window.
        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(cfg.WarmupSec), cts.Token);
                rec.Event("run", "warmup_complete", "baseline established; safe to trigger reshard");
            }
            catch (OperationCanceledException) { }
        });

        try { await Task.Delay(TimeSpan.FromSeconds(cfg.DurationSec), cts.Token); }
        catch (OperationCanceledException) { }

        rec.Event("run", "stopping", "draining in-flight operations");
        cts.Cancel();
        try { await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60)); }
        catch (Exception ex) { rec.Event("run", "drain_incomplete", ex.GetType().Name); }

        await ReconcileAsync(cfg, corrWorkers, rec);
        await WriteReconcileAsync(cfg, corrWorkers, rec);

        foreach (var (name, mux) in muxes)
        {
            rec.Event(name, "final_status", SafeStatus(mux));
            try { await mux.CloseAsync(); } catch { /* shutting down anyway */ }
            mux.Dispose();
        }

        rec.Event("run", "done", cfg.OutDir);
        Pacer.RestoreResolution();
        return 0;
    }

    /// <summary>
    /// Clears the reconciliation counters. Returns false if the endpoint could not be
    /// reached; callers fall back to baselining rather than aborting the run.
    /// </summary>
    private static async Task<bool> ResetCountersAsync(
        IDatabase db, List<CorrectnessWorker> workers, Recorder rec)
    {
        for (var attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                foreach (var w in workers) await db.KeyDeleteAsync(w.Key);
                rec.Event("run", "counters_reset", $"{workers.Count} keys (attempt {attempt})");
                return true;
            }
            catch (Exception ex)
            {
                rec.Event("run", "counters_reset_retry",
                    $"attempt={attempt} {ex.GetType().Name}: {ex.Message}");
                try { await Task.Delay(1000); } catch { }
            }
        }

        rec.Event("run", "counters_reset_failed", "endpoint unreachable at startup");
        return false;
    }

    private static async Task<ConnectionMultiplexer> ConnectAsync(Config cfg, string name, Recorder rec)
    {
        var mux = await ConnectionMultiplexer.ConnectAsync(cfg.ToRedisOptions($"reshardprobe-{cfg.Tag}-{name}"));
        HookEvents(mux, name, rec);
        return mux;
    }

    private static void HookEvents(ConnectionMultiplexer mux, string source, Recorder rec)
    {
        mux.ConnectionFailed += (_, e) =>
            rec.Event(source, "ConnectionFailed",
                $"{e.EndPoint} type={e.ConnectionType} failure={e.FailureType} ex={e.Exception?.GetType().Name}: {e.Exception?.Message}");

        mux.ConnectionRestored += (_, e) =>
            rec.Event(source, "ConnectionRestored",
                $"{e.EndPoint} type={e.ConnectionType} failure={e.FailureType}");

        mux.ErrorMessage += (_, e) =>
            rec.Event(source, "ErrorMessage", $"{e.EndPoint} {e.Message}");

        mux.InternalError += (_, e) =>
            rec.Event(source, "InternalError",
                $"{e.EndPoint} origin={e.Origin} ex={e.Exception?.GetType().Name}: {e.Exception?.Message}");

        mux.ConfigurationChanged += (_, e) =>
            rec.Event(source, "ConfigurationChanged", e.EndPoint?.ToString());

        mux.ConfigurationChangedBroadcast += (_, e) =>
            rec.Event(source, "ConfigurationChangedBroadcast", e.EndPoint?.ToString());
    }

    private static string SafeStatus(ConnectionMultiplexer mux)
    {
        try { return mux.GetStatus().Replace("\r", " ").Replace('\n', ' '); }
        catch (Exception ex) { return "status_unavailable: " + ex.Message; }
    }

    /// <summary>
    /// Read the true server-side counter for each correctness worker. Retries hard,
    /// because a failure here would destroy the run's most valuable metric.
    /// </summary>
    private static async Task ReconcileAsync(Config cfg, List<CorrectnessWorker> workers, Recorder rec)
    {
        var deadline = Clock.Ms + cfg.ReconcileBudgetSec * 1000.0;
        rec.Event("run", "reconcile_start", $"{workers.Count} keys budget={cfg.ReconcileBudgetSec}s");

        var opts = cfg.ToRedisOptions($"reshardprobe-{cfg.Tag}-reconcile");
        opts.SyncTimeout = 10000;
        opts.AsyncTimeout = 10000;
        opts.ConnectTimeout = 10000;
        opts.ConnectRetry = 3;

        for (var attempt = 1; Clock.Ms < deadline; attempt++)
        {
            try
            {
                await using var mux = await ConnectionMultiplexer.ConnectAsync(opts);
                var db = mux.GetDatabase();
                foreach (var w in workers)
                {
                    var raw = await db.StringGetAsync(w.Key);
                    w.ServerValue = raw.IsNullOrEmpty ? 0 : (long)raw;
                    w.ServerValueRead = true;
                }
                rec.Event("run", "reconcile_ok", $"attempt={attempt}");
                return;
            }
            catch (Exception ex)
            {
                rec.Event("run", "reconcile_retry",
                    $"attempt={attempt} {ex.GetType().Name}: {ex.Message}");
                try { await Task.Delay(TimeSpan.FromSeconds(5)); } catch { }
            }
        }

        rec.Event("run", "reconcile_failed",
            $"server-side counters unavailable after {cfg.ReconcileBudgetSec}s budget");
    }

    private static async Task WriteReconcileAsync(Config cfg, List<CorrectnessWorker> workers, Recorder rec)
    {
        var perWorker = workers.Select(w => new
        {
            worker = w.Id,
            key = w.Key,
            attempted = w.Attempted,
            acked = w.Acked,
            fail_definite = w.FailDefinite,
            fail_ambiguous = w.FailAmbiguous,
            fail_server = w.FailServer,
            max_acked_value = w.MaxAckedValue,
            baseline_value = w.BaselineValue,
            server_value = w.ServerValueRead ? w.ServerValue : (long?)null,
            applied_this_run = w.ServerValueRead ? w.AppliedThisRun : (long?)null,
            phantom_writes = w.PhantomWrites,
            lost_writes = w.LostWrites,
        }).ToList();

        var totals = new
        {
            attempted = workers.Sum(w => w.Attempted),
            acked = workers.Sum(w => w.Acked),
            fail_definite = workers.Sum(w => w.FailDefinite),
            fail_ambiguous = workers.Sum(w => w.FailAmbiguous),
            fail_server = workers.Sum(w => w.FailServer),
            applied_this_run = workers.All(w => w.ServerValueRead) ? workers.Sum(w => w.AppliedThisRun) : (long?)null,
            phantom_writes = workers.All(w => w.ServerValueRead) ? workers.Sum(w => w.PhantomWrites) : (long?)null,
            lost_writes = workers.All(w => w.ServerValueRead) ? workers.Sum(w => w.LostWrites) : (long?)null,
        };

        var payload = new
        {
            tag = cfg.Tag,
            t0_utc = Clock.T0Utc.ToString("O", CultureInfo.InvariantCulture),
            per_worker = perWorker,
            totals,
            note = "phantom_writes = server applied but client never got an ack (unsafe to blindly retry). " +
                   "lost_writes should always be 0.",
        };

        var path = Path.Combine(cfg.OutDir, "reconcile.json");
        await File.WriteAllTextAsync(path,
            JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true }));

        rec.Event("run", "reconcile_written",
            $"attempted={totals.attempted} acked={totals.acked} ambiguous={totals.fail_ambiguous} " +
            $"phantom={totals.phantom_writes} lost={totals.lost_writes}");
    }

    private static async Task WriteMetaAsync(Config cfg, bool connected)
    {
        static string? AsmVersion(string name)
        {
            try
            {
                var asm = AppDomain.CurrentDomain.GetAssemblies()
                    .FirstOrDefault(a => a.GetName().Name == name);
                asm ??= Assembly.Load(name);
                return asm.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
                       ?? asm.GetName().Version?.ToString();
            }
            catch { return null; }
        }

        var meta = new
        {
            tag = cfg.Tag,
            t0_utc = Clock.T0Utc.ToString("O", CultureInfo.InvariantCulture),
            connected,
            host = Environment.MachineName,
            os = Environment.OSVersion.VersionString,
            dotnet = Environment.Version.ToString(),
            packages = new
            {
                StackExchange_Redis = AsmVersion("StackExchange.Redis"),
                NRedisStack = AsmVersion("NRedisStack"),
            },
            config = new
            {
                cfg.Endpoint, cfg.Tls, cfg.DurationSec, cfg.WarmupSec,
                cfg.LoadConnections, cfg.LoadRatePerConn, cfg.LoadInFlight,
                cfg.ValueBytes, cfg.KeySpace,
                cfg.CorrWorkers, cfg.CorrIntervalMs,
                cfg.ProbeIntervalMs, cfg.ProbeInFlight,
                se_redis = new
                {
                    cfg.SyncTimeoutMs, cfg.ConnectTimeoutMs, cfg.ConnectRetry,
                    cfg.ConfigCheckSec, cfg.KeepAliveSec, cfg.AbortConnect,
                },
            },
        };

        await File.WriteAllTextAsync(Path.Combine(cfg.OutDir, "meta.json"),
            JsonSerializer.Serialize(meta, new JsonSerializerOptions { WriteIndented = true }));
    }
}

/// <summary>
/// Watches the endpoint's DNS answer. When the endpoint migrates nodes, the time
/// between the server-side rebind and the client seeing the new address is often a
/// large slice of the observed outage.
/// </summary>
public static class DnsWatcher
{
    public static async Task RunAsync(string host, Recorder rec, CancellationToken ct)
    {
        if (IPAddress.TryParse(host, out _))
        {
            rec.Event("dns", "literal_ip", host);
            return;
        }

        string? last = null;
        while (!ct.IsCancellationRequested)
        {
            string current;
            try
            {
                var addrs = await Dns.GetHostAddressesAsync(host, ct).ConfigureAwait(false);
                current = string.Join(" ", addrs.Select(a => a.ToString()).OrderBy(s => s, StringComparer.Ordinal));
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { current = "error: " + ex.Message; }

            if (current != last)
            {
                rec.Event("dns", last is null ? "resolve_initial" : "resolve_changed", current);
                last = current;
            }

            try { await Task.Delay(250, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { break; }
        }
    }
}
RESHARD_EOF_RESHARDPROBE_PROGRAM_CS
  cat > "$WORKDIR/node_driver.py" <<'RESHARD_EOF_NODE_DRIVER_PY'
#!/usr/bin/env python3
"""
Orchestrator for the proxy-policy resharding test, run ON a Redis Enterprise node.

Drives the real .NET client harness (StackExchange.Redis / NRedisStack) - this
script only handles cluster orchestration over REST and never touches the data
path, so the measured client behaviour is genuinely the .NET client's.

Per arm: create a fresh DB (1 master + 1 replica, dense placement, proxy_policy
set at create time), run the harness, reshard 1 -> N, record the server-side
topology timeline, then delete the DB. Shard count cannot be reduced on a normal
database, hence create/delete per arm.

Stdlib only. Analysis is deliberately left out: collect the results directory and
analyse it off-box.
"""

import argparse
import base64
import json
import os
import re
import shlex
import socket
import ssl
import subprocess
import sys
import time
import urllib.parse
import urllib.error
import urllib.request

# rs_test_helpers/infra/database/common.py - hashing policy applied when sharding
# is enabled. Identical across arms, or slot remapping is conflated with the
# proxy_policy variable under test.
DEFAULT_REGEX_RULES = [{"regex": ".*\\{(?<tag>.*)\\}.*"}, {"regex": "(?<tag>.*)"}]

# Redis Enterprise rejects anything else with HTTP 400 invalid_schema. Notably
# underscores are NOT allowed, so a database name cannot be derived from the
# underscored directory tag.
DB_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$")

POLICIES = ["single", "all-master-shards", "all-nodes"]
CONTROL_ARM = "control"


def log(msg):
    sys.stdout.write("[%s] %s\n" % (time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), msg))
    sys.stdout.flush()


class Rest(object):
    def __init__(self, host, port, user, password, timeout=30, verify_tls=False):
        self.base = "https://%s:%d" % (host, port)
        self.timeout = timeout
        self.auth = "Basic " + base64.b64encode(("%s:%s" % (user, password)).encode()).decode()
        self.ctx = ssl.create_default_context()
        if not verify_tls:
            # Redis Enterprise presents a self-signed certificate by default.
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE

    def _open_url(self, method, url, body=None):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Authorization": self.auth, "Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        return urllib.request.urlopen(req, timeout=self.timeout, context=self.ctx)

    def _open(self, method, path, body=None):
        return self._open_url(method, self.base + path, body)

    def get(self, path):
        try:
            with self._open("GET", path) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise SystemExit("REST auth failed (401): check --user/--password")
            if exc.code == 404:
                raise KeyError(path)
            raise SystemExit("REST GET %s -> HTTP %d: %s" % (path, exc.code, exc.read()[:300]))
        except urllib.error.URLError as exc:
            raise SystemExit("REST GET %s failed: %s" % (path, exc.reason))

    def mutate(self, method, path, body=None, max_redirects=3):
        """POST/PUT/DELETE, following redirects while preserving method and body.

        Redis Enterprise serves reads from any node but redirects mutations to the
        cluster master with a 307. urllib follows redirects for GET/HEAD only -
        HTTPRedirectHandler.redirect_request returns None for other methods - so
        without this the request fails on every non-master node, and a master
        change mid-run would break an in-flight matrix.
        """
        url = self.base + path
        cur_method, cur_body = method, body
        for hop in range(max_redirects + 1):
            try:
                with self._open_url(cur_method, url, cur_body) as r:
                    raw = r.read().decode("utf-8", "replace")
                    try:
                        return True, r.status, (json.loads(raw) if raw.strip() else {})
                    except ValueError:
                        return True, r.status, raw
            except urllib.error.HTTPError as exc:
                loc = exc.headers.get("Location") if exc.headers else None
                if exc.code in (301, 302, 303, 307, 308) and loc:
                    if hop >= max_redirects:
                        return False, exc.code, (
                            "too many redirects (>%d) for %s %s; last Location: %s"
                            % (max_redirects, method, path, loc))
                    target = urllib.parse.urljoin(url, loc)
                    parts = urllib.parse.urlsplit(target)
                    new_base = "%s://%s" % (parts.scheme, parts.netloc)
                    log("REST %s %s -> HTTP %d, following redirect to %s" % (
                        cur_method, url, exc.code, new_base))
                    # Remember the master so later mutations go straight there.
                    if new_base != self.base:
                        self.base = new_base
                    if exc.code == 303:
                        # Per HTTP semantics a 303 becomes a GET without a body.
                        cur_method, cur_body = "GET", None
                    url = target
                    continue
                return False, exc.code, exc.read().decode("utf-8", "replace")[:500]
            except urllib.error.URLError as exc:
                return False, 0, str(exc.reason)
            except OSError as exc:
                return False, 0, str(exc)
        return False, 0, "redirect handling fell through for %s %s" % (method, path)


ROLE_ROW_RE = re.compile(
    r"^\*?node:(\d+)\s+(\S+)\s+(\d+\.\d+\.\d+\.\d+)", re.MULTILINE)


def find_api_master(rest):
    """Address of the node that accepts REST mutations, i.e. the cluster master.

    Redis Enterprise serves reads from any node but 307s mutations to the master.
    Resolving it up front makes the intent explicit in the logs and avoids relying
    on redirect handling. Redirect following stays as a backstop for a master
    change mid-run.

    Returns (address, how) or (None, reason).
    """
    # Preferred: the API itself, which also works when run off-box.
    try:
        for n in rest.get("/v1/nodes"):
            for key in ("role", "node_role", "cluster_role"):
                if str(n.get(key, "")).lower() == "master":
                    addr = n.get("addr") or n.get("external_addr")
                    if addr:
                        return addr, "REST /v1/nodes %s field" % key
    # Rest.get raises SystemExit, which is NOT an Exception subclass, so catching
    # only Exception here would abort the run instead of falling back to rladmin.
    except (Exception, SystemExit) as exc:
        log("master discovery via REST failed (%s); trying rladmin" % exc)

    # Fallback: rladmin, available because we run on a node. Use the ROLE column,
    # NOT the leading '*' - that marks the local node, not the master.
    ok, out = _run("rladmin status nodes", timeout=60)
    if ok:
        for uid, role, addr in ROLE_ROW_RE.findall(out):
            if role.lower() == "master":
                return addr, "rladmin status nodes (node%s)" % uid
        return None, "rladmin ran but no node had ROLE=master"

    return None, "neither REST nor rladmin could identify the master"


def node_map(rest):
    return dict((int(n["uid"]), n.get("addr") or n.get("external_addr") or "?")
                for n in rest.get("/v1/nodes"))


def local_addresses():
    addrs = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            addrs.add(info[4][0])
    except Exception:
        pass
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))  # no packets sent; just resolves the route
        addrs.add(s.getsockname()[0])
        s.close()
    except Exception:
        pass
    return addrs


def local_node_uid(rest):
    mine = local_addresses()
    for uid, addr in node_map(rest).items():
        if addr in mine:
            return uid
    return None


def db_topology(rest, uid):
    bdb = rest.get("/v1/bdbs/%d" % uid)
    shards = [s for s in rest.get("/v1/shards") if int(s.get("bdb_uid", -1)) == uid]
    ep_addrs, dns_name, port = [], None, None
    for ep in bdb.get("endpoints") or []:
        dns_name = ep.get("dns_name") or dns_name
        port = ep.get("port") or port
        for a in ep.get("addr") or []:
            ep_addrs.append(str(a))
    return {
        "status": bdb.get("status"),
        "shards_count": bdb.get("shards_count"),
        "proxy_policy": bdb.get("proxy_policy"),
        "shards_placement": bdb.get("shards_placement"),
        "replication": bdb.get("replication"),
        "oss_sharding": bdb.get("oss_sharding", False),
        "master_nodes": sorted(int(s["node_uid"]) for s in shards if s.get("role") == "master"),
        "replica_nodes": sorted(int(s["node_uid"]) for s in shards if s.get("role") == "slave"),
        "endpoint_addrs": sorted(set(ep_addrs)),
        "dns_name": dns_name,
        "port": port,
    }


def describe(topo, nodes):
    def nn(uids):
        return ", ".join("node%d(%s)" % (u, nodes.get(u, "?")) for u in uids) or "none"
    return ("status=%s shards=%s policy=%s placement=%s\n"
            "      masters : %s\n      replicas: %s\n      endpoint: %s:%s addrs=%s" % (
                topo["status"], topo["shards_count"], topo["proxy_policy"],
                topo["shards_placement"], nn(topo["master_nodes"]),
                nn(topo["replica_nodes"]), topo["dns_name"], topo["port"],
                topo["endpoint_addrs"]))


def create_db(rest, name, policy, memory_size, db_password):
    if not DB_NAME_RE.match(name):
        raise SystemExit(
            "database name %r is invalid: must match %s (no underscores)"
            % (name, DB_NAME_RE.pattern))
    body = {"name": name, "type": "redis", "memory_size": memory_size,
            "shards_count": 1, "replication": True, "shards_placement": "dense",
            "proxy_policy": policy}
    if db_password:
        body["authentication_redis_pass"] = db_password
    ok, status, data = rest.mutate("POST", "/v1/bdbs", body)
    if not ok:
        raise SystemExit("DB create failed HTTP %s: %s" % (status, data))
    return int(data["uid"])


def wait_status(rest, uid, want="active", timeout=300):
    deadline = time.monotonic() + timeout
    last = "?"
    while time.monotonic() < deadline:
        last = str(rest.get("/v1/bdbs/%d" % uid).get("status"))
        if last == want:
            return
        if last.endswith("-failed"):
            raise SystemExit("bdb %d terminal status %s" % (uid, last))
        time.sleep(2)
    raise SystemExit("bdb %d status %s, wanted %s in %ds" % (uid, last, want, timeout))


def delete_db(rest, uid, timeout=300):
    ok, status, data = rest.mutate("DELETE", "/v1/bdbs/%d" % uid)
    if not ok:
        log("WARNING: delete bdb %d failed HTTP %s: %s" % (uid, status, data))
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            rest.get("/v1/bdbs/%d" % uid)
        except KeyError:
            log("deleted bdb %d" % uid)
            return
        time.sleep(2)
    log("WARNING: bdb %d still present after %ds" % (uid, timeout))


# --------------------------------------------------------------------------- #
# Node-local commands. Available because we run on a cluster node, and they
# expose things the REST API cannot.
# --------------------------------------------------------------------------- #

CLIENT_PROP_RE = re.compile(r"(\w+(?:-\w+)?)=(\S*)")


def _run(cmd, timeout=30):
    """Run a node command, retrying via sudo -i if it is not on PATH.
    Returns (ok, output)."""
    for attempt in (cmd, "sudo -i " + cmd):
        try:
            p = subprocess.Popen(attempt, shell=True, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            out, _ = p.communicate(timeout=timeout)
            text = out.decode("utf-8", "replace")
            if p.returncode == 0:
                return True, text
            last = text
        except subprocess.TimeoutExpired:
            p.kill()
            last = "timeout after %ss" % timeout
        except Exception as exc:
            last = str(exc)
    return False, last


def ccs_is_balanced(uid):
    """Flex-shard (ASM) scale-out completion signal. CCS-only: not exposed over
    REST. See rs_test_helpers/dmc/reshard_helper.py - 'enabled' means the
    scale-out has finished, 'disabled' means it is still rebalancing."""
    ok, out = _run("ccs-cli hget bdb:%d is_balanced" % uid)
    if not ok:
        return None
    val = out.strip().splitlines()[-1].strip() if out.strip() else ""
    return val if val in ("enabled", "disabled") else None


def capture_rladmin_status(path):
    ok, out = _run("rladmin status extra all", timeout=120)
    try:
        with open(path, "w") as fh:
            fh.write(out if ok else "rladmin status failed:\n" + out)
    except IOError:
        pass
    return ok


def proxy_conn_counts(uid, name_filter="reshardprobe"):
    """Our client connections per proxy, from every proxy serving the database.

    This is the clearest evidence for the proxy_policy question: under 'single'
    all our connections sit on one proxy address and must move wholesale when the
    endpoint re-binds; under all-master-shards / all-nodes they can be spread and
    partially survive. Keyed by laddr (the proxy-side local address).
    """
    ok, out = _run("bdb-cli %d --all-proxies client list" % uid, timeout=60)
    if not ok:
        return None
    counts = {}
    for line in out.splitlines():
        props = dict(CLIENT_PROP_RE.findall(line))
        if not props:
            continue
        if name_filter and name_filter not in props.get("name", ""):
            continue
        laddr = props.get("laddr", "?").split(":")[0]
        counts[laddr] = counts.get(laddr, 0) + 1
    return counts


def reshard_done(rest, uid, target, oss_sharding=False):
    """Completion detection.

    For a flex-shard/ASM database the shard count reaching the target does NOT
    mean the scale-out finished - is_balanced does. Using the wrong signal stops
    the measurement early and under-reports impact.
    """
    bdb = rest.get("/v1/bdbs/%d" % uid)
    if str(bdb.get("status")) != "active":
        return False
    shards = [s for s in rest.get("/v1/shards") if int(s.get("bdb_uid", -1)) == uid]
    masters = len([s for s in shards if s.get("role") == "master"])
    if masters < target:
        return False
    if oss_sharding:
        bal = ccs_is_balanced(uid)
        if bal is not None:
            return bal == "enabled"
        # Could not read CCS; fall back to shard count but the caller has warned.
    return True


def wait_for_event(path, kind, timeout, proc):
    """The harness flushes events.csv per line, so it can be tailed live."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with open(path) as fh:
                if any(kind in line for line in fh):
                    return True
        except IOError:
            pass
        time.sleep(0.25)
    return False


def run_arm(rest, args, policy):
    is_control = (policy == CONTROL_ARM)
    db_policy = "single" if is_control else policy
    tag = policy.replace("-", "_")
    outdir = os.path.join(args.outdir, tag)
    if not os.path.isdir(outdir):
        os.makedirs(outdir)
    markers = os.path.join(outdir, "markers.txt")
    open(markers, "w").close()
    topo_path = os.path.join(outdir, "topology.csv")

    nodes = node_map(rest)
    me = local_node_uid(rest)
    # tag (underscored) names the output directory; the database name must use the
    # hyphenated policy because RE forbids underscores.
    db_name = "%s-%s" % (args.db_prefix, policy)

    log("creating db %s (proxy_policy=%s)" % (db_name, db_policy))
    uid = create_db(rest, db_name, db_policy, args.memory_size, args.db_password)
    result = {"arm": policy, "bdb_uid": uid, "db_name": db_name, "local_node_uid": me}
    proc = None
    topo_fh = None
    pconn_fh = None

    try:
        wait_status(rest, uid, "active", args.create_timeout)
        pre = db_topology(rest, uid)
        log("pre-reshard topology:\n      %s" % describe(pre, nodes))
        result["pre"] = pre

        if len(pre["master_nodes"]) != 1:
            raise SystemExit("expected exactly 1 master, got %s" % pre["master_nodes"])
        if pre["oss_sharding"]:
            log("WARNING: oss_sharding=True (flex/ASM). Scale-out completion is "
                "is_balanced in CCS and not visible over REST, so completion may "
                "be detected early and impact under-reported.")

        # Only shards are a blocking problem: the load generator would compete for
        # CPU with the very shard being measured. A local *proxy* is not - under
        # all-nodes there is one on every node by definition, and under
        # all-master-shards there may be, so refusing on that basis would make
        # those policies untestable. Warn instead.
        if me is not None:
            shard_nodes = set(pre["master_nodes"]) | set(pre["replica_nodes"])
            on_shard = me in shard_nodes
            on_endpoint = nodes.get(me) in pre["endpoint_addrs"]

            if on_shard:
                role = "master" if me in pre["master_nodes"] else "replica"
                msg = ("node%d hosts a %s shard of the test DB, so the load "
                       "generator would compete with it for CPU" % (me, role))
                if not args.allow_colocated:
                    raise SystemExit("REFUSING: %s. Run on a node with no shards of "
                                     "this database, or pass --allow-colocated."
                                     % msg)
                log("WARNING: %s (proceeding due to --allow-colocated)" % msg)
            elif on_endpoint:
                log("client node = node%d: no shards here, but the endpoint is also "
                    "advertised on this node (expected under %s), so the client may "
                    "connect to the local proxy - see proxy_conns.csv for which "
                    "proxy actually served it" % (me, db_policy))
            else:
                log("client node = node%d, hosting neither shards nor the endpoint "
                    "of the test DB - ideal" % me)
        else:
            log("WARNING: could not identify this node; skipping co-location check")

        host, port = pre["dns_name"], pre["port"]
        if not host or not port:
            raise SystemExit("could not determine endpoint for bdb %d" % uid)
        try:
            resolved = sorted(set(i[4][0] for i in socket.getaddrinfo(host, int(port))))
            log("endpoint %s:%s resolves to %s" % (host, port, resolved))
        except Exception as exc:
            raise SystemExit("endpoint %s does not resolve on this node: %s" % (host, exc))

        cmd = [args.dotnet, args.probe_dll,
               "--endpoint", "%s:%s" % (host, port),
               "--tag", tag, "--outdir", outdir, "--markers", markers,
               "--duration-sec", str(args.max_duration),
               "--warmup-sec", str(args.warmup),
               "--load-connections", str(args.load_connections),
               "--load-rate", str(args.load_rate),
               "--corr-workers", str(args.corr_workers),
               "--sync-timeout", str(args.sync_timeout),
               "--connect-timeout", str(args.connect_timeout),
               "--config-check-sec", str(args.config_check_sec),
               "--keepalive-sec", str(args.keepalive_sec),
               "--reconcile-budget-sec", str(args.reconcile_budget)]
        if args.db_password:
            cmd += ["--password", args.db_password]

        log("starting .NET harness: %s" % " ".join(shlex.quote(c) for c in cmd[:6]))
        stdout = open(os.path.join(outdir, "harness_stdout.log"), "w")
        env = dict(os.environ)
        env["DOTNET_ROOT"] = os.path.dirname(args.dotnet)
        proc = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT, env=env)

        events = os.path.join(outdir, "events.csv")
        if not wait_for_event(events, "warmup_complete", args.warmup + 180, proc):
            tail = ""
            try:
                with open(os.path.join(outdir, "harness_stdout.log")) as fh:
                    tail = fh.read()[-2000:]
            except IOError:
                pass
            raise SystemExit("harness never reported warmup_complete. Output:\n%s" % tail)
        log("warmup complete; baseline established")

        capture_rladmin_status(os.path.join(outdir, "rladmin_status_pre.txt"))

        topo_fh = open(topo_path, "w")
        topo_fh.write("utc,elapsed_s,status,shards_count,master_nodes,replica_nodes,endpoint_addrs\n")
        pconn_fh = open(os.path.join(outdir, "proxy_conns.csv"), "w")
        pconn_fh.write("utc,elapsed_s,proxy_addr,our_connections\n")
        t0 = time.monotonic()
        pconn_state = {"next": 0.0}

        def sample():
            now = time.monotonic() - t0
            try:
                t = db_topology(rest, uid)
            except Exception as exc:
                log("topology poll error: %s" % exc)
                t = None
            if t is not None:
                topo_fh.write('%s,%.3f,%s,%s,"%s","%s","%s"\n' % (
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    now, t["status"], t["shards_count"],
                    t["master_nodes"], t["replica_nodes"], t["endpoint_addrs"]))
                topo_fh.flush()

            # bdb-cli forks a process, so sample it on a slower cadence than the
            # REST poll to keep our own overhead off the measurement.
            if not args.no_proxy_conns and now >= pconn_state["next"]:
                pconn_state["next"] = now + args.proxy_conn_interval
                counts = proxy_conn_counts(uid)
                if counts is not None:
                    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    if not counts:
                        pconn_fh.write("%s,%.3f,none,0\n" % (stamp, now))
                    for addr, c in sorted(counts.items()):
                        pconn_fh.write("%s,%.3f,%s,%d\n" % (stamp, now, addr, c))
                    pconn_fh.flush()
            return t

        def mark(text):
            with open(markers, "a") as fh:
                fh.write(text + "\n")

        if is_control:
            log("CONTROL arm: no reshard, holding %ds" % args.control_hold)
            mark("control_hold_start")
            end = time.monotonic() + args.control_hold
            while time.monotonic() < end:
                sample()
                time.sleep(args.poll_interval)
            mark("control_hold_end")
        else:
            mark("reshard_trigger 1->%d" % args.target_shards)
            log("TRIGGERING reshard 1 -> %d" % args.target_shards)
            ok, status, data = rest.mutate("PUT", "/v1/bdbs/%d" % uid, {
                "sharding": True, "shards_count": args.target_shards,
                "shard_key_regex": DEFAULT_REGEX_RULES})
            result["reshard_http"] = status
            if not ok:
                mark("reshard_trigger_failed")
                raise SystemExit("reshard trigger failed HTTP %s: %s" % (status, data))

            prev, done_at = None, None
            deadline = time.monotonic() + args.reshard_timeout
            while time.monotonic() < deadline:
                t = sample()
                if t is not None:
                    sig = (t["status"], t["shards_count"], tuple(t["master_nodes"]),
                           tuple(t["endpoint_addrs"]))
                    if sig != prev:
                        mark("topology status=%s shards=%s masters=%s endpoint=%s" % (
                            t["status"], t["shards_count"], t["master_nodes"],
                            t["endpoint_addrs"]))
                        prev = sig
                try:
                    if reshard_done(rest, uid, args.target_shards,
                                    oss_sharding=pre["oss_sharding"]):
                        done_at = time.monotonic() - t0
                        mark("reshard_complete elapsed_s=%.3f" % done_at)
                        log("reshard complete after %.1fs" % done_at)
                        break
                except Exception as exc:
                    log("completion check error: %s" % exc)
                time.sleep(args.poll_interval)

            if done_at is None:
                mark("reshard_timeout")
                log("WARNING: reshard did not complete within %ds" % args.reshard_timeout)
            result["reshard_elapsed_s"] = done_at

            log("holding %ds for the recovery tail" % args.tail)
            end = time.monotonic() + args.tail
            while time.monotonic() < end:
                sample()
                time.sleep(args.poll_interval)

            post = db_topology(rest, uid)
            log("post-reshard topology:\n      %s" % describe(post, nodes))
            result["post"] = post
            capture_rladmin_status(os.path.join(outdir, "rladmin_status_post.txt"))

        # Clean stop so the harness drains and reconciles; killing it would lose
        # the phantom/lost write counts, which are the point of the exercise.
        log("signalling harness to stop")
        mark("STOP")
        try:
            proc.wait(timeout=args.reconcile_budget + 180)
        except subprocess.TimeoutExpired:
            log("WARNING: harness did not exit in time; terminating")
            proc.terminate()
        proc = None

        rec = os.path.join(outdir, "reconcile.json")
        if os.path.exists(rec):
            with open(rec) as fh:
                result["reconcile"] = json.load(fh).get("totals")
            log("reconcile totals: %s" % json.dumps(result["reconcile"]))
        else:
            log("WARNING: no reconcile.json produced")

    finally:
        for fh in (topo_fh, pconn_fh):
            if fh is not None:
                try:
                    fh.close()
                except Exception:
                    pass
        if proc is not None and proc.poll() is None:
            log("cleaning up harness process")
            proc.terminate()
        if not args.keep_db:
            delete_db(rest, uid)
        else:
            log("--keep-db set; leaving bdb %d" % uid)

    with open(os.path.join(outdir, "arm_result.json"), "w") as fh:
        json.dump(result, fh, indent=2)
    return result


def cmd_check(rest, args):
    log("cluster: %s" % rest.get("/v1/cluster").get("name"))
    me = local_node_uid(rest)
    log("this host looks like node %s (local addrs: %s)" % (
        me, ", ".join(sorted(local_addresses()))))
    for n in sorted(rest.get("/v1/nodes"), key=lambda x: int(x["uid"])):
        log("  node%-3s addr=%-15s status=%-10s shards=%-4s cores=%-3s" % (
            n["uid"], n.get("addr"), n.get("status"), n.get("shard_count"), n.get("cores")))
    bdbs = rest.get("/v1/bdbs")
    log("databases: %d" % len(bdbs))
    for b in bdbs:
        log("  bdb%-4s name=%-24s shards=%-4s policy=%-18s status=%s" % (
            b["uid"], b.get("name"), b.get("shards_count"), b.get("proxy_policy"),
            b.get("status")))
    return 0


def cmd_dryrun(rest, args):
    nodes = node_map(rest)
    me = local_node_uid(rest)
    uid = create_db(rest, "%s-dryrun" % args.db_prefix, "single",
                    args.memory_size, args.db_password)
    try:
        wait_status(rest, uid, "active", args.create_timeout)
        topo = db_topology(rest, uid)
        log("topology:\n      %s" % describe(topo, nodes))
        if topo["oss_sharding"]:
            log("NOTE: oss_sharding=True (flex/ASM); REST cannot observe is_balanced")
        if me is not None and (me in topo["master_nodes"] or
                               nodes.get(me) in topo["endpoint_addrs"]):
            log("WARNING: this node hosts the DB master/endpoint. Use another node "
                "for the real run, or pass --allow-colocated.")
        else:
            log("this node is not hosting the DB - good for the real run")
        try:
            resolved = sorted(set(i[4][0] for i in socket.getaddrinfo(
                topo["dns_name"], int(topo["port"]))))
            log("endpoint %s:%s resolves to %s" % (topo["dns_name"], topo["port"], resolved))
        except Exception as exc:
            log("WARNING: endpoint does not resolve here: %s" % exc)
    finally:
        if not args.keep_db:
            delete_db(rest, uid)
    return 0


def cmd_arm(rest, args):
    run_arm(rest, args, args.policy)
    return 0


def cmd_matrix(rest, args):
    arms = [CONTROL_ARM] + POLICIES
    results = []
    for i, arm in enumerate(arms):
        log("=" * 70)
        log("ARM %d/%d: %s" % (i + 1, len(arms), arm))
        log("=" * 70)
        try:
            results.append(run_arm(rest, args, arm))
        except SystemExit as exc:
            log("ARM %s FAILED: %s" % (arm, exc))
            results.append({"arm": arm, "error": str(exc)})
        except Exception as exc:
            log("ARM %s ERROR: %s: %s" % (arm, type(exc).__name__, exc))
            results.append({"arm": arm, "error": "%s: %s" % (type(exc).__name__, exc)})
        if i < len(arms) - 1:
            log("settling %ds before next arm" % args.between_arms)
            time.sleep(args.between_arms)
    with open(os.path.join(args.outdir, "matrix_results.json"), "w") as fh:
        json.dump(results, fh, indent=2)
    log("matrix complete. Collect with: tar czf results.tgz %s" % args.outdir)
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["check", "dryrun", "arm", "matrix"])
    p.add_argument("--policy", choices=POLICIES + [CONTROL_ARM])

    p.add_argument("--rest-host", default="localhost")
    p.add_argument("--rest-port", type=int, default=9443)
    p.add_argument("--no-master-discovery", action="store_true",
                   help="do not look up the cluster master first; rely only on "
                        "following 307 redirects")
    p.add_argument("--user", default=os.environ.get("RL_REST_USER"))
    p.add_argument("--password", default=os.environ.get("RL_REST_PASSWORD"))

    p.add_argument("--outdir", default="results")
    p.add_argument("--dotnet", default=os.path.expanduser("~/.dotnet/dotnet"))
    p.add_argument("--probe-dll", required=True)

    p.add_argument("--db-prefix", default="reshardtest")
    p.add_argument("--db-password", default=os.environ.get("RL_DB_PASSWORD", ""))
    p.add_argument("--memory-size", type=int, default=1073741824)
    p.add_argument("--target-shards", type=int, default=2)
    p.add_argument("--keep-db", action="store_true")
    p.add_argument("--allow-colocated", action="store_true")

    p.add_argument("--warmup", type=int, default=60)
    p.add_argument("--tail", type=int, default=120)
    p.add_argument("--control-hold", type=int, default=180)
    p.add_argument("--max-duration", type=int, default=1800)
    p.add_argument("--reshard-timeout", type=int, default=900)
    p.add_argument("--create-timeout", type=int, default=300)
    p.add_argument("--reconcile-budget", type=int, default=120)
    p.add_argument("--between-arms", type=int, default=30)
    p.add_argument("--poll-interval", type=float, default=0.5)
    p.add_argument("--proxy-conn-interval", type=float, default=2.0,
                   help="seconds between 'bdb-cli --all-proxies client list' samples")
    p.add_argument("--no-proxy-conns", action="store_true",
                   help="skip per-proxy connection sampling (loses the clearest "
                        "evidence of endpoint/proxy migration)")

    p.add_argument("--load-connections", type=int, default=4)
    p.add_argument("--load-rate", type=float, default=200)
    p.add_argument("--corr-workers", type=int, default=4)
    p.add_argument("--sync-timeout", type=int, default=5000)
    p.add_argument("--connect-timeout", type=int, default=5000)
    p.add_argument("--config-check-sec", type=int, default=60)
    p.add_argument("--keepalive-sec", type=int, default=60)

    args = p.parse_args()
    for name, val in (("--user", args.user), ("--password", args.password)):
        if not val:
            p.error("missing required: %s" % name)
    if args.command == "arm" and not args.policy:
        p.error("--policy is required for 'arm'")
    if not os.path.isdir(args.outdir):
        os.makedirs(args.outdir)

    rest = Rest(args.rest_host, args.rest_port, args.user, args.password)

    # Point mutations at the cluster master up front. Harmless for read-only
    # commands, and it makes the target explicit rather than implicit in a redirect.
    if not args.no_master_discovery:
        addr, how = find_api_master(rest)
        if addr:
            new_base = "https://%s:%d" % (addr, args.rest_port)
            if new_base != rest.base:
                log("API master is %s (via %s); directing REST there (was %s)" % (
                    addr, how, rest.base))
                rest.base = new_base
            else:
                log("API master is %s (via %s); already targeting it" % (addr, how))
        else:
            log("could not determine the API master (%s); relying on 307 "
                "redirect following" % how)

    return {"check": cmd_check, "dryrun": cmd_dryrun, "arm": cmd_arm,
            "matrix": cmd_matrix}[args.command](rest, args)


if __name__ == "__main__":
    sys.exit(main())
RESHARD_EOF_NODE_DRIVER_PY
}

info() { echo "[bundle] $*"; }

# Digest of the C# sources, so we can tell whether the built harness matches
# them. mtime is useless here: extract() rewrites the files on every run.
sources_digest() {
  cat "$WORKDIR"/ReshardProbe/*.csproj "$WORKDIR"/ReshardProbe/*.cs 2>/dev/null |
    { md5sum 2>/dev/null || cksum; } | awk '{print $1}'
}

extract() {
  info "extracting sources to $WORKDIR"
  mkdir -p "$WORKDIR/ReshardProbe" || die "cannot create $WORKDIR"
  _write_payloads
  # Payloads are emitted with Unix line endings, but be defensive in case the
  # copy/paste round-trip introduced CRLF.
  for f in "$WORKDIR"/ReshardProbe/*.cs "$WORKDIR"/ReshardProbe/*.csproj "$WORKDIR"/node_driver.py; do
    [ -f "$f" ] && sed -i 's/\r$//' "$f"
  done
}

cmd_setup() {
  info "bundle version $BUNDLE_VERSION"
  extract

  if [ ! -x "$DOTNET" ]; then
    info "installing .NET SDK 8 into $DOTNET_DIR (user-local, no root)"
    command -v curl >/dev/null 2>&1 || command -v wget >/dev/null 2>&1 \
      || die "need curl or wget to fetch the .NET installer"
    if command -v curl >/dev/null 2>&1; then
      curl -fsSL https://dot.net/v1/dotnet-install.sh -o "$WORKDIR/dotnet-install.sh" \
        || die "could not download dotnet-install.sh (no outbound HTTPS to dot.net?)"
    else
      wget -qO "$WORKDIR/dotnet-install.sh" https://dot.net/v1/dotnet-install.sh \
        || die "could not download dotnet-install.sh (no outbound HTTPS to dot.net?)"
    fi
    bash "$WORKDIR/dotnet-install.sh" --channel 8.0 --install-dir "$DOTNET_DIR" --no-path \
      || die ".NET SDK install failed"
  else
    info ".NET SDK already present at $DOTNET_DIR"
  fi

  export DOTNET_ROOT="$DOTNET_DIR"
  export PATH="$DOTNET_DIR:$PATH"
  export DOTNET_CLI_TELEMETRY_OPTOUT=1
  export DOTNET_NOLOGO=1

  "$DOTNET" --list-sdks || die "dotnet not runnable"

  info "building harness (restores StackExchange.Redis 2.8.x + NRedisStack from nuget.org)"
  ( cd "$WORKDIR/ReshardProbe" && "$DOTNET" build -c Release --nologo ) \
    || die "build failed (no outbound HTTPS to nuget.org?)"

  [ -f "$DLL" ] || die "expected $DLL after build"
  sources_digest > "$WORKDIR/.build_digest"
  info "OK. Harness built at $DLL"
  info "Next: bash $0 check --user <u> --password <p>"
}

need_built() {
  [ -x "$DOTNET" ] || die "run 'bash $0 setup' first (.NET not installed)"
  [ -f "$DLL" ]    || die "run 'bash $0 setup' first (harness not built)"
  [ -f "$WORKDIR/node_driver.py" ] || die "run 'bash $0 setup' first (driver missing)"
}

run_driver() {
  info "bundle version $BUNDLE_VERSION"
  need_built
  local sub="$1"; shift
  export DOTNET_ROOT="$DOTNET_DIR"

  # Re-extract on every run. Otherwise a newer bundle still executes the
  # node_driver.py left behind by a previous 'setup', silently running stale
  # code - which is exactly how an already-fixed bug appeared to persist.
  extract

  # Only 'setup' compiles the C# sources, so warn if what is on disk no longer
  # matches what the harness was built from.
  if [ -f "$WORKDIR/.build_digest" ]; then
    if [ "$(sources_digest)" != "$(cat "$WORKDIR/.build_digest")" ]; then
      echo "[bundle] WARNING: C# sources differ from the built harness." >&2
      echo "[bundle]          Run: bash $0 setup   to rebuild before measuring." >&2
    fi
  fi

  python3 "$WORKDIR/node_driver.py" "$sub" --dotnet "$DOTNET" --probe-dll "$DLL" --outdir "$RESULTS" "$@"
}

cmd_collect() {
  local tgz="$HOME/reshard_results_$(date -u +%Y%m%dT%H%M%SZ).tgz"
  [ -d "$RESULTS" ] || die "no results directory at $RESULTS"
  tar czf "$tgz" -C "$(dirname "$RESULTS")" "$(basename "$RESULTS")" \
    || die "could not create $tgz"
  echo
  echo "Results archived: $tgz"
  echo "Copy that back for analysis."
}

usage() {
  sed -n '2,45p' "$0"
  echo
  echo "Subcommands: setup | check | dryrun | arm --policy <p> | matrix | collect | version"
  exit 1
}

[ $# -ge 1 ] || usage
SUB="$1"; shift || true

case "$SUB" in
  version) echo "$BUNDLE_VERSION" ;;
  setup)   cmd_setup ;;
  collect) cmd_collect ;;
  check|dryrun|arm|matrix) run_driver "$SUB" "$@" ;;
  -h|--help|help) usage ;;
  *) die "unknown subcommand '$SUB' (try: setup, check, dryrun, arm, matrix, collect)" ;;
esac
