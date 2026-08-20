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
