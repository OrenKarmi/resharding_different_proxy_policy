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
