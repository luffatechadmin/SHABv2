using System.Collections.Concurrent;
using System.Globalization;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

static partial class Program
{
  private sealed class DashboardRuntimeState
  {
    public DateTimeOffset? LastSyncStartedAtUtc { get; set; }
    public DateTimeOffset? LastSyncFinishedAtUtc { get; set; }
    public string LastSyncResult { get; set; } = "never";
    public string? LastSyncError { get; set; }
    public DateTimeOffset? NextSyncAtUtc { get; set; }
    public DateTimeOffset? LastSupabaseSyncStartedAtUtc { get; set; }
    public DateTimeOffset? LastSupabaseSyncFinishedAtUtc { get; set; }
    public string LastSupabaseSyncResult { get; set; } = "never";
    public string? LastSupabaseSyncError { get; set; }
    public int LastSupabaseUpsertedCount { get; set; }
    public int? LastSupabaseTotalCount { get; set; }
    public DateTimeOffset? LastSupabaseTotalCountAtUtc { get; set; }
  }

  private sealed class RingBufferTextWriter : TextWriter
  {
    private readonly TextWriter _inner;
    private readonly ConcurrentQueue<string> _lines = new();
    private readonly int _maxLines;
    private readonly object _gate = new();

    public RingBufferTextWriter(TextWriter inner, int maxLines)
    {
      _inner = inner;
      _maxLines = Math.Max(50, maxLines);
    }

    public override Encoding Encoding => _inner.Encoding;

    public override void WriteLine(string? value)
    {
      _inner.WriteLine(value);
      if (value is null) return;
      var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
      var normalized = value.Replace("\r\n", "\n");
      var parts = normalized.Split('\n');
      foreach (var p in parts)
      {
        Enqueue($"{ts} {p}");
      }
    }

    public override void Write(char value)
    {
      _inner.Write(value);
    }

    public override void Write(string? value)
    {
      _inner.Write(value);
    }

    private void Enqueue(string line)
    {
      _lines.Enqueue(line);
      lock (_gate)
      {
        while (_lines.Count > _maxLines && _lines.TryDequeue(out _)) { }
      }
    }

    public string[] Snapshot()
    {
      return _lines.ToArray();
    }
  }

  private sealed class MultiTextWriter : TextWriter
  {
    private readonly TextWriter[] _writers;

    public MultiTextWriter(params TextWriter[] writers)
    {
      _writers = writers?.Where(w => w is not null).ToArray() ?? Array.Empty<TextWriter>();
    }

    public override Encoding Encoding => _writers.Length > 0 ? _writers[0].Encoding : Encoding.UTF8;

    public override void WriteLine(string? value)
    {
      foreach (var w in _writers)
      {
        try { w.WriteLine(value); } catch { }
      }
    }

    public override void Write(char value)
    {
      foreach (var w in _writers)
      {
        try { w.Write(value); } catch { }
      }
    }

    public override void Write(string? value)
    {
      foreach (var w in _writers)
      {
        try { w.Write(value); } catch { }
      }
    }

    public override void Flush()
    {
      foreach (var w in _writers)
      {
        try { w.Flush(); } catch { }
      }
    }
  }

  private sealed class TimestampTextWriter : TextWriter
  {
    private readonly TextWriter _inner;

    public TimestampTextWriter(TextWriter inner)
    {
      _inner = inner;
    }

    public override Encoding Encoding => _inner.Encoding;

    public override void WriteLine(string? value)
    {
      var ts = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
      _inner.WriteLine($"{ts} {value}");
    }

    public override void Write(char value) => _inner.Write(value);
    public override void Write(string? value) => _inner.Write(value);
    public override void Flush() => _inner.Flush();
  }

  private static string? ResolveStaffCsvPath()
  {
    var env = (Environment.GetEnvironmentVariable("WL10_STAFF_CSV_PATH") ?? "").Trim();
    if (!string.IsNullOrWhiteSpace(env)) return env;

    static string? SearchUp(string startDir)
    {
      if (string.IsNullOrWhiteSpace(startDir)) return null;
      try
      {
        var dir = startDir;
        for (var i = 0; i < 10 && !string.IsNullOrWhiteSpace(dir); i++)
        {
          var candidate = Path.Combine(dir, "Database", "Database - Staff WL10.csv");
          if (File.Exists(candidate)) return candidate;
          var parent = Directory.GetParent(dir);
          if (parent is null) break;
          dir = parent.FullName;
        }
      }
      catch { }
      return null;
    }

    var fromCwd = SearchUp(Directory.GetCurrentDirectory());
    if (!string.IsNullOrWhiteSpace(fromCwd)) return fromCwd;

    var fromBase = SearchUp(AppContext.BaseDirectory);
    if (!string.IsNullOrWhiteSpace(fromBase)) return fromBase;

    return null;
  }

  private static Task<T> RunStaAsync<T>(Func<T> fn, CancellationToken ct)
  {
    var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
    var thread = new Thread(() =>
    {
      try
      {
        var res = fn();
        tcs.TrySetResult(res);
      }
      catch (Exception ex)
      {
        tcs.TrySetException(ex);
      }
    });
    thread.IsBackground = true;
    thread.SetApartmentState(ApartmentState.STA);
    thread.Start();
    if (ct.CanBeCanceled)
    {
      ct.Register(() => tcs.TrySetCanceled(ct));
    }
    return tcs.Task;
  }

  private static async Task<(bool ok, List<(string Id, string Name, string Dept, string Status, string ShiftPattern)> rows, string? error)> TryLoadStaffFromSupabase(AppConfig cfg, string? anonKey, CancellationToken ct)
  {
    var serviceKeyRaw = (cfg.SupabaseServiceRoleKey ?? string.Empty).Trim();
    var anonRaw = (anonKey ?? string.Empty).Trim();
    if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || (serviceKeyRaw.Length == 0 && anonRaw.Length == 0))
    {
      return (false, new List<(string, string, string, string, string)>(), "Supabase not configured");
    }

    static async Task<(bool ok, List<(string Id, string Name, string Dept, string Status, string ShiftPattern)> rows, string? error, int statusCode)> FetchAsync(AppConfig cfg, string apiKey, bool includeShiftPattern, CancellationToken ct)
    {
      var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
      var cols = includeShiftPattern
        ? "id,full_name,department,status,shift_pattern"
        : "id,full_name,department,status";
      var url = $"{baseUrl}/rest/v1/staff?select={Uri.EscapeDataString(cols)}&order=id.asc&limit=5000";
      using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
      using var req = new HttpRequestMessage(HttpMethod.Get, url);
      req.Headers.TryAddWithoutValidation("apikey", apiKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
      using var resp = await http.SendAsync(req, ct);
      var body = await resp.Content.ReadAsStringAsync(ct);
      if (!resp.IsSuccessStatusCode)
      {
        return (false, new List<(string, string, string, string, string)>(), body.Length > 350 ? body[..350] : body, (int)resp.StatusCode);
      }

      using var doc = JsonDocument.Parse(body);
      if (doc.RootElement.ValueKind != JsonValueKind.Array)
      {
        return (false, new List<(string, string, string, string, string)>(), "Unexpected response shape", 200);
      }

      var list = new List<(string Id, string Name, string Dept, string Status, string ShiftPattern)>(capacity: 256);
      foreach (var el in doc.RootElement.EnumerateArray())
      {
        var id = el.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.String ? (idEl.GetString() ?? "") : "";
        if (id.Length == 0) continue;
        var name = el.TryGetProperty("full_name", out var nEl) && nEl.ValueKind == JsonValueKind.String ? (nEl.GetString() ?? "") : "";
        var dept = el.TryGetProperty("department", out var dEl) && dEl.ValueKind == JsonValueKind.String ? (dEl.GetString() ?? "") : "";
        var status = el.TryGetProperty("status", out var sEl) && sEl.ValueKind == JsonValueKind.String ? (sEl.GetString() ?? "") : "";
        var sp = includeShiftPattern && el.TryGetProperty("shift_pattern", out var spEl) && spEl.ValueKind == JsonValueKind.String ? (spEl.GetString() ?? "") : "";
        list.Add((id, name, dept, status, sp));
      }
      return (true, list, null, 200);
    }

    async Task<(bool ok, List<(string Id, string Name, string Dept, string Status, string ShiftPattern)> rows, string? error, int statusCode)> FetchWithFallback(bool includeShiftPattern)
    {
      if (serviceKeyRaw.Length > 0)
      {
        var r1 = await FetchAsync(cfg, serviceKeyRaw, includeShiftPattern, ct);
        if (r1.ok) return r1;
        if (r1.statusCode == 401 && anonRaw.Length > 0)
        {
          return await FetchAsync(cfg, anonRaw, includeShiftPattern, ct);
        }
        return r1;
      }
      return await FetchAsync(cfg, anonRaw, includeShiftPattern, ct);
    }

    var r1 = await FetchWithFallback(includeShiftPattern: true);
    if (!r1.ok && r1.error is not null && r1.error.Contains("shift_pattern", StringComparison.OrdinalIgnoreCase))
    {
      var r2 = await FetchWithFallback(includeShiftPattern: false);
      if (!r2.ok) return (false, r2.rows, r2.error);
      return (true, r2.rows.Select(x => (x.Id, x.Name, x.Dept, x.Status, "")).ToList(), null);
    }
    return (r1.ok, r1.rows, r1.error);
  }

  private static async Task<(bool ok, ShiftPatternRow[] rows, string? error)> TryLoadShiftPatternsFromSupabase(AppConfig cfg, string? anonKey, CancellationToken ct)
  {
    var serviceKeyRaw = (cfg.SupabaseServiceRoleKey ?? string.Empty).Trim();
    var anonRaw = (anonKey ?? string.Empty).Trim();
    if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || (serviceKeyRaw.Length == 0 && anonRaw.Length == 0))
    {
      return (false, Array.Empty<ShiftPatternRow>(), "Supabase not configured");
    }
    var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
    var url = $"{baseUrl}/rest/v1/shift_patterns?select=pattern,working_days,working_hours,break_time,notes&order=pattern.asc&limit=500";
    async Task<(bool ok, string body, int statusCode)> FetchAsync(string apiKey)
    {
      using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
      using var req = new HttpRequestMessage(HttpMethod.Get, url);
      req.Headers.TryAddWithoutValidation("apikey", apiKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
      using var resp = await http.SendAsync(req, ct);
      var body = await resp.Content.ReadAsStringAsync(ct);
      return (resp.IsSuccessStatusCode, body, (int)resp.StatusCode);
    }

    var body = string.Empty;
    var ok = false;
    var statusCode = 0;
    if (serviceKeyRaw.Length > 0)
    {
      var r1 = await FetchAsync(serviceKeyRaw);
      ok = r1.ok; body = r1.body; statusCode = r1.statusCode;
      if (!ok && statusCode == 401 && anonRaw.Length > 0)
      {
        var r2 = await FetchAsync(anonRaw);
        ok = r2.ok; body = r2.body; statusCode = r2.statusCode;
      }
    }
    else
    {
      var r = await FetchAsync(anonRaw);
      ok = r.ok; body = r.body; statusCode = r.statusCode;
    }

    if (!ok)
    {
      return (false, Array.Empty<ShiftPatternRow>(), body.Length > 350 ? body[..350] : body);
    }
    using var doc = JsonDocument.Parse(body);
    if (doc.RootElement.ValueKind != JsonValueKind.Array) return (false, Array.Empty<ShiftPatternRow>(), "Unexpected response shape");
    var list = new List<ShiftPatternRow>(capacity: 64);
    foreach (var el in doc.RootElement.EnumerateArray())
    {
      var pattern = el.TryGetProperty("pattern", out var pEl) && pEl.ValueKind == JsonValueKind.String ? (pEl.GetString() ?? "") : "";
      if (pattern.Length == 0) continue;
      var wd = el.TryGetProperty("working_days", out var wdEl) && wdEl.ValueKind == JsonValueKind.String ? (wdEl.GetString() ?? "") : "";
      var wh = el.TryGetProperty("working_hours", out var whEl) && whEl.ValueKind == JsonValueKind.String ? (whEl.GetString() ?? "") : "";
      var br = el.TryGetProperty("break_time", out var bEl) && bEl.ValueKind == JsonValueKind.String ? (bEl.GetString() ?? "") : "";
      var notes = el.TryGetProperty("notes", out var nEl) && nEl.ValueKind == JsonValueKind.String ? (nEl.GetString() ?? "") : "";
      list.Add(new ShiftPatternRow(pattern, wd, wh, br, notes));
    }
    return (true, list.ToArray(), null);
  }

  private static async Task RunDashboard(AppConfig initialConfig, string statePath, string[] args)
  {
    var user = (Environment.GetEnvironmentVariable("WL10_DASHBOARD_USER") ?? "superadmin").Trim();
    var pass = (Environment.GetEnvironmentVariable("WL10_DASHBOARD_PASSWORD") ?? "abcd1234").Trim();

    var bind = (Environment.GetEnvironmentVariable("WL10_DASHBOARD_BIND") ?? "0.0.0.0").Trim();
    if (string.Equals(bind, "localhost", StringComparison.OrdinalIgnoreCase)) bind = "127.0.0.1";
    if (string.Equals(bind, "127.0.0.1", StringComparison.OrdinalIgnoreCase)) bind = "0.0.0.0";
    var portFromArg = GetArgValue(args, "--dashboard-port");
    var portFromEnv = (Environment.GetEnvironmentVariable("WL10_DASHBOARD_PORT") ?? "").Trim();
    var port = 5099;
    if (!string.IsNullOrWhiteSpace(portFromArg) && int.TryParse(portFromArg, out var p1)) port = p1;
    else if (!string.IsNullOrWhiteSpace(portFromEnv) && int.TryParse(portFromEnv, out var p2)) port = p2;

    var urls = $"http://{bind}:{port}";

    var originalOut = Console.Out;
    var originalErr = Console.Error;
    TextWriter? fileWriter = null;
    try
    {
      var logDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "WL10Middleware", "Logs");
      Directory.CreateDirectory(logDir);
      var logPath = Path.Combine(logDir, "middleware.log");
      var sw = new StreamWriter(new FileStream(logPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite), Encoding.UTF8) { AutoFlush = true };
      fileWriter = TextWriter.Synchronized(new TimestampTextWriter(sw));
    }
    catch { fileWriter = null; }

    var outInner = fileWriter is null ? originalOut : new MultiTextWriter(originalOut, fileWriter);
    var errInner = fileWriter is null ? originalErr : new MultiTextWriter(originalErr, fileWriter);
    var ringOut = new RingBufferTextWriter(outInner, maxLines: 500);
    var ringErr = new RingBufferTextWriter(errInner, maxLines: 500);
    Console.SetOut(ringOut);
    Console.SetError(ringErr);

    var runtime = new DashboardRuntimeState();
    var syncGate = new SemaphoreSlim(1, 1);
    var stateGate = new object();

    AppConfig currentConfig;
    int pollIntervalSeconds;
    bool autoSyncEnabled;
    int dashboardRefreshSeconds;
    var dashboardSupabaseAnonKey = string.Empty;
    var dashboardSupabaseProjectId = string.Empty;
    var dashboardSupabaseJwtSecret = string.Empty;
    var syncScheduleLocalTimes = Array.Empty<string>();
    var syncScheduleVersion = 0;
    const string DefaultSupabaseUrl = "https://lmssdqnduaahmqmvpuvn.supabase.co";
    const string DefaultSupabaseProjectId = "lmssdqnduaahmqmvpuvn";
    const string DefaultSupabasePublishableKey = "sb_publishable_RCm7ogQhpvMk9l36ZKc4Mg_U5bLkA3E";

    var startingConfig = initialConfig with { SupabaseSyncEnabled = true };
    var startingPollIntervalSeconds = 0;
    var startingAutoSyncEnabled = true;
    var startingDashboardRefreshSeconds = 600;
    try
    {
      var state = LoadState(statePath);
      var punchesFromState = state.DevicePunches ?? Array.Empty<Punch>();
      var exportPath = TryResolveAttlogExportPath();
      List<Punch>? punchesFromFile = null;
      try
      {
        var exportConfig = startingConfig;
        var mode = (exportConfig.BadDateTimeMode ?? string.Empty).Trim();
        if (!mode.Equals("skip", StringComparison.OrdinalIgnoreCase))
        {
          exportConfig = exportConfig with { BadDateTimeMode = "skip" };
        }
        punchesFromFile = TryReadAttlogPunchesForSync(exportConfig, exportPath, afterUtc: null, onlyToday: false, restrictToStaffIds: null);
      }
      catch { }

      Program.DevicePunches = (punchesFromFile is not null && punchesFromFile.Count > 0)
        ? MergePunches(Array.Empty<Punch>(), punchesFromFile, max: 50000)
        : punchesFromState;
      startingDashboardRefreshSeconds = state.DashboardRefreshSeconds > 0 ? state.DashboardRefreshSeconds : 600;
      var s = state.DashboardSettings ?? state.DbPresets?.OrderByDescending(x => x.SavedAtUtc).FirstOrDefault()?.Settings;
      if (s is not null)
      {
        var key = s.SupabaseKeyIsProtected ? TryUnprotectBase64(s.SupabaseKeyProtectedBase64) : s.SupabaseKeyProtectedBase64;
        if (!string.IsNullOrWhiteSpace(s.SupabaseAttendanceTable)) startingConfig = startingConfig with { SupabaseAttendanceTable = s.SupabaseAttendanceTable };
        startingConfig = startingConfig with { SupabaseSyncEnabled = s.SupabaseSyncEnabled };
        if (!string.IsNullOrWhiteSpace(key)) startingConfig = startingConfig with { SupabaseServiceRoleKey = key };
        dashboardSupabaseJwtSecret = (s.SupabaseJwtSecret ?? string.Empty).Trim();
      }

      var dev = state.DevicePresets?.OrderByDescending(p => p.SavedAtUtc).FirstOrDefault();
      if (dev is not null && !string.IsNullOrWhiteSpace(dev.DeviceIp) && dev.DevicePort > 0 && dev.DevicePort <= 65535)
      {
        startingConfig = startingConfig with { DeviceIp = dev.DeviceIp, DevicePort = dev.DevicePort, ReaderMode = dev.ReaderMode, DeviceId = $"WL10-{dev.DeviceIp}" };
      }

      var poll = state.PollingPresets?.OrderByDescending(p => p.SavedAtUtc).FirstOrDefault();
      if (poll is not null)
      {
        startingPollIntervalSeconds = poll.PollIntervalSeconds;
        if (startingPollIntervalSeconds == 600) startingPollIntervalSeconds = 3600;
        startingAutoSyncEnabled = poll.AutoSyncEnabled;
      }

      syncScheduleLocalTimes = (state.SyncScheduleLocalTimes ?? Array.Empty<string>())
        .Select(s => (s ?? string.Empty).Trim())
        .Where(s => s.Length > 0)
        .Distinct(StringComparer.Ordinal)
        .ToArray();
    }
    catch { }

    startingConfig = startingConfig with { SupabaseUrl = DefaultSupabaseUrl, SupabaseSyncEnabled = true };
    dashboardSupabaseProjectId = DefaultSupabaseProjectId;
    dashboardSupabaseAnonKey = DefaultSupabasePublishableKey;

    static Dictionary<string, string> LoadDotEnv()
    {
      var result = new Dictionary<string, string>(StringComparer.Ordinal);
      try
      {
        var candidates = new[]
        {
          Path.Combine(Directory.GetCurrentDirectory(), ".env.local"),
          Path.Combine(AppContext.BaseDirectory, ".env.local"),
          Path.Combine(Directory.GetCurrentDirectory(), "SHAB Dashboard", ".env.local"),
          Path.Combine(AppContext.BaseDirectory, "SHAB Dashboard", ".env.local"),
        };
        foreach (var path in candidates)
        {
          if (!File.Exists(path)) continue;
          foreach (var rawLine in File.ReadAllLines(path))
          {
            var line = (rawLine ?? string.Empty).Trim();
            if (line.Length == 0) continue;
            if (line.StartsWith("#", StringComparison.Ordinal)) continue;
            if (line.StartsWith("export ", StringComparison.OrdinalIgnoreCase)) line = line["export ".Length..].Trim();

            var eq = line.IndexOf('=', StringComparison.Ordinal);
            if (eq <= 0) continue;
            var key = line[..eq].Trim();
            if (key.Length == 0) continue;
            var value = line[(eq + 1)..].Trim();
            if (value.Length >= 2)
            {
              var first = value[0];
              var last = value[^1];
              if ((first == '"' && last == '"') || (first == '\'' && last == '\''))
              {
                value = value[1..^1];
              }
            }
            result[key] = value;
          }
          break;
        }
      }
      catch { }
      return result;
    }

    var dotenv = LoadDotEnv();
    string Dot(string key) => dotenv.TryGetValue(key, out var v) && !string.IsNullOrWhiteSpace(v) ? v.Trim() : string.Empty;

    var envSupaUrl = (Environment.GetEnvironmentVariable("SUPABASE_URL") ?? "").Trim();
    if (envSupaUrl.Length == 0) envSupaUrl = Dot("SUPABASE_URL");
    var envSupaService = (Environment.GetEnvironmentVariable("SUPABASE_SERVICE_ROLE_KEY") ?? "").Trim();
    if (envSupaService.Length == 0) envSupaService = Dot("SUPABASE_SERVICE_ROLE_KEY");
    var envSupaAnon = (Environment.GetEnvironmentVariable("SUPABASE_PUBLISHABLE_KEY") ?? "").Trim();
    if (envSupaAnon.Length == 0) envSupaAnon = Dot("SUPABASE_PUBLISHABLE_KEY");
    if (envSupaAnon.Length == 0) envSupaAnon = (Environment.GetEnvironmentVariable("SUPABASE_ANON_KEY") ?? "").Trim();
    if (envSupaAnon.Length == 0) envSupaAnon = Dot("SUPABASE_ANON_KEY");
    if (envSupaAnon.Length == 0) envSupaAnon = (Environment.GetEnvironmentVariable("SUPABASE_ANON_PUBLIC_KEY") ?? "").Trim();
    if (envSupaAnon.Length == 0) envSupaAnon = Dot("SUPABASE_ANON_PUBLIC_KEY");
    if (envSupaAnon.Length == 0) envSupaAnon = (Environment.GetEnvironmentVariable("SUPABASE_PUBLIC_ANON_KEY") ?? "").Trim();
    if (envSupaAnon.Length == 0) envSupaAnon = Dot("SUPABASE_PUBLIC_ANON_KEY");
    var envSupaProjectId = (Environment.GetEnvironmentVariable("SUPABASE_PROJECT_ID") ?? "").Trim();
    if (envSupaProjectId.Length == 0) envSupaProjectId = Dot("SUPABASE_PROJECT_ID");
    var envSupaJwt = (Environment.GetEnvironmentVariable("SUPABASE_JWT_SECRET") ?? "").Trim();
    if (envSupaJwt.Length == 0) envSupaJwt = Dot("SUPABASE_JWT_SECRET");
    var envViteSupaUrl = (Environment.GetEnvironmentVariable("VITE_SUPABASE_URL") ?? "").Trim();
    if (envViteSupaUrl.Length == 0) envViteSupaUrl = Dot("VITE_SUPABASE_URL");
    var envViteSupaAnon = (Environment.GetEnvironmentVariable("VITE_SUPABASE_ANON_KEY") ?? "").Trim();
    if (envViteSupaAnon.Length == 0) envViteSupaAnon = Dot("VITE_SUPABASE_ANON_KEY");
    var envViteSupaProjectId = (Environment.GetEnvironmentVariable("VITE_SUPABASE_PROJECT_ID") ?? "").Trim();
    if (envViteSupaProjectId.Length == 0) envViteSupaProjectId = Dot("VITE_SUPABASE_PROJECT_ID");
    if (envSupaService.Length > 0) startingConfig = startingConfig with { SupabaseServiceRoleKey = envSupaService };
    if (envSupaJwt.Length > 0) dashboardSupabaseJwtSecret = envSupaJwt;
    startingConfig = startingConfig with { SupabaseUrl = DefaultSupabaseUrl };
    dashboardSupabaseProjectId = DefaultSupabaseProjectId;
    dashboardSupabaseAnonKey = DefaultSupabasePublishableKey;

    lock (stateGate)
    {
      currentConfig = startingConfig;
      pollIntervalSeconds = Math.Clamp((startingPollIntervalSeconds <= 0 ? (startingConfig.PollIntervalSeconds <= 0 ? 3600 : startingConfig.PollIntervalSeconds) : startingPollIntervalSeconds), 60, 3600);
      autoSyncEnabled = startingAutoSyncEnabled;
      dashboardRefreshSeconds = Math.Clamp(startingDashboardRefreshSeconds <= 0 ? 600 : startingDashboardRefreshSeconds, 10, 3600);
    }

    static TimeZoneInfo GetScheduleTimeZone()
    {
      try { return TimeZoneInfo.FindSystemTimeZoneById("Asia/Kuala_Lumpur"); } catch { }
      try { return TimeZoneInfo.FindSystemTimeZoneById("Singapore Standard Time"); } catch { }
      try { return TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time"); } catch { }
      return TimeZoneInfo.Local;
    }

    static string[] NormalizeScheduleStrings(IEnumerable<string> times)
    {
      static bool TryParse(string s, out TimeSpan t)
      {
        return TimeSpan.TryParseExact(s, "hh\\:mm", CultureInfo.InvariantCulture, out t)
          || TimeSpan.TryParseExact(s, "h\\:mm", CultureInfo.InvariantCulture, out t);
      }

      var set = new HashSet<int>();
      var list = new List<(int mins, string s)>();
      foreach (var raw in times)
      {
        var s = (raw ?? string.Empty).Trim();
        if (s.Length == 0) continue;
        if (!TryParse(s, out var t)) continue;
        if (t.TotalMinutes < 0 || t.TotalMinutes >= 24 * 60) continue;
        var mins = (int)Math.Round(t.TotalMinutes);
        if (!set.Add(mins)) continue;
        var norm = new TimeSpan(mins / 60, mins % 60, 0).ToString("hh\\:mm", CultureInfo.InvariantCulture);
        list.Add((mins, norm));
      }
      return list.OrderBy(x => x.mins).Select(x => x.s).ToArray();
    }

    static DateTimeOffset? ComputeNextScheduledSyncUtc(string[] scheduleLocalTimes, DateTimeOffset nowUtc, TimeZoneInfo tz)
    {
      if (scheduleLocalTimes is null || scheduleLocalTimes.Length == 0) return null;
      static bool TryParse(string s, out TimeSpan t)
      {
        return TimeSpan.TryParseExact(s, "hh\\:mm", CultureInfo.InvariantCulture, out t)
          || TimeSpan.TryParseExact(s, "h\\:mm", CultureInfo.InvariantCulture, out t);
      }

      var nowLocal = TimeZoneInfo.ConvertTime(nowUtc, tz).DateTime;
      var today = nowLocal.Date;
      var times = new List<TimeSpan>(capacity: scheduleLocalTimes.Length);
      foreach (var raw in scheduleLocalTimes)
      {
        var s = (raw ?? string.Empty).Trim();
        if (s.Length == 0) continue;
        if (!TryParse(s, out var t)) continue;
        if (t.TotalMinutes < 0 || t.TotalMinutes >= 24 * 60) continue;
        times.Add(t);
      }
      if (times.Count == 0) return null;
      times.Sort();

      DateTime nextLocal = default;
      foreach (var t in times)
      {
        var cand = today.Add(t);
        if (cand > nowLocal.AddSeconds(1)) { nextLocal = cand; break; }
      }
      if (nextLocal == default) nextLocal = today.AddDays(1).Add(times[0]);

      var unspecified = DateTime.SpecifyKind(nextLocal, DateTimeKind.Unspecified);
      var nextOffset = new DateTimeOffset(unspecified, tz.GetUtcOffset(unspecified));
      return nextOffset.ToUniversalTime();
    }

    async Task<(bool ok, string? error)> ExecuteSync(bool verify, bool today, bool? supabaseOverride, CancellationToken ct)
    {
      if (!await syncGate.WaitAsync(0, ct)) return (false, "sync already running");
      try
      {
        AppConfig cfg;
        lock (stateGate) cfg = currentConfig;
        var supabaseConfigured = !string.IsNullOrWhiteSpace(cfg.SupabaseUrl)
          && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey)
          && !string.IsNullOrWhiteSpace(cfg.SupabaseAttendanceTable);
        var wantsSupabase = supabaseOverride ?? cfg.SupabaseSyncEnabled;
        var doSupabase = wantsSupabase && supabaseConfigured
          && !string.IsNullOrWhiteSpace(cfg.SupabaseUrl)
          && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey)
          && !string.IsNullOrWhiteSpace(cfg.SupabaseAttendanceTable);
        var cfgForRun = cfg with { SupabaseSyncEnabled = doSupabase };

        runtime.LastSyncStartedAtUtc = DateTimeOffset.UtcNow;
        runtime.LastSyncFinishedAtUtc = null;
        runtime.LastSyncError = null;
        runtime.LastSyncResult = "running";
        if (doSupabase)
        {
          runtime.LastSupabaseSyncStartedAtUtc = runtime.LastSyncStartedAtUtc;
          runtime.LastSupabaseSyncFinishedAtUtc = null;
          runtime.LastSupabaseSyncError = null;
          runtime.LastSupabaseSyncResult = "running";
          runtime.LastSupabaseUpsertedCount = 0;
        }

        try
        {
          await RunOnce(cfgForRun, statePath, verify, today);
          runtime.LastSyncResult = "ok";
          if (doSupabase)
          {
            runtime.LastSupabaseSyncResult = "ok";
            runtime.LastSupabaseUpsertedCount = LastRunUpsertedCount;
          }
          return (true, null);
        }
        catch (Exception ex)
        {
          runtime.LastSyncResult = "error";
          runtime.LastSyncError = ex.ToString();
          if (doSupabase)
          {
            runtime.LastSupabaseSyncResult = "error";
            runtime.LastSupabaseSyncError = ex.ToString();
          }
          return (false, ex.Message);
        }
        finally
        {
          runtime.LastSyncFinishedAtUtc = DateTimeOffset.UtcNow;
          if (doSupabase) runtime.LastSupabaseSyncFinishedAtUtc = runtime.LastSyncFinishedAtUtc;
        }
      }
      finally
      {
        syncGate.Release();
      }
    }

    var builder = WebApplication.CreateBuilder();
    builder.WebHost.UseUrls(urls);
    builder.Logging.ClearProviders();
    builder.Logging.AddConsole();
    builder.Logging.SetMinimumLevel(LogLevel.Warning);

    builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme)
      .AddCookie(o =>
      {
        o.Cookie.Name = "wl10dash";
        o.LoginPath = "/login";
        o.LogoutPath = "/logout";
        o.SlidingExpiration = true;
        o.ExpireTimeSpan = TimeSpan.FromHours(12);
      });
    builder.Services.AddAuthorization();

    var app = builder.Build();

    app.UseAuthentication();
    app.UseAuthorization();

    app.MapGet("/login", async (HttpContext ctx) =>
    {
      if (ctx.User?.Identity?.IsAuthenticated == true)
      {
        ctx.Response.Redirect("/?tab=settings&subtab=settings:connection");
        return;
      }
      ctx.Response.ContentType = "text/html; charset=utf-8";
      await ctx.Response.WriteAsync(LoginHtml());
    }).AllowAnonymous();

    app.MapPost("/login", async (HttpContext ctx) =>
    {
      var form = await ctx.Request.ReadFormAsync();
      var username = (form["username"].ToString() ?? "").Trim();
      var password = (form["password"].ToString() ?? "").Trim();

      if (!string.Equals(username, user, StringComparison.Ordinal) || !string.Equals(password, pass, StringComparison.Ordinal))
      {
        ctx.Response.StatusCode = 401;
        ctx.Response.ContentType = "text/html; charset=utf-8";
        await ctx.Response.WriteAsync(LoginHtml("Invalid credentials."));
        return;
      }

      var claims = new List<Claim>
      {
        new(ClaimTypes.Name, username),
        new("role", "superadmin"),
      };
      var identity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
      var principal = new ClaimsPrincipal(identity);
      await ctx.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, principal);
      ctx.Response.Redirect("/?tab=summary&subtab=summary:attendance");
    }).AllowAnonymous();

    app.MapPost("/logout", async (HttpContext ctx) =>
    {
      await ctx.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
      ctx.Response.Redirect("/login");
    });

    static string? TryFindBrandAsset(string startDir, params string[] relativeCandidates)
    {
      try
      {
        var d = new DirectoryInfo(startDir);
        for (var i = 0; i < 10 && d is not null; i++)
        {
          foreach (var rel in relativeCandidates)
          {
            if (string.IsNullOrWhiteSpace(rel)) continue;
            var p = Path.GetFullPath(Path.Combine(d.FullName, rel));
            if (File.Exists(p)) return p;
          }
          d = d.Parent;
        }
      }
      catch { }
      return null;
    }

    app.MapGet("/assets/logo.png", (HttpContext ctx) =>
    {
      var baseDir = AppContext.BaseDirectory;
      var path =
        TryFindBrandAsset(baseDir,
          Path.Combine("Reference", "Communications", "Client Communication", "Design Layout", "SHAB Attendance Dashboard - Logo - Transparent.png"),
          Path.Combine("SHAB Attendance System", "Client Package", "Assets", "SHAB Attendance Dashboard.png"),
          Path.Combine("Assets", "SHAB Attendance Dashboard.png"),
          Path.Combine("..", "..", "Assets", "SHAB Attendance Dashboard.png")
        )
        ?? TryFindBrandAsset(Directory.GetCurrentDirectory(),
          Path.Combine("Reference", "Communications", "Client Communication", "Design Layout", "SHAB Attendance Dashboard - Logo - Transparent.png"),
          Path.Combine("SHAB Attendance System", "Client Package", "Assets", "SHAB Attendance Dashboard.png")
        );

      if (string.IsNullOrWhiteSpace(path)) return Results.NotFound();
      return Results.File(path, "image/png");
    }).AllowAnonymous();

    app.MapGet("/assets/logo.ico", (HttpContext ctx) =>
    {
      var baseDir = AppContext.BaseDirectory;
      var path =
        TryFindBrandAsset(baseDir,
          Path.Combine("Reference", "Communications", "Client Communication", "Design Layout", "SHAB Attendance Dashboard - Logo - Transparent.ico"),
          Path.Combine("SHAB Attendance System", "Client Package", "Assets", "SHAB Attendance Dashboard.ico"),
          Path.Combine("Assets", "SHAB Attendance Dashboard.ico"),
          Path.Combine("..", "..", "Assets", "SHAB Attendance Dashboard.ico")
        )
        ?? TryFindBrandAsset(Directory.GetCurrentDirectory(),
          Path.Combine("Reference", "Communications", "Client Communication", "Design Layout", "SHAB Attendance Dashboard - Logo - Transparent.ico"),
          Path.Combine("SHAB Attendance System", "Client Package", "Assets", "SHAB Attendance Dashboard.ico")
        );

      if (string.IsNullOrWhiteSpace(path)) return Results.NotFound();
      return Results.File(path, "image/x-icon");
    }).AllowAnonymous();

    app.MapGet("/", async (HttpContext ctx) =>
    {
      ctx.Response.Headers.CacheControl = "no-store, no-cache, must-revalidate, max-age=0";
      ctx.Response.Headers.Pragma = "no-cache";
      ctx.Response.ContentType = "text/html; charset=utf-8";
      await ctx.Response.WriteAsync(DashboardHtml());
    }).RequireAuthorization();

    app.MapGet("/api/status", async (HttpContext ctx) =>
    {
      AppConfig cfg;
      int interval;
      bool auto;
      int dashRefresh;
      string[] sched;
      string anon;
      string pid;
      string jwt;
      lock (stateGate)
      {
        cfg = currentConfig;
        interval = pollIntervalSeconds;
        auto = autoSyncEnabled;
        dashRefresh = dashboardRefreshSeconds;
        sched = syncScheduleLocalTimes;
        anon = dashboardSupabaseAnonKey;
        pid = dashboardSupabaseProjectId;
        jwt = dashboardSupabaseJwtSecret;
      }

      var localWatermark = TryReadLocalWatermarkUtc(statePath);
      var (isReachable, reachError, rttMs) = await ProbeTcpAsync(cfg.DeviceIp, cfg.DevicePort, TimeSpan.FromSeconds(5), ctx.RequestAborted);
      var dbWatermark = await TryGetDbWatermarkUtc(cfg, ctx.RequestAborted);
      var pcNet = GetPcNetworkInfo(cfg.DeviceIp);
      var deviceTotal = (Program.DevicePunches ?? Array.Empty<Punch>()).Length;
      var isSuperadmin = string.Equals(ctx.User.FindFirstValue("role") ?? string.Empty, "superadmin", StringComparison.Ordinal);
      var configured = EnsureConfiguredDevices(saveIfSeeded: true);
      var latestState = LoadState(statePath);
      var processedFiles = (latestState.ProcessedFiles ?? Array.Empty<ProcessedFileEntry>()).ToArray();
      var devices = configured.Select(d => new
      {
        type = d.DeviceType,
        ip = d.DeviceIp,
        port = d.DevicePort,
        deviceId = d.DeviceId,
        readerMode = d.ReaderMode,
        logDir = d.LogDir,
        filePattern = d.FilePattern,
        lastOkAtUtc = d.LastOkAtUtc?.ToString("O"),
        savedAtUtc = d.SavedAtUtc == default ? null : d.SavedAtUtc.ToString("O"),
        processedFilesCount = processedFiles.Count(p => string.Equals(p.DeviceId, d.DeviceId, StringComparison.Ordinal)),
        lastProcessedAtUtc = processedFiles
          .Where(p => string.Equals(p.DeviceId, d.DeviceId, StringComparison.Ordinal))
          .OrderByDescending(p => p.ProcessedAtUtc)
          .Select(p => (DateTimeOffset?)p.ProcessedAtUtc)
          .FirstOrDefault()
          ?.ToString("O"),
        active = string.Equals(d.DeviceId, cfg.DeviceId, StringComparison.Ordinal)
      }).ToArray();

      int? supaTotal = null;
      var supaConfigured = !string.IsNullOrWhiteSpace(cfg.SupabaseUrl)
        && !string.IsNullOrWhiteSpace(cfg.SupabaseAttendanceTable)
        && (!string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || !string.IsNullOrWhiteSpace(anon));
      if (supaConfigured)
      {
        var now = DateTimeOffset.UtcNow;
        var stale = runtime.LastSupabaseTotalCountAtUtc is null || (now - runtime.LastSupabaseTotalCountAtUtc.Value) > TimeSpan.FromMinutes(5);
        if (stale)
        {
          var (ok, count, _) = await TryGetSupabaseTotalCount(cfg, anon, ctx.RequestAborted);
          runtime.LastSupabaseTotalCountAtUtc = now;
          runtime.LastSupabaseTotalCount = ok ? count : null;
        }
        supaTotal = runtime.LastSupabaseTotalCount;
      }

      ctx.Response.ContentType = "application/json; charset=utf-8";
      await ctx.Response.WriteAsync(JsonSerializer.Serialize(new
      {
        isSuperadmin,
        devices,
        device = new
        {
          ip = cfg.DeviceIp,
          port = cfg.DevicePort,
          deviceId = cfg.DeviceId,
          readerMode = cfg.ReaderMode,
          reachable = isReachable,
          reachError,
          rttMs,
        },
        dashboard = new
        {
          bind,
          port,
          url = urls,
          refreshSeconds = dashRefresh,
          refreshMinutes = Math.Max(1, dashRefresh / 60),
        },
        sync = new
        {
          pollIntervalSeconds = interval,
          pollIntervalMinutes = Math.Max(1, interval / 60),
          autoSyncEnabled = auto,
          scheduleLocalTimes = sched,
          nextSyncAtUtc = runtime.NextSyncAtUtc?.ToString("O"),
          lastRunPunchCount = LastRunPunchCount,
          lastRunUpsertedCount = LastRunUpsertedCount,
          lastRunSkippedUpsertCount = LastRunSkippedUpsertCount,
          deviceRecordsTotal = deviceTotal,
          dbRecordsTotal = supaTotal,
          lastLocalWatermarkUtc = localWatermark?.ToString("O"),
          lastDbWatermarkUtc = dbWatermark?.ToString("O"),
          lastSyncStartedAtUtc = runtime.LastSyncStartedAtUtc?.ToString("O"),
          lastSyncFinishedAtUtc = runtime.LastSyncFinishedAtUtc?.ToString("O"),
          lastSyncResult = runtime.LastSyncResult,
          lastSyncError = runtime.LastSyncError,
          lastSupabaseSyncStartedAtUtc = runtime.LastSupabaseSyncStartedAtUtc?.ToString("O"),
          lastSupabaseSyncFinishedAtUtc = runtime.LastSupabaseSyncFinishedAtUtc?.ToString("O"),
          lastSupabaseSyncResult = runtime.LastSupabaseSyncResult,
          lastSupabaseSyncError = runtime.LastSupabaseSyncError,
          lastSupabaseUpsertedCount = runtime.LastSupabaseUpsertedCount,
        },
        supabase = new
        {
          configured = supaConfigured,
          syncEnabled = cfg.SupabaseSyncEnabled,
          url = cfg.SupabaseUrl,
          projectId = string.IsNullOrWhiteSpace(pid) ? InferSupabaseProjectId(cfg.SupabaseUrl) : pid,
          attendanceTable = cfg.SupabaseAttendanceTable,
          apiKeyConfigured = !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey),
          hasServiceRoleKey = !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey),
          serviceRoleKey = isSuperadmin ? cfg.SupabaseServiceRoleKey : string.Empty,
          anonKey = anon,
          hasJwtSecret = !string.IsNullOrWhiteSpace(jwt),
          jwtSecret = string.Empty,
        },
        pc = pcNet,
        process = new
        {
          pid = Environment.ProcessId,
          startedAtUtc = Process.GetCurrentProcess().StartTime.ToUniversalTime().ToString("O"),
          baseDir = AppContext.BaseDirectory,
        }
      }, JsonOptions));
    }).RequireAuthorization();

    app.MapGet("/api/logs", (HttpContext ctx) =>
    {
      var merged = ringOut.Snapshot()
        .Concat(ringErr.Snapshot())
        .TakeLast(500)
        .ToArray();
      return Results.Json(new { lines = merged }, JsonOptions);
    }).RequireAuthorization();

    var lastSystemLogSyncedAtLocal = DateTime.MinValue;
    var activitySeq = 0L;
    var lastActivitySyncedSeq = 0L;
    var activityQ = new ConcurrentQueue<(long seq, DateTimeOffset ts, string scope, string level, string message)>();

    static bool TryParseTsPrefix(string line, out DateTime local)
    {
      local = default;
      if (string.IsNullOrWhiteSpace(line) || line.Length < 23) return false;
      var ts = line[..23];
      return DateTime.TryParseExact(ts, "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out local);
    }

    static (string level, string msg) SplitLevelMessage(string raw)
    {
      var s = (raw ?? string.Empty).Trim();
      var level = "INFO";
      foreach (var p in new[] { "[ERROR]", "[WARN]", "[INFO]", "[DEBUG]" })
      {
        if (s.Contains(p, StringComparison.Ordinal))
        {
          level = p.Trim('[', ']');
          var idx = s.IndexOf(p, StringComparison.Ordinal);
          var msg = (idx >= 0 ? s[(idx + p.Length)..] : s).Trim();
          return (level, msg);
        }
      }
      return (level, s);
    }

    async Task<(bool ok, int inserted, string? error)> SyncLogsToSupabaseAsync(IEnumerable<(DateTime localTs, string level, string message, string source)> rows, CancellationToken ct)
    {
      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        return (false, 0, "Supabase not configured");
      }
      var url = cfg.SupabaseUrl.TrimEnd('/') + "/rest/v1/middleware_logs";

      var tz = GetScheduleTimeZone();
      var list = rows.Take(200).Select(r => new
      {
        device_id = cfg.DeviceId,
        level = r.level,
        message = r.message,
        meta = new
        {
          source = r.source,
          ts_local = r.localTs.ToString("O"),
          ts_kl = TimeZoneInfo.ConvertTime(r.localTs, tz).ToString("O"),
          ts_utc = TimeZoneInfo.ConvertTimeToUtc(r.localTs, TimeZoneInfo.Local).ToString("O"),
        }
      }).ToArray();
      if (list.Length == 0) return (true, 0, null);

      using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(12) };
      using var req = new HttpRequestMessage(HttpMethod.Post, url);
      req.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
      req.Headers.TryAddWithoutValidation("Prefer", "return=minimal");
      req.Content = new StringContent(JsonSerializer.Serialize(list, JsonOptions), Encoding.UTF8, "application/json");
      using var resp = await http.SendAsync(req, ct);
      if (!resp.IsSuccessStatusCode)
      {
        var body = await resp.Content.ReadAsStringAsync(ct);
        return (false, 0, body.Length > 250 ? body[..250] : body);
      }
      try
      {
        var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
        var nthUrl = $"{baseUrl}/rest/v1/middleware_logs?select=id&order=id.desc&limit=1&offset=1999";
        using var nthReq = new HttpRequestMessage(HttpMethod.Get, nthUrl);
        nthReq.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
        nthReq.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
        using var nthResp = await http.SendAsync(nthReq, ct);
        if (nthResp.IsSuccessStatusCode)
        {
          var nthBody = await nthResp.Content.ReadAsStringAsync(ct);
          using var nthDoc = JsonDocument.Parse(nthBody);
          if (nthDoc.RootElement.ValueKind == JsonValueKind.Array && nthDoc.RootElement.GetArrayLength() > 0)
          {
            var first = nthDoc.RootElement[0];
            if (first.ValueKind == JsonValueKind.Object && first.TryGetProperty("id", out var idProp) && idProp.ValueKind == JsonValueKind.Number && idProp.TryGetInt64(out var cutoffId) && cutoffId > 0)
            {
              var delUrl = $"{baseUrl}/rest/v1/middleware_logs?id=lt.{cutoffId}";
              using var delReq = new HttpRequestMessage(HttpMethod.Delete, delUrl);
              delReq.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
              delReq.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
              delReq.Headers.TryAddWithoutValidation("Prefer", "return=minimal");
              _ = await http.SendAsync(delReq, ct);
            }
          }
        }
      }
      catch { }
      return (true, list.Length, null);
    }

    app.MapPost("/api/logs/sync", async (HttpContext ctx) =>
    {
      var merged = ringOut.Snapshot().Concat(ringErr.Snapshot()).TakeLast(500).ToArray();
      var parsed = new List<(DateTime localTs, string level, string message, string source)>(capacity: 200);
      foreach (var line in merged)
      {
        if (!TryParseTsPrefix(line, out var tsLocal)) continue;
        if (tsLocal <= lastSystemLogSyncedAtLocal) continue;
        var rest = line.Length > 24 ? line[24..] : string.Empty;
        var (lvl, msg) = SplitLevelMessage(rest);
        if (string.IsNullOrWhiteSpace(msg)) msg = rest.Trim();
        parsed.Add((tsLocal, lvl, msg, "system"));
      }
      parsed.Sort((a, b) => a.localTs.CompareTo(b.localTs));
      var (ok, inserted, error) = await SyncLogsToSupabaseAsync(parsed, ctx.RequestAborted);
      if (ok && inserted > 0)
      {
        lastSystemLogSyncedAtLocal = parsed.Take(inserted).Max(x => x.localTs);
      }
      return Results.Json(new { ok, inserted, error }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/activity/append", async (HttpContext ctx) =>
    {
      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var root = doc.RootElement;
      var scope = root.TryGetProperty("scope", out var sEl) && sEl.ValueKind == JsonValueKind.String ? (sEl.GetString() ?? "") : "";
      var message = root.TryGetProperty("message", out var mEl) && mEl.ValueKind == JsonValueKind.String ? (mEl.GetString() ?? "") : "";
      var level = root.TryGetProperty("level", out var lEl) && lEl.ValueKind == JsonValueKind.String ? (lEl.GetString() ?? "") : "INFO";
      var ts = DateTimeOffset.Now;
      if (root.TryGetProperty("ts", out var tsEl) && tsEl.ValueKind == JsonValueKind.String)
      {
        _ = DateTimeOffset.TryParse(tsEl.GetString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out ts);
      }
      var seq = Interlocked.Increment(ref activitySeq);
      activityQ.Enqueue((seq, ts, scope, level, message));
      while (activityQ.Count > 5000 && activityQ.TryDequeue(out _)) { }
      try { Console.WriteLine($"[INFO] [Activity] [{scope}] [{level}] {message}"); } catch { }
      return Results.Json(new { ok = true }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/activity/sync", async (HttpContext ctx) =>
    {
      var pending = activityQ.Where(x => x.seq > lastActivitySyncedSeq).OrderBy(x => x.seq).Take(200).ToList();
      var rows = pending.Select(x => (localTs: x.ts.LocalDateTime, level: (x.level ?? "INFO").Trim().ToUpperInvariant(), message: $"[{x.scope}] {x.message}", source: "activity"));
      var (ok, inserted, error) = await SyncLogsToSupabaseAsync(rows, ctx.RequestAborted);
      if (ok && inserted > 0)
      {
        lastActivitySyncedSeq = pending.Take(inserted).Max(x => x.seq);
      }
      return Results.Json(new { ok, inserted, error }, JsonOptions);
    }).RequireAuthorization();

    _ = Task.Run(async () =>
    {
      while (!app.Lifetime.ApplicationStopping.IsCancellationRequested)
      {
        try
        {
          var sysRows = ringOut.Snapshot().Concat(ringErr.Snapshot())
            .Select(l => (ok: TryParseTsPrefix(l, out var ts), ts, line: l))
            .Where(x => x.ok && x.ts > lastSystemLogSyncedAtLocal)
            .Select(x =>
            {
              var rest = x.line.Length > 24 ? x.line[24..] : string.Empty;
              var (lvl, msg) = SplitLevelMessage(rest);
              if (string.IsNullOrWhiteSpace(msg)) msg = rest.Trim();
              return (localTs: x.ts, level: lvl, message: msg, source: "system");
            })
            .OrderBy(x => x.localTs)
            .Take(200)
            .ToList();
          var r = await SyncLogsToSupabaseAsync(sysRows, CancellationToken.None);
          if (r.ok && r.inserted > 0) lastSystemLogSyncedAtLocal = sysRows.Take(r.inserted).Max(x => x.localTs);
        }
        catch { }

        try
        {
          var pending = activityQ.Where(x => x.seq > lastActivitySyncedSeq).OrderBy(x => x.seq).Take(200).ToList();
          if (pending.Count > 0)
          {
            var rows = pending.Select(x => (localTs: x.ts.LocalDateTime, level: (x.level ?? "INFO").Trim().ToUpperInvariant(), message: $"[{x.scope}] {x.message}", source: "activity"));
            var r = await SyncLogsToSupabaseAsync(rows, CancellationToken.None);
            if (r.ok && r.inserted > 0) lastActivitySyncedSeq = pending.Take(r.inserted).Max(x => x.seq);
          }
        }
        catch { }

        try { await Task.Delay(TimeSpan.FromMinutes(2), app.Lifetime.ApplicationStopping); }
        catch { }
      }
    });

    app.MapPost("/api/restart", async (HttpContext ctx) =>
    {
      static string Quote(string s)
      {
        if (string.IsNullOrEmpty(s)) return "\"\"";
        return s.IndexOfAny(new[] { ' ', '\t', '\n', '\r', '"' }) >= 0
          ? "\"" + s.Replace("\"", "\\\"", StringComparison.Ordinal) + "\""
          : s;
      }

      _ = Task.Run(async () =>
      {
        try
        {
          await Task.Delay(350);
          var fileName = Environment.ProcessPath ?? Process.GetCurrentProcess().MainModule?.FileName;
          if (!string.IsNullOrWhiteSpace(fileName))
          {
            var argsRaw = Environment.GetCommandLineArgs().Skip(1).Select(Quote);
            var psi = new ProcessStartInfo
            {
              FileName = fileName,
              Arguments = string.Join(" ", argsRaw),
              WorkingDirectory = AppContext.BaseDirectory,
              UseShellExecute = false,
            };
            Process.Start(psi);
          }
        }
        catch (Exception ex)
        {
          Console.Error.WriteLine("Restart failed: " + ex);
        }
        finally
        {
          try { app.Lifetime.StopApplication(); } catch { }
          await Task.Delay(500);
          Environment.Exit(0);
        }
      });

      ctx.Response.ContentType = "application/json; charset=utf-8";
      await ctx.Response.WriteAsync(JsonSerializer.Serialize(new { ok = true, restarting = true }, JsonOptions));
    }).RequireAuthorization();

    app.MapPost("/api/sync", async (HttpContext ctx) =>
    {
      var verify = ctx.Request.Query.TryGetValue("verify", out var v) && string.Equals(v.ToString(), "1", StringComparison.OrdinalIgnoreCase);
      var today = ctx.Request.Query.TryGetValue("today", out var t) && string.Equals(t.ToString(), "1", StringComparison.OrdinalIgnoreCase);
      bool? supabaseOverride = null;
      if (ctx.Request.Query.TryGetValue("supabase", out var s))
      {
        var raw = (s.ToString() ?? string.Empty).Trim();
        if (raw == "0") supabaseOverride = false;
        else if (raw == "1") supabaseOverride = true;
      }
      var (ok, error) = await ExecuteSync(verify, today, supabaseOverride, ctx.RequestAborted);
      return Results.Json(new { ok, error }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/settings", async (HttpContext ctx) =>
    {
      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var root = doc.RootElement;

      var saveSupabaseSettings = root.TryGetProperty("supabaseUrl", out _)
        || root.TryGetProperty("supabaseApiKey", out _)
        || root.TryGetProperty("supabaseServiceRoleKey", out _)
        || root.TryGetProperty("supabaseAnonKey", out _)
        || root.TryGetProperty("supabaseProjectId", out _)
        || root.TryGetProperty("supabaseJwt", out _)
        || root.TryGetProperty("supabaseAttendanceTable", out _)
        || root.TryGetProperty("supabaseSyncEnabled", out _);

      var savePollingSettings = root.TryGetProperty("pollIntervalMinutes", out _)
        || root.TryGetProperty("pollIntervalSeconds", out _)
        || root.TryGetProperty("autoSyncEnabled", out _)
        || root.TryGetProperty("scheduleLocalTimes", out _);

      var now = DateTimeOffset.UtcNow;
      DashboardSettings? dashboardSettingsToSave = null;
      DevicePreset? devicePresetToSave = null;
      PollingPreset? pollingPresetToSave = null;
      string[]? scheduleToSave = null;
      var saveUiSettings = root.TryGetProperty("dashboardRefreshMinutes", out _)
        || root.TryGetProperty("dashboardRefreshSeconds", out _);
      int? dashboardRefreshSecondsToSave = null;

      lock (stateGate)
      {
        var savedPreset = false;
        string? savedIp = null;
        int? savedPort = null;
        string? savedReader = null;

        if (root.TryGetProperty("deviceIp", out var ipEl))
        {
          var ip = (ipEl.GetString() ?? "").Trim();
          if (!string.IsNullOrWhiteSpace(ip))
          {
            currentConfig = currentConfig with { DeviceIp = ip, DeviceId = $"WL10-{ip}" };
            savedIp = ip;
            savedPreset = true;
          }
        }

        if (root.TryGetProperty("devicePort", out var portEl) && portEl.TryGetInt32(out var newPort))
        {
          if (newPort > 0 && newPort <= 65535)
          {
            currentConfig = currentConfig with { DevicePort = newPort };
            savedPort = newPort;
            savedPreset = true;
          }
        }

        if (root.TryGetProperty("readerMode", out var rmEl))
        {
          var rm = (rmEl.GetString() ?? "").Trim();
          if (!string.IsNullOrWhiteSpace(rm))
          {
            currentConfig = currentConfig with { ReaderMode = rm };
            savedReader = rm;
            savedPreset = true;
          }
        }

        if (root.TryGetProperty("pollIntervalMinutes", out var pollMinEl) && pollMinEl.TryGetInt32(out var min))
        {
          var sec = min * 60;
          pollIntervalSeconds = Math.Clamp(sec, 60, 3600);
        }
        else if (root.TryGetProperty("pollIntervalSeconds", out var pollEl) && pollEl.TryGetInt32(out var sec))
        {
          pollIntervalSeconds = Math.Clamp(sec, 60, 3600);
        }

        if (root.TryGetProperty("autoSyncEnabled", out var autoEl) && (autoEl.ValueKind == JsonValueKind.True || autoEl.ValueKind == JsonValueKind.False))
        {
          autoSyncEnabled = autoEl.GetBoolean();
        }

        if (root.TryGetProperty("dashboardRefreshMinutes", out var dashMinEl) && dashMinEl.TryGetInt32(out var dashMin))
        {
          dashboardRefreshSeconds = Math.Clamp(dashMin * 60, 10, 3600);
        }
        else if (root.TryGetProperty("dashboardRefreshSeconds", out var dashSecEl) && dashSecEl.TryGetInt32(out var dashSec))
        {
          dashboardRefreshSeconds = Math.Clamp(dashSec, 10, 3600);
        }

        if (saveUiSettings)
        {
          dashboardRefreshSecondsToSave = dashboardRefreshSeconds;
        }

        if (root.TryGetProperty("supabaseUrl", out var supaUrlEl))
        {
          _ = (supaUrlEl.GetString() ?? "").Trim();
          currentConfig = currentConfig with { SupabaseUrl = DefaultSupabaseUrl };
        }

        if (root.TryGetProperty("supabaseAttendanceTable", out var supaTableEl))
        {
          var t = (supaTableEl.GetString() ?? "").Trim();
          if (!string.IsNullOrWhiteSpace(t)) currentConfig = currentConfig with { SupabaseAttendanceTable = t };
        }

        if (root.TryGetProperty("supabaseSyncEnabled", out var supaSyncEl) && (supaSyncEl.ValueKind == JsonValueKind.True || supaSyncEl.ValueKind == JsonValueKind.False))
        {
          currentConfig = currentConfig with { SupabaseSyncEnabled = supaSyncEl.GetBoolean() };
        }

        if (root.TryGetProperty("supabaseServiceRoleKey", out var supaSrvEl))
        {
          var k = (supaSrvEl.GetString() ?? "").Trim();
          if (!string.IsNullOrWhiteSpace(k)) currentConfig = currentConfig with { SupabaseServiceRoleKey = k };
        }
        else if (root.TryGetProperty("supabaseApiKey", out var supaKeyEl))
        {
          var k = (supaKeyEl.GetString() ?? "").Trim();
          if (!string.IsNullOrWhiteSpace(k)) currentConfig = currentConfig with { SupabaseServiceRoleKey = k };
        }

        if (root.TryGetProperty("supabaseAnonKey", out var supaAnonEl))
        {
          _ = (supaAnonEl.GetString() ?? string.Empty).Trim();
          dashboardSupabaseAnonKey = DefaultSupabasePublishableKey;
        }

        if (root.TryGetProperty("supabaseProjectId", out var supaPidEl))
        {
          _ = (supaPidEl.GetString() ?? string.Empty).Trim();
          dashboardSupabaseProjectId = DefaultSupabaseProjectId;
        }

        if (root.TryGetProperty("supabaseJwt", out var supaJwtEl))
        {
          var jwt = (supaJwtEl.GetString() ?? string.Empty).Trim();
          if (!string.IsNullOrWhiteSpace(jwt)) dashboardSupabaseJwtSecret = jwt;
        }

        if (saveSupabaseSettings)
        {
          currentConfig = currentConfig with { SupabaseUrl = DefaultSupabaseUrl };
          dashboardSupabaseProjectId = DefaultSupabaseProjectId;
          dashboardSupabaseAnonKey = DefaultSupabasePublishableKey;

          var (protectedKey, isProtected) = TryProtectBase64(currentConfig.SupabaseServiceRoleKey ?? string.Empty);
          dashboardSettingsToSave = new DashboardSettings(
            currentConfig.SupabaseUrl ?? string.Empty,
            protectedKey,
            isProtected,
            currentConfig.SupabaseAttendanceTable ?? string.Empty,
            currentConfig.SupabaseSyncEnabled,
            dashboardSupabaseAnonKey ?? string.Empty,
            dashboardSupabaseProjectId ?? string.Empty,
            dashboardSupabaseJwtSecret ?? string.Empty
          );
        }

        if (savedPreset && savedIp is not null && savedPort is not null && savedReader is not null)
        {
          devicePresetToSave = new DevicePreset(savedIp, savedPort.Value, savedReader, now, null);
        }

        if (savePollingSettings)
        {
          pollingPresetToSave = new PollingPreset(pollIntervalSeconds, autoSyncEnabled, now);
        }

        if (root.TryGetProperty("scheduleLocalTimes", out var schedEl) && schedEl.ValueKind == JsonValueKind.Array)
        {
          var list = new List<string>();
          foreach (var el in schedEl.EnumerateArray())
          {
            if (el.ValueKind != JsonValueKind.String) continue;
            var s = (el.GetString() ?? string.Empty).Trim();
            if (s.Length > 0) list.Add(s);
          }
          var normalized = NormalizeScheduleStrings(list);
          syncScheduleLocalTimes = normalized;
          scheduleToSave = normalized;
          syncScheduleVersion++;
        }
      }

      if (dashboardSettingsToSave is not null || devicePresetToSave is not null || pollingPresetToSave is not null || dashboardRefreshSecondsToSave is not null || scheduleToSave is not null)
      {
        var state = LoadState(statePath);
        var updated = state;

        if (dashboardRefreshSecondsToSave is not null)
        {
          updated = updated with { DashboardRefreshSeconds = dashboardRefreshSecondsToSave.Value };
        }

        if (dashboardSettingsToSave is not null)
        {
          var dbPresets = (updated.DbPresets ?? Array.Empty<DbPreset>()).ToList();
          dbPresets.Insert(0, new DbPreset(dashboardSettingsToSave, now, null));
          var keepDb = dbPresets
            .GroupBy(p => $"{p.Settings.SupabaseUrl}|{p.Settings.SupabaseAttendanceTable}|{p.Settings.SupabaseAnonKey}|{p.Settings.SupabaseProjectId}|{p.Settings.SupabaseJwtSecret}", StringComparer.Ordinal)
            .Select(g => g.First())
            .Take(5)
            .ToArray();
          updated = updated with { DashboardSettings = dashboardSettingsToSave, DbPresets = keepDb };
        }

        if (devicePresetToSave is not null)
        {
          var list = (updated.DevicePresets ?? Array.Empty<DevicePreset>()).ToList();
          list.Insert(0, devicePresetToSave);
          var keep = list
            .GroupBy(p => $"{p.DeviceIp}|{p.DevicePort}|{p.ReaderMode}", StringComparer.Ordinal)
            .Select(g => g.First())
            .Take(5)
            .ToArray();
          updated = updated with { DevicePresets = keep };
        }

        if (pollingPresetToSave is not null)
        {
          var list = (updated.PollingPresets ?? Array.Empty<PollingPreset>()).ToList();
          list.Insert(0, pollingPresetToSave);
          var keep = list
            .GroupBy(p => $"{p.PollIntervalSeconds}|{p.AutoSyncEnabled}", StringComparer.Ordinal)
            .Select(g => g.First())
            .Take(5)
            .ToArray();
          updated = updated with { PollingPresets = keep };
        }

        if (scheduleToSave is not null)
        {
          updated = updated with { SyncScheduleLocalTimes = scheduleToSave };
        }

        SaveState(statePath, updated);
      }

      return Results.Json(new { ok = true }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/supabase/test", async (HttpContext ctx) =>
    {
      AppConfig cfg;
      string anon;
      string pid;
      string jwt;
      lock (stateGate)
      {
        cfg = currentConfig;
        anon = dashboardSupabaseAnonKey;
        pid = dashboardSupabaseProjectId;
        jwt = dashboardSupabaseJwtSecret;
      }
      var result = await TryTestSupabase(cfg, anon, ctx.RequestAborted);
      if (result.Ok)
      {
        var now = DateTimeOffset.UtcNow;
        var (protectedKey, isProtected) = TryProtectBase64(cfg.SupabaseServiceRoleKey ?? string.Empty);
        var ds = new DashboardSettings(
          cfg.SupabaseUrl ?? string.Empty,
          protectedKey,
          isProtected,
          cfg.SupabaseAttendanceTable ?? string.Empty,
          cfg.SupabaseSyncEnabled,
          anon ?? string.Empty,
          pid ?? string.Empty,
          jwt ?? string.Empty
        );
        var state = LoadState(statePath);
        var list = (state.DbPresets ?? Array.Empty<DbPreset>()).ToList();
        var key = $"{ds.SupabaseUrl}|{ds.SupabaseAttendanceTable}|{ds.SupabaseAnonKey}|{ds.SupabaseProjectId}|{ds.SupabaseJwtSecret}";
        var i = list.FindIndex(p => $"{p.Settings.SupabaseUrl}|{p.Settings.SupabaseAttendanceTable}|{p.Settings.SupabaseAnonKey}|{p.Settings.SupabaseProjectId}|{p.Settings.SupabaseJwtSecret}" == key);
        if (i >= 0)
        {
          var existing = list[i];
          list[i] = existing with { LastOkAtUtc = now };
        }
        else
        {
          list.Insert(0, new DbPreset(ds, now, now));
        }
        var keep = list
          .GroupBy(p => $"{p.Settings.SupabaseUrl}|{p.Settings.SupabaseAttendanceTable}|{p.Settings.SupabaseAnonKey}|{p.Settings.SupabaseProjectId}|{p.Settings.SupabaseJwtSecret}", StringComparer.Ordinal)
          .Select(g => g.First())
          .OrderByDescending(p => p.LastOkAtUtc ?? p.SavedAtUtc)
          .Take(5)
          .ToArray();
        SaveState(statePath, state with { DashboardSettings = ds, DbPresets = keep });
      }
      return Results.Json(result, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/events", async (HttpContext ctx) =>
    {
      AppConfig cfg;
      string anon;
      lock (stateGate)
      {
        cfg = currentConfig;
        anon = dashboardSupabaseAnonKey;
      }
      var (ok, rows, error) = await TryFetchLatestAttendanceEventsVerbose(cfg, anon, limit: 50, null, null, deviceIdFilter: null, ctx.RequestAborted);
      return Results.Json(new { ok, rows, error }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/db/records", async (HttpContext ctx) =>
    {
      AppConfig cfg;
      string anon;
      lock (stateGate)
      {
        cfg = currentConfig;
        anon = dashboardSupabaseAnonKey;
      }
      var limit = 200;
      if (ctx.Request.Query.TryGetValue("limit", out var l) && int.TryParse(l.ToString(), out var li))
      {
        limit = Math.Clamp(li, 1, 200);
      }

      string? from = null;
      string? to = null;
      if (ctx.Request.Query.TryGetValue("from", out var f))
      {
        var s = (f.ToString() ?? string.Empty).Trim();
        if (s.Length > 0) from = s;
      }
      if (ctx.Request.Query.TryGetValue("to", out var t))
      {
        var s = (t.ToString() ?? string.Empty).Trim();
        if (s.Length > 0) to = s;
      }

      string? deviceIdFilter = cfg.DeviceId;
      if (ctx.Request.Query.TryGetValue("deviceId", out var dv))
      {
        var s = (dv.ToString() ?? string.Empty).Trim();
        if (s.Length == 0) deviceIdFilter = cfg.DeviceId;
        else if (s.Equals("all", StringComparison.OrdinalIgnoreCase)) deviceIdFilter = null;
        else deviceIdFilter = s;
      }

      var (ok, rows, error) = await TryFetchLatestAttendanceEventsVerbose(cfg, anon, limit: limit, from, to, deviceIdFilter: deviceIdFilter, ctx.RequestAborted);
      return Results.Json(new { ok, rows, error }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/supabase/update", async (HttpContext ctx) =>
    {
      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;

      var configured = !string.IsNullOrWhiteSpace(cfg.SupabaseUrl)
        && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey)
        && !string.IsNullOrWhiteSpace(cfg.SupabaseAttendanceTable);
      if (!configured) return Results.Json(new { ok = false, error = "Supabase not configured" }, JsonOptions);

      runtime.LastSupabaseSyncStartedAtUtc = DateTimeOffset.UtcNow;
      runtime.LastSupabaseSyncFinishedAtUtc = null;
      runtime.LastSupabaseSyncError = null;
      runtime.LastSupabaseSyncResult = "running";
      runtime.LastSupabaseUpsertedCount = 0;

      try
      {
        var path = TryResolveAttlogExportPath();
        if (string.IsNullOrWhiteSpace(path)) throw new InvalidOperationException("Could not resolve 1_attlog.dat path");
        if (!File.Exists(path)) throw new InvalidOperationException($"File not found: {path}");

        var rows = ReadAttlogRows(path);
        var keys = new HashSet<string>(StringComparer.Ordinal);
        var distinctRows = new List<AttlogRow>(capacity: rows.Count);
        string? minDt = null;
        string? maxDt = null;
        for (var i = 0; i < rows.Count; i++)
        {
          var r = rows[i];
          var k = r.StaffId + "|" + r.DateTime;
          if (!keys.Add(k)) continue;
          distinctRows.Add(r);
          var dt = r.DateTime ?? string.Empty;
          if (dt.Length == 0) continue;
          if (minDt is null || string.CompareOrdinal(dt, minDt) < 0) minDt = dt;
          if (maxDt is null || string.CompareOrdinal(dt, maxDt) > 0) maxDt = dt;
        }

        DateTimeOffset? FromLocal8ToUtc(string dtRaw)
        {
          if (string.IsNullOrWhiteSpace(dtRaw)) return null;
          if (!DateTime.TryParseExact(dtRaw.Trim(), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var localUnspec)) return null;
          var localOffset = new DateTimeOffset(DateTime.SpecifyKind(localUnspec, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
          return localOffset.ToUniversalTime();
        }

        var rangeFromUtc = minDt is null ? null : FromLocal8ToUtc(minDt);
        var rangeToUtc = maxDt is null ? null : FromLocal8ToUtc(maxDt);
        string? deleted = null;
        if (rangeFromUtc.HasValue && rangeToUtc.HasValue && !string.IsNullOrWhiteSpace(cfg.DeviceId))
        {
          var fromUtc = rangeFromUtc.Value;
          var toUtc = rangeToUtc.Value;
          if (toUtc < fromUtc)
          {
            var tmp = fromUtc;
            fromUtc = toUtc;
            toUtc = tmp;
          }

          var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
          var table = cfg.SupabaseAttendanceTable.Trim();
          var deviceId = Uri.EscapeDataString(cfg.DeviceId.Trim());
          var fromIso = Uri.EscapeDataString(fromUtc.ToString("O", CultureInfo.InvariantCulture));
          var toIso = Uri.EscapeDataString(toUtc.ToString("O", CultureInfo.InvariantCulture));
          var delUrl = $"{baseUrl}/rest/v1/{table}?device_id=eq.{deviceId}&occurred_at=gte.{fromIso}&occurred_at=lte.{toIso}";

          using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(30) };
          using var delReq = new HttpRequestMessage(HttpMethod.Delete, delUrl);
          delReq.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
          delReq.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
          delReq.Headers.TryAddWithoutValidation("Prefer", "count=exact,return=minimal");
          using var delResp = await http.SendAsync(delReq, ctx.RequestAborted);
          if (!delResp.IsSuccessStatusCode)
          {
            var body = await delResp.Content.ReadAsStringAsync(ctx.RequestAborted);
            throw new InvalidOperationException($"Supabase delete failed: {(int)delResp.StatusCode} {delResp.ReasonPhrase}. Body={body}");
          }
          deleted = delResp.Headers.TryGetValues("Content-Range", out var ranges) ? ranges.FirstOrDefault() : null;
        }

        await UpsertAttlogRowsToSupabase(cfg, distinctRows, ctx.RequestAborted);

        int w30FilesProcessed = 0;
        int w30RowsUpserted = 0;
        var w30Details = new List<object>(capacity: 5);
        try
        {
          var deviceCfg = EnsureConfiguredDevices(saveIfSeeded: true);
          var w30Devices = deviceCfg.Where(d => d.DeviceType == "W30").ToArray();
          if (w30Devices.Length > 0)
          {
            var baseDir = Path.GetDirectoryName(Path.GetFullPath(path)) ?? string.Empty;
            var state = LoadState(statePath);
            var processedKeys = (state.ProcessedFiles ?? Array.Empty<ProcessedFileEntry>())
              .Where(x => x is not null && !string.IsNullOrWhiteSpace(x.DeviceId) && !string.IsNullOrWhiteSpace(x.FileName))
              .Select(x => $"{x.DeviceId}|{x.FileName}|{x.SizeBytes}|{x.LastWriteUtcTicks}")
              .ToHashSet(StringComparer.Ordinal);

            var newEntries = new List<ProcessedFileEntry>(capacity: 64);

            foreach (var dev in w30Devices)
            {
              var dir = (dev.LogDir ?? string.Empty).Trim();
              if (dir.Length == 0) dir = baseDir.Length > 0 ? baseDir : Directory.GetCurrentDirectory();
              var pat = (dev.FilePattern ?? string.Empty).Trim();
              if (pat.Length == 0) pat = "AttendanceLog*.dat";
              if (!Directory.Exists(dir)) continue;

              List<(FileInfo file, string key)> candidates;
              try
              {
                candidates = new DirectoryInfo(dir)
                  .EnumerateFiles(pat, SearchOption.TopDirectoryOnly)
                  .Select(f =>
                  {
                    var full = f.FullName;
                    var lw = 0L;
                    var sz = 0L;
                    try { lw = f.LastWriteTimeUtc.Ticks; } catch { lw = 0L; }
                    try { sz = f.Length; } catch { sz = 0L; }
                    var k = $"{dev.DeviceId}|{full}|{sz}|{lw}";
                    return (file: f, key: k);
                  })
                  .OrderBy(x => x.file.LastWriteTimeUtc)
                  .ToList();
              }
              catch
              {
                candidates = new List<(FileInfo, string)>();
              }

              var w30Rows = new List<AttlogRow>(capacity: 1024);
              foreach (var c in candidates)
              {
                if (processedKeys.Contains(c.key)) continue;
                try
                {
                  foreach (var r in ReadW30AttlogRows(c.file.FullName)) w30Rows.Add(r);
                  w30FilesProcessed++;
                  var lw = 0L;
                  var sz = 0L;
                  try { lw = c.file.LastWriteTimeUtc.Ticks; } catch { lw = 0L; }
                  try { sz = c.file.Length; } catch { sz = 0L; }
                  newEntries.Add(new ProcessedFileEntry(dev.DeviceId, c.file.FullName, sz, lw, DateTimeOffset.UtcNow));
                  processedKeys.Add(c.key);
                }
                catch { }
              }

              int upserted = 0;
              if (w30Rows.Count > 0)
              {
                var w30Cfg = cfg with { DeviceId = dev.DeviceId };
                var w30DistinctKeys = new HashSet<string>(StringComparer.Ordinal);
                var w30Distinct = new List<AttlogRow>(capacity: w30Rows.Count);
                foreach (var r in w30Rows)
                {
                  var k = (r.StaffId ?? string.Empty) + "|" + (r.DateTime ?? string.Empty);
                  if (!w30DistinctKeys.Add(k)) continue;
                  w30Distinct.Add(r);
                }
                await UpsertAttlogRowsToSupabase(w30Cfg, w30Distinct, ctx.RequestAborted);
                upserted = w30Distinct.Count;
                w30RowsUpserted += upserted;
              }

              w30Details.Add(new { deviceId = dev.DeviceId, logDir = dir, filePattern = pat, filesProcessed = candidates.Count, newFilesImported = w30Rows.Count > 0 ? 1 : 0, rowsUpserted = upserted });
            }

            if (newEntries.Count > 0)
            {
              var latest = LoadState(statePath);
              var merged = (latest.ProcessedFiles ?? Array.Empty<ProcessedFileEntry>()).ToList();
              merged.AddRange(newEntries);
              var keep = merged
                .Where(x => x is not null && !string.IsNullOrWhiteSpace(x.DeviceId) && !string.IsNullOrWhiteSpace(x.FileName))
                .OrderByDescending(x => x.ProcessedAtUtc)
                .Take(5000)
                .ToArray();
              SaveState(statePath, latest with { ProcessedFiles = keep });
            }
          }
        }
        catch { }

        runtime.LastSupabaseSyncResult = "ok";
        runtime.LastSupabaseUpsertedCount = keys.Count;
        return Results.Json(new
        {
          ok = true,
          upserted = rows.Count,
          distinct = keys.Count,
          rangeFrom = minDt,
          rangeTo = maxDt,
          deleted,
          w30 = new { filesProcessed = w30FilesProcessed, rowsUpserted = w30RowsUpserted, devices = w30Details.ToArray() }
        }, JsonOptions);
      }
      catch (Exception ex)
      {
        runtime.LastSupabaseSyncResult = "error";
        runtime.LastSupabaseSyncError = ex.ToString();
        return Results.Json(new { ok = false, error = ex.Message }, JsonOptions);
      }
      finally
      {
        runtime.LastSupabaseSyncFinishedAtUtc = DateTimeOffset.UtcNow;
      }
    }).RequireAuthorization();

    app.MapGet("/api/supabase/validate", async (HttpContext ctx) =>
    {
      AppConfig cfg;
      string anon;
      lock (stateGate)
      {
        cfg = currentConfig;
        anon = dashboardSupabaseAnonKey;
      }

      var from = (ctx.Request.Query["from"].ToString() ?? string.Empty).Trim();
      var to = (ctx.Request.Query["to"].ToString() ?? string.Empty).Trim();
      var expectedRaw = (ctx.Request.Query["expected"].ToString() ?? string.Empty).Trim();
      _ = int.TryParse(expectedRaw, out var expected);
      expected = Math.Max(0, expected);

      if (from.Length == 0 || to.Length == 0)
      {
        return Results.Json(new { ok = false, error = "from/to required" }, JsonOptions);
      }

      var (ok, count, error) = await TryGetSupabaseCountInRange(cfg, anon, from, to, ctx.RequestAborted);
      if (!ok) return Results.Json(new { ok = false, error }, JsonOptions);

      var pass = expected == 0 ? (count >= 0) : (count >= expected);
      var discrepancy = expected > 0 && count < expected ? (expected - count) : 0;
      return Results.Json(new { ok = pass, expected, found = count, discrepancy }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/files/converted", () =>
    {
      var export = TryResolveAttlogExportPath();
      var root = export is not null ? (Path.GetDirectoryName(export) ?? Path.Combine(Directory.GetCurrentDirectory(), "Reference")) : Path.Combine(Directory.GetCurrentDirectory(), "Reference");
      var items = new List<object>(capacity: 20);
      if (Directory.Exists(root))
      {
        var txtFiles = Directory.EnumerateFiles(root, "*.txt", SearchOption.TopDirectoryOnly)
          .Select(p => new FileInfo(p))
          .OrderByDescending(f => f.LastWriteTimeUtc)
          .Take(50);

        foreach (var txt in txtFiles)
        {
          var datPath = Path.ChangeExtension(txt.FullName, ".dat");
          if (!File.Exists(datPath)) continue;
          var dat = new FileInfo(datPath);
          items.Add(new
          {
            baseName = Path.GetFileNameWithoutExtension(txt.Name),
            dat = dat.Name,
            txt = txt.Name,
            datMtime = dat.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss"),
            txtMtime = txt.LastWriteTime.ToString("yyyy-MM-dd HH:mm:ss"),
            downloadUrl = "/api/files/download?name=" + Uri.EscapeDataString(txt.Name),
          });
          if (items.Count >= 20) break;
        }
      }
      return Results.Json(new { items }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/files/download", (HttpContext ctx) =>
    {
      var name = (ctx.Request.Query["name"].ToString() ?? string.Empty).Trim();
      if (name.Length == 0) return Results.NotFound();
      if (name.Contains('/') || name.Contains('\\') || name.Contains("..", StringComparison.Ordinal)) return Results.NotFound();
      if (!name.EndsWith(".txt", StringComparison.OrdinalIgnoreCase)) return Results.NotFound();
      var export = TryResolveAttlogExportPath();
      var root = export is not null ? (Path.GetDirectoryName(export) ?? Path.Combine(Directory.GetCurrentDirectory(), "Reference")) : Path.Combine(Directory.GetCurrentDirectory(), "Reference");
      var full = Path.Combine(root, name);
      if (!File.Exists(full)) return Results.NotFound();
      return Results.File(full, "text/plain; charset=utf-8", fileDownloadName: name);
    }).RequireAuthorization();

    app.MapPost("/api/env/write", async (HttpContext ctx) =>
    {
      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var root = doc.RootElement;
      var url = root.TryGetProperty("supabaseUrl", out var urlEl) ? (urlEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var anon = root.TryGetProperty("supabaseAnonKey", out var anonEl) ? (anonEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var pid = root.TryGetProperty("supabaseProjectId", out var pidEl) ? (pidEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var service = root.TryGetProperty("supabaseServiceRoleKey", out var srvEl) ? (srvEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var jwt = root.TryGetProperty("supabaseJwtSecret", out var jwtEl) ? (jwtEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var outDir = Path.Combine(Directory.GetCurrentDirectory(), "SHAB Dashboard");
      var outPath = Path.Combine(outDir, ".env.local");
      Directory.CreateDirectory(outDir);
      if (File.Exists(outPath) && (service.Length == 0 || jwt.Length == 0))
      {
        try
        {
          foreach (var rawLine in File.ReadAllLines(outPath))
          {
            var line = (rawLine ?? string.Empty).Trim();
            if (line.Length == 0) continue;
            if (line.StartsWith("#", StringComparison.Ordinal)) continue;
            if (line.StartsWith("export ", StringComparison.OrdinalIgnoreCase)) line = line["export ".Length..].Trim();

            var eq = line.IndexOf('=', StringComparison.Ordinal);
            if (eq <= 0) continue;
            var k = line[..eq].Trim();
            var v = line[(eq + 1)..].Trim();
            if (v.Length >= 2)
            {
              var first = v[0];
              var last = v[^1];
              if ((first == '"' && last == '"') || (first == '\'' && last == '\''))
              {
                v = v[1..^1];
              }
            }
            if (service.Length == 0 && k.Equals("SUPABASE_SERVICE_ROLE_KEY", StringComparison.OrdinalIgnoreCase)) service = v;
            if (jwt.Length == 0 && k.Equals("SUPABASE_JWT_SECRET", StringComparison.OrdinalIgnoreCase)) jwt = v;
          }
        }
        catch { }
      }
      var text =
        $"VITE_SUPABASE_URL={url}\n" +
        $"VITE_SUPABASE_ANON_KEY={anon}\n" +
        $"VITE_SUPABASE_PROJECT_ID={pid}\n" +
        $"SUPABASE_URL={url}\n" +
        $"SUPABASE_PROJECT_ID={pid}\n" +
        (service.Length > 0 ? $"SUPABASE_SERVICE_ROLE_KEY={service}\n" : string.Empty) +
        (jwt.Length > 0 ? $"SUPABASE_JWT_SECRET={jwt}\n" : string.Empty);
      await File.WriteAllTextAsync(outPath, text, Encoding.UTF8, ctx.RequestAborted);
      return Results.Json(new { ok = true, path = outPath }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/device/connect", async (HttpContext ctx) =>
    {
      string? bodyIp = null;
      int? bodyPort = null;
      string? bodyReaderMode = null;
      try
      {
        using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
        var root = doc.RootElement;
        if (root.TryGetProperty("deviceIp", out var ipEl))
        {
          var ip = (ipEl.GetString() ?? string.Empty).Trim();
          if (!string.IsNullOrWhiteSpace(ip)) bodyIp = ip;
        }
        if (root.TryGetProperty("devicePort", out var portEl) && portEl.TryGetInt32(out var p))
        {
          if (p > 0 && p <= 65535) bodyPort = p;
        }
        if (root.TryGetProperty("readerMode", out var rmEl))
        {
          var rm = (rmEl.GetString() ?? string.Empty).Trim();
          if (!string.IsNullOrWhiteSpace(rm)) bodyReaderMode = rm;
        }
      }
      catch { }

      AppConfig cfg;
      var now = DateTimeOffset.UtcNow;
      lock (stateGate)
      {
        if (bodyIp is not null) currentConfig = currentConfig with { DeviceIp = bodyIp, DeviceId = $"WL10-{bodyIp}" };
        if (bodyPort is not null) currentConfig = currentConfig with { DevicePort = bodyPort.Value };
        if (bodyReaderMode is not null) currentConfig = currentConfig with { ReaderMode = bodyReaderMode };
        cfg = currentConfig;
      }

      var (ok, err, rttMs) = await ProbeTcpAsync(cfg.DeviceIp, cfg.DevicePort, TimeSpan.FromSeconds(5), ctx.RequestAborted);
      if (ok) Console.WriteLine($"TCP OK to {cfg.DeviceIp}:{cfg.DevicePort} {(rttMs is not null ? $"{rttMs}ms" : "")}".Trim());
      else Console.WriteLine($"TCP FAIL to {cfg.DeviceIp}:{cfg.DevicePort} {(err ?? "error")}".Trim());

      var state = LoadState(statePath);
      var list = (state.DevicePresets ?? Array.Empty<DevicePreset>()).ToList();
      var i = list.FindIndex(p => string.Equals(p.DeviceIp, cfg.DeviceIp, StringComparison.Ordinal) && p.DevicePort == cfg.DevicePort && string.Equals(p.ReaderMode, cfg.ReaderMode, StringComparison.Ordinal));
      if (i >= 0)
      {
        var existing = list[i];
        list[i] = existing with { SavedAtUtc = now, LastOkAtUtc = ok ? now : existing.LastOkAtUtc };
      }
      else
      {
        list.Insert(0, new DevicePreset(cfg.DeviceIp, cfg.DevicePort, cfg.ReaderMode, now, ok ? now : null));
      }
      var keep = list
        .GroupBy(p => $"{p.DeviceIp}|{p.DevicePort}|{p.ReaderMode}", StringComparer.Ordinal)
        .Select(g => g.First())
        .OrderByDescending(p => p.LastOkAtUtc ?? p.SavedAtUtc)
        .Take(5)
        .ToArray();
      SaveState(statePath, state with { DevicePresets = keep });

      bool? verifyOk = null;
      string? verifyError = null;
      if (ok)
      {
        var (vOk, vErr) = await ExecuteSync(verify: true, today: true, supabaseOverride: false, ctx.RequestAborted);
        verifyOk = vOk;
        verifyError = vErr;
        if (vOk) Console.WriteLine($"VERIFY OK (read) from {cfg.DeviceIp}:{cfg.DevicePort}");
        else Console.WriteLine($"VERIFY FAIL (read) from {cfg.DeviceIp}:{cfg.DevicePort} {(vErr ?? "error")}".Trim());
        lock (stateGate) autoSyncEnabled = true;
      }

      return Results.Json(new
      {
        ok,
        error = err,
        rttMs,
        deviceIp = cfg.DeviceIp,
        devicePort = cfg.DevicePort,
        readerMode = cfg.ReaderMode,
        verifyOk,
        verifyError,
        autoSyncEnabled = ok ? true : (bool?)null
      }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/device/test", async (HttpContext ctx) =>
    {
      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      var (ok, err, rttMs) = await ProbeTcpAsync(cfg.DeviceIp, cfg.DevicePort, TimeSpan.FromSeconds(5), ctx.RequestAborted);
      if (ok) Console.WriteLine($"TCP OK to {cfg.DeviceIp}:{cfg.DevicePort} {(rttMs is not null ? $"{rttMs}ms" : "")}".Trim());
      else Console.WriteLine($"TCP FAIL to {cfg.DeviceIp}:{cfg.DevicePort} {(err ?? "error")}".Trim());
      if (ok)
      {
        var now = DateTimeOffset.UtcNow;
        var state = LoadState(statePath);
        var list = (state.DevicePresets ?? Array.Empty<DevicePreset>()).ToList();
        var i = list.FindIndex(p => string.Equals(p.DeviceIp, cfg.DeviceIp, StringComparison.Ordinal) && p.DevicePort == cfg.DevicePort && string.Equals(p.ReaderMode, cfg.ReaderMode, StringComparison.Ordinal));
        if (i >= 0)
        {
          var existing = list[i];
          list[i] = existing with { LastOkAtUtc = now };
        }
        else
        {
          list.Insert(0, new DevicePreset(cfg.DeviceIp, cfg.DevicePort, cfg.ReaderMode, now, now));
        }
        var keep = list
          .GroupBy(p => $"{p.DeviceIp}|{p.DevicePort}|{p.ReaderMode}", StringComparer.Ordinal)
          .Select(g => g.First())
          .OrderByDescending(p => p.LastOkAtUtc ?? p.SavedAtUtc)
          .Take(5)
          .ToArray();
        SaveState(statePath, state with { DevicePresets = keep });
      }
      return Results.Json(new { ok, error = err, rttMs, deviceIp = cfg.DeviceIp, devicePort = cfg.DevicePort, readerMode = cfg.ReaderMode }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/device/ping", async (HttpContext ctx) =>
    {
      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      try
      {
        using var ping = new System.Net.NetworkInformation.Ping();
        var reply = await ping.SendPingAsync(cfg.DeviceIp, 3000);
        var ok = reply.Status == System.Net.NetworkInformation.IPStatus.Success;
        var err = ok ? null : reply.Status.ToString();
        if (ok) Console.WriteLine($"Ping OK to {cfg.DeviceIp} {(reply.RoundtripTime > 0 ? $"{reply.RoundtripTime}ms" : "")}".Trim());
        else Console.WriteLine($"Ping FAIL to {cfg.DeviceIp} {(err ?? "error")}".Trim());
        return Results.Json(new { ok, error = err, rttMs = (int?)reply.RoundtripTime, deviceIp = cfg.DeviceIp, devicePort = cfg.DevicePort, readerMode = cfg.ReaderMode }, JsonOptions);
      }
      catch (Exception ex)
      {
        Console.WriteLine($"Ping FAIL to {cfg.DeviceIp} {ex.Message}".Trim());
        return Results.Json(new { ok = false, error = ex.Message, deviceIp = cfg.DeviceIp, devicePort = cfg.DevicePort, readerMode = cfg.ReaderMode }, JsonOptions);
      }
    }).RequireAuthorization();

    app.MapPost("/api/device/disconnect", (HttpContext ctx) =>
    {
      lock (stateGate)
      {
        autoSyncEnabled = false;
      }
      Console.WriteLine("Disconnect requested.");
      return Results.Json(new { ok = true }, JsonOptions);
    }).RequireAuthorization();

    static string NormalizeDeviceType(string raw)
    {
      var t = (raw ?? string.Empty).Trim();
      if (t.Length == 0) return "WL10";
      if (t.Equals("WL10", StringComparison.OrdinalIgnoreCase)) return "WL10";
      if (t.Equals("W30", StringComparison.OrdinalIgnoreCase)) return "W30";
      return t.ToUpperInvariant();
    }

    static string DefaultDeviceId(string deviceType, string deviceIp)
    {
      var t = NormalizeDeviceType(deviceType);
      var ip = (deviceIp ?? string.Empty).Trim();
      if (ip.Length == 0) ip = "unknown";
      return $"{t}-{ip}";
    }

    static ConfiguredDeviceEntry NormalizeDeviceEntry(ConfiguredDeviceEntry d)
    {
      var type = NormalizeDeviceType(d.DeviceType);
      var ip = (d.DeviceIp ?? string.Empty).Trim();
      var id = (d.DeviceId ?? string.Empty).Trim();
      if (id.Length == 0) id = DefaultDeviceId(type, ip);
      var rm = (d.ReaderMode ?? string.Empty).Trim();
      if (rm.Length == 0) rm = type == "W30" ? "file" : "auto";
      var dir = (d.LogDir ?? string.Empty).Trim();
      var pat = (d.FilePattern ?? string.Empty).Trim();
      if (type == "W30" && pat.Length == 0) pat = "AttendanceLog*.dat";
      var port = d.DevicePort;
      if (type == "WL10" && port <= 0) port = 4370;
      return d with { DeviceId = id, DeviceType = type, DeviceIp = ip, DevicePort = port, ReaderMode = rm, LogDir = dir, FilePattern = pat };
    }

    ConfiguredDeviceEntry[] EnsureConfiguredDevices(bool saveIfSeeded)
    {
      var now = DateTimeOffset.UtcNow;
      var state = LoadState(statePath);
      var list = (state.ConfiguredDevices ?? Array.Empty<ConfiguredDeviceEntry>()).ToList();
      if (list.Count > 0) return list.Select(NormalizeDeviceEntry).ToArray();

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      list.Add(new ConfiguredDeviceEntry(cfg.DeviceId, "WL10", cfg.DeviceIp, cfg.DevicePort, cfg.ReaderMode, "", "", null, now));

      var w30EnabledRaw = (Environment.GetEnvironmentVariable("WL10_W30_ENABLED") ?? "").Trim();
      var w30Enabled = w30EnabledRaw.Length == 0
        || w30EnabledRaw.Equals("1", StringComparison.OrdinalIgnoreCase)
        || w30EnabledRaw.Equals("true", StringComparison.OrdinalIgnoreCase)
        || w30EnabledRaw.Equals("yes", StringComparison.OrdinalIgnoreCase)
        || w30EnabledRaw.Equals("on", StringComparison.OrdinalIgnoreCase);
      if (w30Enabled)
      {
        var ip = (Environment.GetEnvironmentVariable("WL10_W30_IP") ?? "192.168.1.177").Trim();
        if (ip.Length == 0) ip = "192.168.1.177";
        var id = (Environment.GetEnvironmentVariable("WL10_W30_DEVICE_ID") ?? $"W30-{ip}").Trim();
        if (id.Length == 0) id = $"W30-{ip}";
        var dir = (Environment.GetEnvironmentVariable("WL10_W30_LOG_DIR") ?? "").Trim();
        var pat = (Environment.GetEnvironmentVariable("WL10_W30_FILE_PATTERN") ?? "AttendanceLog*.dat").Trim();
        list.Add(new ConfiguredDeviceEntry(id, "W30", ip, 0, "file", dir, pat, null, now));
      }

      var normalized = list.Select(NormalizeDeviceEntry).ToArray();
      if (saveIfSeeded)
      {
        SaveState(statePath, state with { ConfiguredDevices = normalized });
      }
      return normalized;
    }

    app.MapGet("/api/devices/list", (HttpContext ctx) =>
    {
      var devices = EnsureConfiguredDevices(saveIfSeeded: true);
      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      return Results.Json(new
      {
        ok = true,
        activeDeviceId = cfg.DeviceId,
        devices = devices.Select(d => new
        {
          deviceId = d.DeviceId,
          deviceType = d.DeviceType,
          deviceIp = d.DeviceIp,
          devicePort = d.DevicePort,
          readerMode = d.ReaderMode,
          logDir = d.LogDir,
          filePattern = d.FilePattern,
          lastOkAtUtc = d.LastOkAtUtc?.ToString("O"),
          savedAtUtc = d.SavedAtUtc == default ? null : d.SavedAtUtc.ToString("O"),
        }).ToArray()
      }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/devices/upsert", async (HttpContext ctx) =>
    {
      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var root = doc.RootElement;
      var now = DateTimeOffset.UtcNow;

      var deviceId = root.TryGetProperty("deviceId", out var idEl) ? (idEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var deviceType = root.TryGetProperty("deviceType", out var tEl) ? (tEl.GetString() ?? string.Empty).Trim() : "WL10";
      var deviceIp = root.TryGetProperty("deviceIp", out var ipEl) ? (ipEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var readerMode = root.TryGetProperty("readerMode", out var rmEl) ? (rmEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var logDir = root.TryGetProperty("logDir", out var ldEl) ? (ldEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var filePattern = root.TryGetProperty("filePattern", out var fpEl) ? (fpEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var port = root.TryGetProperty("devicePort", out var portEl) && portEl.TryGetInt32(out var p) ? p : 0;

      if (deviceIp.Length == 0) return Results.Json(new { ok = false, error = "deviceIp required" }, JsonOptions);
      var entry = NormalizeDeviceEntry(new ConfiguredDeviceEntry(deviceId, deviceType, deviceIp, port, readerMode, logDir, filePattern, null, now));

      var state = LoadState(statePath);
      var list = (state.ConfiguredDevices ?? Array.Empty<ConfiguredDeviceEntry>()).Select(NormalizeDeviceEntry).ToList();
      var i = list.FindIndex(x => string.Equals(x.DeviceId, entry.DeviceId, StringComparison.Ordinal));
      if (i >= 0)
      {
        var existing = list[i];
        list[i] = entry with { LastOkAtUtc = existing.LastOkAtUtc, SavedAtUtc = now };
      }
      else
      {
        list.Add(entry with { SavedAtUtc = now });
      }

      var keep = list
        .Where(x => x is not null && !string.IsNullOrWhiteSpace(x.DeviceId) && !string.IsNullOrWhiteSpace(x.DeviceIp))
        .GroupBy(x => x.DeviceId, StringComparer.Ordinal)
        .Select(g => g.First())
        .OrderByDescending(x => x.SavedAtUtc)
        .Take(20)
        .ToArray();
      SaveState(statePath, state with { ConfiguredDevices = keep });

      return Results.Json(new { ok = true, deviceId = entry.DeviceId }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/devices/delete", async (HttpContext ctx) =>
    {
      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var root = doc.RootElement;
      var deviceId = root.TryGetProperty("deviceId", out var idEl) ? (idEl.GetString() ?? string.Empty).Trim() : string.Empty;
      if (deviceId.Length == 0) return Results.Json(new { ok = false, error = "deviceId required" }, JsonOptions);

      var state = LoadState(statePath);
      var list = (state.ConfiguredDevices ?? Array.Empty<ConfiguredDeviceEntry>()).Select(NormalizeDeviceEntry).ToList();
      var next = list.Where(x => !string.Equals(x.DeviceId, deviceId, StringComparison.Ordinal)).ToArray();
      SaveState(statePath, state with { ConfiguredDevices = next });
      return Results.Json(new { ok = true }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/devices/connect", async (HttpContext ctx) =>
    {
      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var root = doc.RootElement;
      var deviceId = root.TryGetProperty("deviceId", out var idEl) ? (idEl.GetString() ?? string.Empty).Trim() : string.Empty;
      if (deviceId.Length == 0) return Results.Json(new { ok = false, error = "deviceId required" }, JsonOptions);

      var devices = EnsureConfiguredDevices(saveIfSeeded: true);
      var dev = devices.FirstOrDefault(d => string.Equals(d.DeviceId, deviceId, StringComparison.Ordinal));
      if (dev is null) return Results.Json(new { ok = false, error = "not found" }, JsonOptions);

      var now = DateTimeOffset.UtcNow;
      var type = NormalizeDeviceType(dev.DeviceType);
      if (type == "WL10")
      {
        lock (stateGate)
        {
          currentConfig = currentConfig with { DeviceIp = dev.DeviceIp, DevicePort = dev.DevicePort, ReaderMode = dev.ReaderMode, DeviceId = dev.DeviceId };
        }
        var cfg = currentConfig;
        var (ok, err, rttMs) = await ProbeTcpAsync(cfg.DeviceIp, cfg.DevicePort, TimeSpan.FromSeconds(5), ctx.RequestAborted);
        bool? verifyOk = null;
        string? verifyError = null;
        if (ok)
        {
          var (vOk, vErr) = await ExecuteSync(verify: true, today: true, supabaseOverride: false, ctx.RequestAborted);
          verifyOk = vOk;
          verifyError = vErr;
          lock (stateGate) autoSyncEnabled = true;
        }

        var state = LoadState(statePath);
        var list = (state.ConfiguredDevices ?? Array.Empty<ConfiguredDeviceEntry>()).Select(NormalizeDeviceEntry).ToList();
        var i = list.FindIndex(x => string.Equals(x.DeviceId, dev.DeviceId, StringComparison.Ordinal));
        if (i >= 0)
        {
          var existing = list[i];
          list[i] = existing with { LastOkAtUtc = ok ? now : existing.LastOkAtUtc, SavedAtUtc = now };
          SaveState(statePath, state with { ConfiguredDevices = list.ToArray() });
        }

        return Results.Json(new { ok, error = err, rttMs, deviceId = cfg.DeviceId, deviceIp = cfg.DeviceIp, devicePort = cfg.DevicePort, readerMode = cfg.ReaderMode, verifyOk, verifyError }, JsonOptions);
      }

      var (ok2, err2, rtt2) = await ProbeTcpAsync(dev.DeviceIp, dev.DevicePort > 0 ? dev.DevicePort : 80, TimeSpan.FromSeconds(3), ctx.RequestAborted);
      var state2 = LoadState(statePath);
      var list2 = (state2.ConfiguredDevices ?? Array.Empty<ConfiguredDeviceEntry>()).Select(NormalizeDeviceEntry).ToList();
      var j = list2.FindIndex(x => string.Equals(x.DeviceId, dev.DeviceId, StringComparison.Ordinal));
      if (j >= 0)
      {
        var existing = list2[j];
        list2[j] = existing with { LastOkAtUtc = ok2 ? now : existing.LastOkAtUtc, SavedAtUtc = now };
        SaveState(statePath, state2 with { ConfiguredDevices = list2.ToArray() });
      }
      return Results.Json(new { ok = ok2, error = err2, rttMs = rtt2, deviceId = dev.DeviceId, deviceIp = dev.DeviceIp, devicePort = dev.DevicePort, readerMode = dev.ReaderMode }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/devices/disconnect", async (HttpContext ctx) =>
    {
      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var root = doc.RootElement;
      var deviceId = root.TryGetProperty("deviceId", out var idEl) ? (idEl.GetString() ?? string.Empty).Trim() : string.Empty;
      if (deviceId.Length == 0) return Results.Json(new { ok = false, error = "deviceId required" }, JsonOptions);
      lock (stateGate)
      {
        if (string.Equals(currentConfig.DeviceId, deviceId, StringComparison.Ordinal))
        {
          autoSyncEnabled = false;
        }
      }
      return Results.Json(new { ok = true }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/analytics", async (HttpContext ctx) =>
    {
      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      var todayLocal = DateOnly.FromDateTime(DateTime.Now).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
      var device = await TryComputeTodayAnalytics(cfg with { SupabaseSyncEnabled = false }, todayLocal, ctx.RequestAborted);
      var db = await TryComputeTodayAnalytics(cfg with { SupabaseSyncEnabled = true }, todayLocal, ctx.RequestAborted);
      return Results.Json(new { device, db }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/attendance/insights", async (HttpContext ctx) =>
    {
      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;

      var q = ctx.Request.Query;
      var dateRaw = (q.TryGetValue("date", out var v1) ? v1.ToString() : string.Empty).Trim();
      var deptRaw = (q.TryGetValue("department", out var v2) ? v2.ToString() : string.Empty).Trim();
      var monthRaw = (q.TryGetValue("calendarMonth", out var v3) ? v3.ToString() : string.Empty).Trim();

      var dateLocal = DateOnly.FromDateTime(DateTime.Now);
      if (!string.IsNullOrWhiteSpace(dateRaw))
      {
        _ = DateOnly.TryParseExact(dateRaw, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out dateLocal);
      }
      var calMonthRequested = new DateOnly(dateLocal.Year, dateLocal.Month, 1);
      if (!string.IsNullOrWhiteSpace(monthRaw))
      {
        var raw = monthRaw.Trim();
        if (raw.Length == 7)
        {
          var p = raw.Split('-', StringSplitOptions.RemoveEmptyEntries);
          if (p.Length == 2
              && int.TryParse(p[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var y)
              && int.TryParse(p[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var m)
              && y >= 2000 && y <= 2100 && m >= 1 && m <= 12)
          {
            calMonthRequested = new DateOnly(y, m, 1);
          }
        }
      }

      var source = "db";
      var deptFilter = string.IsNullOrWhiteSpace(deptRaw) || string.Equals(deptRaw, "all", StringComparison.OrdinalIgnoreCase) ? string.Empty : deptRaw;

      static string[] ParseCsv(string line)
      {
        var res = new List<string>();
        var sb = new StringBuilder();
        var inQ = false;
        for (var i = 0; i < line.Length; i++)
        {
          var c = line[i];
          if (inQ)
          {
            if (c == '"')
            {
              if (i + 1 < line.Length && line[i + 1] == '"') { sb.Append('"'); i++; }
              else inQ = false;
            }
            else sb.Append(c);
          }
          else
          {
            if (c == '"') inQ = true;
            else if (c == ',') { res.Add(sb.ToString()); sb.Clear(); }
            else sb.Append(c);
          }
        }
        res.Add(sb.ToString());
        return res.Select(x => (x ?? string.Empty).Trim()).ToArray();
      }

      var staffRows = new List<(string Id, string Name, string Dept, bool Active, string ShiftPattern)>(capacity: 256);
      var departments = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
      string anon;
      lock (stateGate) anon = dashboardSupabaseAnonKey;
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && (!string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || !string.IsNullOrWhiteSpace(anon)))
      {
        var supa = await TryLoadStaffFromSupabase(cfg, anon, ctx.RequestAborted);
        if (!supa.ok)
        {
          return Results.Json(new { ok = false, error = $"Failed to load staff from Supabase: {supa.error}" }, JsonOptions);
        }
        foreach (var r in supa.rows)
        {
          var status = (r.Status ?? string.Empty).Trim();
          var active = status.Length == 0 || string.Equals(status, "Active", StringComparison.OrdinalIgnoreCase);
          staffRows.Add((r.Id, r.Name, r.Dept, active, r.ShiftPattern));
          if (!string.IsNullOrWhiteSpace(r.Dept)) departments.Add(r.Dept);
        }
      }
      else
      {
        var staffPath = ResolveStaffCsvPath();
        if (string.IsNullOrWhiteSpace(staffPath) || !File.Exists(staffPath))
        {
          return Results.Json(new { ok = false, error = "Staff list not found. Configure Supabase staff table or provide Database - Staff WL10.csv." }, JsonOptions);
        }
        var first = true;
        foreach (var raw in File.ReadLines(staffPath))
        {
          var line = raw ?? string.Empty;
          if (first) { first = false; continue; }
          if (string.IsNullOrWhiteSpace(line)) continue;
          var parts = ParseCsv(line);
          if (parts.Length < 7) continue;
          var id = (parts[0] ?? string.Empty).Trim();
          if (id.Length == 0) continue;
          var name = (parts[1] ?? string.Empty).Trim();
          var dept = (parts[3] ?? string.Empty).Trim();
          var status = (parts[4] ?? string.Empty).Trim();
          var shiftPattern = (parts[6] ?? string.Empty).Trim();
          var active = status.Length == 0 || string.Equals(status, "Active", StringComparison.OrdinalIgnoreCase);
          staffRows.Add((id, name, dept, active, shiftPattern));
          if (!string.IsNullOrWhiteSpace(dept)) departments.Add(dept);
        }
      }

      var anyActive = staffRows.Any(r => r.Active);
      var rosterAll = anyActive ? staffRows.Where(r => r.Active).ToArray() : staffRows.ToArray();
      var roster = string.IsNullOrWhiteSpace(deptFilter)
        ? rosterAll
        : rosterAll.Where(r => string.Equals(r.Dept, deptFilter, StringComparison.OrdinalIgnoreCase)).ToArray();

      var deptList = departments.Where(d => !string.IsNullOrWhiteSpace(d)).OrderBy(d => d, StringComparer.OrdinalIgnoreCase).ToArray();

      var start7 = dateLocal.AddDays(-6);
      var start30 = dateLocal.AddDays(-29);
      var start6Months = new DateOnly(dateLocal.AddMonths(-5).Year, dateLocal.AddMonths(-5).Month, 1);
      var rangeStart = start6Months;
      if (calMonthRequested < rangeStart) rangeStart = calMonthRequested;

      var lateAfter = new TimeOnly(9, 15, 0);

      static bool TryParseHm(string raw, out TimeOnly t)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) { t = default; return false; }
        if (TimeOnly.TryParseExact(raw, "HH:mm", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        if (TimeOnly.TryParseExact(raw, "HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        return false;
      }

      static bool TryParseTimeRange(string raw, out TimeOnly start, out TimeOnly end)
      {
        start = default;
        end = default;
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return false;
        var sep = raw.Contains('–', StringComparison.Ordinal) ? '–' : '-';
        var parts = raw.Split(sep, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2) return false;
        return TryParseHm(parts[0], out start) && TryParseHm(parts[1], out end);
      }

      static int MinutesBetween(TimeOnly a, TimeOnly b)
      {
        var am = a.Hour * 60 + a.Minute;
        var bm = b.Hour * 60 + b.Minute;
        if (bm < am) bm += 24 * 60;
        return bm - am;
      }

      var shiftRows = (LoadState(statePath).ShiftPatterns ?? Array.Empty<ShiftPatternRow>()).ToArray();
      if (shiftRows.Length == 0)
      {
        shiftRows = new[]
        {
          new ShiftPatternRow("Normal", "Mon–Fri", "09:00–18:00", "13:00–14:00", "Default"),
          new ShiftPatternRow("Shift 1", "Mon–Fri", "08:00–16:00", "12:00–13:00", "Default"),
          new ShiftPatternRow("Shift 2", "Mon–Fri", "16:00–00:00", "20:00–20:30", "Default"),
          new ShiftPatternRow("Shift 3", "Mon–Fri", "00:00–08:00", "04:00–04:30", "Default"),
        };
      }

      var shiftByPattern = shiftRows
        .Where(r => !string.IsNullOrWhiteSpace(r.Pattern))
        .ToDictionary(r => r.Pattern.Trim(), r => r, StringComparer.OrdinalIgnoreCase);

      double ShiftScheduledHours(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) pattern = "Normal";
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return 8.0;
        if (!TryParseTimeRange(row.WorkingHours ?? string.Empty, out var st, out var en)) return 8.0;
        var minutes = MinutesBetween(st, en);
        if (TryParseTimeRange(row.Break ?? string.Empty, out var bs, out var be))
        {
          minutes = Math.Max(0, minutes - MinutesBetween(bs, be));
        }
        return Math.Max(0, minutes / 60.0);
      }

      static HashSet<DayOfWeek> ParseWorkingDays(string raw)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        raw = raw.Replace("to", "–", StringComparison.OrdinalIgnoreCase);
        raw = raw.Replace("-", "–", StringComparison.Ordinal);
        var parts = raw.Split(new[] { ',', ';', '/', ' ' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        static bool TryMap(string s, out DayOfWeek d)
        {
          d = default;
          s = (s ?? string.Empty).Trim();
          if (s.Length < 3) return false;
          var k = s[..3].ToLowerInvariant();
          if (k == "mon") { d = DayOfWeek.Monday; return true; }
          if (k == "tue") { d = DayOfWeek.Tuesday; return true; }
          if (k == "wed") { d = DayOfWeek.Wednesday; return true; }
          if (k == "thu") { d = DayOfWeek.Thursday; return true; }
          if (k == "fri") { d = DayOfWeek.Friday; return true; }
          if (k == "sat") { d = DayOfWeek.Saturday; return true; }
          if (k == "sun") { d = DayOfWeek.Sunday; return true; }
          return false;
        }

        if (raw.Contains('–', StringComparison.Ordinal))
        {
          var dashParts = raw.Split('–', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
          if (dashParts.Length == 2 && TryMap(dashParts[0], out var a) && TryMap(dashParts[1], out var b))
          {
            var set = new HashSet<DayOfWeek>();
            var cur = (int)a;
            var endI = (int)b;
            for (var i = 0; i < 7; i++)
            {
              set.Add((DayOfWeek)cur);
              if (cur == endI) break;
              cur = (cur + 1) % 7;
            }
            return set;
          }
        }

        var res = new HashSet<DayOfWeek>();
        foreach (var p in parts)
        {
          if (!TryMap(p, out var d)) continue;
          res.Add(d);
        }
        if (res.Count == 0) res = new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        return res;
      }

      TimeOnly ShiftStart(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) pattern = "Normal";
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return new TimeOnly(9, 0, 0);
        if (!TryParseTimeRange(row.WorkingHours ?? string.Empty, out var st, out _)) return new TimeOnly(9, 0, 0);
        return st;
      }

      HashSet<DayOfWeek> ShiftWorkingDays(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) pattern = "Normal";
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        return ParseWorkingDays(row.WorkingDays ?? string.Empty);
      }

      var dayStaff = new Dictionary<DateOnly, Dictionary<string, List<TimeOnly>>>();

      void AddEvent(string staffId, DateOnly d, TimeOnly t)
      {
        if (string.IsNullOrWhiteSpace(staffId)) return;
        if (!dayStaff.TryGetValue(d, out var map))
        {
          map = new Dictionary<string, List<TimeOnly>>(StringComparer.Ordinal);
          dayStaff[d] = map;
        }
        if (map.TryGetValue(staffId, out var cur))
        {
          cur.Add(t);
        }
        else
        {
          map[staffId] = new List<TimeOnly> { t };
        }
      }

      var includeDb = true;

      if (includeDb)
      {
        if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || string.IsNullOrWhiteSpace(cfg.SupabaseAttendanceTable))
        {
          return Results.Json(new { ok = false, error = "Supabase not configured." }, JsonOptions);
        }
        var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
        var select = Uri.EscapeDataString("staff_id,occurred_at");
        var sg = TimeSpan.FromHours(8);
        var startUtc = new DateTimeOffset(rangeStart.ToDateTime(new TimeOnly(0, 0, 0)), sg).ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
        var endUtc = new DateTimeOffset(dateLocal.ToDateTime(new TimeOnly(23, 59, 59)), sg).ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
        var startDt = Uri.EscapeDataString(startUtc);
        var endDt = Uri.EscapeDataString(endUtc);
        var url =
          $"{baseUrl}/rest/v1/{cfg.SupabaseAttendanceTable}?select={select}" +
          $"&occurred_at=gte.{startDt}" +
          $"&occurred_at=lte.{endDt}" +
          $"&order=occurred_at.desc&limit=50000";

        using var http = new HttpClient();
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
        req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
        req.Headers.TryAddWithoutValidation("Accept", "application/json");

        using var res = await http.SendAsync(req, ctx.RequestAborted);
        if (!res.IsSuccessStatusCode)
        {
          var body = await res.Content.ReadAsStringAsync(ctx.RequestAborted);
          return Results.Json(new { ok = false, error = $"Supabase HTTP {(int)res.StatusCode} {res.ReasonPhrase}. {body}" }, JsonOptions);
        }

        var text = await res.Content.ReadAsStringAsync(ctx.RequestAborted);
        using var doc = JsonDocument.Parse(text);
        if (doc.RootElement.ValueKind != JsonValueKind.Array)
        {
          return Results.Json(new { ok = false, error = "Supabase returned invalid JSON." }, JsonOptions);
        }

        foreach (var el in doc.RootElement.EnumerateArray())
        {
          var staffId = el.TryGetProperty("staff_id", out var staffEl) && staffEl.ValueKind == JsonValueKind.String ? staffEl.GetString() : null;
          var dtRaw = el.TryGetProperty("occurred_at", out var dtEl) && dtEl.ValueKind == JsonValueKind.String ? dtEl.GetString() : null;
          if (string.IsNullOrWhiteSpace(staffId) || string.IsNullOrWhiteSpace(dtRaw)) continue;
          if (!DateTimeOffset.TryParse(dtRaw.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto)) continue;
          var dt = dto.ToOffset(sg).DateTime;
          var d = DateOnly.FromDateTime(dt);
          if (d < rangeStart || d > dateLocal) continue;
          var t = TimeOnly.FromDateTime(dt);
          AddEvent(staffId.Trim(), d, t);
        }
      }

      static string ShortDow(DateOnly d)
      {
        var s = d.DayOfWeek.ToString();
        return s.Length >= 3 ? s[..3] : s;
      }

      var rosterIds = new HashSet<string>(roster.Select(r => r.Id), StringComparer.Ordinal);

      var patternById = roster.ToDictionary(
        x => x.Id,
        x => string.IsNullOrWhiteSpace(x.ShiftPattern) ? "Normal" : x.ShiftPattern,
        StringComparer.Ordinal
      );

      var summaryByDayStaff = new Dictionary<(DateOnly Day, string StaffId), (TimeOnly? FirstIn, bool Late, double WorkedHours, double BreakHours, double OtHours, bool MissingOut, int Duplicates)>();
      var dayAgg = new Dictionary<DateOnly, (int Present, int Late, double WorkedHours, double BreakHours, double OtHours, int MissingOut, int Duplicates)>();
      TimeOnly? lastPunchLocalTime = null;
      foreach (var kv in dayStaff)
      {
        var day = kv.Key;
        var map = kv.Value;
        foreach (var kv2 in map)
        {
          var staffId = kv2.Key;
          if (!rosterIds.Contains(staffId)) continue;
          var rawTimes = kv2.Value;
          if (rawTimes is null || rawTimes.Count == 0) continue;
          var times = rawTimes.OrderBy(x => x).ToArray();

          var kept = new List<TimeOnly>(capacity: times.Length);
          TimeOnly? lastKept = null;
          var duplicates = 0;
          foreach (var t in times)
          {
            if (lastKept is not null)
            {
              var diff = MinutesBetween(lastKept.Value, t);
              if (diff >= 0 && diff < 3) { duplicates++; continue; }
            }
            kept.Add(t);
            lastKept = t;
          }
          if (kept.Count == 0) continue;
          if (day == dateLocal)
          {
            var lp = kept[^1];
            if (lastPunchLocalTime is null || lp > lastPunchLocalTime.Value) lastPunchLocalTime = lp;
          }

          var workedHours = 0.0;
          var breakHours = 0.0;
          var missingOut = kept.Count < 2;
          if (!missingOut)
          {
            var spanMin = MinutesBetween(kept[0], kept[^1]);
            var breakMin = 0;
            for (var i = 0; i + 1 < kept.Count; i++)
            {
              var gap = MinutesBetween(kept[i], kept[i + 1]);
              if (gap >= 30 && gap <= 180) breakMin += gap;
            }
            var workedMin = Math.Max(0, spanMin - breakMin);
            workedHours = workedMin / 60.0;
            breakHours = breakMin / 60.0;
          }
          var scheduled = ShiftScheduledHours(patternById.TryGetValue(staffId, out var pat) ? pat : "Normal");
          var otHours = Math.Max(0, workedHours - scheduled);
          var firstIn = kept[0];
          var shiftStart = ShiftStart(patternById.TryGetValue(staffId, out var pat2) ? pat2 : "Normal");
          var late = firstIn > shiftStart.Add(TimeSpan.FromMinutes(15));

          summaryByDayStaff[(day, staffId)] = (firstIn, late, workedHours, breakHours, otHours, missingOut, duplicates);
          var agg = dayAgg.TryGetValue(day, out var cur) ? cur : default;
          agg.Present += 1;
          if (late) agg.Late += 1;
          agg.WorkedHours += workedHours;
          agg.BreakHours += breakHours;
          agg.OtHours += otHours;
          if (missingOut) agg.MissingOut += 1;
          agg.Duplicates += duplicates;
          dayAgg[day] = agg;
        }
      }

      int PresentCount(DateOnly d)
      {
        return dayAgg.TryGetValue(d, out var a) ? a.Present : 0;
      }

      int LateCount(DateOnly d)
      {
        return dayAgg.TryGetValue(d, out var a) ? a.Late : 0;
      }

      var rosterTotal = roster.Length;
      var presentToday = PresentCount(dateLocal);
      var lateToday = LateCount(dateLocal);
      var absentToday = Math.Max(0, rosterTotal - presentToday);
      var attendanceRate = rosterTotal > 0 ? (presentToday * 100.0) / rosterTotal : 0.0;
      var absenceRate = rosterTotal > 0 ? (absentToday * 100.0) / rosterTotal : 0.0;

      var totalByDept = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
      foreach (var r in rosterAll)
      {
        if (string.IsNullOrWhiteSpace(r.Dept)) continue;
        totalByDept[r.Dept] = totalByDept.TryGetValue(r.Dept, out var c) ? (c + 1) : 1;
      }
      var deptByIdAll = rosterAll.ToDictionary(x => x.Id, x => x.Dept, StringComparer.Ordinal);
      var presentByDept = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
      foreach (var kv in summaryByDayStaff)
      {
        if (kv.Key.Day != dateLocal) continue;
        if (!deptByIdAll.TryGetValue(kv.Key.StaffId, out var dep) || string.IsNullOrWhiteSpace(dep)) continue;
        presentByDept[dep] = presentByDept.TryGetValue(dep, out var c) ? (c + 1) : 1;
      }
      var byDept = deptList
        .Select(dep =>
        {
          var total = totalByDept.TryGetValue(dep, out var t) ? t : 0;
          var present = presentByDept.TryGetValue(dep, out var p) ? p : 0;
          var pct = total > 0 ? (present * 100.0) / total : 0.0;
          return new { department = dep, total, present, attendancePct = pct };
        })
        .Where(x => string.IsNullOrWhiteSpace(deptFilter) || string.Equals(x.department, deptFilter, StringComparison.OrdinalIgnoreCase))
        .OrderByDescending(x => x.present)
        .ToArray();

      Dictionary<string, int> StaffDaysPresentByDept(DateOnly start, DateOnly end)
      {
        var res = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var kv in summaryByDayStaff)
        {
          if (kv.Key.Day < start || kv.Key.Day > end) continue;
          if (!deptByIdAll.TryGetValue(kv.Key.StaffId, out var dep) || string.IsNullOrWhiteSpace(dep)) continue;
          res[dep] = res.TryGetValue(dep, out var c) ? (c + 1) : 1;
        }
        return res;
      }

      var weekStart = dateLocal.AddDays(-((int)dateLocal.DayOfWeek + 6) % 7);
      var weekEnd = weekStart.AddDays(6);
      var staffDaysWeek = StaffDaysPresentByDept(weekStart, weekEnd);
      var byDeptWeek = deptList
        .Select(dep =>
        {
          var total = totalByDept.TryGetValue(dep, out var t) ? t : 0;
          var days = 7;
          var staffDaysPresent = staffDaysWeek.TryGetValue(dep, out var sdp) ? sdp : 0;
          var denom = Math.Max(1, total * days);
          var pct = (staffDaysPresent * 100.0) / denom;
          var avgPresent = days > 0 ? staffDaysPresent / (double)days : 0.0;
          return new { department = dep, total, avgPresentPerDay = avgPresent, attendancePct = pct };
        })
        .Where(x => string.IsNullOrWhiteSpace(deptFilter) || string.Equals(x.department, deptFilter, StringComparison.OrdinalIgnoreCase))
        .OrderByDescending(x => x.avgPresentPerDay)
        .ToArray();

      var monthStartDept = new DateOnly(dateLocal.Year, dateLocal.Month, 1);
      var daysInMonthToDate = Math.Max(1, dateLocal.DayNumber - monthStartDept.DayNumber + 1);
      var staffDaysMonth = StaffDaysPresentByDept(monthStartDept, dateLocal);
      var byDeptMonth = deptList
        .Select(dep =>
        {
          var total = totalByDept.TryGetValue(dep, out var t) ? t : 0;
          var staffDaysPresent = staffDaysMonth.TryGetValue(dep, out var sdp) ? sdp : 0;
          var denom = Math.Max(1, total * daysInMonthToDate);
          var pct = (staffDaysPresent * 100.0) / denom;
          var avgPresent = daysInMonthToDate > 0 ? staffDaysPresent / (double)daysInMonthToDate : 0.0;
          return new { department = dep, total, avgPresentPerDay = avgPresent, attendancePct = pct };
        })
        .Where(x => string.IsNullOrWhiteSpace(deptFilter) || string.Equals(x.department, deptFilter, StringComparison.OrdinalIgnoreCase))
        .OrderByDescending(x => x.avgPresentPerDay)
        .ToArray();

      var last7 = Enumerable.Range(0, 7)
        .Select(i =>
        {
          var d = start7.AddDays(i);
          var p = PresentCount(d);
          var a = Math.Max(0, rosterTotal - p);
          var l = LateCount(d);
          return new { date = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), dow = ShortDow(d), present = p, absent = a, late = l };
        })
        .ToArray();

      var daysTotal = Math.Max(1, dateLocal.DayNumber - start30.DayNumber + 1);
      var presentDays = new Dictionary<string, int>(StringComparer.Ordinal);
      for (var i = 0; i < daysTotal; i++)
      {
        var d = start30.AddDays(i);
        if (!dayStaff.TryGetValue(d, out var m)) continue;
        foreach (var id in m.Keys)
        {
          if (!rosterIds.Contains(id)) continue;
          presentDays[id] = presentDays.TryGetValue(id, out var c) ? (c + 1) : 1;
        }
      }

      var nameById = roster.ToDictionary(x => x.Id, x => x.Name, StringComparer.Ordinal);
      var deptById = roster.ToDictionary(x => x.Id, x => x.Dept, StringComparer.Ordinal);

      var topEmployees = presentDays
        .Select(kv =>
        {
          var id = kv.Key;
          var pd = kv.Value;
          var pct = (pd * 100.0) / daysTotal;
          return new { staff_id = id, full_name = nameById.TryGetValue(id, out var n) ? n : id, department = deptById.TryGetValue(id, out var d) ? d : string.Empty, daysPresent = pd, daysTotal, attendancePct = pct };
        })
        .OrderByDescending(x => x.attendancePct)
        .ThenBy(x => x.full_name, StringComparer.OrdinalIgnoreCase)
        .Take(5)
        .ToArray();

      var months = new List<object>(capacity: 6);
      for (var i = 0; i < 6; i++)
      {
        var m0 = start6Months.AddMonths(i);
        var monthStart = new DateOnly(m0.Year, m0.Month, 1);
        var monthEnd = monthStart.AddMonths(1).AddDays(-1);
        if (monthEnd > dateLocal) monthEnd = dateLocal;
        var days = Math.Max(1, monthEnd.DayNumber - monthStart.DayNumber + 1);
        var staffDaysPresent = 0;
        var totalWorkedHours = 0.0;
        for (var d = monthStart; d <= monthEnd; d = d.AddDays(1))
        {
          staffDaysPresent += PresentCount(d);
          if (dayAgg.TryGetValue(d, out var agg)) totalWorkedHours += agg.WorkedHours;
        }
        var denom = Math.Max(1, rosterTotal * days);
        var attPct = (staffDaysPresent * 100.0) / denom;
        var absPct = 100.0 - attPct;
        var label = monthStart.ToString("MMM yyyy", CultureInfo.InvariantCulture);
        months.Add(new
        {
          month = monthStart.ToString("yyyy-MM", CultureInfo.InvariantCulture),
          label,
          attendancePct = attPct,
          absenteePct = absPct,
          totalWorkedHours,
        });
      }

      var thisWeekStart = dateLocal.AddDays(-((int)dateLocal.DayOfWeek + 6) % 7);
      var thisWeek = Enumerable.Range(0, 7)
        .Select(i =>
        {
          var d = thisWeekStart.AddDays(i);
          var p = PresentCount(d);
          var a = Math.Max(0, rosterTotal - p);
          var l = LateCount(d);
          return new { date = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), dow = ShortDow(d), present = p, absent = a, late = l };
        })
        .ToArray();

      var todayAgg = dayAgg.TryGetValue(dateLocal, out var ta) ? ta : default;
      var avgWork = presentToday > 0 ? todayAgg.WorkedHours / presentToday : 0.0;
      var avgBreak = presentToday > 0 ? todayAgg.BreakHours / presentToday : 0.0;
      var avgOt = presentToday > 0 ? todayAgg.OtHours / presentToday : 0.0;

      var monthStartCur = new DateOnly(dateLocal.Year, dateLocal.Month, 1);
      var calMonth = calMonthRequested;

      var monthEndCur = calMonth.AddMonths(1).AddDays(-1);
      var calendarDays = new List<object>(capacity: 31);
      var maxPresent = 0;
      for (var d = calMonth; d <= monthEndCur; d = d.AddDays(1))
      {
        var c = dayAgg.TryGetValue(d, out var v) ? v.Present : 0;
        if (c > maxPresent) maxPresent = c;
        calendarDays.Add(new { date = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), day = d.Day, present = c });
      }
      var monthWorked = 0.0;
      for (var d = monthStartCur; d <= dateLocal; d = d.AddDays(1))
      {
        if (dayAgg.TryGetValue(d, out var a)) monthWorked += a.WorkedHours;
      }

      var expectedMonthHours = 0.0;
      for (var d = monthStartCur; d <= dateLocal; d = d.AddDays(1))
      {
        foreach (var r in roster)
        {
          var pattern = patternById.TryGetValue(r.Id, out var pat3) ? pat3 : "Normal";
          var wds = ShiftWorkingDays(pattern);
          if (!wds.Contains(d.DayOfWeek)) continue;
          expectedMonthHours += ShiftScheduledHours(pattern);
        }
      }

      return Results.Json(new
      {
        ok = true,
        date = dateLocal.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
        source,
        department = string.IsNullOrWhiteSpace(deptFilter) ? "All" : deptFilter,
        departments = deptList,
        lastPunchLocalTime = lastPunchLocalTime is null ? string.Empty : lastPunchLocalTime.Value.ToString("HH:mm", CultureInfo.InvariantCulture),
        calendar = new
        {
          month = calMonth.ToString("yyyy-MM", CultureInfo.InvariantCulture),
          label = calMonth.ToString("MMM yyyy", CultureInfo.InvariantCulture),
          cutoffDate = monthEndCur.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
          maxPresent = maxPresent,
          days = calendarDays,
        },
        roster = new
        {
          totalEmployees = rosterTotal,
          present = presentToday,
          absent = absentToday,
          lateComers = lateToday,
          attendanceRatePct = attendanceRate,
          absenteeRatePct = absenceRate,
          avgWorkingHours = avgWork,
          avgBreakHours = avgBreak,
          avgOtHours = avgOt,
          monthWorkingHours = monthWorked,
          monthExpectedHours = expectedMonthHours,
          missingOutCount = todayAgg.MissingOut,
          duplicatePunches = todayAgg.Duplicates,
        },
        byDepartment = byDept,
        byDepartmentWeek = byDeptWeek,
        byDepartmentMonth = byDeptMonth,
        topEmployees,
        last7Days = last7,
        thisWeek,
        last6Months = months,
      }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/spreadsheet/daily", async (HttpContext ctx) =>
    {
      var q = ctx.Request.Query;
      var dateRaw = (q.TryGetValue("date", out var v1) ? v1.ToString() : string.Empty).Trim();

      var dateLocal = DateOnly.FromDateTime(DateTime.Now);
      if (!string.IsNullOrWhiteSpace(dateRaw))
      {
        _ = DateOnly.TryParseExact(dateRaw, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out dateLocal);
      }

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;

      static string[] ParseCsv(string line)
      {
        var res = new List<string>();
        var sb = new StringBuilder();
        var inQ = false;
        for (var i = 0; i < line.Length; i++)
        {
          var c = line[i];
          if (inQ)
          {
            if (c == '"')
            {
              if (i + 1 < line.Length && line[i + 1] == '"') { sb.Append('"'); i++; }
              else inQ = false;
            }
            else sb.Append(c);
          }
          else
          {
            if (c == '"') inQ = true;
            else if (c == ',') { res.Add(sb.ToString()); sb.Clear(); }
            else sb.Append(c);
          }
        }
        res.Add(sb.ToString());
        return res.Select(x => (x ?? string.Empty).Trim()).ToArray();
      }

      static bool TryParseHm(string raw, out TimeOnly t)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) { t = default; return false; }
        if (TimeOnly.TryParseExact(raw, "HH:mm", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        if (TimeOnly.TryParseExact(raw, "HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        return false;
      }

      static bool TryParseTimeRange(string raw, out TimeOnly start, out TimeOnly end)
      {
        start = default;
        end = default;
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return false;
        var sep = raw.Contains('–', StringComparison.Ordinal) ? '–' : '-';
        var parts = raw.Split(sep, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2) return false;
        return TryParseHm(parts[0], out start) && TryParseHm(parts[1], out end);
      }

      static int MinutesBetween(TimeOnly a, TimeOnly b)
      {
        var am = a.Hour * 60 + a.Minute;
        var bm = b.Hour * 60 + b.Minute;
        if (bm < am) bm += 24 * 60;
        return bm - am;
      }

      var anyActive = false;
      var staffAll = new List<(string Id, string Name, string Status, string ShiftPattern)>(capacity: 256);
      string anonKeyLocal;
      lock (stateGate) anonKeyLocal = dashboardSupabaseAnonKey;
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && (!string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || !string.IsNullOrWhiteSpace(anonKeyLocal)))
      {
        var supa = await TryLoadStaffFromSupabase(cfg, anonKeyLocal, ctx.RequestAborted);
        if (!supa.ok)
        {
          return Results.Json(new { ok = false, error = $"Failed to load staff from Supabase: {supa.error}" }, JsonOptions);
        }
        foreach (var r in supa.rows)
        {
          var status = (r.Status ?? string.Empty).Trim();
          if (status.Length == 0 || string.Equals(status, "Active", StringComparison.OrdinalIgnoreCase)) anyActive = true;
          staffAll.Add((r.Id, r.Name, status, r.ShiftPattern));
        }
      }
      else
      {
        var staffPath = ResolveStaffCsvPath();
        if (string.IsNullOrWhiteSpace(staffPath) || !File.Exists(staffPath))
        {
          return Results.Json(new { ok = false, error = "Staff list not found. Configure Supabase staff table or provide Database - Staff WL10.csv." }, JsonOptions);
        }
        var first = true;
        foreach (var raw in File.ReadLines(staffPath))
        {
          var line = raw ?? string.Empty;
          if (first) { first = false; continue; }
          if (string.IsNullOrWhiteSpace(line)) continue;
          var parts = ParseCsv(line);
          if (parts.Length < 7) continue;
          var id = (parts[0] ?? string.Empty).Trim();
          if (id.Length == 0) continue;
          var name = (parts[1] ?? string.Empty).Trim();
          var status = (parts[4] ?? string.Empty).Trim();
          var shiftPattern = (parts[6] ?? string.Empty).Trim();
          if (status.Length == 0 || string.Equals(status, "Active", StringComparison.OrdinalIgnoreCase)) anyActive = true;
          staffAll.Add((id, name, status, shiftPattern));
        }
      }

      var roster = anyActive
        ? staffAll.Where(x => x.Status.Length == 0 || string.Equals(x.Status, "Active", StringComparison.OrdinalIgnoreCase)).ToArray()
        : staffAll.ToArray();

      static ShiftPatternRow[] DefaultShiftRows()
      {
        return new[]
        {
          new ShiftPatternRow("Normal", "Mon–Fri", "09:00–18:00", "13:00–14:00", "Default"),
          new ShiftPatternRow("Shift 1", "Mon–Sat", "08:00–16:00", "12:00–13:00", "Default"),
          new ShiftPatternRow("Shift 2", "Mon–Sat", "16:00–00:00", "20:00–20:30", "Default"),
          new ShiftPatternRow("Shift 3", "Mon–Sat", "00:00–08:00", "04:00–04:30", "Default"),
        };
      }

      ShiftPatternRow[] shiftRows;
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && (!string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || !string.IsNullOrWhiteSpace(anonKeyLocal)))
      {
        var supaShifts = await TryLoadShiftPatternsFromSupabase(cfg, anonKeyLocal, ctx.RequestAborted);
        shiftRows = supaShifts.ok ? supaShifts.rows : Array.Empty<ShiftPatternRow>();
      }
      else
      {
        shiftRows = Array.Empty<ShiftPatternRow>();
      }

      if (shiftRows.Length == 0)
      {
        var state = LoadState(statePath);
        shiftRows = (state.ShiftPatterns ?? Array.Empty<ShiftPatternRow>()).ToArray();
      }
      if (shiftRows.Length == 0) shiftRows = DefaultShiftRows();

      var shiftByPattern = shiftRows
        .Where(r => !string.IsNullOrWhiteSpace(r.Pattern))
        .ToDictionary(r => r.Pattern.Trim(), r => r, StringComparer.OrdinalIgnoreCase);

      double ShiftScheduledHours(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) return 0;
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return 0;
        if (!TryParseTimeRange(row.WorkingHours ?? string.Empty, out var st, out var en)) return 0;
        var minutes = MinutesBetween(st, en);
        if (TryParseTimeRange(row.Break ?? string.Empty, out var bs, out var be))
        {
          minutes = Math.Max(0, minutes - MinutesBetween(bs, be));
        }
        return minutes / 60.0;
      }

      TimeOnly? ShiftStart(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) return null;
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return null;
        if (!TryParseTimeRange(row.WorkingHours ?? string.Empty, out var st, out _)) return null;
        return st;
      }

      TimeOnly? ShiftEnd(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) return null;
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return null;
        if (!TryParseTimeRange(row.WorkingHours ?? string.Empty, out _, out var en)) return null;
        return en;
      }

      var punchByStaff = Program.DevicePunches
        .Where(p => string.Equals(p.EventDate, dateLocal.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), StringComparison.Ordinal))
        .GroupBy(p => (p.StaffId ?? string.Empty).Trim(), StringComparer.Ordinal)
        .ToDictionary(
          g => g.Key,
          g => g
            .Select(x => (occurredAtUtc: x.OccurredAtUtc, localDt: x.OccurredAtUtc.ToLocalTime().DateTime, deviceId: x.DeviceId ?? string.Empty, verifyMode: x.VerifyMode))
            .OrderBy(x => x.localDt)
            .ToArray(),
          StringComparer.Ordinal
        );

      static string Hm(TimeOnly t) => t.ToString("HH:mm", CultureInfo.InvariantCulture);

      var outRows = new List<object>(capacity: roster.Length);
      foreach (var s in roster)
      {
        var id = s.Id;
        var name = string.IsNullOrWhiteSpace(s.Name) ? id : s.Name;
        var pattern = string.IsNullOrWhiteSpace(s.ShiftPattern) ? "Normal" : s.ShiftPattern;
        var entries = punchByStaff.TryGetValue(id, out var xs)
          ? xs
          : Array.Empty<(DateTimeOffset occurredAtUtc, DateTime localDt, string deviceId, int verifyMode)>();

        var kept = new List<(DateTimeOffset occurredAtUtc, DateTime localDt, string deviceId, int verifyMode)>(capacity: entries.Length);
        var flagged = new List<(DateTimeOffset occurredAtUtc, DateTime localDt, string deviceId, int verifyMode)>(capacity: Math.Min(8, entries.Length));
        var flaggedUtc = new HashSet<DateTimeOffset>();
        DateTime? lastKeptLocal = null;
        foreach (var e in entries)
        {
          if (lastKeptLocal is not null)
          {
            var diffMin = (e.localDt - lastKeptLocal.Value).TotalMinutes;
            if (diffMin >= 0 && diffMin < 60)
            {
              flagged.Add(e);
              flaggedUtc.Add(e.occurredAtUtc);
              continue;
            }
          }
          kept.Add(e);
          lastKeptLocal = e.localDt;
        }

        var times = kept.Select(e => TimeOnly.FromDateTime(e.localDt)).ToArray();
        var flaggedTimes = flagged.Select(e => TimeOnly.FromDateTime(e.localDt)).ToArray();

        var oddScan = times.Any(t => t.Hour < 5 || t.Hour > 22);
        var missingOut = times.Length > 0 && (times.Length % 2 == 1);

        var workMinutes = 0;
        var breakMinutes = 0;
        for (var i = 0; i + 1 < times.Length; i++)
        {
          var a = times[i];
          var b = times[i + 1];
          var min = MinutesBetween(a, b);
          if (i % 2 == 0) workMinutes += min;
          else breakMinutes += min;
        }

        var scheduled = ShiftScheduledHours(pattern);
        var worked = workMinutes / 60.0;
        var ot = Math.Max(0, worked - scheduled);

        var firstIn = times.Length > 0 ? Hm(times[0]) : "-";
        var lastOut = times.Length > 0 ? Hm(times[^1]) : "-";

        var lateMin = 0;
        var st = ShiftStart(pattern);
        if (st is not null && times.Length > 0)
        {
          var startMin = st.Value.Hour * 60 + st.Value.Minute;
          var endT = ShiftEnd(pattern);
          var endMin = endT is not null ? (endT.Value.Hour * 60 + endT.Value.Minute) : startMin;
          var cross = endT is not null && endMin <= startMin;
          var firstMin = times[0].Hour * 60 + times[0].Minute;
          if (cross && firstMin < startMin) firstMin += 24 * 60;
          lateMin = Math.Max(0, firstMin - startMin);
        }

        var earlyMin = 0;
        var en = ShiftEnd(pattern);
        if (en is not null && st is not null && times.Length > 0)
        {
          var startMin = st.Value.Hour * 60 + st.Value.Minute;
          var endMin = en.Value.Hour * 60 + en.Value.Minute;
          var cross = endMin <= startMin;
          var lastMin = times[^1].Hour * 60 + times[^1].Minute;
          if (cross) endMin += 24 * 60;
          if (cross && lastMin < startMin) lastMin += 24 * 60;
          earlyMin = Math.Max(0, endMin - lastMin);
        }

        var flags = new List<string>(capacity: 4);
        if (missingOut) flags.Add("MISSING_OUT");
        if (flaggedTimes.Length > 0) flags.Add("DUPLICATE x" + flaggedTimes.Length);
        if (oddScan) flags.Add("ODD");

        var status = times.Length == 0 ? "Absent" : (missingOut ? "Incomplete" : "Present");
        var details = new
        {
          punches = entries.Select(e => Hm(TimeOnly.FromDateTime(e.localDt))).ToArray(),
          used_punches = times.Select(Hm).ToArray(),
          flagged_punches = flaggedTimes.Select(Hm).ToArray(),
          punch_entries = entries.Select(e => new
          {
            occurred_at_utc = e.occurredAtUtc.ToString("O"),
            time = Hm(TimeOnly.FromDateTime(e.localDt)),
            flagged = flaggedUtc.Contains(e.occurredAtUtc),
            device_id = e.deviceId,
            verify_mode = e.verifyMode,
          }).ToArray(),
          first_in = times.Length > 0 ? Hm(times[0]) : "",
          first_out = times.Length > 1 ? Hm(times[1]) : "",
          second_in = times.Length > 2 ? Hm(times[2]) : "",
          second_out = times.Length > 3 ? Hm(times[3]) : "",
          ot_in = times.Length > 4 ? Hm(times[4]) : "",
          ot_out = times.Length > 5 ? Hm(times[5]) : "",
          break_minutes = breakMinutes,
          late_minutes = lateMin,
          early_leave_minutes = earlyMin,
          scheduled_hours = Math.Round(scheduled, 2),
        };

        outRows.Add(new
        {
          staff_id = id,
          name,
          date = dateLocal.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
          shift = pattern,
          first_in = firstIn,
          last_out = lastOut,
          total_hours = Math.Round(worked, 2),
          ot_hours = Math.Round(ot, 2),
          status,
          flags = string.Join(" | ", flags),
          flagged_punches = flaggedTimes.Select(Hm).ToArray(),
          details,
        });
      }

      var sorted = outRows
        .Select(x => new { k = (string)((dynamic)x).name, v = x })
        .OrderBy(x => x.k, StringComparer.OrdinalIgnoreCase)
        .Select(x => x.v)
        .ToArray();

      return Results.Json(new { ok = true, date = dateLocal.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), rows = sorted }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/spreadsheet/weekly", async (HttpContext ctx) =>
    {
      var q = ctx.Request.Query;
      var dateRaw = (q.TryGetValue("date", out var v1) ? v1.ToString() : string.Empty).Trim();

      var dateLocal = DateOnly.FromDateTime(DateTime.Now);
      if (!string.IsNullOrWhiteSpace(dateRaw))
      {
        _ = DateOnly.TryParseExact(dateRaw, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out dateLocal);
      }

      var offset = ((int)dateLocal.DayOfWeek - (int)DayOfWeek.Monday + 7) % 7;
      var weekStart = dateLocal.AddDays(-offset);
      var weekEnd = weekStart.AddDays(6);

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;

      static string[] ParseCsv(string line)
      {
        var res = new List<string>();
        var sb = new StringBuilder();
        var inQ = false;
        for (var i = 0; i < line.Length; i++)
        {
          var c = line[i];
          if (inQ)
          {
            if (c == '"')
            {
              if (i + 1 < line.Length && line[i + 1] == '"') { sb.Append('"'); i++; }
              else inQ = false;
            }
            else sb.Append(c);
          }
          else
          {
            if (c == '"') inQ = true;
            else if (c == ',') { res.Add(sb.ToString()); sb.Clear(); }
            else sb.Append(c);
          }
        }
        res.Add(sb.ToString());
        return res.Select(x => (x ?? string.Empty).Trim()).ToArray();
      }

      static bool TryParseHm(string raw, out TimeOnly t)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) { t = default; return false; }
        if (TimeOnly.TryParseExact(raw, "HH:mm", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        if (TimeOnly.TryParseExact(raw, "HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        return false;
      }

      static bool TryParseTimeRange(string raw, out TimeOnly start, out TimeOnly end)
      {
        start = default;
        end = default;
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return false;
        var sep = raw.Contains('–', StringComparison.Ordinal) ? '–' : '-';
        var parts = raw.Split(sep, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2) return false;
        return TryParseHm(parts[0], out start) && TryParseHm(parts[1], out end);
      }

      static HashSet<DayOfWeek> ParseWorkingDays(string raw)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        raw = raw.Replace("to", "–", StringComparison.OrdinalIgnoreCase);
        raw = raw.Replace("-", "–", StringComparison.Ordinal);

        static bool TryMap(string s, out DayOfWeek d)
        {
          d = default;
          s = (s ?? string.Empty).Trim();
          if (s.Length < 3) return false;
          var k = s[..3].ToLowerInvariant();
          if (k == "mon") { d = DayOfWeek.Monday; return true; }
          if (k == "tue") { d = DayOfWeek.Tuesday; return true; }
          if (k == "wed") { d = DayOfWeek.Wednesday; return true; }
          if (k == "thu") { d = DayOfWeek.Thursday; return true; }
          if (k == "fri") { d = DayOfWeek.Friday; return true; }
          if (k == "sat") { d = DayOfWeek.Saturday; return true; }
          if (k == "sun") { d = DayOfWeek.Sunday; return true; }
          return false;
        }

        if (raw.Contains('–', StringComparison.Ordinal))
        {
          var dashParts = raw.Split('–', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
          if (dashParts.Length == 2 && TryMap(dashParts[0], out var a) && TryMap(dashParts[1], out var b))
          {
            var set = new HashSet<DayOfWeek>();
            var cur = (int)a;
            var endI = (int)b;
            for (var i = 0; i < 7; i++)
            {
              set.Add((DayOfWeek)cur);
              if (cur == endI) break;
              cur = (cur + 1) % 7;
            }
            return set;
          }
        }

        var parts = raw.Split(new[] { ',', ';', '/', ' ' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var res = new HashSet<DayOfWeek>();
        foreach (var p in parts)
        {
          if (!TryMap(p, out var d)) continue;
          res.Add(d);
        }
        if (res.Count == 0) res = new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        return res;
      }

      static int MinutesBetween(TimeOnly a, TimeOnly b)
      {
        var am = a.Hour * 60 + a.Minute;
        var bm = b.Hour * 60 + b.Minute;
        if (bm < am) bm += 24 * 60;
        return bm - am;
      }

      var anyActive = false;
      var staffAll = new List<(string Id, string Name, string Status, string ShiftPattern)>(capacity: 256);
      string anonKeyLocal;
      lock (stateGate) anonKeyLocal = dashboardSupabaseAnonKey;
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && (!string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || !string.IsNullOrWhiteSpace(anonKeyLocal)))
      {
        var supa = await TryLoadStaffFromSupabase(cfg, anonKeyLocal, ctx.RequestAborted);
        if (!supa.ok)
        {
          return Results.Json(new { ok = false, error = $"Failed to load staff from Supabase: {supa.error}" }, JsonOptions);
        }
        foreach (var r in supa.rows)
        {
          var status = (r.Status ?? string.Empty).Trim();
          if (status.Length == 0 || string.Equals(status, "Active", StringComparison.OrdinalIgnoreCase)) anyActive = true;
          staffAll.Add((r.Id, r.Name, status, r.ShiftPattern));
        }
      }
      else
      {
        var staffPath = ResolveStaffCsvPath();
        if (string.IsNullOrWhiteSpace(staffPath) || !File.Exists(staffPath))
        {
          return Results.Json(new { ok = false, error = "Staff list not found. Configure Supabase staff table or provide Database - Staff WL10.csv." }, JsonOptions);
        }
        var first = true;
        foreach (var raw in File.ReadLines(staffPath))
        {
          var line = raw ?? string.Empty;
          if (first) { first = false; continue; }
          if (string.IsNullOrWhiteSpace(line)) continue;
          var parts = ParseCsv(line);
          if (parts.Length < 7) continue;
          var id = (parts[0] ?? string.Empty).Trim();
          if (id.Length == 0) continue;
          var name = (parts[1] ?? string.Empty).Trim();
          var status = (parts[4] ?? string.Empty).Trim();
          var shiftPattern = (parts[6] ?? string.Empty).Trim();
          if (status.Length == 0 || string.Equals(status, "Active", StringComparison.OrdinalIgnoreCase)) anyActive = true;
          staffAll.Add((id, name, status, shiftPattern));
        }
      }

      var roster = anyActive
        ? staffAll.Where(x => x.Status.Length == 0 || string.Equals(x.Status, "Active", StringComparison.OrdinalIgnoreCase)).ToArray()
        : staffAll.ToArray();

      static ShiftPatternRow[] DefaultShiftRows()
      {
        return new[]
        {
          new ShiftPatternRow("Normal", "Mon–Fri", "09:00–18:00", "13:00–14:00", "Default"),
          new ShiftPatternRow("Shift 1", "Mon–Sat", "08:00–16:00", "12:00–13:00", "Default"),
          new ShiftPatternRow("Shift 2", "Mon–Sat", "16:00–00:00", "20:00–20:30", "Default"),
          new ShiftPatternRow("Shift 3", "Mon–Sat", "00:00–08:00", "04:00–04:30", "Default"),
        };
      }

      ShiftPatternRow[] shiftRows;
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && (!string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || !string.IsNullOrWhiteSpace(anonKeyLocal)))
      {
        var supaShifts = await TryLoadShiftPatternsFromSupabase(cfg, anonKeyLocal, ctx.RequestAborted);
        shiftRows = supaShifts.ok ? supaShifts.rows : Array.Empty<ShiftPatternRow>();
      }
      else
      {
        shiftRows = Array.Empty<ShiftPatternRow>();
      }

      if (shiftRows.Length == 0)
      {
        var state = LoadState(statePath);
        shiftRows = (state.ShiftPatterns ?? Array.Empty<ShiftPatternRow>()).ToArray();
      }
      if (shiftRows.Length == 0) shiftRows = DefaultShiftRows();

      var shiftByPattern = shiftRows
        .Where(r => !string.IsNullOrWhiteSpace(r.Pattern))
        .ToDictionary(r => r.Pattern.Trim(), r => r, StringComparer.OrdinalIgnoreCase);

      double ShiftScheduledHours(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) return 0;
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return 0;
        if (!TryParseTimeRange(row.WorkingHours ?? string.Empty, out var st, out var en)) return 0;
        var minutes = MinutesBetween(st, en);
        if (TryParseTimeRange(row.Break ?? string.Empty, out var bs, out var be))
        {
          minutes = Math.Max(0, minutes - MinutesBetween(bs, be));
        }
        return minutes / 60.0;
      }

      HashSet<DayOfWeek> ShiftWorkingDays(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        return ParseWorkingDays(row.WorkingDays ?? string.Empty);
      }

      var startStr = weekStart.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
      var endStr = weekEnd.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
      var punchRange = Program.DevicePunches
        .Where(p =>
        {
          var d = (p.EventDate ?? string.Empty).Trim();
          return d.Length == 10 && string.CompareOrdinal(d, startStr) >= 0 && string.CompareOrdinal(d, endStr) <= 0;
        })
        .GroupBy(p => $"{p.StaffId}|{p.EventDate}", StringComparer.Ordinal)
        .ToDictionary(
          g => g.Key,
          g => g.Select(x => TimeOnly.FromDateTime(x.OccurredAtUtc.ToLocalTime().DateTime)).OrderBy(x => x).ToArray(),
          StringComparer.Ordinal
        );

      var outRows = new List<object>(capacity: roster.Length);
      foreach (var s in roster)
      {
        var id = s.Id;
        var name = string.IsNullOrWhiteSpace(s.Name) ? id : s.Name;
        var pattern = string.IsNullOrWhiteSpace(s.ShiftPattern) ? "Normal" : s.ShiftPattern;

        var workingDays = ShiftWorkingDays(pattern);
        var totalWorkingDays = 0;
        var daysPresent = 0;
        var totalHours = 0.0;
        var totalOt = 0.0;
        var flaggedPunches = 0;
        var schedPerDay = ShiftScheduledHours(pattern);

        for (var d = weekStart; d <= weekEnd; d = d.AddDays(1))
        {
          if (workingDays.Contains(d.DayOfWeek)) totalWorkingDays++;
          var key = id + "|" + d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
          if (!punchRange.TryGetValue(key, out var times) || times.Length == 0) continue;
          var kept = new List<TimeOnly>(capacity: times.Length);
          TimeOnly? lastKept = null;
          foreach (var t in times)
          {
            if (lastKept is not null)
            {
              var am = lastKept.Value.Hour * 60 + lastKept.Value.Minute;
              var bm = t.Hour * 60 + t.Minute;
              var diff = bm - am;
              if (diff < 0) diff += 24 * 60;
              if (diff >= 0 && diff < 60)
              {
                flaggedPunches++;
                continue;
              }
            }
            kept.Add(t);
            lastKept = t;
          }
          times = kept.ToArray();
          if (times.Length == 0) continue;
          if (workingDays.Contains(d.DayOfWeek)) daysPresent++;

          var workMinutes = 0;
          for (var i = 0; i + 1 < times.Length; i++)
          {
            if (i % 2 != 0) continue;
            workMinutes += MinutesBetween(times[i], times[i + 1]);
          }
          var worked = workMinutes / 60.0;
          totalHours += worked;
          totalOt += Math.Max(0, worked - schedPerDay);
        }

        var daysAbsent = Math.Max(0, totalWorkingDays - daysPresent);
        var attPct = totalWorkingDays > 0 ? (daysPresent * 100.0) / totalWorkingDays : 0.0;

        outRows.Add(new
        {
          staff_id = id,
          name,
          week_start = weekStart.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
          week_end = weekEnd.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
          week = weekStart.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + " to " + weekEnd.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
          flagged_punches = flaggedPunches,
          total_hours = Math.Round(totalHours, 2),
          ot_hours = Math.Round(totalOt, 2),
          days_present = daysPresent,
          days_absent = daysAbsent,
          attendance_pct = Math.Round(attPct, 1),
        });
      }

      var sorted = outRows
        .Select(x => new { k = (string)((dynamic)x).name, v = x })
        .OrderBy(x => x.k, StringComparer.OrdinalIgnoreCase)
        .Select(x => x.v)
        .ToArray();

      return Results.Json(new { ok = true, week_start = weekStart.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), week_end = weekEnd.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), rows = sorted }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/spreadsheet/punch", async (HttpContext ctx) =>
    {
      var isSuperadmin = string.Equals(ctx.User.FindFirstValue("role") ?? string.Empty, "superadmin", StringComparison.Ordinal);
      if (!isSuperadmin) return Results.StatusCode(StatusCodes.Status403Forbidden);

      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var root = doc.RootElement;
      var op = root.TryGetProperty("op", out var opEl) ? (opEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var staffId = root.TryGetProperty("staff_id", out var sidEl) ? (sidEl.GetString() ?? string.Empty).Trim() : string.Empty;
      if (op.Length == 0 || staffId.Length == 0) return Results.Json(new { ok = false, error = "missing op or staff_id" }, JsonOptions);

      static bool TryParseUtcIso(string? s, out DateTimeOffset dto)
      {
        return DateTimeOffset.TryParseExact((s ?? string.Empty).Trim(), "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dto);
      }

      static bool TryParseLocalInput(string? s, out DateTime local)
      {
        local = default;
        var raw = (s ?? string.Empty).Trim();
        if (raw.Length == 0) return false;
        if (DateTime.TryParseExact(raw, "yyyy-MM-ddTHH:mm", CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var dt)) { local = dt; return true; }
        if (DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out dt)) { local = dt; return true; }
        return false;
      }

      var tz = GetScheduleTimeZone();
      var state = LoadState(statePath);
      var list = (Program.DevicePunches ?? Array.Empty<Punch>()).ToList();

      if (op.Equals("add", StringComparison.OrdinalIgnoreCase))
      {
        var localRaw = root.TryGetProperty("occurred_at_local", out var lEl) ? lEl.GetString() : null;
        if (!TryParseLocalInput(localRaw, out var localIn)) return Results.Json(new { ok = false, error = "invalid occurred_at_local" }, JsonOptions);
        var localUnspec = DateTime.SpecifyKind(localIn, DateTimeKind.Unspecified);
        var utc = TimeZoneInfo.ConvertTimeToUtc(localUnspec, tz);
        var utcOff = new DateTimeOffset(utc, TimeSpan.Zero);
        var local = TimeZoneInfo.ConvertTimeFromUtc(utcOff.UtcDateTime, tz);
        var eventDate = DateOnly.FromDateTime(local).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        var deviceId = root.TryGetProperty("device_id", out var dEl) ? (dEl.GetString() ?? string.Empty).Trim() : string.Empty;
        if (deviceId.Length == 0) deviceId = "manual";
        var verifyMode = root.TryGetProperty("verify_mode", out var vEl) && vEl.TryGetInt32(out var vm) ? vm : 255;
        if (verifyMode < 0 || verifyMode > 255) verifyMode = 255;

        list.Add(new Punch(staffId, utcOff, eventDate, deviceId, verifyMode));
        Console.WriteLine($"AUDIT spreadsheet punch add staff_id={staffId} occurred_at_utc={utcOff:O} event_date={eventDate} device_id={deviceId}");
      }
      else if (op.Equals("edit", StringComparison.OrdinalIgnoreCase))
      {
        var targetRaw = root.TryGetProperty("occurred_at_utc", out var tEl) ? tEl.GetString() : null;
        if (!TryParseUtcIso(targetRaw, out var targetUtc)) return Results.Json(new { ok = false, error = "invalid occurred_at_utc" }, JsonOptions);
        var newLocalRaw = root.TryGetProperty("new_occurred_at_local", out var nlEl) ? nlEl.GetString() : null;
        if (!TryParseLocalInput(newLocalRaw, out var newLocalIn)) return Results.Json(new { ok = false, error = "invalid new_occurred_at_local" }, JsonOptions);
        var newLocalUnspec = DateTime.SpecifyKind(newLocalIn, DateTimeKind.Unspecified);
        var newUtc = TimeZoneInfo.ConvertTimeToUtc(newLocalUnspec, tz);
        var newUtcOff = new DateTimeOffset(newUtc, TimeSpan.Zero);
        var newLocal = TimeZoneInfo.ConvertTimeFromUtc(newUtcOff.UtcDateTime, tz);
        var newEventDate = DateOnly.FromDateTime(newLocal).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        var idx = list.FindIndex(p => string.Equals(p.StaffId, staffId, StringComparison.Ordinal) && p.OccurredAtUtc == targetUtc);
        if (idx < 0) return Results.Json(new { ok = false, error = "punch not found" }, JsonOptions);

        var old = list[idx];
        list[idx] = old with { OccurredAtUtc = newUtcOff, EventDate = newEventDate };
        Console.WriteLine($"AUDIT spreadsheet punch edit staff_id={staffId} occurred_at_utc={old.OccurredAtUtc:O} -> {newUtcOff:O} event_date={old.EventDate} -> {newEventDate}");
      }
      else if (op.Equals("delete", StringComparison.OrdinalIgnoreCase))
      {
        var targetRaw = root.TryGetProperty("occurred_at_utc", out var tEl) ? tEl.GetString() : null;
        if (!TryParseUtcIso(targetRaw, out var targetUtc)) return Results.Json(new { ok = false, error = "invalid occurred_at_utc" }, JsonOptions);
        var removed = list.RemoveAll(p => string.Equals(p.StaffId, staffId, StringComparison.Ordinal) && p.OccurredAtUtc == targetUtc);
        if (removed == 0) return Results.Json(new { ok = false, error = "punch not found" }, JsonOptions);
        Console.WriteLine($"AUDIT spreadsheet punch delete staff_id={staffId} occurred_at_utc={targetUtc:O}");
      }
      else
      {
        return Results.Json(new { ok = false, error = "invalid op" }, JsonOptions);
      }

      var merged = MergePunches(Array.Empty<Punch>(), list, max: 50000);
      Program.DevicePunches = merged;
      SaveState(statePath, state with { DevicePunches = merged });
      return Results.Json(new { ok = true }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/spreadsheet/report", async (HttpContext ctx) =>
    {
      var isSuperadmin = string.Equals(ctx.User.FindFirstValue("role") ?? string.Empty, "superadmin", StringComparison.Ordinal);
      if (!isSuperadmin) return Results.StatusCode(StatusCodes.Status403Forbidden);

      var staffId = (ctx.Request.Query["staff_id"].ToString() ?? string.Empty).Trim();
      var monthRaw = (ctx.Request.Query["month"].ToString() ?? string.Empty).Trim();
      if (staffId.Length == 0 || monthRaw.Length == 0) return Results.Json(new { ok = false, error = "missing staff_id or month" }, JsonOptions);

      var now = DateTime.Now;
      var year = now.Year;
      var month = now.Month;
      var parts = monthRaw.Split('-', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
      if (parts.Length == 2 && int.TryParse(parts[0], out var y) && int.TryParse(parts[1], out var m))
      {
        if (y >= 2000 && y <= 2100) year = y;
        if (m >= 1 && m <= 12) month = m;
      }
      var monthStart = new DateOnly(year, month, 1);
      var monthEnd = monthStart.AddMonths(1).AddDays(-1);

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;

      static string CsvCell(string? v)
      {
        var s = v ?? string.Empty;
        return "\"" + s.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
      }

      static string CsvRow(params string[] cells)
      {
        return string.Join(",", cells.Select(CsvCell));
      }

      static string[] ParseCsv(string line)
      {
        var res = new List<string>();
        var sb = new StringBuilder();
        var inQ = false;
        for (var i = 0; i < line.Length; i++)
        {
          var c = line[i];
          if (inQ)
          {
            if (c == '"')
            {
              if (i + 1 < line.Length && line[i + 1] == '"') { sb.Append('"'); i++; }
              else inQ = false;
            }
            else sb.Append(c);
          }
          else
          {
            if (c == '"') inQ = true;
            else if (c == ',') { res.Add(sb.ToString()); sb.Clear(); }
            else sb.Append(c);
          }
        }
        res.Add(sb.ToString());
        return res.Select(x => (x ?? string.Empty).Trim()).ToArray();
      }

      static bool TryParseHm(string raw, out TimeOnly t)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) { t = default; return false; }
        if (TimeOnly.TryParseExact(raw, "HH:mm", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        if (TimeOnly.TryParseExact(raw, "HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        return false;
      }

      static bool TryParseTimeRange(string raw, out TimeOnly start, out TimeOnly end)
      {
        start = default;
        end = default;
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return false;
        var sep = raw.Contains('–', StringComparison.Ordinal) ? '–' : '-';
        var p2 = raw.Split(sep, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (p2.Length != 2) return false;
        return TryParseHm(p2[0], out start) && TryParseHm(p2[1], out end);
      }

      static HashSet<DayOfWeek> ParseWorkingDays(string raw)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        raw = raw.Replace("to", "–", StringComparison.OrdinalIgnoreCase);
        raw = raw.Replace("-", "–", StringComparison.Ordinal);

        static bool TryMap(string s, out DayOfWeek d)
        {
          d = default;
          s = (s ?? string.Empty).Trim();
          if (s.Length < 3) return false;
          var k = s[..3].ToLowerInvariant();
          if (k == "mon") { d = DayOfWeek.Monday; return true; }
          if (k == "tue") { d = DayOfWeek.Tuesday; return true; }
          if (k == "wed") { d = DayOfWeek.Wednesday; return true; }
          if (k == "thu") { d = DayOfWeek.Thursday; return true; }
          if (k == "fri") { d = DayOfWeek.Friday; return true; }
          if (k == "sat") { d = DayOfWeek.Saturday; return true; }
          if (k == "sun") { d = DayOfWeek.Sunday; return true; }
          return false;
        }

        if (raw.Contains('–', StringComparison.Ordinal))
        {
          var dashParts = raw.Split('–', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
          if (dashParts.Length == 2 && TryMap(dashParts[0], out var a) && TryMap(dashParts[1], out var b))
          {
            var set = new HashSet<DayOfWeek>();
            var cur = (int)a;
            var endI = (int)b;
            for (var i = 0; i < 7; i++)
            {
              set.Add((DayOfWeek)cur);
              if (cur == endI) break;
              cur = (cur + 1) % 7;
            }
            return set;
          }
        }

        var parts2 = raw.Split(new[] { ',', ';', '/', ' ' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var res = new HashSet<DayOfWeek>();
        foreach (var p in parts2)
        {
          if (!TryMap(p, out var d)) continue;
          res.Add(d);
        }
        if (res.Count == 0) res = new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        return res;
      }

      static int MinutesBetween(TimeOnly a, TimeOnly b)
      {
        var am = a.Hour * 60 + a.Minute;
        var bm = b.Hour * 60 + b.Minute;
        if (bm < am) bm += 24 * 60;
        return bm - am;
      }

      string anonKeyLocal;
      lock (stateGate) anonKeyLocal = dashboardSupabaseAnonKey;

      var staffName = staffId;
      var staffPattern = "Normal";
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && (!string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || !string.IsNullOrWhiteSpace(anonKeyLocal)))
      {
        var supa = await TryLoadStaffFromSupabase(cfg, anonKeyLocal, ctx.RequestAborted);
        if (!supa.ok)
        {
          return Results.Json(new { ok = false, error = $"Failed to load staff from Supabase: {supa.error}" }, JsonOptions);
        }
        var match = supa.rows.FirstOrDefault(r => string.Equals(r.Id, staffId, StringComparison.Ordinal));
        if (!string.IsNullOrWhiteSpace(match.Id))
        {
          staffName = string.IsNullOrWhiteSpace(match.Name) ? staffId : match.Name;
          staffPattern = string.IsNullOrWhiteSpace(match.ShiftPattern) ? "Normal" : match.ShiftPattern;
        }
      }
      else
      {
        var staffPath = ResolveStaffCsvPath();
        if (string.IsNullOrWhiteSpace(staffPath) || !File.Exists(staffPath))
        {
          return Results.Json(new { ok = false, error = "Staff list not found. Configure Supabase staff table or provide Database - Staff WL10.csv." }, JsonOptions);
        }
        var firstRow = true;
        foreach (var raw in File.ReadLines(staffPath))
        {
          var line = raw ?? string.Empty;
          if (firstRow) { firstRow = false; continue; }
          if (line.Trim().Length == 0) continue;
          var cols = ParseCsv(line);
          if (cols.Length < 7) continue;
          var id = (cols[0] ?? string.Empty).Trim();
          if (!string.Equals(id, staffId, StringComparison.Ordinal)) continue;
          staffName = (cols[1] ?? string.Empty).Trim();
          if (staffName.Length == 0) staffName = staffId;
          staffPattern = (cols[6] ?? string.Empty).Trim();
          if (staffPattern.Length == 0) staffPattern = "Normal";
          break;
        }
      }

      static ShiftPatternRow[] DefaultShiftRows()
      {
        return new[]
        {
          new ShiftPatternRow("Normal", "Mon–Fri", "09:00–18:00", "13:00–14:00", "Default"),
          new ShiftPatternRow("Shift 1", "Mon–Sat", "08:00–16:00", "12:00–13:00", "Default"),
          new ShiftPatternRow("Shift 2", "Mon–Sat", "16:00–00:00", "20:00–20:30", "Default"),
          new ShiftPatternRow("Shift 3", "Mon–Sat", "00:00–08:00", "04:00–04:30", "Default"),
        };
      }

      ShiftPatternRow[] shiftRows;
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && (!string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || !string.IsNullOrWhiteSpace(anonKeyLocal)))
      {
        var supaShifts = await TryLoadShiftPatternsFromSupabase(cfg, anonKeyLocal, ctx.RequestAborted);
        shiftRows = supaShifts.ok ? supaShifts.rows : Array.Empty<ShiftPatternRow>();
      }
      else
      {
        shiftRows = Array.Empty<ShiftPatternRow>();
      }

      if (shiftRows.Length == 0)
      {
        var state = LoadState(statePath);
        shiftRows = (state.ShiftPatterns ?? Array.Empty<ShiftPatternRow>()).ToArray();
      }
      if (shiftRows.Length == 0) shiftRows = DefaultShiftRows();
      var shiftByPattern = shiftRows
        .Where(r => !string.IsNullOrWhiteSpace(r.Pattern))
        .ToDictionary(r => r.Pattern.Trim(), r => r, StringComparer.OrdinalIgnoreCase);

      double ShiftScheduledHours(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) return 0;
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return 0;
        if (!TryParseTimeRange(row.WorkingHours ?? string.Empty, out var st, out var en)) return 0;
        var minutes = MinutesBetween(st, en);
        if (TryParseTimeRange(row.Break ?? string.Empty, out var bs, out var be))
        {
          minutes = Math.Max(0, minutes - MinutesBetween(bs, be));
        }
        return minutes / 60.0;
      }

      HashSet<DayOfWeek> ShiftWorkingDays(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        return ParseWorkingDays(row.WorkingDays ?? string.Empty);
      }

      var startStr = monthStart.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
      var endStr = monthEnd.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
      var punchRange = (Program.DevicePunches ?? Array.Empty<Punch>())
        .Where(p => string.Equals(p.StaffId, staffId, StringComparison.Ordinal) && !string.IsNullOrWhiteSpace(p.EventDate))
        .Where(p => string.CompareOrdinal(p.EventDate, startStr) >= 0 && string.CompareOrdinal(p.EventDate, endStr) <= 0)
        .GroupBy(p => p.EventDate, StringComparer.Ordinal)
        .ToDictionary(
          g => g.Key,
          g => g.Select(x => TimeOnly.FromDateTime(x.OccurredAtUtc.ToLocalTime().DateTime)).OrderBy(x => x).ToArray(),
          StringComparer.Ordinal
        );

      var schedPerDay = ShiftScheduledHours(staffPattern);
      var workingDays = ShiftWorkingDays(staffPattern);

      var sb = new StringBuilder();
      var monthDisplay = monthStart.ToString("MMM-yyyy", CultureInfo.InvariantCulture);
      sb.AppendLine(CsvRow("Staff ID", "Name", "Month"));
      sb.AppendLine(CsvRow(staffId, staffName, monthDisplay));
      sb.AppendLine();

      sb.AppendLine(CsvRow("Daily"));
      sb.AppendLine(CsvRow("Date", "Shift", "First In", "Last Out", "Total Hours", "OT Hours", "Status", "Flags", "Flagged Punch"));

      var monthlyDaysPresent = 0;
      var monthlyWorkingDays = 0;
      var monthlyHours = 0.0;
      var monthlyOt = 0.0;

      for (var d = monthStart; d <= monthEnd; d = d.AddDays(1))
      {
        if (workingDays.Contains(d.DayOfWeek)) monthlyWorkingDays++;
        var key = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var times = punchRange.TryGetValue(key, out var ts) ? ts : Array.Empty<TimeOnly>();

        var kept = new List<TimeOnly>(capacity: times.Length);
        var flagged = new List<TimeOnly>(capacity: Math.Min(8, times.Length));
        TimeOnly? lastKept = null;
        foreach (var t in times)
        {
          if (lastKept is not null)
          {
            var diff = MinutesBetween(lastKept.Value, t);
            if (diff >= 0 && diff < 60) { flagged.Add(t); continue; }
          }
          kept.Add(t);
          lastKept = t;
        }
        var used = kept.ToArray();

        if (used.Length > 0 && workingDays.Contains(d.DayOfWeek)) monthlyDaysPresent++;

        var workMinutes = 0;
        for (var i = 0; i + 1 < used.Length; i++)
        {
          if (i % 2 != 0) continue;
          workMinutes += MinutesBetween(used[i], used[i + 1]);
        }
        var worked = workMinutes / 60.0;
        monthlyHours += worked;
        monthlyOt += Math.Max(0, worked - schedPerDay);

        var status = used.Length == 0 ? "Absent" : (used.Length % 2 == 1 ? "Incomplete" : "Present");
        var flags = new List<string>();
        if (used.Length > 0 && used.Length % 2 == 1) flags.Add("MISSING_OUT");
        if (flagged.Count > 0) flags.Add("DUPLICATE x" + flagged.Count);

        var firstIn = used.Length > 0 ? used[0].ToString("HH:mm", CultureInfo.InvariantCulture) : "-";
        var lastOut = used.Length > 0 ? used[^1].ToString("HH:mm", CultureInfo.InvariantCulture) : "-";
        sb.AppendLine(CsvRow(
          key,
          staffPattern,
          firstIn,
          lastOut,
          Math.Round(worked, 2).ToString(CultureInfo.InvariantCulture),
          Math.Round(Math.Max(0, worked - schedPerDay), 2).ToString(CultureInfo.InvariantCulture),
          status,
          string.Join(" | ", flags),
          string.Join("; ", flagged.Select(x => x.ToString("HH:mm", CultureInfo.InvariantCulture)))
        ));
      }

      sb.AppendLine();
      sb.AppendLine(CsvRow("Weekly"));
      sb.AppendLine(CsvRow("Week", "Flagged Punches", "Total Hours", "OT Hours", "Days Present", "Days Absent", "Attendance %"));

      var weekStart = monthStart.AddDays(-(((int)monthStart.DayOfWeek - (int)DayOfWeek.Monday + 7) % 7));
      for (var ws = weekStart; ws <= monthEnd; ws = ws.AddDays(7))
      {
        var we = ws.AddDays(6);
        var from = ws < monthStart ? monthStart : ws;
        var to = we > monthEnd ? monthEnd : we;

        var totalWorkingDays = 0;
        var daysPresent = 0;
        var totalHours = 0.0;
        var totalOt = 0.0;
        var flaggedPunches = 0;

        for (var d = from; d <= to; d = d.AddDays(1))
        {
          if (workingDays.Contains(d.DayOfWeek)) totalWorkingDays++;
          var key = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
          var times = punchRange.TryGetValue(key, out var ts) ? ts : Array.Empty<TimeOnly>();
          if (times.Length == 0) continue;

          var kept = new List<TimeOnly>(capacity: times.Length);
          TimeOnly? lastKept = null;
          foreach (var t in times)
          {
            if (lastKept is not null)
            {
              var diff = MinutesBetween(lastKept.Value, t);
              if (diff >= 0 && diff < 60) { flaggedPunches++; continue; }
            }
            kept.Add(t);
            lastKept = t;
          }
          times = kept.ToArray();
          if (times.Length == 0) continue;
          if (workingDays.Contains(d.DayOfWeek)) daysPresent++;

          var workMinutes = 0;
          for (var i = 0; i + 1 < times.Length; i++)
          {
            if (i % 2 != 0) continue;
            workMinutes += MinutesBetween(times[i], times[i + 1]);
          }
          var worked = workMinutes / 60.0;
          totalHours += worked;
          totalOt += Math.Max(0, worked - schedPerDay);
        }

        var daysAbsent = Math.Max(0, totalWorkingDays - daysPresent);
        var attPct = totalWorkingDays > 0 ? (daysPresent * 100.0) / totalWorkingDays : 0.0;
        sb.AppendLine(CsvRow(
          ws.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + " to " + we.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
          flaggedPunches.ToString(CultureInfo.InvariantCulture),
          Math.Round(totalHours, 2).ToString(CultureInfo.InvariantCulture),
          Math.Round(totalOt, 2).ToString(CultureInfo.InvariantCulture),
          daysPresent.ToString(CultureInfo.InvariantCulture),
          daysAbsent.ToString(CultureInfo.InvariantCulture),
          Math.Round(attPct, 1).ToString(CultureInfo.InvariantCulture) + "%"
        ));
      }

      sb.AppendLine();
      sb.AppendLine(CsvRow("Monthly"));
      sb.AppendLine(CsvRow("Month", "Total Hours", "OT Hours", "Days Present", "Days Absent", "Attendance %"));
      var daysAbsentM = Math.Max(0, monthlyWorkingDays - monthlyDaysPresent);
      var attPctM = monthlyWorkingDays > 0 ? (monthlyDaysPresent * 100.0) / monthlyWorkingDays : 0.0;
      sb.AppendLine(CsvRow(
        monthDisplay,
        Math.Round(monthlyHours, 2).ToString(CultureInfo.InvariantCulture),
        Math.Round(monthlyOt, 2).ToString(CultureInfo.InvariantCulture),
        monthlyDaysPresent.ToString(CultureInfo.InvariantCulture),
        daysAbsentM.ToString(CultureInfo.InvariantCulture),
        Math.Round(attPctM, 1).ToString(CultureInfo.InvariantCulture) + "%"
      ));

      ctx.Response.Headers.ContentDisposition = $"attachment; filename=attendance_report_{staffId}_{monthStart:yyyy-MM}.csv";
      return Results.Text(sb.ToString(), "text/csv; charset=utf-8");
    }).RequireAuthorization();

    app.MapGet("/api/spreadsheet/monthly", async (HttpContext ctx) =>
    {
      var q = ctx.Request.Query;
      var monthRaw = (q.TryGetValue("month", out var v1) ? v1.ToString() : string.Empty).Trim();

      var now = DateTime.Now;
      var year = now.Year;
      var month = now.Month;
      if (!string.IsNullOrWhiteSpace(monthRaw))
      {
        var parts = monthRaw.Split('-', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 2 && int.TryParse(parts[0], out var y) && int.TryParse(parts[1], out var m))
        {
          if (y >= 2000 && y <= 2100) year = y;
          if (m >= 1 && m <= 12) month = m;
        }
      }

      var monthStart = new DateOnly(year, month, 1);
      var monthEnd = monthStart.AddMonths(1).AddDays(-1);

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;

      static string[] ParseCsv(string line)
      {
        var res = new List<string>();
        var sb = new StringBuilder();
        var inQ = false;
        for (var i = 0; i < line.Length; i++)
        {
          var c = line[i];
          if (inQ)
          {
            if (c == '"')
            {
              if (i + 1 < line.Length && line[i + 1] == '"') { sb.Append('"'); i++; }
              else inQ = false;
            }
            else sb.Append(c);
          }
          else
          {
            if (c == '"') inQ = true;
            else if (c == ',') { res.Add(sb.ToString()); sb.Clear(); }
            else sb.Append(c);
          }
        }
        res.Add(sb.ToString());
        return res.Select(x => (x ?? string.Empty).Trim()).ToArray();
      }

      static bool TryParseHm(string raw, out TimeOnly t)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) { t = default; return false; }
        if (TimeOnly.TryParseExact(raw, "HH:mm", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        if (TimeOnly.TryParseExact(raw, "HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        return false;
      }

      static bool TryParseTimeRange(string raw, out TimeOnly start, out TimeOnly end)
      {
        start = default;
        end = default;
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return false;
        var sep = raw.Contains('–', StringComparison.Ordinal) ? '–' : '-';
        var parts = raw.Split(sep, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2) return false;
        return TryParseHm(parts[0], out start) && TryParseHm(parts[1], out end);
      }

      static HashSet<DayOfWeek> ParseWorkingDays(string raw)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        raw = raw.Replace("to", "–", StringComparison.OrdinalIgnoreCase);
        raw = raw.Replace("-", "–", StringComparison.Ordinal);

        static bool TryMap(string s, out DayOfWeek d)
        {
          d = default;
          s = (s ?? string.Empty).Trim();
          if (s.Length < 3) return false;
          var k = s[..3].ToLowerInvariant();
          if (k == "mon") { d = DayOfWeek.Monday; return true; }
          if (k == "tue") { d = DayOfWeek.Tuesday; return true; }
          if (k == "wed") { d = DayOfWeek.Wednesday; return true; }
          if (k == "thu") { d = DayOfWeek.Thursday; return true; }
          if (k == "fri") { d = DayOfWeek.Friday; return true; }
          if (k == "sat") { d = DayOfWeek.Saturday; return true; }
          if (k == "sun") { d = DayOfWeek.Sunday; return true; }
          return false;
        }

        if (raw.Contains('–', StringComparison.Ordinal))
        {
          var dashParts = raw.Split('–', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
          if (dashParts.Length == 2 && TryMap(dashParts[0], out var a) && TryMap(dashParts[1], out var b))
          {
            var set = new HashSet<DayOfWeek>();
            var cur = (int)a;
            var endI = (int)b;
            for (var i = 0; i < 7; i++)
            {
              set.Add((DayOfWeek)cur);
              if (cur == endI) break;
              cur = (cur + 1) % 7;
            }
            return set;
          }
        }

        var parts = raw.Split(new[] { ',', ';', '/', ' ' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var res = new HashSet<DayOfWeek>();
        foreach (var p in parts)
        {
          if (!TryMap(p, out var d)) continue;
          res.Add(d);
        }
        if (res.Count == 0) res = new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        return res;
      }

      static int MinutesBetween(TimeOnly a, TimeOnly b)
      {
        var am = a.Hour * 60 + a.Minute;
        var bm = b.Hour * 60 + b.Minute;
        if (bm < am) bm += 24 * 60;
        return bm - am;
      }

      var anyActive = false;
      var staffAll = new List<(string Id, string Name, string Status, string ShiftPattern)>(capacity: 256);
      string anonKeyLocal;
      lock (stateGate) anonKeyLocal = dashboardSupabaseAnonKey;
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && (!string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || !string.IsNullOrWhiteSpace(anonKeyLocal)))
      {
        var supa = await TryLoadStaffFromSupabase(cfg, anonKeyLocal, ctx.RequestAborted);
        if (!supa.ok)
        {
          return Results.Json(new { ok = false, error = $"Failed to load staff from Supabase: {supa.error}" }, JsonOptions);
        }
        foreach (var r in supa.rows)
        {
          var status = (r.Status ?? string.Empty).Trim();
          if (status.Length == 0 || string.Equals(status, "Active", StringComparison.OrdinalIgnoreCase)) anyActive = true;
          staffAll.Add((r.Id, r.Name, status, r.ShiftPattern));
        }
      }
      else
      {
        var staffPath = ResolveStaffCsvPath();
        if (string.IsNullOrWhiteSpace(staffPath) || !File.Exists(staffPath))
        {
          return Results.Json(new { ok = false, error = "Staff list not found. Configure Supabase staff table or provide Database - Staff WL10.csv." }, JsonOptions);
        }
        var first = true;
        foreach (var raw in File.ReadLines(staffPath))
        {
          var line = raw ?? string.Empty;
          if (first) { first = false; continue; }
          if (string.IsNullOrWhiteSpace(line)) continue;
          var parts = ParseCsv(line);
          if (parts.Length < 7) continue;
          var id = (parts[0] ?? string.Empty).Trim();
          if (id.Length == 0) continue;
          var name = (parts[1] ?? string.Empty).Trim();
          var status = (parts[4] ?? string.Empty).Trim();
          var shiftPattern = (parts[6] ?? string.Empty).Trim();
          if (status.Length == 0 || string.Equals(status, "Active", StringComparison.OrdinalIgnoreCase)) anyActive = true;
          staffAll.Add((id, name, status, shiftPattern));
        }
      }

      var roster = anyActive
        ? staffAll.Where(x => x.Status.Length == 0 || string.Equals(x.Status, "Active", StringComparison.OrdinalIgnoreCase)).ToArray()
        : staffAll.ToArray();

      static ShiftPatternRow[] DefaultShiftRows()
      {
        return new[]
        {
          new ShiftPatternRow("Normal", "Mon–Fri", "09:00–18:00", "13:00–14:00", "Default"),
          new ShiftPatternRow("Shift 1", "Mon–Sat", "08:00–16:00", "12:00–13:00", "Default"),
          new ShiftPatternRow("Shift 2", "Mon–Sat", "16:00–00:00", "20:00–20:30", "Default"),
          new ShiftPatternRow("Shift 3", "Mon–Sat", "00:00–08:00", "04:00–04:30", "Default"),
        };
      }

      ShiftPatternRow[] shiftRows;
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && (!string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || !string.IsNullOrWhiteSpace(anonKeyLocal)))
      {
        var supaShifts = await TryLoadShiftPatternsFromSupabase(cfg, anonKeyLocal, ctx.RequestAborted);
        shiftRows = supaShifts.ok ? supaShifts.rows : Array.Empty<ShiftPatternRow>();
      }
      else
      {
        shiftRows = Array.Empty<ShiftPatternRow>();
      }

      if (shiftRows.Length == 0)
      {
        var state = LoadState(statePath);
        shiftRows = (state.ShiftPatterns ?? Array.Empty<ShiftPatternRow>()).ToArray();
      }
      if (shiftRows.Length == 0) shiftRows = DefaultShiftRows();

      var shiftByPattern = shiftRows
        .Where(r => !string.IsNullOrWhiteSpace(r.Pattern))
        .ToDictionary(r => r.Pattern.Trim(), r => r, StringComparer.OrdinalIgnoreCase);

      double ShiftScheduledHours(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) return 0;
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return 0;
        if (!TryParseTimeRange(row.WorkingHours ?? string.Empty, out var st, out var en)) return 0;
        var minutes = MinutesBetween(st, en);
        if (TryParseTimeRange(row.Break ?? string.Empty, out var bs, out var be))
        {
          minutes = Math.Max(0, minutes - MinutesBetween(bs, be));
        }
        return minutes / 60.0;
      }

      HashSet<DayOfWeek> ShiftWorkingDays(string pattern)
      {
        if (string.IsNullOrWhiteSpace(pattern)) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        if (!shiftByPattern.TryGetValue(pattern.Trim(), out var row)) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        return ParseWorkingDays(row.WorkingDays ?? string.Empty);
      }

      var startStr = monthStart.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
      var endStr = monthEnd.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
      var punchRange = Program.DevicePunches
        .Where(p =>
        {
          var d = (p.EventDate ?? string.Empty).Trim();
          return d.Length == 10 && string.CompareOrdinal(d, startStr) >= 0 && string.CompareOrdinal(d, endStr) <= 0;
        })
        .GroupBy(p => $"{p.StaffId}|{p.EventDate}", StringComparer.Ordinal)
        .ToDictionary(
          g => g.Key,
          g => g.Select(x => TimeOnly.FromDateTime(x.OccurredAtUtc.ToLocalTime().DateTime)).OrderBy(x => x).ToArray(),
          StringComparer.Ordinal
        );

      var outRows = new List<object>(capacity: roster.Length);
      foreach (var s in roster)
      {
        var id = s.Id;
        var name = string.IsNullOrWhiteSpace(s.Name) ? id : s.Name;
        var pattern = string.IsNullOrWhiteSpace(s.ShiftPattern) ? "Normal" : s.ShiftPattern;

        var workingDays = ShiftWorkingDays(pattern);
        var totalWorkingDays = 0;
        var daysPresent = 0;
        var totalHours = 0.0;
        var totalOt = 0.0;
        var flaggedPunches = 0;
        var schedPerDay = ShiftScheduledHours(pattern);

        for (var d = monthStart; d <= monthEnd; d = d.AddDays(1))
        {
          if (workingDays.Contains(d.DayOfWeek)) totalWorkingDays++;
          var key = id + "|" + d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
          if (!punchRange.TryGetValue(key, out var times) || times.Length == 0) continue;
          var kept = new List<TimeOnly>(capacity: times.Length);
          TimeOnly? lastKept = null;
          foreach (var t in times)
          {
            if (lastKept is not null)
            {
              var am = lastKept.Value.Hour * 60 + lastKept.Value.Minute;
              var bm = t.Hour * 60 + t.Minute;
              var diff = bm - am;
              if (diff < 0) diff += 24 * 60;
              if (diff >= 0 && diff < 60)
              {
                flaggedPunches++;
                continue;
              }
            }
            kept.Add(t);
            lastKept = t;
          }
          times = kept.ToArray();
          if (times.Length == 0) continue;
          if (workingDays.Contains(d.DayOfWeek)) daysPresent++;

          var workMinutes = 0;
          for (var i = 0; i + 1 < times.Length; i++)
          {
            if (i % 2 != 0) continue;
            workMinutes += MinutesBetween(times[i], times[i + 1]);
          }
          var worked = workMinutes / 60.0;
          totalHours += worked;
          totalOt += Math.Max(0, worked - schedPerDay);
        }

        var daysAbsent = Math.Max(0, totalWorkingDays - daysPresent);
        var attPct = totalWorkingDays > 0 ? (daysPresent * 100.0) / totalWorkingDays : 0.0;

        outRows.Add(new
        {
          staff_id = id,
          name,
          month = monthStart.ToString("yyyy-MM", CultureInfo.InvariantCulture),
          flagged_punches = flaggedPunches,
          total_hours = Math.Round(totalHours, 2),
          ot_hours = Math.Round(totalOt, 2),
          days_present = daysPresent,
          days_absent = daysAbsent,
          attendance_pct = Math.Round(attPct, 1),
        });
      }

      var sorted = outRows
        .Select(x => new { k = (string)((dynamic)x).name, v = x })
        .OrderBy(x => x.k, StringComparer.OrdinalIgnoreCase)
        .Select(x => x.v)
        .ToArray();

      return Results.Json(new { ok = true, month = monthStart.ToString("yyyy-MM", CultureInfo.InvariantCulture), rows = sorted }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/records/device", () =>
    {
      var dayIndex = Program.DevicePunches
        .GroupBy(p => $"{p.StaffId}|{p.EventDate}", StringComparer.Ordinal)
        .ToDictionary(
          g => g.Key,
          g =>
          {
            var localTimes = g.Select(x => x.OccurredAtUtc.ToLocalTime().TimeOfDay).ToArray();
            if (localTimes.Length == 0) return ((string?)null, (string?)null);
            var min = localTimes.Min();
            var max = localTimes.Max();
            return (TimeOnly.FromTimeSpan(min).ToString("HH:mm:ss", CultureInfo.InvariantCulture), TimeOnly.FromTimeSpan(max).ToString("HH:mm:ss", CultureInfo.InvariantCulture));
          },
          StringComparer.Ordinal
        );

      var rows = Program.DevicePunches
        .OrderByDescending(p => p.OccurredAtUtc)
        .Select(p => new
        {
          staff_id = p.StaffId,
          occurred_at = p.OccurredAtUtc.ToString("O"),
          event_date = p.EventDate,
          day_clock_in = dayIndex.TryGetValue($"{p.StaffId}|{p.EventDate}", out var t) ? t.Item1 : null,
          day_clock_out = dayIndex.TryGetValue($"{p.StaffId}|{p.EventDate}", out var t2) ? t2.Item2 : null,
          device_id = p.DeviceId,
          source = "wl10",
        })
        .ToArray();
      return Results.Json(new { rows }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/presets", (HttpContext ctx) =>
    {
      var state = LoadState(statePath);
      var deviceList = (state.DevicePresets ?? Array.Empty<DevicePreset>())
        .Where(p => p.LastOkAtUtc is not null)
        .OrderByDescending(p => p.LastOkAtUtc)
        .Take(5)
        .ToArray();
      var pollingList = (state.PollingPresets ?? Array.Empty<PollingPreset>())
        .OrderByDescending(p => p.SavedAtUtc)
        .Take(5)
        .ToArray();
      var dbList = (state.DbPresets ?? Array.Empty<DbPreset>())
        .Where(p => p.LastOkAtUtc is not null)
        .OrderByDescending(p => p.LastOkAtUtc)
        .Take(5)
        .ToArray();

      var device = deviceList.Select((p, idx) => new { idx, p.DeviceIp, p.DevicePort, p.ReaderMode, p.SavedAtUtc, p.LastOkAtUtc }).ToArray();
      var polling = pollingList.Select((p, idx) => new { idx, p.PollIntervalSeconds, p.AutoSyncEnabled, p.SavedAtUtc }).ToArray();
      var db = dbList.Select((p, idx) => new { idx, p.Settings.SupabaseUrl, p.Settings.SupabaseAttendanceTable, p.SavedAtUtc, p.LastOkAtUtc }).ToArray();

      return Results.Json(new { device, polling, db }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/presets/apply", async (HttpContext ctx) =>
    {
      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var root = doc.RootElement;
      var kind = root.TryGetProperty("kind", out var kEl) ? (kEl.GetString() ?? string.Empty).Trim() : string.Empty;
      var idx = root.TryGetProperty("idx", out var iEl) && iEl.TryGetInt32(out var iVal) ? iVal : -1;
      if (idx < 0 || idx > 10) return Results.Json(new { ok = false, error = "invalid idx" }, JsonOptions);

      var state = LoadState(statePath);
      var now = DateTimeOffset.UtcNow;

      if (string.Equals(kind, "device", StringComparison.OrdinalIgnoreCase))
      {
        var list = (state.DevicePresets ?? Array.Empty<DevicePreset>())
          .Where(p => p.LastOkAtUtc is not null)
          .OrderByDescending(p => p.LastOkAtUtc)
          .Take(5)
          .ToArray();
        if (idx >= list.Length) return Results.Json(new { ok = false, error = "not found" }, JsonOptions);
        var p = list[idx];
        lock (stateGate)
        {
          currentConfig = currentConfig with { DeviceIp = p.DeviceIp, DevicePort = p.DevicePort, ReaderMode = p.ReaderMode, DeviceId = $"WL10-{p.DeviceIp}" };
        }
        var updatedList = (state.DevicePresets ?? Array.Empty<DevicePreset>()).ToList();
        updatedList.Insert(0, p with { SavedAtUtc = now });
        var keep = updatedList
          .GroupBy(x => $"{x.DeviceIp}|{x.DevicePort}|{x.ReaderMode}", StringComparer.Ordinal)
          .Select(g => g.First())
          .OrderByDescending(x => x.LastOkAtUtc ?? x.SavedAtUtc)
          .Take(5)
          .ToArray();
        SaveState(statePath, state with { DevicePresets = keep });
        return Results.Json(new { ok = true }, JsonOptions);
      }

      if (string.Equals(kind, "polling", StringComparison.OrdinalIgnoreCase))
      {
        var list = (state.PollingPresets ?? Array.Empty<PollingPreset>())
          .OrderByDescending(p => p.SavedAtUtc)
          .Take(5)
          .ToArray();
        if (idx >= list.Length) return Results.Json(new { ok = false, error = "not found" }, JsonOptions);
        var p = list[idx];
        lock (stateGate)
        {
          var sec = p.PollIntervalSeconds <= 0 ? 3600 : p.PollIntervalSeconds;
          if (sec == 600) sec = 3600;
          pollIntervalSeconds = Math.Clamp(sec, 60, 3600);
          autoSyncEnabled = p.AutoSyncEnabled;
        }
        var updatedList = (state.PollingPresets ?? Array.Empty<PollingPreset>()).ToList();
        updatedList.Insert(0, p with { SavedAtUtc = now });
        var keep = updatedList
          .GroupBy(x => $"{x.PollIntervalSeconds}|{x.AutoSyncEnabled}", StringComparer.Ordinal)
          .Select(g => g.First())
          .OrderByDescending(x => x.SavedAtUtc)
          .Take(5)
          .ToArray();
        SaveState(statePath, state with { PollingPresets = keep });
        return Results.Json(new { ok = true }, JsonOptions);
      }

      if (string.Equals(kind, "db", StringComparison.OrdinalIgnoreCase))
      {
        var list = (state.DbPresets ?? Array.Empty<DbPreset>())
          .Where(p => p.LastOkAtUtc is not null)
          .OrderByDescending(p => p.LastOkAtUtc)
          .Take(5)
          .ToArray();
        if (idx >= list.Length) return Results.Json(new { ok = false, error = "not found" }, JsonOptions);
        var p = list[idx];
        var ds = p.Settings;
        var key = ds.SupabaseKeyIsProtected ? TryUnprotectBase64(ds.SupabaseKeyProtectedBase64) : ds.SupabaseKeyProtectedBase64;
        lock (stateGate)
        {
          currentConfig = currentConfig with
          {
            SupabaseUrl = ds.SupabaseUrl,
            SupabaseAttendanceTable = ds.SupabaseAttendanceTable,
            SupabaseSyncEnabled = ds.SupabaseSyncEnabled,
            SupabaseServiceRoleKey = key,
          };
          dashboardSupabaseAnonKey = (ds.SupabaseAnonKey ?? string.Empty).Trim();
          dashboardSupabaseProjectId = (ds.SupabaseProjectId ?? string.Empty).Trim();
          dashboardSupabaseJwtSecret = (ds.SupabaseJwtSecret ?? string.Empty).Trim();
        }
        var updatedList = (state.DbPresets ?? Array.Empty<DbPreset>()).ToList();
        updatedList.Insert(0, p with { SavedAtUtc = now });
        var keep = updatedList
          .GroupBy(x => $"{x.Settings.SupabaseUrl}|{x.Settings.SupabaseAttendanceTable}|{x.Settings.SupabaseAnonKey}|{x.Settings.SupabaseProjectId}|{x.Settings.SupabaseJwtSecret}", StringComparer.Ordinal)
          .Select(g => g.First())
          .OrderByDescending(x => x.LastOkAtUtc ?? x.SavedAtUtc)
          .Take(5)
          .ToArray();
        SaveState(statePath, state with { DashboardSettings = ds, DbPresets = keep });
        return Results.Json(new { ok = true }, JsonOptions);
      }

      return Results.Json(new { ok = false, error = "invalid kind" }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/records/file", (HttpContext ctx) =>
    {
      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;

      var deviceIdRaw = ctx.Request.Query.TryGetValue("deviceId", out var dv) ? (dv.ToString() ?? "") : "";
      var deviceId = deviceIdRaw.Trim();
      if (deviceId.Length == 0) deviceId = cfg.DeviceId;
      var configured = EnsureConfiguredDevices(saveIfSeeded: true);

      var path = TryResolveAttlogExportPath();
      if (string.IsNullOrWhiteSpace(path)) return Results.Json(new { rows = Array.Empty<object>(), error = "Could not resolve 1_attlog.dat path" }, JsonOptions);
      if (!File.Exists(path))
      {
        try
        {
          var dir = Path.GetDirectoryName(Path.GetFullPath(path));
          if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir);
          File.WriteAllText(path, string.Empty, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
        }
        catch
        {
          return Results.Json(new { rows = Array.Empty<object>(), error = $"File not found: {path}" }, JsonOptions);
        }
      }

      static List<object> ReadWl10FileRows(string filePath, string wl10DeviceId)
      {
        var rows = new List<object>(capacity: 200);
        foreach (var raw in File.ReadLines(filePath))
        {
          var line = (raw ?? string.Empty).Trim();
          if (line.Length == 0) continue;

          var parts = line.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
          if (parts.Length < 7) continue;

          var staffId = parts[0].Trim();
          if (staffId.Length == 0) continue;

          var dtRaw = parts[1].Trim() + " " + parts[2].Trim();
          _ = DateTime.TryParseExact(dtRaw, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out _);

          rows.Add(new
          {
            device_id = wl10DeviceId,
            staff_id = staffId,
            datetime = dtRaw,
            verified = parts[3].Trim(),
            status = parts[4].Trim(),
            workcode = parts[5].Trim(),
            reserved = parts[6].Trim(),
          });
        }
        return rows;
      }

      static List<object> ReadW30FileRows(ConfiguredDeviceEntry dev, string baseDir)
      {
        var rows = new List<object>(capacity: 200);
        var w30Dir = (dev.LogDir ?? string.Empty).Trim();
        if (w30Dir.Length == 0) w30Dir = baseDir;
        if (w30Dir.Length == 0) w30Dir = Directory.GetCurrentDirectory();
        if (!Directory.Exists(w30Dir)) return rows;

        var pat = (dev.FilePattern ?? string.Empty).Trim();
        if (pat.Length == 0) pat = "AttendanceLog*.dat";
        IEnumerable<FileInfo> files;
        try { files = new DirectoryInfo(w30Dir).EnumerateFiles(pat, SearchOption.TopDirectoryOnly); }
        catch { files = Array.Empty<FileInfo>(); }
        foreach (var f in files.OrderByDescending(x => x.LastWriteTimeUtc).Take(50))
        {
          try
          {
            foreach (var r in ReadW30AttlogRows(f.FullName))
            {
              rows.Add(new
              {
                device_id = dev.DeviceId,
                staff_id = r.StaffId,
                datetime = r.DateTime,
                verified = r.Verified,
                status = r.Status,
                workcode = r.Workcode,
                reserved = r.Reserved,
              });
            }
          }
          catch { }
        }
        return rows;
      }

      var baseDir = Path.GetDirectoryName(Path.GetFullPath(path)) ?? string.Empty;

      var wl10Rows = new List<object>(capacity: 0);
      if (deviceId.Equals("all", StringComparison.OrdinalIgnoreCase) || deviceId.Equals(cfg.DeviceId, StringComparison.Ordinal))
      {
        wl10Rows = ReadWl10FileRows(path, cfg.DeviceId);
      }

      var w30Rows = new List<object>(capacity: 0);
      if (deviceId.Equals("all", StringComparison.OrdinalIgnoreCase))
      {
        foreach (var dev in configured.Where(d => d.DeviceType == "W30"))
        {
          try { w30Rows.AddRange(ReadW30FileRows(dev, baseDir)); } catch { }
        }
      }
      else
      {
        var dev = configured.FirstOrDefault(d => string.Equals(d.DeviceId, deviceId, StringComparison.Ordinal));
        if (dev is not null && dev.DeviceType == "W30")
        {
          w30Rows = ReadW30FileRows(dev, baseDir);
        }
      }

      var rows = new List<object>(capacity: wl10Rows.Count + w30Rows.Count);
      if (wl10Rows.Count > 0) rows.AddRange(wl10Rows);
      if (w30Rows.Count > 0) rows.AddRange(w30Rows);

      if (rows.Count == 0 && !deviceId.Equals("all", StringComparison.OrdinalIgnoreCase) && !deviceId.Equals(cfg.DeviceId, StringComparison.Ordinal))
      {
        return Results.Json(new { rows = rows.ToArray(), error = $"No records for {deviceId}. If this is a WL10 device, click Connect first (WL10 file is shared)." }, JsonOptions);
      }

      return Results.Json(new { rows = rows.ToArray() }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/staff/file", (HttpContext ctx) =>
    {
      AppConfig cfg;
      string anonKey;
      lock (stateGate)
      {
        cfg = currentConfig;
        anonKey = dashboardSupabaseAnonKey;
      }
      var serviceKeyRaw = (cfg.SupabaseServiceRoleKey ?? string.Empty).Trim();
      var anonRaw = (anonKey ?? string.Empty).Trim();
      if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || (serviceKeyRaw.Length == 0 && anonRaw.Length == 0))
      {
        return Results.Json(new { rows = Array.Empty<object>(), error = "Supabase not configured. Set Supabase URL + API key in Database Sync settings." }, JsonOptions);
      }

      static string FmtDateOnly(string? raw)
      {
        var s = (raw ?? string.Empty).Trim();
        if (s.Length == 0) return string.Empty;
        if (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
        {
          return dto.ToLocalTime().ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }
        if (DateOnly.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out var d))
        {
          return d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }
        return s.Length >= 10 ? s[..10] : s;
      }

      static async Task<(bool ok, object[] rows, string? error, bool shiftPatternSupported, int statusCode)> TryFetchAsync(AppConfig cfg, string apiKey, bool includeShiftPattern, CancellationToken ct)
      {
        var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
        var cols = includeShiftPattern
          ? "id,full_name,role,department,status,date_joined,shift_pattern"
          : "id,full_name,role,department,status,date_joined";
        var url = $"{baseUrl}/rest/v1/staff?select={Uri.EscapeDataString(cols)}&order=id.asc&limit=5000";
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.TryAddWithoutValidation("apikey", apiKey);
        req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
        using var resp = await http.SendAsync(req, ct);
        var body = await resp.Content.ReadAsStringAsync(ct);
        if (!resp.IsSuccessStatusCode)
        {
          return (false, Array.Empty<object>(), body.Length > 350 ? body[..350] : body, includeShiftPattern, (int)resp.StatusCode);
        }

        using var doc = JsonDocument.Parse(body);
        if (doc.RootElement.ValueKind != JsonValueKind.Array) return (false, Array.Empty<object>(), "Unexpected response shape", includeShiftPattern, 200);
        var list = new List<object>(capacity: 256);
        foreach (var el in doc.RootElement.EnumerateArray())
        {
          var id = el.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.String ? (idEl.GetString() ?? "") : "";
          if (id.Length == 0) continue;
          var fullName = el.TryGetProperty("full_name", out var nEl) && nEl.ValueKind == JsonValueKind.String ? (nEl.GetString() ?? "") : "";
          var role = el.TryGetProperty("role", out var rEl) && rEl.ValueKind == JsonValueKind.String ? (rEl.GetString() ?? "") : "";
          if (id == "16" && role.Equals("superadmin", StringComparison.OrdinalIgnoreCase)) role = "Manager";
          var dept = el.TryGetProperty("department", out var dEl) && dEl.ValueKind == JsonValueKind.String ? (dEl.GetString() ?? "") : "";
          var status = el.TryGetProperty("status", out var stEl) && stEl.ValueKind == JsonValueKind.String ? (stEl.GetString() ?? "") : "";
          var dj = el.TryGetProperty("date_joined", out var djEl) && djEl.ValueKind == JsonValueKind.String ? djEl.GetString() : null;
          var sp = includeShiftPattern && el.TryGetProperty("shift_pattern", out var spEl) && spEl.ValueKind == JsonValueKind.String ? (spEl.GetString() ?? "") : "";
          list.Add(new
          {
            user_id = id,
            full_name = fullName,
            role,
            department = dept,
            status,
            date_joined = FmtDateOnly(dj),
            shift_pattern = sp,
          });
        }
        return (true, list.ToArray(), null, includeShiftPattern, 200);
      }

      var apiKey = serviceKeyRaw.Length > 0 ? serviceKeyRaw : anonRaw;
      var t = TryFetchAsync(cfg, apiKey, includeShiftPattern: true, ctx.RequestAborted).GetAwaiter().GetResult();
      if (!t.ok && t.statusCode == 401 && apiKey == serviceKeyRaw && anonRaw.Length > 0)
      {
        t = TryFetchAsync(cfg, anonRaw, includeShiftPattern: true, ctx.RequestAborted).GetAwaiter().GetResult();
      }
      if (!t.ok && t.error is not null && t.error.Contains("shift_pattern", StringComparison.OrdinalIgnoreCase))
      {
        t = TryFetchAsync(cfg, apiKey, includeShiftPattern: false, ctx.RequestAborted).GetAwaiter().GetResult();
        if (!t.ok && t.statusCode == 401 && apiKey == serviceKeyRaw && anonRaw.Length > 0)
        {
          t = TryFetchAsync(cfg, anonRaw, includeShiftPattern: false, ctx.RequestAborted).GetAwaiter().GetResult();
        }
      }
      if (!t.ok) return Results.Json(new { rows = Array.Empty<object>(), error = t.error ?? "Failed to load staff" }, JsonOptions);
      return Results.Json(new { rows = t.rows }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/staff/attendance/month", async (HttpContext ctx) =>
    {
      var q = ctx.Request.Query;
      var staffId = (q.TryGetValue("staffId", out var v1) ? v1.ToString() : string.Empty).Trim();
      var monthRaw = (q.TryGetValue("month", out var v2) ? v2.ToString() : string.Empty).Trim();
      if (string.IsNullOrWhiteSpace(staffId)) return Results.Json(new { ok = false, error = "staffId required" }, JsonOptions);

      var now = DateTime.Now;
      var year = now.Year;
      var month = now.Month;
      if (!string.IsNullOrWhiteSpace(monthRaw))
      {
        var parts = monthRaw.Split('-', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 2 && int.TryParse(parts[0], out var y) && int.TryParse(parts[1], out var m))
        {
          if (y >= 2000 && y <= 2100) year = y;
          if (m >= 1 && m <= 12) month = m;
        }
      }

      var monthStart = new DateOnly(year, month, 1);
      var monthEnd = monthStart.AddMonths(1).AddDays(-1);

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;

      if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey) || string.IsNullOrWhiteSpace(cfg.SupabaseAttendanceTable))
      {
        return Results.Json(new { ok = false, error = "Supabase not configured." }, JsonOptions);
      }

      static bool TryParseHm(string raw, out TimeOnly t)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) { t = default; return false; }
        if (TimeOnly.TryParseExact(raw, "HH:mm", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        if (TimeOnly.TryParseExact(raw, "HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out t)) return true;
        return false;
      }

      static bool TryParseTimeRange(string raw, out TimeOnly start, out TimeOnly end)
      {
        start = default;
        end = default;
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return false;
        var sep = raw.Contains('–', StringComparison.Ordinal) ? '–' : '-';
        var parts = raw.Split(sep, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2) return false;
        return TryParseHm(parts[0], out start) && TryParseHm(parts[1], out end);
      }

      static HashSet<DayOfWeek> ParseWorkingDays(string raw)
      {
        raw = (raw ?? string.Empty).Trim();
        if (raw.Length == 0) return new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        raw = raw.Replace("to", "–", StringComparison.OrdinalIgnoreCase);
        raw = raw.Replace("-", "–", StringComparison.Ordinal);

        static bool TryMap(string s, out DayOfWeek d)
        {
          d = default;
          s = (s ?? string.Empty).Trim();
          if (s.Length < 3) return false;
          var k = s[..3].ToLowerInvariant();
          if (k == "mon") { d = DayOfWeek.Monday; return true; }
          if (k == "tue") { d = DayOfWeek.Tuesday; return true; }
          if (k == "wed") { d = DayOfWeek.Wednesday; return true; }
          if (k == "thu") { d = DayOfWeek.Thursday; return true; }
          if (k == "fri") { d = DayOfWeek.Friday; return true; }
          if (k == "sat") { d = DayOfWeek.Saturday; return true; }
          if (k == "sun") { d = DayOfWeek.Sunday; return true; }
          return false;
        }

        if (raw.Contains('–', StringComparison.Ordinal))
        {
          var dashParts = raw.Split('–', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
          if (dashParts.Length == 2 && TryMap(dashParts[0], out var a) && TryMap(dashParts[1], out var b))
          {
            var set = new HashSet<DayOfWeek>();
            var cur = (int)a;
            while (true)
            {
              set.Add((DayOfWeek)cur);
              if (cur == (int)b) break;
              cur = (cur + 1) % 7;
            }
            return set;
          }
        }

        var res = new HashSet<DayOfWeek>();
        foreach (var p in raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
          if (!TryMap(p, out var d)) continue;
          res.Add(d);
        }
        if (res.Count == 0) res = new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday });
        return res;
      }

      var state = LoadState(statePath);
      string anon;
      lock (stateGate) anon = dashboardSupabaseAnonKey;

      var staffName = staffId;
      var staffDept = string.Empty;
      var staffPattern = "Normal";
      var supa = await TryLoadStaffFromSupabase(cfg, anon, ctx.RequestAborted);
      if (supa.ok)
      {
        var r = supa.rows.FirstOrDefault(x => string.Equals((x.Id ?? string.Empty).Trim(), staffId, StringComparison.Ordinal));
        if (!string.IsNullOrWhiteSpace(r.Id))
        {
          staffName = string.IsNullOrWhiteSpace(r.Name) ? staffName : r.Name;
          staffDept = (r.Dept ?? string.Empty).Trim();
          staffPattern = string.IsNullOrWhiteSpace(r.ShiftPattern) ? staffPattern : r.ShiftPattern.Trim();
        }
      }

      var supaShifts = await TryLoadShiftPatternsFromSupabase(cfg, anon, ctx.RequestAborted);
      var shiftRows = supaShifts.ok ? supaShifts.rows : Array.Empty<ShiftPatternRow>();
      if (shiftRows.Length == 0) shiftRows = (state.ShiftPatterns ?? Array.Empty<ShiftPatternRow>()).ToArray();
      if (shiftRows.Length == 0)
      {
        shiftRows = new[]
        {
          new ShiftPatternRow("Normal", "Mon–Fri", "09:00–18:00", "13:00–14:00", "Default"),
          new ShiftPatternRow("Shift 1", "Mon–Sat", "08:00–16:00", "12:00–13:00", "Default"),
          new ShiftPatternRow("Shift 2", "Mon–Sat", "16:00–00:00", "20:00–20:30", "Default"),
          new ShiftPatternRow("Shift 3", "Mon–Sat", "00:00–08:00", "04:00–04:30", "Default"),
        };
      }

      var shiftByPattern = shiftRows
        .Where(r => !string.IsNullOrWhiteSpace(r.Pattern))
        .GroupBy(r => r.Pattern.Trim(), StringComparer.OrdinalIgnoreCase)
        .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

      ShiftPatternRow? shiftRow = shiftByPattern.TryGetValue(staffPattern.Trim(), out var sr) ? sr : null;
      shiftRow ??= shiftByPattern.TryGetValue("Normal", out var sr2) ? sr2 : null;
      var workingDays = shiftRow is null ? new HashSet<DayOfWeek>(new[] { DayOfWeek.Monday, DayOfWeek.Tuesday, DayOfWeek.Wednesday, DayOfWeek.Thursday, DayOfWeek.Friday }) : ParseWorkingDays(shiftRow.WorkingDays ?? string.Empty);
      var schedStart = new TimeOnly(9, 0, 0);
      var schedEnd = new TimeOnly(18, 0, 0);
      if (shiftRow is not null && TryParseTimeRange(shiftRow.WorkingHours ?? string.Empty, out var st, out var en))
      {
        schedStart = st;
        schedEnd = en;
      }
      var breakStart = new TimeOnly(13, 0, 0);
      var breakEnd = new TimeOnly(14, 0, 0);
      var hasBreak = false;
      if (shiftRow is not null && TryParseTimeRange(shiftRow.Break ?? string.Empty, out var bst, out var ben))
      {
        breakStart = bst;
        breakEnd = ben;
        hasBreak = true;
      }

      var sg = TimeSpan.FromHours(8);
      var startUtc = new DateTimeOffset(monthStart.ToDateTime(new TimeOnly(0, 0, 0)), sg).ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
      var endUtc = new DateTimeOffset(monthEnd.ToDateTime(new TimeOnly(23, 59, 59)), sg).ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

      var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
      var select = Uri.EscapeDataString("staff_id,occurred_at");
      var sid = Uri.EscapeDataString(staffId);
      var url =
        $"{baseUrl}/rest/v1/{cfg.SupabaseAttendanceTable}?select={select}" +
        $"&staff_id=eq.{sid}" +
        $"&occurred_at=gte.{Uri.EscapeDataString(startUtc)}" +
        $"&occurred_at=lte.{Uri.EscapeDataString(endUtc)}" +
        $"&order=occurred_at.asc&limit=50000";

      var byDay = new Dictionary<DateOnly, List<TimeOnly>>();
      using (var http = new HttpClient())
      using (var req = new HttpRequestMessage(HttpMethod.Get, url))
      {
        req.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
        req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
        req.Headers.TryAddWithoutValidation("Accept", "application/json");
        using var res = await http.SendAsync(req, ctx.RequestAborted);
        if (!res.IsSuccessStatusCode)
        {
          var body = await res.Content.ReadAsStringAsync(ctx.RequestAborted);
          return Results.Json(new { ok = false, error = $"Supabase HTTP {(int)res.StatusCode} {res.ReasonPhrase}. {body}" }, JsonOptions);
        }
        var text = await res.Content.ReadAsStringAsync(ctx.RequestAborted);
        using var doc = JsonDocument.Parse(text);
        if (doc.RootElement.ValueKind != JsonValueKind.Array) return Results.Json(new { ok = false, error = "Supabase returned invalid JSON." }, JsonOptions);
        foreach (var el in doc.RootElement.EnumerateArray())
        {
          var dtRaw = el.TryGetProperty("occurred_at", out var dtEl) && dtEl.ValueKind == JsonValueKind.String ? dtEl.GetString() : null;
          if (string.IsNullOrWhiteSpace(dtRaw)) continue;
          if (!DateTimeOffset.TryParse(dtRaw.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto)) continue;
          var dt = dto.ToOffset(sg).DateTime;
          var d = DateOnly.FromDateTime(dt);
          if (d < monthStart || d > monthEnd) continue;
          var t = TimeOnly.FromDateTime(dt);
          if (!byDay.TryGetValue(d, out var list)) { list = new List<TimeOnly>(capacity: 8); byDay[d] = list; }
          list.Add(t);
        }
      }

      static int MinutesBetween(TimeOnly a, TimeOnly b)
      {
        var am = a.Hour * 60 + a.Minute;
        var bm = b.Hour * 60 + b.Minute;
        return bm - am;
      }

      int MinuteOfDay(TimeOnly t) => t.Hour * 60 + t.Minute;

      var days = new List<object>(capacity: 35);
      var presentDays = 0;
      var lateDays = 0;
      var missingOutDays = 0;
      var duplicatePunches = 0;

      var tzLocal = GetScheduleTimeZone();
      var todayLocal = DateOnly.FromDateTime(TimeZoneInfo.ConvertTime(DateTime.UtcNow, tzLocal));
      var cutoff = (todayLocal.Year == monthStart.Year && todayLocal.Month == monthStart.Month) ? todayLocal : monthEnd;
      if (cutoff > monthEnd) cutoff = monthEnd;
      if (cutoff < monthStart) cutoff = monthStart;

      var loopEnd = cutoff;
      if (monthStart.Year != todayLocal.Year || monthStart.Month != todayLocal.Month) loopEnd = monthEnd;

      var schedStartMin0 = MinuteOfDay(schedStart);
      var schedEndMin0 = MinuteOfDay(schedEnd);
      var schedStartMin = schedStartMin0;
      var schedEndMin = schedEndMin0;
      var overnight = schedEndMin0 <= schedStartMin0;
      if (overnight) schedEndMin += 1440;

      var breakStartMin = 0;
      var breakEndMin = 0;
      if (hasBreak)
      {
        breakStartMin = MinuteOfDay(breakStart);
        breakEndMin = MinuteOfDay(breakEnd);
        if (overnight && breakStartMin < schedStartMin0) breakStartMin += 1440;
        if (overnight && breakEndMin < schedStartMin0) breakEndMin += 1440;
        if (breakEndMin <= breakStartMin) breakEndMin += 1440;
      }

      int AdjustPunchMin(int m)
      {
        if (overnight && m < schedEndMin0) return m + 1440;
        return m;
      }

      static void AddSeg(List<(int s, int e, string kind)> segs, int s, int e, string kind)
      {
        if (e <= s) return;
        segs.Add((s, e, kind));
      }

      static List<(int s, int e, string kind)> MergeSegs(List<(int s, int e, string kind)> segs)
      {
        if (segs.Count <= 1) return segs;
        segs.Sort((a, b) => a.s.CompareTo(b.s));
        var res = new List<(int s, int e, string kind)>(capacity: segs.Count);
        var cur = segs[0];
        for (var i = 1; i < segs.Count; i++)
        {
          var n = segs[i];
          if (n.kind == cur.kind && n.s <= cur.e)
          {
            cur.e = Math.Max(cur.e, n.e);
            continue;
          }
          res.Add(cur);
          cur = n;
        }
        res.Add(cur);
        return res;
      }
      for (var d = monthStart; d <= loopEnd; d = d.AddDays(1))
      {
        var punches = byDay.TryGetValue(d, out var list) ? list.OrderBy(x => x).ToList() : new List<TimeOnly>();
        var kept = new List<TimeOnly>(capacity: punches.Count);
        TimeOnly? lastKept = null;
        var dup = 0;
        foreach (var t in punches)
        {
          if (lastKept is not null)
          {
            var diff = MinutesBetween(lastKept.Value, t);
            if (diff >= 0 && diff < 3) { dup++; continue; }
          }
          kept.Add(t);
          lastKept = t;
        }

        var isWorkingDay = workingDays.Contains(d.DayOfWeek);
        var isPresent = kept.Count > 0;
        if (d <= cutoff)
        {
          if (isWorkingDay)
          {
            duplicatePunches += dup;
            if (isPresent) presentDays++;
            if (isPresent && kept.Count % 2 == 1) missingOutDays++;
            if (isPresent)
            {
              var firstIn = kept[0];
              var late = MinutesBetween(schedStart, firstIn) > 15;
              if (late) lateDays++;
            }
          }
        }

        var segTriples = new List<(int s, int e, string kind)>(capacity: 12);
        for (var i = 0; i + 1 < kept.Count; i += 2)
        {
          var a = AdjustPunchMin(MinuteOfDay(kept[i]));
          var b = AdjustPunchMin(MinuteOfDay(kept[i + 1]));
          if (b <= a) b += 1440;

          AddSeg(segTriples, a, Math.Min(b, schedStartMin), "extra");

          var inS = Math.Max(a, schedStartMin);
          var inE = Math.Min(b, schedEndMin);
          if (inE > inS)
          {
            if (hasBreak)
            {
              AddSeg(segTriples, inS, Math.Min(inE, breakStartMin), "work");
              AddSeg(segTriples, Math.Max(inS, breakStartMin), Math.Min(inE, breakEndMin), "break");
              AddSeg(segTriples, Math.Max(inS, breakEndMin), inE, "work");
            }
            else
            {
              AddSeg(segTriples, inS, inE, "work");
            }
          }

          AddSeg(segTriples, Math.Max(a, schedEndMin), b, "extra");
        }
        segTriples = MergeSegs(segTriples);
        var segs = segTriples
          .Select(s => new { start = Math.Max(0, Math.Min(1440, s.s)), end = Math.Max(0, Math.Min(1440, s.e)), kind = s.kind })
          .Where(x => x.end > x.start)
          .ToArray();

        var missingOutAtMin = 0;
        if (isWorkingDay && kept.Count % 2 == 1 && kept.Count > 0)
        {
          var mm = AdjustPunchMin(MinuteOfDay(kept[^1]));
          missingOutAtMin = Math.Max(0, Math.Min(1440, mm));
        }

        var clockIn = kept.Count > 0 ? kept[0].ToString("HH:mm", CultureInfo.InvariantCulture) : "-";
        var clockOut = kept.Count > 1 ? kept[^1].ToString("HH:mm", CultureInfo.InvariantCulture) : "-";
        var durationMin = (kept.Count > 1) ? Math.Max(0, MinutesBetween(kept[0], kept[^1])) : 0;

        days.Add(new
        {
          date = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
          dow = d.DayOfWeek.ToString(),
          workingDay = isWorkingDay,
          present = isPresent,
          clockIn,
          clockOut,
          durationMin,
          punches = kept.Select(x => x.ToString("HH:mm", CultureInfo.InvariantCulture)).ToArray(),
          segments = segs,
          missingOutAtMin = missingOutAtMin
        });
      }

      var workingDaysElapsed = 0;
      for (var d = monthStart; d <= cutoff; d = d.AddDays(1)) if (workingDays.Contains(d.DayOfWeek)) workingDaysElapsed++;
      var absentDays = Math.Max(0, workingDaysElapsed - presentDays);

      return Results.Json(new
      {
        ok = true,
        staff = new { id = staffId, name = staffName, department = staffDept, shiftPattern = staffPattern },
        month = monthStart.ToString("yyyy-MM", CultureInfo.InvariantCulture),
        stats = new { presentDays, absentDays, lateDays, missingOutDays, duplicatePunches },
        schedule = new
        {
          start = schedStart.ToString("HH:mm", CultureInfo.InvariantCulture),
          end = schedEnd.ToString("HH:mm", CultureInfo.InvariantCulture),
          breakStart = hasBreak ? breakStart.ToString("HH:mm", CultureInfo.InvariantCulture) : string.Empty,
          breakEnd = hasBreak ? breakEnd.ToString("HH:mm", CultureInfo.InvariantCulture) : string.Empty,
        },
        days
      }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/staff/save", async (HttpContext ctx) =>
    {
      static string GetProp(JsonElement el, string name)
      {
        if (el.ValueKind != JsonValueKind.Object) return string.Empty;
        if (!el.TryGetProperty(name, out var v)) return string.Empty;
        return (v.GetString() ?? string.Empty).Trim();
      }

      static string? ToIsoDate(string raw)
      {
        var s = (raw ?? string.Empty).Trim();
        if (s.Length == 0) return null;
        if (DateOnly.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.None, out var d))
        {
          return d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "T00:00:00Z";
        }
        if (DateTimeOffset.TryParse(s, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
        {
          return dto.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
        }
        return s;
      }

      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      if (!doc.RootElement.TryGetProperty("rows", out var rowsEl) || rowsEl.ValueKind != JsonValueKind.Array)
      {
        return Results.Json(new { ok = false, error = "rows missing" }, JsonOptions);
      }

      var rows = new List<(string userId, string fullName, string role, string department, string status, string dateJoined, string shiftPattern)>(capacity: 256);
      foreach (var el in rowsEl.EnumerateArray())
      {
        var userId = GetProp(el, "user_id");
        if (userId.Length == 0) continue;
        rows.Add((
          userId,
          GetProp(el, "full_name"),
          GetProp(el, "role"),
          GetProp(el, "department"),
          GetProp(el, "status"),
          GetProp(el, "date_joined"),
          GetProp(el, "shift_pattern")
        ));
      }

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        return Results.Json(new { ok = false, error = "Supabase not configured. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in Database Sync settings." }, JsonOptions);
      }

      var payloadWithShift = rows.Select(r => new Dictionary<string, object?>
      {
        ["id"] = r.userId,
        ["full_name"] = r.fullName,
        ["role"] = r.role,
        ["department"] = r.department,
        ["status"] = r.status,
        ["date_joined"] = ToIsoDate(r.dateJoined),
        ["shift_pattern"] = r.shiftPattern,
      }).ToArray();

      var payloadNoShift = rows.Select(r => new Dictionary<string, object?>
      {
        ["id"] = r.userId,
        ["full_name"] = r.fullName,
        ["role"] = r.role,
        ["department"] = r.department,
        ["status"] = r.status,
        ["date_joined"] = ToIsoDate(r.dateJoined),
      }).ToArray();

      async Task<(bool ok, string? error)> UpsertAsync(object payload, CancellationToken ct)
      {
        var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
        var url = $"{baseUrl}/rest/v1/staff?on_conflict=id";
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(20) };
        using var req = new HttpRequestMessage(HttpMethod.Post, url);
        req.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
        req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
        req.Headers.TryAddWithoutValidation("Prefer", "resolution=merge-duplicates,return=minimal");
        req.Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
        using var resp = await http.SendAsync(req, ct);
        var body = await resp.Content.ReadAsStringAsync(ct);
        if (!resp.IsSuccessStatusCode) return (false, body.Length > 350 ? body[..350] : body);
        return (true, null);
      }

      var r1 = await UpsertAsync(payloadWithShift, ctx.RequestAborted);
      if (!r1.ok && r1.error is not null && r1.error.Contains("shift_pattern", StringComparison.OrdinalIgnoreCase))
      {
        var r2 = await UpsertAsync(payloadNoShift, ctx.RequestAborted);
        if (!r2.ok) return Results.Json(new { ok = false, error = r2.error ?? "Save failed" }, JsonOptions);
        return Results.Json(new { ok = true }, JsonOptions);
      }
      if (!r1.ok) return Results.Json(new { ok = false, error = r1.error ?? "Save failed" }, JsonOptions);
      return Results.Json(new { ok = true }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/device/user/create", async (HttpContext ctx) =>
    {
      var isSuperadmin = string.Equals(ctx.User.FindFirstValue("role") ?? string.Empty, "superadmin", StringComparison.Ordinal);
      if (!isSuperadmin) return Results.StatusCode(StatusCodes.Status403Forbidden);
      var trace = new List<string>(capacity: 32);

      static string GetProp(JsonElement el, string name)
      {
        if (el.ValueKind != JsonValueKind.Object) return string.Empty;
        if (!el.TryGetProperty(name, out var v)) return string.Empty;
        return (v.GetString() ?? string.Empty).Trim();
      }

      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var userIdRaw = GetProp(doc.RootElement, "user_id");
      var firstName = GetProp(doc.RootElement, "first_name");
      var fullName = GetProp(doc.RootElement, "full_name");
      var role = GetProp(doc.RootElement, "role");
      trace.Add("Received request: user_id=" + userIdRaw + " first_name=" + firstName + " full_name=" + fullName + " role=" + role);
      if (userIdRaw.Length == 0) return Results.Json(new { ok = false, error = "user_id is required", trace }, JsonOptions);

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      trace.Add("Device target: " + cfg.DeviceIp + ":" + cfg.DevicePort + " machine_number=" + cfg.MachineNumber);

      try
      {
        if (Environment.Is64BitProcess)
        {
          trace.Add("Process architecture: x64 (COM requires x86 for this SDK build).");
          return Results.Json(new
          {
            ok = false,
            error = "ZKTeco SDK is 32-bit and cannot be loaded by a 64-bit process. Run WL10Middleware as x86.",
            hint = @"If running from source: set SHAB_BUILD_ARCH=x86 then restart. If using client package: use win-x86 build.",
            trace
          }, JsonOptions);
        }

        var staffNumber = ExtractStaffNumber(cfg, userIdRaw);
        if (staffNumber is null)
        {
          return Results.Json(new { ok = false, error = "user_id must contain a valid staff number for WL10 enrollment.", trace }, JsonOptions);
        }
        var enrollNumber = staffNumber.Value.ToString(CultureInfo.InvariantCulture);
        trace.Add("Resolved enrollment_number=" + enrollNumber);

        var nameToSet = (firstName.Length > 0 ? firstName : fullName).Trim();
        if (nameToSet.Length == 0) nameToSet = userIdRaw;
        var isTargetSuperadmin = role.Length > 0 && role.Contains("superadmin", StringComparison.OrdinalIgnoreCase);
        var desiredPrivilege = isTargetSuperadmin ? 3 : 0;
        trace.Add("Target privilege: " + (isTargetSuperadmin ? "admin(3)" : "user(0)"));

        var result = await RunStaAsync<object>(() =>
        {
          var t = new List<string>(capacity: 64);
          t.AddRange(trace);
          t.Add("STA thread started.");
          t.Add("Initializing ZKTeco SDK (zkemkeeper) COM...");

          Type? type = null;
          foreach (var progId in new[] { "zkemkeeper.CZKEM", "zkemkeeper.ZKEM", "zkemkeeper.ZKEM.1" })
          {
            try
            {
              var candidate = Type.GetTypeFromProgID(progId, throwOnError: false);
              if (candidate is not null && candidate.GUID != Guid.Empty)
              {
                type = candidate;
                t.Add("Using ProgID: " + progId);
                break;
              }
              if (candidate is not null && candidate.GUID == Guid.Empty)
              {
                t.Add("ProgID has no CLSID mapping: " + progId);
              }
            }
            catch { }
          }
          if (type is null)
          {
            try
            {
              type = Type.GetTypeFromCLSID(new Guid("00853A19-BD51-419B-9269-2DABE57EB61F"), throwOnError: false);
              if (type is not null) t.Add("Using CLSID: {00853A19-BD51-419B-9269-2DABE57EB61F}");
            }
            catch { type = null; }
          }
          if (type is null)
          {
            return (object)new { ok = false, error = "ZKTeco SDK (zkemkeeper) is not installed/registered on this PC.", trace = t.ToArray() };
          }

          t.Add("Process architecture: " + (Environment.Is64BitProcess ? "x64" : "x86") + " OS: " + (Environment.Is64BitOperatingSystem ? "x64" : "x86"));

          dynamic? zk = null;
          try { zk = Activator.CreateInstance(type); }
          catch (Exception ex)
          {
            t.Add("CreateInstance exception: " + ex.GetType().FullName + " " + ex.Message);
            if (ex.HResult != 0) t.Add("CreateInstance HRESULT: 0x" + ex.HResult.ToString("X", CultureInfo.InvariantCulture));
            zk = null;
          }
          if (zk is null)
          {
            var hint = Environment.Is64BitProcess
              ? "This is usually caused by using a 32-bit ZKTeco SDK with a 64-bit process. Install/register the ZKTeco SDK and run the middleware as x86."
              : "Install/register the ZKTeco SDK (zkemkeeper.dll) on this PC.";
            return (object)new
            {
              ok = false,
              error = "Failed to initialize zkemkeeper COM object. " + hint,
              hint = @"Run as Administrator: ""SHAB Attendance System\Client Package\ZKTecoSDK\x86\Auto-install_sdk.bat""",
              trace = t.ToArray()
            };
          }

          try
          {
            t.Add("Connecting to device...");
            try { _ = zk.SetCommPassword(cfg.CommPassword); } catch { }
            var connected = false;
            try { connected = (bool)zk.Connect_Net(cfg.DeviceIp, cfg.DevicePort); } catch { connected = false; }
            if (!connected)
            {
              return (object)new { ok = false, error = $"Failed to connect to WL10 at {cfg.DeviceIp}:{cfg.DevicePort}.", device_ip = cfg.DeviceIp, device_port = cfg.DevicePort, machine_number = cfg.MachineNumber, trace = t.ToArray() };
            }
            t.Add("Connected.");

            t.Add("Checking whether user already exists...");
            try { _ = (bool)zk.ReadAllUserID(cfg.MachineNumber); } catch { }

            var exists = false;
            var existingName = "";
            var existingPassword = "";
            var existingPrivilege = 0;
            var existingEnabled = true;
            while (true)
            {
              string enrollRead;
              string nameRead;
              string password;
              int privilege;
              bool enabled;

              bool ok;
              try { ok = (bool)zk.SSR_GetAllUserInfo(cfg.MachineNumber, out enrollRead, out nameRead, out password, out privilege, out enabled); }
              catch { break; }
              if (!ok) break;
              if (string.Equals((enrollRead ?? string.Empty).Trim(), enrollNumber, StringComparison.Ordinal))
              {
                t.Add("User already exists on device.");
                exists = true;
                existingName = (nameRead ?? string.Empty).Trim();
                existingPassword = (password ?? string.Empty).Trim();
                existingPrivilege = privilege;
                existingEnabled = enabled;
                break;
              }
            }

            if (exists)
            {
              var existingPrivRaw = existingPrivilege;
              var currentPriv = Math.Clamp(existingPrivRaw, 0, 3);
              var targetPriv = isTargetSuperadmin ? 3 : currentPriv;
              var needsName = nameToSet.Length > 0 && !string.Equals(existingName, nameToSet, StringComparison.Ordinal);
              var needsPriv = targetPriv != existingPrivRaw;
              if (needsName || needsPriv)
              {
                if (needsName) t.Add("Updating user name on device: from='" + existingName + "' to='" + nameToSet + "'");
                if (needsPriv) t.Add("Updating privilege on device: from=" + currentPriv + " to=" + targetPriv);
                try { _ = zk.EnableDevice(cfg.MachineNumber, false); } catch { }

                var updated = false;
                var nameFinal = needsName ? nameToSet : existingName;
                try { updated = (bool)zk.SSR_SetUserInfo(cfg.MachineNumber, enrollNumber, nameFinal, existingPassword, targetPriv, existingEnabled); } catch { updated = false; }
                if (!updated)
                {
                  try { updated = (bool)zk.SetUserInfo(cfg.MachineNumber, int.Parse(enrollNumber, CultureInfo.InvariantCulture), nameFinal, existingPassword, targetPriv, existingEnabled); } catch { updated = false; }
                }

                try { _ = (bool)zk.RefreshData(cfg.MachineNumber); } catch { }
                try { _ = zk.EnableDevice(cfg.MachineNumber, true); } catch { }

                return (object)new
                {
                  ok = updated,
                  already_exists = true,
                  updated,
                  device_ip = cfg.DeviceIp,
                  device_port = cfg.DevicePort,
                  machine_number = cfg.MachineNumber,
                  enrollment_number = enrollNumber,
                  privilege_to = targetPriv,
                  trace = t.ToArray()
                };
              }

              return (object)new
              {
                ok = true,
                already_exists = true,
                device_ip = cfg.DeviceIp,
                device_port = cfg.DevicePort,
                machine_number = cfg.MachineNumber,
                enrollment_number = enrollNumber,
                trace = t.ToArray()
              };
            }

            t.Add("User not found. Writing user to device...");
            try { _ = zk.EnableDevice(cfg.MachineNumber, false); } catch { }

            var created = false;
            try { created = (bool)zk.SSR_SetUserInfo(cfg.MachineNumber, enrollNumber, nameToSet, "", desiredPrivilege, true); } catch { created = false; }
            if (!created)
            {
              try { created = (bool)zk.SetUserInfo(cfg.MachineNumber, int.Parse(enrollNumber, CultureInfo.InvariantCulture), nameToSet, "", desiredPrivilege, true); } catch { created = false; }
            }
            if (!created)
            {
              var lastErr = 0;
              try { _ = (bool)zk.GetLastError(out lastErr); } catch { lastErr = 0; }
              try { _ = zk.EnableDevice(cfg.MachineNumber, true); } catch { }
              t.Add("Create failed. last_error=" + lastErr);
              return (object)new { ok = false, error = "Device rejected user create request.", last_error = lastErr, device_ip = cfg.DeviceIp, device_port = cfg.DevicePort, machine_number = cfg.MachineNumber, trace = t.ToArray() };
            }

            t.Add("Create succeeded. Refreshing device data...");
            try { _ = (bool)zk.RefreshData(cfg.MachineNumber); } catch { }
            try { _ = zk.EnableDevice(cfg.MachineNumber, true); } catch { }

            t.Add("Verifying user exists after write...");
            var verified = false;
            try
            {
              string vName;
              string vPwd;
              int vPriv;
              bool vEnabled;
              verified = (bool)zk.SSR_GetUserInfo(cfg.MachineNumber, enrollNumber, out vName, out vPwd, out vPriv, out vEnabled);
            }
            catch
            {
              verified = false;
            }

            if (!verified)
            {
              try { _ = (bool)zk.ReadAllUserID(cfg.MachineNumber); } catch { }
              while (true)
              {
                string enrollNumberRead;
                string name;
                string password;
                int privilege;
                bool enabled;

                bool ok;
                try { ok = (bool)zk.SSR_GetAllUserInfo(cfg.MachineNumber, out enrollNumberRead, out name, out password, out privilege, out enabled); }
                catch { break; }
                if (!ok) break;
                if (string.Equals((enrollNumberRead ?? string.Empty).Trim(), enrollNumber, StringComparison.Ordinal))
                {
                  verified = true;
                  break;
                }
              }
            }

            t.Add(verified ? "Verified on device." : "Could not verify on device.");
            return (object)new { ok = true, created = true, verified, device_ip = cfg.DeviceIp, device_port = cfg.DevicePort, machine_number = cfg.MachineNumber, enrollment_number = enrollNumber, trace = t.ToArray() };
          }
          finally
          {
            try { zk.Disconnect(); } catch { }
          }
        }, ctx.RequestAborted);

        return Results.Json(result, JsonOptions);
      }
      catch (Exception ex)
      {
        trace.Add("Exception: " + ex.GetType().FullName + " " + ex.Message);
        return Results.Json(new { ok = false, error = ex.Message, trace }, JsonOptions);
      }
    }).RequireAuthorization();

    app.MapPost("/api/device/user/grant-admin", async (HttpContext ctx) =>
    {
      var isSuperadmin = string.Equals(ctx.User.FindFirstValue("role") ?? string.Empty, "superadmin", StringComparison.Ordinal);
      if (!isSuperadmin) return Results.StatusCode(StatusCodes.Status403Forbidden);
      var trace = new List<string>(capacity: 32);

      static string GetProp(JsonElement el, string name)
      {
        if (el.ValueKind != JsonValueKind.Object) return string.Empty;
        if (!el.TryGetProperty(name, out var v)) return string.Empty;
        return (v.GetString() ?? string.Empty).Trim();
      }

      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var userIdRaw = GetProp(doc.RootElement, "user_id");
      trace.Add("Received request: user_id=" + userIdRaw);
      if (userIdRaw.Length == 0) return Results.Json(new { ok = false, error = "user_id is required", trace }, JsonOptions);

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      trace.Add("Device target: " + cfg.DeviceIp + ":" + cfg.DevicePort + " machine_number=" + cfg.MachineNumber);

      try
      {
        if (Environment.Is64BitProcess)
        {
          trace.Add("Process architecture: x64 (COM requires x86 for this SDK build).");
          return Results.Json(new
          {
            ok = false,
            error = "ZKTeco SDK is 32-bit and cannot be loaded by a 64-bit process. Run WL10Middleware as x86.",
            hint = @"If running from source: set SHAB_BUILD_ARCH=x86 then restart. If using client package: use win-x86 build.",
            trace
          }, JsonOptions);
        }

        var staffNumber = ExtractStaffNumber(cfg, userIdRaw);
        if (staffNumber is null)
        {
          return Results.Json(new { ok = false, error = "user_id must contain a valid staff number for WL10 enrollment.", trace }, JsonOptions);
        }
        var enrollNumber = staffNumber.Value.ToString(CultureInfo.InvariantCulture);
        trace.Add("Resolved enrollment_number=" + enrollNumber);

        var result = await RunStaAsync<object>(() =>
        {
          var t = new List<string>(capacity: 64);
          t.AddRange(trace);
          t.Add("STA thread started.");
          t.Add("Initializing ZKTeco SDK (zkemkeeper) COM...");

          Type? type = null;
          foreach (var progId in new[] { "zkemkeeper.CZKEM", "zkemkeeper.ZKEM", "zkemkeeper.ZKEM.1" })
          {
            try
            {
              var candidate = Type.GetTypeFromProgID(progId, throwOnError: false);
              if (candidate is not null && candidate.GUID != Guid.Empty)
              {
                type = candidate;
                t.Add("Using ProgID: " + progId);
                break;
              }
            }
            catch { }
          }
          if (type is null)
          {
            try
            {
              type = Type.GetTypeFromCLSID(new Guid("00853A19-BD51-419B-9269-2DABE57EB61F"), throwOnError: false);
              if (type is not null) t.Add("Using CLSID: {00853A19-BD51-419B-9269-2DABE57EB61F}");
            }
            catch { type = null; }
          }
          if (type is null)
          {
            return (object)new { ok = false, error = "ZKTeco SDK (zkemkeeper) is not installed/registered on this PC.", trace = t.ToArray() };
          }

          dynamic? zk = null;
          try { zk = Activator.CreateInstance(type); } catch { zk = null; }
          if (zk is null)
          {
            return (object)new
            {
              ok = false,
              error = "Failed to initialize zkemkeeper COM object.",
              hint = @"Run as Administrator: ""SHAB Attendance System\Client Package\ZKTecoSDK\x86\Auto-install_sdk.bat""",
              trace = t.ToArray()
            };
          }

          try
          {
            t.Add("Connecting to device...");
            try { _ = zk.SetCommPassword(cfg.CommPassword); } catch { }
            var connected = false;
            try { connected = (bool)zk.Connect_Net(cfg.DeviceIp, cfg.DevicePort); } catch { connected = false; }
            if (!connected)
            {
              return (object)new { ok = false, error = $"Failed to connect to WL10 at {cfg.DeviceIp}:{cfg.DevicePort}.", trace = t.ToArray() };
            }
            t.Add("Connected.");

            t.Add("Searching user on device...");
            try { _ = (bool)zk.ReadAllUserID(cfg.MachineNumber); } catch { }

            var found = false;
            var name = "";
            var password = "";
            var privilege = 0;
            var enabled = true;

            while (true)
            {
              string enrollRead;
              string nameRead;
              string passwordRead;
              int privilegeRead;
              bool enabledRead;

              bool ok;
              try { ok = (bool)zk.SSR_GetAllUserInfo(cfg.MachineNumber, out enrollRead, out nameRead, out passwordRead, out privilegeRead, out enabledRead); }
              catch { break; }
              if (!ok) break;
              if (!string.Equals((enrollRead ?? string.Empty).Trim(), enrollNumber, StringComparison.Ordinal)) continue;

              found = true;
              name = (nameRead ?? string.Empty).Trim();
              password = (passwordRead ?? string.Empty).Trim();
              privilege = privilegeRead;
              enabled = enabledRead;
              break;
            }

            if (!found)
            {
              return (object)new { ok = false, error = "User not found on device. Create the user first, then grant admin.", enrollment_number = enrollNumber, trace = t.ToArray() };
            }

            const int adminPrivilege = 3;
            if (privilege == adminPrivilege)
            {
              return (object)new { ok = true, already_admin = true, enrollment_number = enrollNumber, privilege_from = privilege, privilege_to = adminPrivilege, trace = t.ToArray() };
            }

            t.Add("Granting admin privilege...");
            try { _ = zk.EnableDevice(cfg.MachineNumber, false); } catch { }

            var updated = false;
            try { updated = (bool)zk.SSR_SetUserInfo(cfg.MachineNumber, enrollNumber, name, password, adminPrivilege, enabled); } catch { updated = false; }
            if (!updated)
            {
              try { updated = (bool)zk.SetUserInfo(cfg.MachineNumber, int.Parse(enrollNumber, CultureInfo.InvariantCulture), name, password, adminPrivilege, enabled); } catch { updated = false; }
            }

            try { _ = (bool)zk.RefreshData(cfg.MachineNumber); } catch { }
            try { _ = zk.EnableDevice(cfg.MachineNumber, true); } catch { }

            return (object)new
            {
              ok = updated,
              updated,
              enrollment_number = enrollNumber,
              privilege_from = privilege,
              privilege_to = adminPrivilege,
              trace = t.ToArray()
            };
          }
          finally
          {
            try { zk.Disconnect(); } catch { }
          }
        }, ctx.RequestAborted);

        return Results.Json(result, JsonOptions);
      }
      catch (Exception ex)
      {
        trace.Add("Exception: " + ex.GetType().FullName + " " + ex.Message);
        return Results.Json(new { ok = false, error = ex.Message, trace }, JsonOptions);
      }
    }).RequireAuthorization();

    app.MapGet("/api/shifts", (HttpContext ctx) =>
    {
      AppConfig cfg;
      string anonKey;
      lock (stateGate)
      {
        cfg = currentConfig;
        anonKey = dashboardSupabaseAnonKey;
      }
      var serviceKeyRaw = (cfg.SupabaseServiceRoleKey ?? string.Empty).Trim();
      var anonRaw = (anonKey ?? string.Empty).Trim();

      static ShiftPatternRow[] DefaultRows()
      {
        return new[]
        {
          new ShiftPatternRow("Normal", "Mon–Fri", "09:00–18:00", "13:00–14:00", "Default"),
          new ShiftPatternRow("Shift 1", "Mon–Sat", "08:00–16:00", "12:00–13:00", "Default"),
          new ShiftPatternRow("Shift 2", "Mon–Sat", "16:00–00:00", "20:00–20:30", "Default"),
          new ShiftPatternRow("Shift 3", "Mon–Sat", "00:00–08:00", "04:00–04:30", "Default"),
        };
      }

      if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || (serviceKeyRaw.Length == 0 && anonRaw.Length == 0))
      {
        var state = LoadState(statePath);
        var rows = (state.ShiftPatterns ?? Array.Empty<ShiftPatternRow>()).ToArray();
        if (rows.Length == 0) rows = DefaultRows();
        return Results.Json(new { rows }, JsonOptions);
      }

      try
      {
        var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
        var url = $"{baseUrl}/rest/v1/shift_patterns?select=pattern,working_days,working_hours,break_time,notes&order=pattern.asc&limit=500";
        static (bool ok, string body, int statusCode) Fetch(string url, string apiKey)
        {
          using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
          using var req = new HttpRequestMessage(HttpMethod.Get, url);
          req.Headers.TryAddWithoutValidation("apikey", apiKey);
          req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
          using var resp = http.Send(req);
          var body = resp.Content.ReadAsStringAsync().GetAwaiter().GetResult();
          return (resp.IsSuccessStatusCode, body, (int)resp.StatusCode);
        }

        var apiKey = serviceKeyRaw.Length > 0 ? serviceKeyRaw : anonRaw;
        var r1 = Fetch(url, apiKey);
        if (!r1.ok && r1.statusCode == 401 && apiKey == serviceKeyRaw && anonRaw.Length > 0)
        {
          r1 = Fetch(url, anonRaw);
        }
        if (!r1.ok) throw new InvalidOperationException(r1.body);

        using var doc = JsonDocument.Parse(r1.body);
        if (doc.RootElement.ValueKind != JsonValueKind.Array) throw new InvalidOperationException("Unexpected response shape");
        var list = new List<ShiftPatternRow>(capacity: 64);
        foreach (var el in doc.RootElement.EnumerateArray())
        {
          var pattern = el.TryGetProperty("pattern", out var pEl) && pEl.ValueKind == JsonValueKind.String ? (pEl.GetString() ?? "") : "";
          if (pattern.Length == 0) continue;
          var wd = el.TryGetProperty("working_days", out var wdEl) && wdEl.ValueKind == JsonValueKind.String ? (wdEl.GetString() ?? "") : "";
          var wh = el.TryGetProperty("working_hours", out var whEl) && whEl.ValueKind == JsonValueKind.String ? (whEl.GetString() ?? "") : "";
          var br = el.TryGetProperty("break_time", out var bEl) && bEl.ValueKind == JsonValueKind.String ? (bEl.GetString() ?? "") : "";
          var notes = el.TryGetProperty("notes", out var nEl) && nEl.ValueKind == JsonValueKind.String ? (nEl.GetString() ?? "") : "";
          list.Add(new ShiftPatternRow(pattern, wd, wh, br, notes));
        }
        var rows = list.Count > 0 ? list.ToArray() : DefaultRows();
        return Results.Json(new { rows }, JsonOptions);
      }
      catch
      {
        var state = LoadState(statePath);
        var rows = (state.ShiftPatterns ?? Array.Empty<ShiftPatternRow>()).ToArray();
        if (rows.Length == 0) rows = DefaultRows();
        return Results.Json(new { rows }, JsonOptions);
      }
    }).RequireAuthorization();

    app.MapPost("/api/shifts/save", async (HttpContext ctx) =>
    {
      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      if (!doc.RootElement.TryGetProperty("rows", out var rowsEl) || rowsEl.ValueKind != JsonValueKind.Array)
      {
        return Results.Json(new { ok = false, error = "rows missing" }, JsonOptions);
      }

      static string GetProp(JsonElement el, string name)
      {
        if (el.ValueKind != JsonValueKind.Object) return string.Empty;
        if (!el.TryGetProperty(name, out var v)) return string.Empty;
        return (v.GetString() ?? string.Empty).Trim();
      }

      var rows = new List<ShiftPatternRow>(capacity: 64);
      foreach (var el in rowsEl.EnumerateArray())
      {
        var pattern = GetProp(el, "pattern");
        if (pattern.Length == 0) continue;
        rows.Add(new ShiftPatternRow(
          pattern,
          GetProp(el, "workingDays"),
          GetProp(el, "workingHours"),
          GetProp(el, "break"),
          GetProp(el, "notes")
        ));
      }

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        try
        {
          var payload = rows.Select(r => new Dictionary<string, object?>
          {
            ["pattern"] = r.Pattern,
            ["working_days"] = r.WorkingDays,
            ["working_hours"] = r.WorkingHours,
            ["break_time"] = r.Break,
            ["notes"] = r.Notes,
          }).ToArray();
          var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
          var url = $"{baseUrl}/rest/v1/shift_patterns?on_conflict=pattern";
          using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(20) };
          using var req = new HttpRequestMessage(HttpMethod.Post, url);
          req.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
          req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
          req.Headers.TryAddWithoutValidation("Prefer", "resolution=merge-duplicates,return=minimal");
          req.Content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");
          using var resp = await http.SendAsync(req, ctx.RequestAborted);
          var body = await resp.Content.ReadAsStringAsync(ctx.RequestAborted);
          if (!resp.IsSuccessStatusCode)
          {
            return Results.Json(new { ok = false, error = body.Length > 350 ? body[..350] : body }, JsonOptions);
          }
          return Results.Json(new { ok = true }, JsonOptions);
        }
        catch (Exception ex)
        {
          return Results.Json(new { ok = false, error = ex.Message }, JsonOptions);
        }
      }

      lock (stateGate)
      {
        var state = LoadState(statePath);
        SaveState(statePath, state with { ShiftPatterns = rows.ToArray() });
      }

      return Results.Json(new { ok = true }, JsonOptions);
    }).RequireAuthorization();

    async Task<(bool ok, string? error)> ExecuteSupabaseOnlyUpdateFromAttlog(CancellationToken ct)
    {
      if (!await syncGate.WaitAsync(0, ct)) return (false, "sync already running");
      try
      {
        AppConfig cfg;
        lock (stateGate) cfg = currentConfig;

        var configured = !string.IsNullOrWhiteSpace(cfg.SupabaseUrl)
          && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey)
          && !string.IsNullOrWhiteSpace(cfg.SupabaseAttendanceTable);
        var enabled = cfg.SupabaseSyncEnabled && configured;
        if (!enabled) return (false, "supabase not configured or disabled");

        runtime.LastSyncStartedAtUtc = DateTimeOffset.UtcNow;
        runtime.LastSyncFinishedAtUtc = null;
        runtime.LastSyncError = null;
        runtime.LastSyncResult = "running";

        runtime.LastSupabaseSyncStartedAtUtc = runtime.LastSyncStartedAtUtc;
        runtime.LastSupabaseSyncFinishedAtUtc = null;
        runtime.LastSupabaseSyncError = null;
        runtime.LastSupabaseSyncResult = "running";
        runtime.LastSupabaseUpsertedCount = 0;

        try
        {
          var path = TryResolveAttlogExportPath();
          if (string.IsNullOrWhiteSpace(path)) throw new InvalidOperationException("Could not resolve 1_attlog.dat path");
          if (!File.Exists(path)) throw new InvalidOperationException($"File not found: {path}");

          var rows = ReadAttlogRows(path);
          var keys = new HashSet<string>(StringComparer.Ordinal);
          var distinctRows = new List<AttlogRow>(capacity: rows.Count);
          for (var i = 0; i < rows.Count; i++)
          {
            var r = rows[i];
            var k = r.StaffId + "|" + r.DateTime;
            if (!keys.Add(k)) continue;
            distinctRows.Add(r);
          }

          await UpsertAttlogRowsToSupabase(cfg, distinctRows, ct);
          runtime.LastSupabaseSyncResult = "ok";
          runtime.LastSupabaseUpsertedCount = keys.Count;
          runtime.LastSyncResult = "ok";
          return (true, null);
        }
        catch (Exception ex)
        {
          runtime.LastSyncResult = "error";
          runtime.LastSyncError = ex.ToString();
          runtime.LastSupabaseSyncResult = "error";
          runtime.LastSupabaseSyncError = ex.ToString();
          return (false, ex.Message);
        }
        finally
        {
          runtime.LastSyncFinishedAtUtc = DateTimeOffset.UtcNow;
          runtime.LastSupabaseSyncFinishedAtUtc = runtime.LastSyncFinishedAtUtc;
        }
      }
      finally
      {
        syncGate.Release();
      }
    }

    async Task ExecuteTargetedAutoSync(CancellationToken ct)
    {
      try
      {
        AppConfig cfg;
        lock (stateGate) cfg = currentConfig;

        var supabaseConfigured = !string.IsNullOrWhiteSpace(cfg.SupabaseUrl)
          && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey)
          && !string.IsNullOrWhiteSpace(cfg.SupabaseAttendanceTable);
        var supaOk = cfg.SupabaseSyncEnabled && supabaseConfigured;

        var (devOk, _, _) = await ProbeTcpAsync(cfg.DeviceIp, cfg.DevicePort, TimeSpan.FromSeconds(2), ct);
        if (devOk)
        {
          _ = ExecuteSync(verify: false, today: false, supabaseOverride: supaOk ? (bool?)null : false, ct);
          return;
        }

        if (supaOk)
        {
          _ = await ExecuteSupabaseOnlyUpdateFromAttlog(ct);
        }
      }
      catch { }
    }

    var poller = Task.Run(async () =>
    {
      using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
      var tz = GetScheduleTimeZone();
      DateTimeOffset? nextUtc = null;
      var lastSeenVersion = -1;

      while (await timer.WaitForNextTickAsync(app.Lifetime.ApplicationStopping))
      {
        int interval;
        bool auto;
        string[] sched;
        int version;
        lock (stateGate)
        {
          interval = pollIntervalSeconds;
          auto = autoSyncEnabled;
          sched = syncScheduleLocalTimes;
          version = syncScheduleVersion;
        }

        var nowUtc = DateTimeOffset.UtcNow;
        if (!auto)
        {
          nextUtc = null;
          runtime.NextSyncAtUtc = null;
          lastSeenVersion = version;
          continue;
        }

        var hasSchedule = sched is not null && sched.Length > 0;
        if (hasSchedule)
        {
          if (nextUtc is null || version != lastSeenVersion)
          {
            nextUtc = ComputeNextScheduledSyncUtc(sched ?? Array.Empty<string>(), nowUtc, tz);
            runtime.NextSyncAtUtc = nextUtc;
            lastSeenVersion = version;
          }
        }
        else
        {
          if (nextUtc is null || version != lastSeenVersion)
          {
            nextUtc = nowUtc.AddSeconds(Math.Clamp(interval, 60, 3600));
            runtime.NextSyncAtUtc = nextUtc;
            lastSeenVersion = version;
          }
        }

        if (nextUtc is null)
        {
          runtime.NextSyncAtUtc = null;
          continue;
        }

        if (nowUtc >= nextUtc.Value)
        {
          _ = ExecuteTargetedAutoSync(app.Lifetime.ApplicationStopping);
          if (hasSchedule)
          {
            nextUtc = ComputeNextScheduledSyncUtc(sched ?? Array.Empty<string>(), nowUtc.AddSeconds(1), tz);
          }
          else
          {
            nextUtc = nowUtc.AddSeconds(Math.Clamp(interval, 60, 3600));
          }
          runtime.NextSyncAtUtc = nextUtc;
        }
      }
    });

    Console.WriteLine($"Dashboard running at {urls}");
    Console.WriteLine("Dashboard login configured. Override credentials with WL10_DASHBOARD_USER and WL10_DASHBOARD_PASSWORD.");

    await app.RunAsync();
    await poller;
  }

  private static async Task<(bool ok, string? error, long? rttMs)> ProbeTcpAsync(string ip, int port, TimeSpan timeout, CancellationToken ct)
  {
    var sw = Stopwatch.StartNew();
    try
    {
      using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      cts.CancelAfter(timeout);
      using var client = new TcpClient();
      await client.ConnectAsync(ip, port, cts.Token);
      sw.Stop();
      return (true, null, sw.ElapsedMilliseconds);
    }
    catch (OperationCanceledException)
    {
      return (false, "timeout", null);
    }
    catch (Exception ex)
    {
      return (false, ex.Message, null);
    }
  }

  private static object GetPcNetworkInfo(string deviceIp)
  {
    var addrs = new List<(string name, System.Net.IPAddress ip)>();
    try
    {
      foreach (var ni in System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces())
      {
        if (ni.OperationalStatus != System.Net.NetworkInformation.OperationalStatus.Up) continue;
        if (ni.NetworkInterfaceType == System.Net.NetworkInformation.NetworkInterfaceType.Loopback) continue;
        if (ni.NetworkInterfaceType == System.Net.NetworkInformation.NetworkInterfaceType.Tunnel) continue;

        var props = ni.GetIPProperties();
        foreach (var ua in props.UnicastAddresses)
        {
          if (ua.Address.AddressFamily != System.Net.Sockets.AddressFamily.InterNetwork) continue;
          var ip = ua.Address;
          if (System.Net.IPAddress.IsLoopback(ip)) continue;
          addrs.Add((ni.Name, ip));
        }
      }
    }
    catch { }

    var labels = addrs.Select(x => $"{x.name}:{x.ip}").Distinct().Take(6).ToArray();
    var best = addrs.Count > 0 ? addrs[0] : default;
    var bestIp = addrs.Count > 0 ? best.ip.ToString() : string.Empty;
    var bestLabel = addrs.Count > 0 ? $"{best.name}:{best.ip}" : string.Empty;
    var bestInterfaceName = addrs.Count > 0 ? best.name : string.Empty;

    bool? same24 = null;
    try
    {
      if (System.Net.IPAddress.TryParse(deviceIp, out var dev) && dev.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork && addrs.Count > 0)
      {
        var d = dev.GetAddressBytes();
        same24 = addrs.Any(p =>
        {
          var b = p.ip.GetAddressBytes();
          return b.Length == 4 && d.Length == 4 && b[0] == d[0] && b[1] == d[1] && b[2] == d[2];
        });
      }
    }
    catch { }

    return new
    {
      ipv4 = labels,
      sameSubnet24 = same24,
      bestIpv4 = bestIp,
      bestLabel,
      bestInterfaceName,
    };
  }

  private static DateTimeOffset? TryReadLocalWatermarkUtc(string statePath)
  {
    try
    {
      if (!File.Exists(statePath)) return null;
      var json = File.ReadAllText(statePath);
      var state = JsonSerializer.Deserialize<PollState>(json, JsonOptions);
      return state?.LastSeenOccurredAtUtc;
    }
    catch
    {
      return null;
    }
  }

  private static async Task<DateTimeOffset?> TryGetDbWatermarkUtc(AppConfig config, CancellationToken ct)
  {
    if (!config.SupabaseSyncEnabled) return null;
    if (string.IsNullOrWhiteSpace(config.SupabaseUrl) || string.IsNullOrWhiteSpace(config.SupabaseServiceRoleKey) || string.IsNullOrWhiteSpace(config.SupabaseAttendanceTable)) return null;

    try
    {
      var baseUrl = config.SupabaseUrl.TrimEnd('/');
      var url = $"{baseUrl}/rest/v1/{config.SupabaseAttendanceTable}?select=occurred_at&order=occurred_at.desc&limit=1";

      using var http = new HttpClient();
      using var req = new HttpRequestMessage(HttpMethod.Get, url);
      req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");
      req.Headers.TryAddWithoutValidation("Accept", "application/json");

      using var res = await http.SendAsync(req, ct);
      if (!res.IsSuccessStatusCode) return null;
      var text = await res.Content.ReadAsStringAsync(ct);
      using var doc = JsonDocument.Parse(text);
      if (doc.RootElement.ValueKind != JsonValueKind.Array || doc.RootElement.GetArrayLength() == 0) return null;
      var first = doc.RootElement[0];
      if (!first.TryGetProperty("occurred_at", out var dtEl)) return null;
      var s = dtEl.GetString();
      if (string.IsNullOrWhiteSpace(s)) return null;
      if (!DateTimeOffset.TryParse(s.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto)) return null;
      return dto.ToUniversalTime();
    }
    catch
    {
      return null;
    }
  }

  private static async Task<object[]> TryFetchLatestAttendanceEvents(AppConfig config, int limit, CancellationToken ct)
  {
    if (string.IsNullOrWhiteSpace(config.SupabaseUrl) || string.IsNullOrWhiteSpace(config.SupabaseServiceRoleKey) || string.IsNullOrWhiteSpace(config.SupabaseAttendanceTable)) return Array.Empty<object>();

    try
    {
      var baseUrl = config.SupabaseUrl.TrimEnd('/');
      var select = Uri.EscapeDataString("staff_id,device_id,datetime,occurred_at,event_date,verified,status,workcode,reserved,created_at");
      var url = $"{baseUrl}/rest/v1/{config.SupabaseAttendanceTable}?select={select}&order=occurred_at.desc&limit={Math.Clamp(limit, 1, 200)}";

      using var http = new HttpClient();
      using var req = new HttpRequestMessage(HttpMethod.Get, url);
      req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");
      req.Headers.TryAddWithoutValidation("Accept", "application/json");

      using var res = await http.SendAsync(req, ct);
      if (!res.IsSuccessStatusCode) return Array.Empty<object>();
      var text = await res.Content.ReadAsStringAsync(ct);
      using var doc = JsonDocument.Parse(text);
      if (doc.RootElement.ValueKind != JsonValueKind.Array) return Array.Empty<object>();
      var list = new List<object>(capacity: doc.RootElement.GetArrayLength());
      foreach (var el in doc.RootElement.EnumerateArray())
      {
        var staffId = el.TryGetProperty("staff_id", out var sidEl) && sidEl.ValueKind == JsonValueKind.String ? (sidEl.GetString() ?? "") : "";
        var dtText = el.TryGetProperty("datetime", out var dtEl) && dtEl.ValueKind == JsonValueKind.String ? (dtEl.GetString() ?? "") : "";
        var occurredAt = el.TryGetProperty("occurred_at", out var oaEl) && oaEl.ValueKind == JsonValueKind.String ? (oaEl.GetString() ?? "") : "";
        var dtOut = dtText.Trim();
        if (dtOut.Length == 0)
        {
          dtOut = occurredAt;
          if (DateTimeOffset.TryParse(occurredAt, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
          {
            dtOut = dto.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
          }
        }
        var verified = el.TryGetProperty("verified", out var vEl) && vEl.ValueKind == JsonValueKind.String ? (vEl.GetString() ?? "") : "";
        var status = el.TryGetProperty("status", out var stEl) && stEl.ValueKind == JsonValueKind.String ? (stEl.GetString() ?? "") : "";
        var workcode = el.TryGetProperty("workcode", out var wcEl) && wcEl.ValueKind == JsonValueKind.String ? (wcEl.GetString() ?? "") : "";
        var reserved = el.TryGetProperty("reserved", out var rsEl) && rsEl.ValueKind == JsonValueKind.String ? (rsEl.GetString() ?? "") : "";
        list.Add(new { staff_id = staffId, datetime = dtOut, verified, status, workcode, reserved });
      }
      return list.ToArray();
    }
    catch
    {
      return Array.Empty<object>();
    }
  }

  private static async Task<(bool Ok, object[] Rows, string? Error)> TryFetchLatestAttendanceEventsVerbose(AppConfig config, string? anonKey, int limit, string? from, string? to, string? deviceIdFilter, CancellationToken ct)
  {
    var baseUrlRaw = (config.SupabaseUrl ?? string.Empty).Trim();
    var tableRaw = (config.SupabaseAttendanceTable ?? string.Empty).Trim();
    var serviceKeyRaw = (config.SupabaseServiceRoleKey ?? string.Empty).Trim();
    var anonRaw = (anonKey ?? string.Empty).Trim();
    var apiKey = serviceKeyRaw.Length > 0 ? serviceKeyRaw : anonRaw;
    if (baseUrlRaw.Length == 0 || tableRaw.Length == 0 || apiKey.Length == 0)
    {
      return (false, Array.Empty<object>(), "Supabase not configured");
    }

    try
    {
      var baseUrl = baseUrlRaw.TrimEnd('/');
      var select = Uri.EscapeDataString("staff_id,device_id,datetime,occurred_at,event_date,verified,status,workcode,reserved,created_at");
      var url = $"{baseUrl}/rest/v1/{tableRaw}?select={select}&order=occurred_at.desc&limit={Math.Clamp(limit, 1, 200)}";
      if (!string.IsNullOrWhiteSpace(from)) url += "&occurred_at=gte." + Uri.EscapeDataString(from.Trim());
      if (!string.IsNullOrWhiteSpace(to)) url += "&occurred_at=lte." + Uri.EscapeDataString(to.Trim());
      if (!string.IsNullOrWhiteSpace(deviceIdFilter)) url += "&device_id=eq." + Uri.EscapeDataString(deviceIdFilter.Trim());

      static async Task<(bool ok, object[] rows, string? error, int statusCode)> FetchAsync(string url, string apiKey, CancellationToken ct)
      {
        using var http = new HttpClient();
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.TryAddWithoutValidation("apikey", apiKey);
        req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
        req.Headers.TryAddWithoutValidation("Accept", "application/json");

        using var res = await http.SendAsync(req, ct);
        if (!res.IsSuccessStatusCode)
        {
          var body = await res.Content.ReadAsStringAsync(ct);
          var msg = $"Supabase query failed: {(int)res.StatusCode} {res.ReasonPhrase}. Body={body}";
          return (false, Array.Empty<object>(), msg, (int)res.StatusCode);
        }

        var text = await res.Content.ReadAsStringAsync(ct);
        using var doc = JsonDocument.Parse(text);
        if (doc.RootElement.ValueKind != JsonValueKind.Array) return (false, Array.Empty<object>(), "Supabase returned non-array JSON", 200);

        var list = new List<object>(capacity: doc.RootElement.GetArrayLength());
        foreach (var el in doc.RootElement.EnumerateArray())
        {
          var staffId = el.TryGetProperty("staff_id", out var sidEl) && sidEl.ValueKind == JsonValueKind.String ? (sidEl.GetString() ?? "") : "";
          var dtText = el.TryGetProperty("datetime", out var dtEl) && dtEl.ValueKind == JsonValueKind.String ? (dtEl.GetString() ?? "") : "";
          var occurredAt = el.TryGetProperty("occurred_at", out var oaEl) && oaEl.ValueKind == JsonValueKind.String ? (oaEl.GetString() ?? "") : "";
          var dtOut = dtText.Trim();
          if (dtOut.Length == 0)
          {
            dtOut = occurredAt;
            if (DateTimeOffset.TryParse(occurredAt, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto))
            {
              dtOut = dto.ToLocalTime().ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            }
          }
          var verified = el.TryGetProperty("verified", out var vEl) && vEl.ValueKind == JsonValueKind.String ? (vEl.GetString() ?? "") : "";
          var status = el.TryGetProperty("status", out var stEl) && stEl.ValueKind == JsonValueKind.String ? (stEl.GetString() ?? "") : "";
          var workcode = el.TryGetProperty("workcode", out var wcEl) && wcEl.ValueKind == JsonValueKind.String ? (wcEl.GetString() ?? "") : "";
          var reserved = el.TryGetProperty("reserved", out var rsEl) && rsEl.ValueKind == JsonValueKind.String ? (rsEl.GetString() ?? "") : "";
          list.Add(new { staff_id = staffId, datetime = dtOut, verified, status, workcode, reserved });
        }

        return (true, list.ToArray(), null, 200);
      }

      var r1 = await FetchAsync(url, apiKey, ct);
      if (!r1.ok && r1.statusCode == 401 && serviceKeyRaw.Length > 0 && anonRaw.Length > 0)
      {
        var r2 = await FetchAsync(url, anonRaw, ct);
        return (r2.ok, r2.rows, r2.error);
      }
      return (r1.ok, r1.rows, r1.error);
    }
    catch (Exception ex)
    {
      return (false, Array.Empty<object>(), ex.Message);
    }
  }

  private static async Task<(bool Ok, int Count, string? Error)> TryGetSupabaseTotalCount(AppConfig config, string? anonKey, CancellationToken ct)
  {
    var baseUrlRaw = (config.SupabaseUrl ?? string.Empty).Trim();
    var tableRaw = (config.SupabaseAttendanceTable ?? string.Empty).Trim();
    var serviceKeyRaw = (config.SupabaseServiceRoleKey ?? string.Empty).Trim();
    var anonRaw = (anonKey ?? string.Empty).Trim();
    var apiKey = serviceKeyRaw.Length > 0 ? serviceKeyRaw : anonRaw;
    if (baseUrlRaw.Length == 0 || tableRaw.Length == 0 || apiKey.Length == 0)
    {
      return (false, 0, "Supabase not configured");
    }

    try
    {
      var baseUrl = baseUrlRaw.TrimEnd('/');
      var url = $"{baseUrl}/rest/v1/{tableRaw}?select=staff_id";

      static async Task<(bool ok, HttpResponseMessage res, string body)> SendAsync(string url, string apiKey, CancellationToken ct)
      {
        var http = new HttpClient();
        var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.TryAddWithoutValidation("apikey", apiKey);
        req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
        req.Headers.TryAddWithoutValidation("Accept", "application/json");
        req.Headers.TryAddWithoutValidation("Prefer", "count=exact");
        req.Headers.TryAddWithoutValidation("Range-Unit", "items");
        req.Headers.TryAddWithoutValidation("Range", "0-0");
        var res = await http.SendAsync(req, ct);
        var body = await res.Content.ReadAsStringAsync(ct);
        req.Dispose();
        http.Dispose();
        return (res.IsSuccessStatusCode, res, body);
      }

      var (ok1, res1, body1) = await SendAsync(url, apiKey, ct);
      if (!ok1 && (int)res1.StatusCode == 401 && serviceKeyRaw.Length > 0 && anonRaw.Length > 0)
      {
        res1.Dispose();
        (ok1, res1, body1) = await SendAsync(url, anonRaw, ct);
      }
      using var res = res1;
      if (!ok1)
      {
        return (false, 0, $"Supabase count failed: {(int)res.StatusCode} {res.ReasonPhrase}. Body={body1}");
      }

      string? contentRange = null;
      if (res.Headers.TryGetValues("Content-Range", out var h1)) contentRange = h1.FirstOrDefault();
      if (contentRange is null && res.Content.Headers.TryGetValues("Content-Range", out var h2)) contentRange = h2.FirstOrDefault();
      if (!string.IsNullOrWhiteSpace(contentRange))
      {
        var idx = contentRange.LastIndexOf("/", StringComparison.Ordinal);
        if (idx >= 0 && idx + 1 < contentRange.Length)
        {
          var tail = contentRange[(idx + 1)..].Trim();
          if (int.TryParse(tail, NumberStyles.Integer, CultureInfo.InvariantCulture, out var total))
          {
            return (true, Math.Max(0, total), null);
          }
        }
      }

      using var doc = JsonDocument.Parse(body1);
      if (doc.RootElement.ValueKind != JsonValueKind.Array) return (false, 0, "Supabase returned non-array JSON");
      return (true, doc.RootElement.GetArrayLength(), null);
    }
    catch (Exception ex)
    {
      return (false, 0, ex.Message);
    }
  }

  private static IEnumerable<AttlogRow> ReadW30AttlogRows(string path)
  {
    foreach (var raw in File.ReadLines(path))
    {
      var line = (raw ?? string.Empty).Trim();
      if (line.Length == 0) continue;
      var parts = line.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
      if (parts.Length < 2) continue;

      var staffId = parts[0].Trim();
      if (staffId.Length == 0) continue;

      var dtRaw = parts.Length >= 3 ? (parts[1].Trim() + " " + parts[2].Trim()) : parts[1].Trim();
      if (dtRaw.Length < 10) continue;

      var status = "0";
      var reserved = "0";
      if (parts.Length >= 5)
      {
        status = parts[3].Trim();
        reserved = parts[4].Trim();
      }
      else if (parts.Length >= 4)
      {
        status = parts[2].Trim();
        reserved = parts[3].Trim();
      }
      if (status.Length == 0) status = "0";
      if (reserved.Length == 0) reserved = "0";

      yield return new AttlogRow(staffId, dtRaw, "1", status, "1", reserved);
    }
  }

  private static async Task<(bool Ok, int Count, string? Error)> TryGetSupabaseCountInRange(AppConfig config, string? anonKey, string from, string to, CancellationToken ct)
  {
    var baseUrlRaw = (config.SupabaseUrl ?? string.Empty).Trim();
    var tableRaw = (config.SupabaseAttendanceTable ?? string.Empty).Trim();
    var serviceKeyRaw = (config.SupabaseServiceRoleKey ?? string.Empty).Trim();
    var anonRaw = (anonKey ?? string.Empty).Trim();
    var apiKey = serviceKeyRaw.Length > 0 ? serviceKeyRaw : anonRaw;
    if (baseUrlRaw.Length == 0 || tableRaw.Length == 0 || apiKey.Length == 0)
    {
      return (false, 0, "Supabase not configured");
    }

    try
    {
      var baseUrl = baseUrlRaw.TrimEnd('/');
      static string NormalizeRange(string raw)
      {
        var s = (raw ?? string.Empty).Trim();
        if (s.Length == 0) return string.Empty;
        if (s.Contains('T', StringComparison.Ordinal)) return s;
        if (DateTime.TryParseExact(s, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
        {
          var dto = new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
          return dto.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
        }
        return s;
      }

      var fromNorm = NormalizeRange(from);
      var toNorm = NormalizeRange(to);
      var url = $"{baseUrl}/rest/v1/{tableRaw}?select=staff_id&occurred_at=gte.{Uri.EscapeDataString(fromNorm)}&occurred_at=lte.{Uri.EscapeDataString(toNorm)}";

      static async Task<(bool ok, HttpResponseMessage res, string body)> SendAsync(string url, string apiKey, CancellationToken ct)
      {
        var http = new HttpClient();
        var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.TryAddWithoutValidation("apikey", apiKey);
        req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
        req.Headers.TryAddWithoutValidation("Accept", "application/json");
        req.Headers.TryAddWithoutValidation("Prefer", "count=exact");
        req.Headers.TryAddWithoutValidation("Range-Unit", "items");
        req.Headers.TryAddWithoutValidation("Range", "0-0");
        var res = await http.SendAsync(req, ct);
        var body = await res.Content.ReadAsStringAsync(ct);
        req.Dispose();
        http.Dispose();
        return (res.IsSuccessStatusCode, res, body);
      }

      var (ok1, res1, body1) = await SendAsync(url, apiKey, ct);
      if (!ok1 && (int)res1.StatusCode == 401 && serviceKeyRaw.Length > 0 && anonRaw.Length > 0)
      {
        res1.Dispose();
        (ok1, res1, body1) = await SendAsync(url, anonRaw, ct);
      }
      using var res = res1;
      if (!ok1)
      {
        return (false, 0, $"Supabase count failed: {(int)res.StatusCode} {res.ReasonPhrase}. Body={body1}");
      }

      string? contentRange = null;
      if (res.Headers.TryGetValues("Content-Range", out var h1)) contentRange = h1.FirstOrDefault();
      if (contentRange is null && res.Content.Headers.TryGetValues("Content-Range", out var h2)) contentRange = h2.FirstOrDefault();
      if (!string.IsNullOrWhiteSpace(contentRange))
      {
        var idx = contentRange.LastIndexOf("/", StringComparison.Ordinal);
        if (idx >= 0 && idx + 1 < contentRange.Length)
        {
          var tail = contentRange[(idx + 1)..].Trim();
          if (int.TryParse(tail, NumberStyles.Integer, CultureInfo.InvariantCulture, out var total))
          {
            return (true, Math.Max(0, total), null);
          }
        }
      }

      using var doc = JsonDocument.Parse(body1);
      if (doc.RootElement.ValueKind != JsonValueKind.Array) return (false, 0, "Supabase returned non-array JSON");
      return (true, doc.RootElement.GetArrayLength(), null);
    }
    catch (Exception ex)
    {
      return (false, 0, ex.Message);
    }
  }

  private static async Task<object> TryComputeTodayAnalytics(AppConfig config, string todayLocal, CancellationToken ct)
  {
    if (!DateOnly.TryParseExact(todayLocal, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var todayDate))
    {
      todayDate = DateOnly.FromDateTime(DateTime.Now);
    }

    var start7 = todayDate.AddDays(-6);
    var weekStart = todayDate.AddDays(-(((int)todayDate.DayOfWeek + 6) % 7));

    static string ShortDow(DateOnly d)
    {
      var s = d.DayOfWeek.ToString();
      return s.Length >= 3 ? s[..3] : s;
    }

    if (!config.SupabaseSyncEnabled)
    {
      var total = 0;
      var staff = new HashSet<string>(StringComparer.Ordinal);
      var perHour = new int[24];
      var perDay = new Dictionary<DateOnly, int>();
      var perWeekday = new Dictionary<DayOfWeek, int>();

      foreach (var p in Program.DevicePunches)
      {
        var local = p.OccurredAtUtc.ToLocalTime();
        var d = DateOnly.FromDateTime(local.DateTime);

        if (string.Equals(p.EventDate, todayLocal, StringComparison.Ordinal))
        {
          total++;
          if (!string.IsNullOrWhiteSpace(p.StaffId)) staff.Add(p.StaffId);
          var h = local.Hour;
          if (h >= 0 && h <= 23) perHour[h]++;
        }

        if (d >= start7 && d <= todayDate)
        {
          perDay[d] = perDay.TryGetValue(d, out var c) ? (c + 1) : 1;
        }

        if (d >= weekStart && d <= todayDate)
        {
          var dow = d.DayOfWeek;
          perWeekday[dow] = perWeekday.TryGetValue(dow, out var c2) ? (c2 + 1) : 1;
        }
      }

      var series = perHour
        .Select((count, hour) => new { hour, count })
        .Where(x => x.count > 0)
        .ToArray();

      var last7Days = Enumerable.Range(0, 7)
        .Select(i =>
        {
          var d = start7.AddDays(i);
          return new { date = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), dow = ShortDow(d), count = perDay.TryGetValue(d, out var c) ? c : 0 };
        })
        .ToArray();

      var thisWeek = Enumerable.Range(0, 7)
        .Select(i =>
        {
          var d = weekStart.AddDays(i);
          return new { date = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), dow = ShortDow(d), count = perWeekday.TryGetValue(d.DayOfWeek, out var c) ? c : 0 };
        })
        .ToArray();

      return new
      {
        ok = true,
        today = todayLocal,
        totalPunches = total,
        uniqueStaff = staff.Count,
        perHour = series,
        last7Days,
        thisWeek,
        source = "wl10",
      };
    }

    if (string.IsNullOrWhiteSpace(config.SupabaseUrl) || string.IsNullOrWhiteSpace(config.SupabaseServiceRoleKey) || string.IsNullOrWhiteSpace(config.SupabaseAttendanceTable))
    {
      return new { ok = false, reason = "supabase not configured" };
    }

    try
    {
      var baseUrl = config.SupabaseUrl.TrimEnd('/');
      var rangeStart = (start7 < weekStart) ? start7 : weekStart;
      var select = Uri.EscapeDataString("staff_id,occurred_at");
      var sg = TimeSpan.FromHours(8);
      var startUtc = new DateTimeOffset(rangeStart.ToDateTime(new TimeOnly(0, 0, 0)), sg).ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
      var endUtc = new DateTimeOffset(todayDate.ToDateTime(new TimeOnly(23, 59, 59)), sg).ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);
      var startDt = Uri.EscapeDataString(startUtc);
      var endDt = Uri.EscapeDataString(endUtc);
      var url =
        $"{baseUrl}/rest/v1/{config.SupabaseAttendanceTable}?select={select}" +
        $"&occurred_at=gte.{startDt}" +
        $"&occurred_at=lte.{endDt}" +
        $"&order=occurred_at.desc&limit=50000";

      using var http = new HttpClient();
      using var req = new HttpRequestMessage(HttpMethod.Get, url);
      req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");
      req.Headers.TryAddWithoutValidation("Accept", "application/json");

      using var res = await http.SendAsync(req, ct);
      if (!res.IsSuccessStatusCode) return new { ok = false, reason = $"http {(int)res.StatusCode}" };
      var text = await res.Content.ReadAsStringAsync(ct);
      using var doc = JsonDocument.Parse(text);
      if (doc.RootElement.ValueKind != JsonValueKind.Array) return new { ok = false, reason = "bad json" };

      var total = 0;
      var staff = new HashSet<string>(StringComparer.Ordinal);
      var perHour = new int[24];
      var perDay = new Dictionary<DateOnly, int>();
      var perWeekday = new Dictionary<DayOfWeek, int>();

      foreach (var el in doc.RootElement.EnumerateArray())
      {
        var staffId = el.TryGetProperty("staff_id", out var staffEl) && staffEl.ValueKind == JsonValueKind.String ? staffEl.GetString() : null;
        var dtRaw = el.TryGetProperty("occurred_at", out var dtEl) && dtEl.ValueKind == JsonValueKind.String ? dtEl.GetString() : null;
        if (string.IsNullOrWhiteSpace(dtRaw)) continue;
        if (!DateTimeOffset.TryParse(dtRaw.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto)) continue;
        var dt = dto.ToOffset(sg).DateTime;
        var d = DateOnly.FromDateTime(dt);

        if (d == todayDate)
        {
          total++;
          if (!string.IsNullOrWhiteSpace(staffId)) staff.Add(staffId);
        }

        if (d == todayDate)
        {
          var h = dt.Hour;
          if (h >= 0 && h <= 23) perHour[h]++;
        }

        if (d >= start7 && d <= todayDate)
        {
          perDay[d] = perDay.TryGetValue(d, out var c) ? (c + 1) : 1;
        }

        if (d >= weekStart && d <= todayDate)
        {
          var dow = d.DayOfWeek;
          perWeekday[dow] = perWeekday.TryGetValue(dow, out var c2) ? (c2 + 1) : 1;
        }
      }

      var series = perHour
        .Select((count, hour) => new { hour, count })
        .Where(x => x.count > 0)
        .ToArray();

      return new
      {
        ok = true,
        today = todayLocal,
        totalPunches = total,
        uniqueStaff = staff.Count,
        perHour = series,
        last7Days = Enumerable.Range(0, 7)
          .Select(i =>
          {
            var d = start7.AddDays(i);
            return new { date = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), dow = ShortDow(d), count = perDay.TryGetValue(d, out var c) ? c : 0 };
          })
          .ToArray(),
        thisWeek = Enumerable.Range(0, 7)
          .Select(i =>
          {
            var d = weekStart.AddDays(i);
            return new { date = d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), dow = ShortDow(d), count = perWeekday.TryGetValue(d.DayOfWeek, out var c) ? c : 0 };
          })
          .ToArray(),
      };
    }
    catch (Exception ex)
    {
      return new { ok = false, reason = ex.Message };
    }
  }

  private static (string valueBase64OrRaw, bool protectedOk) TryProtectBase64(string raw)
  {
    if (string.IsNullOrWhiteSpace(raw)) return (string.Empty, false);
    try
    {
      var bytes = Encoding.UTF8.GetBytes(raw);
      var protectedBytes = ProtectedData.Protect(bytes, optionalEntropy: null, scope: DataProtectionScope.CurrentUser);
      return (Convert.ToBase64String(protectedBytes), true);
    }
    catch
    {
      return (raw, false);
    }
  }

  private static string TryUnprotectBase64(string protectedBase64)
  {
    if (string.IsNullOrWhiteSpace(protectedBase64)) return string.Empty;
    try
    {
      var bytes = Convert.FromBase64String(protectedBase64);
      var unprotectedBytes = ProtectedData.Unprotect(bytes, optionalEntropy: null, scope: DataProtectionScope.CurrentUser);
      return Encoding.UTF8.GetString(unprotectedBytes);
    }
    catch
    {
      return string.Empty;
    }
  }

  sealed record SupabaseTestResult(bool Ok, string? Reason = null, int? Status = null, string? Body = null, long RttMs = 0);

  private static string InferSupabaseProjectId(string? supabaseUrl)
  {
    var raw = (supabaseUrl ?? string.Empty).Trim();
    if (raw.Length == 0) return string.Empty;
    if (!Uri.TryCreate(raw, UriKind.Absolute, out var uri)) return string.Empty;
    var host = (uri.Host ?? string.Empty).Trim();
    if (host.Length == 0) return string.Empty;
    if (!host.EndsWith(".supabase.co", StringComparison.OrdinalIgnoreCase)) return string.Empty;
    var parts = host.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    if (parts.Length < 3) return string.Empty;
    return parts[0];
  }

  private static async Task<SupabaseTestResult> TryTestSupabase(AppConfig config, string? anonKey, CancellationToken ct)
  {
    var baseUrlRaw = (config.SupabaseUrl ?? string.Empty).Trim();
    var tableRaw = (config.SupabaseAttendanceTable ?? string.Empty).Trim();
    var serviceKeyRaw = (config.SupabaseServiceRoleKey ?? string.Empty).Trim();
    var anonRaw = (anonKey ?? string.Empty).Trim();

    if (baseUrlRaw.Length == 0 || tableRaw.Length == 0)
    {
      return new SupabaseTestResult(false, "supabase not configured");
    }

    var apiKey = serviceKeyRaw.Length > 0 ? serviceKeyRaw : anonRaw;
    if (apiKey.Length == 0) return new SupabaseTestResult(false, "supabase not configured");

    var sw = Stopwatch.StartNew();
    try
    {
      var baseUrl = baseUrlRaw.TrimEnd('/');
      var table = tableRaw;
      var url = $"{baseUrl}/rest/v1/{table}?select=*&limit=1";

      using var http = new HttpClient();
      http.Timeout = TimeSpan.FromSeconds(10);

      using var req = new HttpRequestMessage(HttpMethod.Get, url);
      req.Headers.TryAddWithoutValidation("apikey", apiKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
      req.Headers.TryAddWithoutValidation("Accept", "application/json");

      using var res = await http.SendAsync(req, ct);
      var body = await res.Content.ReadAsStringAsync(ct);
      sw.Stop();

      if (!res.IsSuccessStatusCode)
      {
        return new SupabaseTestResult(false, res.ReasonPhrase ?? "http error", (int)res.StatusCode, body, sw.ElapsedMilliseconds);
      }

      return new SupabaseTestResult(true, null, (int)res.StatusCode, null, sw.ElapsedMilliseconds);
    }
    catch (Exception ex)
    {
      sw.Stop();
      return new SupabaseTestResult(false, ex.Message, null, null, sw.ElapsedMilliseconds);
    }
  }

  private static string LoginHtml(string? error = null)
  {
    var e = string.IsNullOrWhiteSpace(error) ? "" : $"<div class='err'>{WebUtility.HtmlEncode(error)}</div>";
    var tpl = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>SHAB Attendance Dashboard - Login</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root{--bg1:#fff7ed;--bg2:#f8fafc;--text:#0f172a;--muted:#5b6b83;--border:rgba(255,255,255,.42);--border2:rgba(15,23,42,.14);--accent:#2563eb;--accent2:#0ea5e9;--bad:#dc2626;--fontHead:"Nunito","Segoe UI Rounded","Arial Rounded MT Bold","Trebuchet MS",system-ui,sans-serif}
    *{box-sizing:border-box}
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;color:var(--text);background:linear-gradient(180deg,#fff7ed,#f8fafc);min-height:100vh;overflow-x:hidden}
    .bg{position:fixed;inset:0;pointer-events:none;z-index:0}
    .bg svg{position:absolute;inset:0}

    .wrap{position:relative;z-index:1;max-width:1100px;margin:0 auto;min-height:100vh;padding:clamp(12px,2.2vh,22px) 18px;display:flex;flex-direction:column;gap:clamp(12px,2vh,16px);justify-content:center}
    .topGrid{display:grid;grid-template-columns:2fr 1fr;gap:clamp(12px,2vh,16px);align-items:stretch}
    @media (max-width: 980px){.wrap{padding:18px 14px;justify-content:flex-start}.topGrid{grid-template-columns:1fr}}

    .card{border:1px solid rgba(15,23,42,.10);background:rgba(255,255,255,.72);backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);border-radius:18px;box-shadow:0 16px 50px rgba(2,6,23,.10);min-width:0}
    .heroCard{padding:20px 22px}
    .loginCard{padding:20px 22px}
    .cardInner{min-height:clamp(250px,34vh,320px);display:flex;flex-direction:column;justify-content:center}
    @media (max-width: 980px){.cardInner{min-height:auto}}

    .brand{display:flex;gap:12px;align-items:center}
    .logo{width:44px;height:44px;border-radius:14px;background:linear-gradient(135deg,rgba(37,99,235,.95),rgba(14,165,233,.92));box-shadow:0 14px 34px rgba(2,6,23,.16);display:grid;place-items:center;color:#fff;flex:0 0 auto}
    .logo svg{width:22px;height:22px}
    .brandText{min-width:0}
    .brandName{font-family:var(--fontHead);font-weight:900;font-size:16px;letter-spacing:.01em;margin:0}
    .brandTag{margin-top:2px;color:var(--muted);font-size:12px}

    .heroTitle{font-family:var(--fontHead);font-weight:950;letter-spacing:-.03em;font-size:44px;line-height:1.02;margin:14px 0 0}
    @media (max-width: 980px){.heroTitle{font-size:36px}}
    .heroSub{margin:12px 0 0;color:var(--muted);font-size:13px;max-width:78ch;line-height:1.55}

    .loginHead{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:8px}
    .loginTitle{font-family:var(--fontHead);font-size:16px;font-weight:950;margin:0}
    .loginMeta{font-size:12px;color:var(--muted)}
    .form{max-width:340px}
    label{display:block;font-size:11px;color:var(--muted);margin:12px 0 6px}
    input{width:100%;padding:10px 12px;border-radius:12px;border:1px solid rgba(15,23,42,.12);background:rgba(255,255,255,.86);color:var(--text);outline:none}
    input:focus{border-color:rgba(37,99,235,.55);box-shadow:0 0 0 4px rgba(37,99,235,.14)}
    button{margin-top:14px;width:100%;padding:10px 12px;border-radius:12px;border:1px solid rgba(37,99,235,.85);background:linear-gradient(135deg,rgba(37,99,235,.95),rgba(14,165,233,.92));color:#fff;cursor:pointer;font-weight:800}
    .err{margin:12px 0;padding:10px 12px;border-radius:12px;background:rgba(220,38,38,.10);border:1px solid rgba(220,38,38,.26);color:#7f1d1d}
    .hint{margin-top:12px;color:var(--muted);font-size:12px}

    .featuresGrid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
    @media (max-width: 980px){.featuresGrid{grid-template-columns:repeat(2,minmax(0,1fr))}}
    @media (max-width: 560px){.featuresGrid{grid-template-columns:1fr}}
    .fCard{border:1px solid rgba(15,23,42,.10);background:rgba(255,255,255,.70);backdrop-filter:blur(12px);-webkit-backdrop-filter:blur(12px);border-radius:16px;padding:12px;box-shadow:0 12px 40px rgba(2,6,23,.08);min-width:0}
    .fTop{display:flex;gap:10px;align-items:flex-start}
    .fIco{width:34px;height:34px;border-radius:12px;background:rgba(255,255,255,.86);border:1px solid rgba(15,23,42,.10);display:grid;place-items:center;flex:0 0 auto}
    .fIco svg{width:18px;height:18px;stroke:rgba(37,99,235,.95);fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
    .fT{font-family:var(--fontHead);font-size:11px;font-weight:900;letter-spacing:.06em;text-transform:uppercase;margin:0}
    .fD{margin-top:6px;color:var(--muted);font-size:12px;line-height:1.35}
  </style>
</head>
<body>
  <div class="bg">
    <svg viewBox="0 0 1200 800" preserveAspectRatio="none" aria-hidden="true">
      <defs>
        <linearGradient id="g1" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0" stop-color="rgba(37,99,235,.30)"/>
          <stop offset="1" stop-color="rgba(14,165,233,.14)"/>
        </linearGradient>
        <linearGradient id="g2" x1="1" y1="0" x2="0" y2="1">
          <stop offset="0" stop-color="rgba(245,158,11,.16)"/>
          <stop offset="1" stop-color="rgba(220,38,38,.08)"/>
        </linearGradient>
        <linearGradient id="g3" x1="0" y1="1" x2="1" y2="0">
          <stop offset="0" stop-color="rgba(14,165,233,.16)"/>
          <stop offset="1" stop-color="rgba(37,99,235,.10)"/>
        </linearGradient>
      </defs>
      <path d="M0,180 C160,140 260,240 420,220 C580,200 630,80 790,70 C960,60 1010,200 1200,160 L1200,0 L0,0 Z" fill="url(#g2)" opacity=".9"/>
      <path d="M0,560 C170,520 260,650 430,620 C600,590 640,430 820,420 C1000,410 1040,520 1200,480 L1200,800 L0,800 Z" fill="url(#g3)" opacity=".95"/>
      <circle cx="210" cy="220" r="92" fill="rgba(255,255,255,.20)"/>
      <circle cx="980" cy="210" r="120" fill="rgba(255,255,255,.16)"/>
    </svg>
  </div>

  <div class="wrap">
    <div class="topGrid">
      <div class="card heroCard">
        <div class="cardInner">
          <div class="brand">
            <div class="logo" aria-hidden="true">
              <svg viewBox="0 0 24 24"><path d="M7 12l3 3 7-7" stroke="white" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg>
            </div>
            <div class="brandText">
              <div class="brandName">SHAB Attendance Dashboard</div>
              <div class="brandTag">Middleware Monitoring • Attendance Analytics • Roster Management</div>
            </div>
          </div>

          <h1 class="heroTitle">Attendance intelligence, built for daily operations.</h1>
          <div class="heroSub">SHAB Attendance Dashboard helps HR and operations teams monitor attendance capture, validate sync integrity, and analyze daily patterns across departments — with clear visuals and actionable insights.</div>
        </div>
      </div>

      <div class="card loginCard">
        <div class="cardInner">
          <div class="loginHead">
            <div class="loginTitle">Secure Login</div>
            <div class="loginMeta">Admin access</div>
          </div>
          __ERR__
          <form class="form" method="post" action="/login">
            <label for="username">ID</label>
            <input id="username" name="username" autocomplete="username" />
            <label for="password">Password</label>
            <input id="password" name="password" type="password" autocomplete="current-password" />
            <button type="submit">Login</button>
          </form>
        </div>
      </div>
    </div>

    <div class="featuresGrid">
      <div class="fCard"><div class="fTop"><div class="fIco" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-3-6.7"/><path d="M21 3v7h-7"/></svg></div><div><div class="fT">Live Status</div><div class="fD">Check device reachability, reader mode, and sync health at a glance.</div></div></div></div>
      <div class="fCard"><div class="fTop"><div class="fIco" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M4 19V5"/><path d="M4 19h16"/><path d="M8 16v-5"/><path d="M12 16v-8"/><path d="M16 16v-3"/></svg></div><div><div class="fT">Hourly Trends</div><div class="fD">Visualize punch activity by hour to spot peaks and anomalies.</div></div></div></div>
      <div class="fCard"><div class="fTop"><div class="fIco" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M3 7h18"/><path d="M7 3v18"/><path d="M3 21h18"/></svg></div><div><div class="fT">Department Split</div><div class="fD">Break down presence by department and track attendance rate.</div></div></div></div>
      <div class="fCard"><div class="fTop"><div class="fIco" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M8 7V3"/><path d="M16 7V3"/><path d="M4 7h16v14H4z"/><path d="M7 11h10"/></svg></div><div><div class="fT">7-Day View</div><div class="fD">See recent attendance and late counts with quick comparisons.</div></div></div></div>
      <div class="fCard"><div class="fTop"><div class="fIco" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M19 8v6"/><path d="M22 11h-6"/></svg></div><div><div class="fT">Staff Directory</div><div class="fD">Maintain staff roster details and filter quickly by name or ID.</div></div></div></div>
      <div class="fCard"><div class="fTop"><div class="fIco" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M12 8v4l3 3"/><circle cx="12" cy="12" r="9"/></svg></div><div><div class="fT">Shift Patterns</div><div class="fD">Manage shift patterns to align operational rules with reporting.</div></div></div></div>
      <div class="fCard"><div class="fTop"><div class="fIco" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M3 12h18"/><path d="M7 12v8"/><path d="M17 12v8"/><path d="M9 20h6"/></svg></div><div><div class="fT">Database Comparison</div><div class="fD">Compare device punches with database records to reconcile discrepancies.</div></div></div></div>
      <div class="fCard"><div class="fTop"><div class="fIco" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M12 2v4"/><path d="M5 7h14"/><path d="M7 7v14"/><path d="M17 7v14"/><path d="M10 11h4"/></svg></div><div><div class="fT">Monthly Trends</div><div class="fD">Track attendance and absenteeism rates over time for better planning.</div></div></div></div>
      <div class="fCard"><div class="fTop"><div class="fIco" aria-hidden="true"><svg viewBox="0 0 24 24"><path d="M4 4h16v16H4z"/><path d="M8 16l2-2 2 2 4-4"/></svg></div><div><div class="fT">Export & Audit</div><div class="fD">Export logs and reports for payroll support, compliance, and auditing.</div></div></div></div>
    </div>
  </div>
</body>
</html>
""";
    return tpl.Replace("__ERR__", e);
  }

  private static string DashboardHtml()
  {
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>SHAB Attendance Dashboard</title>
  <link rel="icon" href="/assets/logo.ico" />
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap" rel="stylesheet">
  <style>
    *{box-sizing:border-box}
    :root{--bg:#F7F9FC;--panel:#ffffff;--panel2:#f8fafc;--text:#0f172a;--muted:#6b7280;--border:#e5e7eb;--border2:#d1d5db;--btn:#f3f4f6;--btnH:#e5e7eb;--navy:#163A70;--teal:#2BB7A9;--activeBg:#D6F2EE;--activeText:#163A70;--accent:var(--navy);--accent2:var(--teal);--ok:#16a34a;--bad:#dc2626;--fontHead:"Nunito","Segoe UI","Inter",system-ui,sans-serif}
    body{font-family:"Nunito",system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;background:var(--bg);color:var(--text);overflow-x:hidden}
    header{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid var(--border);background:var(--panel);position:sticky;top:0;z-index:2000}
    h1{font-family:var(--fontHead);font-size:16px;margin:0;font-weight:900}
    .title,.summaryTitle,.summaryWrap .title,.kTitle,.miniKpiTitle,.donutTitle,.sparkTitle,.subTitle,.modalTitle{font-family:var(--fontHead)}
    .btn{padding:8px 10px;border-radius:10px;border:1px solid var(--border2);background:var(--btn);color:var(--text);cursor:pointer;font-family:inherit;font-size:12px;transition:background .12s ease,border-color .12s ease,transform .05s ease,box-shadow .12s ease}
    .btn:hover{background:var(--btnH);border-color:var(--border2);box-shadow:0 0 0 3px rgba(43,183,169,.14)}
    .btn:active{transform:translateY(1px);box-shadow:0 0 0 3px rgba(43,183,169,.20)}
    .btn:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
    .btn:disabled{opacity:.55;cursor:not-allowed;box-shadow:none}
    .btn.primary{background:var(--accent);border-color:var(--accent);color:#fff}
    main{max-width:none;margin:0;padding:0;overflow-x:hidden}
    .tabs{display:none}
    .tabBtn{padding:9px 12px;border-radius:12px;border:1px solid var(--border);background:var(--panel);color:var(--text);cursor:pointer;font-size:12px;transition:background .12s ease,border-color .12s ease,transform .05s ease,box-shadow .12s ease}
    .tabBtn:hover{background:var(--btn);border-color:var(--border2);box-shadow:0 0 0 3px rgba(43,183,169,.12)}
    .tabBtn:active{transform:translateY(1px)}
    .tabBtn:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
    .tabBtn.active{background:var(--btn);border-color:var(--border2)}
    .tabPanel{display:none}
    .tabPanel.active{display:block}
    .subTabs{display:none}
    .subTabBtn{padding:9px 12px;border-radius:12px;border:1px solid #e5e7eb;background:#ffffff;color:#0f172a;cursor:pointer;font-size:12px;transition:background .12s ease,border-color .12s ease,transform .05s ease,box-shadow .12s ease}
    .subTabBtn:hover{background:#f1f5f9;border-color:#cbd5e1;box-shadow:0 0 0 3px rgba(43,183,169,.12)}
    .subTabBtn:active{transform:translateY(1px)}
    .subTabBtn:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
    .subTabBtn.active{background:#f1f5f9;border-color:#cbd5e1}
    .subTabPanel{display:none}
    .subTabPanel.active{display:block}
    .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;overflow-x:hidden}
    .card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:14px;min-width:0}
    .title{font-size:12px;color:var(--muted);margin:0 0 10px;min-width:0}
    .titleRow{display:flex;align-items:flex-start;justify-content:space-between;gap:10px;margin:0 0 10px;min-width:0}
    .titleRow .title{margin:0}
    .staffLog{flex:1;min-width:0;text-align:right;font-variant-numeric:tabular-nums;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word}
    .staffLog.okText{color:var(--ok)}
    .staffLog.badText{color:var(--bad)}
    .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;min-width:0}
    .row.noWrap{flex-wrap:nowrap}
    .toolbarRight{justify-content:flex-end}
    .kv{font-size:12px;color:var(--muted);min-width:0}
    .val{color:var(--text)}
    .pill{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:999px;border:1px solid var(--border2);background:var(--panel2);font-size:12px}
    .ok{border-color:rgba(22,163,74,.45);background:rgba(22,163,74,.10)}
    .bad{border-color:rgba(220,38,38,.45);background:rgba(220,38,38,.10)}
    input,select,textarea{padding:8px 10px;border-radius:10px;border:1px solid var(--border2);background:var(--panel);color:var(--text);font-family:inherit;font-size:12px}
    button{font-family:inherit;font-size:12px}
    pre{margin:0;max-height:60vh;overflow-y:auto;overflow-x:hidden;background:var(--panel2);border:1px solid var(--border);border-radius:10px;padding:10px;font-size:12px;line-height:1.35;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word}
    table{width:100%;border-collapse:collapse;table-layout:fixed}
    th,td{border-bottom:1px solid var(--border);padding:8px 6px;font-size:12px;text-align:left;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    th{color:var(--muted);font-weight:600}
    td .btn{display:inline-block;margin:4px 0}
    .muted{color:var(--muted)}
    .formGrid{display:grid;grid-template-columns:160px minmax(0,1fr);gap:10px 12px;align-items:center}
    .formGrid input,.formGrid select,.formGrid textarea{width:100%;min-width:0}
    .formGrid textarea{padding:8px 10px;border-radius:10px;border:1px solid var(--border2);background:var(--panel);color:var(--text);font-family:inherit;resize:none;overflow:hidden}
    label{font-size:12px;color:var(--muted)}
    .hint{font-size:12px;color:var(--muted)}
    th.sortable{cursor:pointer;user-select:none}
    th.sortable:hover{text-decoration:underline}
    .kpis{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
    .kpis.kpis1{grid-template-columns:1fr}
    .kpis.kpis2{grid-template-columns:repeat(2,minmax(0,1fr))}
    .kpi{background:var(--panel2);border:1px solid var(--border);border-radius:12px;padding:16px}
    .kTitle{font-size:12px;color:var(--muted);margin-bottom:6px}
    .kVal{font-size:28px;font-weight:700}
    .charts{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
    .bars{display:flex;align-items:flex-end;gap:4px;height:120px;padding:8px;border:1px dashed var(--border2);border-radius:10px;background:linear-gradient(180deg,#ffffff,var(--panel2))}
    .bar{width:14px;background:var(--accent);border-radius:4px 4px 0 0}
    .bar.db{background:var(--accent2)}
    .barLabel{font-size:10px;color:var(--muted);margin-top:6px;text-align:center}
    .summaryWrap{background:transparent;border:0;border-radius:16px;padding:0;color:var(--text)}
    .summaryWrap .card{background:#ffffff;border-color:#e5e7eb;color:#0f172a;box-shadow:0 2px 18px rgba(15,23,42,.06)}
    .summaryWrap .kTitle,.summaryWrap .hint,.summaryWrap .kv,.summaryWrap th{color:#64748b}
    .summaryWrap .title{font-size:14px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:#0f172a;margin:0 0 12px}
    .summaryWrap .val{color:#0f172a}
    .summaryHeader{display:flex;align-items:flex-end;justify-content:space-between;gap:12px;margin-bottom:14px}
    .summaryTitle{font-size:22px;font-weight:800;letter-spacing:-.02em;margin:0;line-height:1.1}
    .summarySub{margin-top:4px;color:#64748b;font-size:13px}
    .summaryWrap .kpis{grid-template-columns:repeat(4,minmax(0,1fr))}
    @media (max-width: 900px){.summaryWrap .kpis{grid-template-columns:repeat(2,minmax(0,1fr))}}
    @media (max-width: 520px){.summaryWrap .kpis{grid-template-columns:1fr}}
    .summaryWrap .kpi{background:linear-gradient(180deg,#ffffff,#f8fafc);border-color:#e5e7eb;position:relative;overflow:hidden}
    .summaryWrap .kpi::after{content:'';position:absolute;inset:-2px -2px auto auto;width:120px;height:120px;background:radial-gradient(circle at 40% 40%,rgba(43,183,169,.18),transparent 60%);pointer-events:none}
    .summaryWrap .kTitle{font-size:12px;font-weight:600;letter-spacing:.02em;text-transform:uppercase}
    .summaryWrap .kVal{font-size:34px;color:#0f172a}
    .summaryWrap .pill{background:#f8fafc;border-color:#e2e8f0;color:#0f172a}
    .summaryWrap .pill.ok{border-color:#10b981;background:#ecfdf5}
    .summaryWrap .pill.bad{border-color:#ef4444;background:#fef2f2}
    .summaryRow{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}
    .kMeta{font-size:12px;color:#64748b;margin-top:8px}
    .kMeta strong{color:#0f172a;font-weight:700}
    .chartHost{min-width:0}
    .chartWrap{display:grid;grid-template-columns:28px minmax(0,1fr);gap:10px;align-items:stretch}
    .yAxisLabel{writing-mode:vertical-rl;transform:rotate(180deg);font-size:11px;color:#64748b;display:flex;align-items:center;justify-content:center}
    .plot{position:relative;border:1px solid #e5e7eb;background:linear-gradient(180deg,#ffffff,#f8fafc);border-radius:12px;padding:10px 10px 8px 40px;overflow:hidden}
    .plot::before{content:'';position:absolute;inset:0;background-image:linear-gradient(to right,rgba(15,23,42,.06) 1px,transparent 1px),linear-gradient(to top,rgba(15,23,42,.06) 1px,transparent 1px);background-size:calc(100%/6) calc(100%/5);filter:blur(.35px);opacity:.55;pointer-events:none}
    .yNums{position:absolute;left:8px;top:10px;bottom:28px;width:28px;display:flex;flex-direction:column;justify-content:space-between;align-items:flex-end;font-size:10px;color:#64748b;font-variant-numeric:tabular-nums;pointer-events:none}
    .barsGrid{position:relative;display:grid;grid-template-columns:repeat(24,minmax(0,1fr));gap:4px;align-items:end;height:160px}
    .barsGrid .bar2{border-radius:6px 6px 0 0;min-height:2px}
    .barsGrid .bar2.device{background:linear-gradient(180deg,#163A70,#0f2a56)}
    .barsGrid .bar2.db{background:linear-gradient(180deg,#2BB7A9,#169a8e)}
    .xAxis{position:relative;display:grid;grid-template-columns:repeat(24,minmax(0,1fr));gap:4px;margin-top:6px;font-size:9px;color:#64748b}
    .xAxis span{text-align:center;opacity:.85}
    .legend{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:10px}
    .legItem{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#64748b}
    .dot{width:10px;height:10px;border-radius:999px}
    .dot.device{background:#163A70}
    .dot.db{background:#2BB7A9}
    .groupGrid{position:relative;display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:10px;align-items:end;height:170px}
    .gCol{position:relative;display:flex;flex-direction:column;align-items:stretch;gap:6px;min-width:0}
    .gBars{display:flex;gap:4px;align-items:flex-end;justify-content:center;height:140px}
    .gBar{width:16px;border-radius:6px 6px 0 0;min-height:4px}
    .gBar.device{background:linear-gradient(180deg,#163A70,#0f2a56)}
    .gBar.db{background:linear-gradient(180deg,#2BB7A9,#169a8e)}
    .gLbl{font-size:10px;color:#64748b;text-align:center;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .summaryWrap .muted{color:#64748b}
    .subCards{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}
    @media (max-width: 520px){.subCards{grid-template-columns:1fr}}
    .subCard{border:1px solid #e5e7eb;background:linear-gradient(180deg,#ffffff,#f8fafc);border-radius:12px;padding:12px;min-width:0}
    .subTitle{font-size:11px;font-weight:700;letter-spacing:.02em;text-transform:uppercase;color:#64748b;margin:0 0 6px}
    .subVal{font-size:14px;font-weight:700;color:#0f172a;line-height:18px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .subVal .pill{box-sizing:border-box;height:18px;padding:0 8px}
    .subVal.mono{font-variant-numeric:tabular-nums;font-feature-settings:"tnum" 1;white-space:pre-wrap;overflow-wrap:anywhere}
    .flagWrap{white-space:normal;overflow-wrap:anywhere;line-height:1.15}
    .loadRow{display:flex;align-items:center;gap:12px;padding:10px 12px;border:1px dashed #e5e7eb;border-radius:12px;background:linear-gradient(180deg,#ffffff,#f8fafc);margin-bottom:10px}
    .ring{position:relative;width:38px;height:38px;border-radius:999px;display:grid;place-items:center;background:conic-gradient(#163A70 var(--pct), #e2e8f0 0)}
    .ring::before{content:'';width:30px;height:30px;border-radius:999px;background:#fff}
    .ringText{position:absolute;font-size:11px;font-weight:800;color:#0f172a}
    .loadText{font-size:12px;color:#64748b}
    .loadText strong{color:#0f172a}
    .modalBack{position:fixed;inset:0;background:rgba(15,23,42,.35);display:none;align-items:flex-start;justify-content:center;padding:16px;z-index:5000;overflow:auto}
    .modalCard{width:min(720px,100%);max-height:calc(100vh - 32px);background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:14px;display:flex;flex-direction:column;overflow:hidden}
    .modalCard.wide{width:min(1100px,100%)}
    .modalHead{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px}
    .modalTitle{font-size:13px;color:var(--text);font-weight:700}
    .modalBody{flex:1;min-height:0;overflow:auto}
    .staffHead{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px}
    .staffLeft{display:flex;gap:12px;align-items:center;min-width:0}
    .staffAvatar{width:44px;height:44px;border-radius:999px;background:linear-gradient(180deg,#163A70,#2BB7A9);color:#fff;font-weight:900;display:flex;align-items:center;justify-content:center;flex:0 0 auto}
    .staffName{font-weight:900;color:var(--text);font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .staffMeta{color:var(--muted);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .staffStats{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:10px;margin:10px 0 14px}
    @media (max-width: 900px){.staffStats{grid-template-columns:repeat(2,minmax(0,1fr))}}
    .staffStat{border:1px solid var(--border2);border-radius:12px;background:var(--panel2);padding:10px}
    .staffStatT{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;font-weight:800}
    .staffStatV{font-size:18px;color:var(--text);font-weight:900;margin-top:4px}
    .monthRow{display:flex;gap:10px;align-items:center;justify-content:space-between;margin-bottom:10px}
    .dayList{display:flex;flex-direction:column;gap:10px}
    .dayCard{border:1px solid var(--border2);border-radius:14px;background:linear-gradient(180deg,#ffffff,#f8fafc);padding:10px 12px}
    .dayTop{display:flex;align-items:center;justify-content:space-between;gap:10px}
    .dayBadge{display:inline-flex;align-items:center;gap:6px;border-radius:999px;padding:4px 10px;border:1px solid rgba(226,232,240,.95);background:#fff;font-size:11px;font-weight:900;color:#0f172a;white-space:nowrap}
    .dayBadge.bad{border-color:rgba(239,68,68,.35);background:rgba(239,68,68,.08);color:#7f1d1d}
    .dayBadge.warn{border-color:rgba(245,158,11,.40);background:rgba(245,158,11,.10);color:#7c2d12}
    .dayBadge.off{border-color:rgba(148,163,184,.55);background:rgba(148,163,184,.12);color:#334155}
    .dayTitle{font-weight:900;color:#0f172a;font-size:12px}
    .dayTimes{font-size:12px;color:#334155;font-variant-numeric:tabular-nums}
    .tl{margin-top:8px}
    .axis{display:flex;justify-content:space-between;color:#94a3b8;font-size:10px;font-variant-numeric:tabular-nums}
    .track{position:relative;height:14px;border-radius:999px;background:rgba(226,232,240,.85);border:1px solid rgba(226,232,240,.95);overflow:visible;margin-top:6px}
    .seg{position:absolute;top:0;bottom:0;border-radius:999px;overflow:visible}
    .seg.work{background:linear-gradient(90deg,#163A70,#0f2a56)}
    .seg.break{background:linear-gradient(90deg,#94a3b8,#64748b)}
    .seg.extra{background:linear-gradient(90deg,#fbbf24,#f59e0b)}
    .missDot{position:absolute;top:50%;transform:translate(-50%,-50%);width:8px;height:8px;border-radius:999px;background:#ef4444;border:2px solid #fff;box-shadow:0 10px 20px rgba(2,6,23,.18);z-index:25}
    .tlLegend{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:8px 0 10px}
    .tlLeg{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#64748b}
    .tlDot{width:10px;height:10px;border-radius:999px}
    .tlDot.work{background:#163A70}
    .tlDot.break{background:#64748b}
    .tlDot.extra{background:#f59e0b}
    .tlDot.miss{background:#ef4444}
    .segTip{display:none;position:absolute;left:50%;transform:translateX(-50%);top:-54px;background:#0f172a;color:#fff;padding:10px 12px;border-radius:14px;font-size:12px;line-height:1.25;box-shadow:0 18px 42px rgba(2,6,23,.28);white-space:nowrap;z-index:30}
    .segTip::after{content:'';position:absolute;left:50%;transform:translateX(-50%);bottom:-6px;width:12px;height:12px;background:#0f172a;transform-origin:center;rotate:45deg;border-radius:3px}
    .seg:hover .segTip{display:block}
    .modalGrid{display:grid;grid-template-columns:160px minmax(0,1fr);gap:10px 12px;align-items:center}
    .modalGrid label{color:var(--muted)}
    .modalTable{width:100%;border-collapse:collapse;font-size:12px}
    .modalTable th,.modalTable td{border:1px solid var(--border);padding:8px 10px;vertical-align:top}
    .modalTable th{background:var(--panel2);text-align:left;color:var(--muted);font-weight:800;width:220px}
    .modalPunchTable{width:100%;border-collapse:collapse;font-size:12px;margin-top:10px}
    .modalPunchTable th,.modalPunchTable td{border:1px solid var(--border);padding:8px 10px;vertical-align:top}
    .modalPunchTable th{background:var(--panel2);text-align:left;color:var(--muted);font-weight:800}
    .modalFoot{display:flex;gap:10px;justify-content:flex-end;margin-top:12px}
    .iconBtn{display:inline-flex;align-items:center;justify-content:center;width:30px;height:30px;padding:0;border-radius:10px;border:1px solid var(--border2);background:var(--btn);color:var(--text);cursor:pointer}
    .iconBtn:hover{background:var(--btnH);border-color:var(--border2)}
    .iconBtn svg{width:16px;height:16px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
    .iconBtn.sm{width:28px;height:28px;border-radius:9px}
    .iconBtn.danger{border-color:rgba(220,38,38,.35);color:var(--bad)}
    .iconBtn.danger:hover{border-color:rgba(220,38,38,.55)}
    .actionIcons{display:flex;align-items:center;justify-content:flex-start;gap:6px}
    .actionSep{width:1px;height:18px;background:var(--border2);opacity:.8}
    .devConnTable .actionIcons{justify-content:flex-end;flex-wrap:nowrap}
    .devConnTable td:last-child{overflow:visible}
    .settingsDeviceTable td{white-space:nowrap}
    .dlIconBtn{display:inline-flex;align-items:center;justify-content:center;width:30px;height:30px;padding:0;border-radius:10px;border:1px solid var(--border2);background:var(--btn);color:var(--text);cursor:pointer}
    .dlIconBtn:hover{background:var(--btnH);border-color:var(--border2)}
    .dlIconBtn svg{width:16px;height:16px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
    .analysisGrid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:14px}
    .analysisCard{grid-column:1/-1}
    .analysisFilters{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:10px}
    .snapGrid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
    .snapGrid3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
    @media (max-width: 900px){.snapGrid3{grid-template-columns:repeat(2,minmax(0,1fr))}}
    @media (max-width: 520px){.snapGrid3{grid-template-columns:1fr}}
    .snapCol{display:grid;grid-template-columns:1fr;gap:12px;margin-top:10px}
    @media (max-width: 900px){.snapGrid{grid-template-columns:repeat(2,minmax(0,1fr))}}
    @media (max-width: 520px){.snapGrid{grid-template-columns:1fr}}
    .snapCard{border:1px solid #e5e7eb;border-radius:14px;background:linear-gradient(180deg,#ffffff,#f8fafc);padding:14px 14px 12px;cursor:pointer;display:flex;flex-direction:column;gap:8px;min-width:0;text-align:left}
    .snapCard:hover{border-color:rgba(22,58,112,.85);background:linear-gradient(180deg,#163A70,#0f2a56);box-shadow:0 10px 26px rgba(2,6,23,.14);transform:translateY(-1px)}
    .snapCard:hover .snapTitle,.snapCard:hover .snapMeta,.snapCard:hover .snapVal{color:#ffffff}
    .snapCard:hover .snapGo{background:rgba(255,255,255,.12);border-color:rgba(255,255,255,.35);color:#ffffff}
    .snapCard.teal:hover{border-color:rgba(16,185,129,.85);background:linear-gradient(180deg,#0f766e,#064e3b);box-shadow:0 10px 26px rgba(2,6,23,.14);transform:translateY(-1px)}
    .snapCard.teal:hover .snapTitle,.snapCard.teal:hover .snapMeta,.snapCard.teal:hover .snapVal{color:#ffffff}
    .snapCard.teal:hover .snapGo{background:rgba(255,255,255,.12);border-color:rgba(255,255,255,.35);color:#ffffff}
    .snapCard:active{transform:translateY(0)}
    .snapTop{display:flex;align-items:flex-start;justify-content:space-between;gap:10px}
    .snapTitle{font-size:12px;font-weight:700;color:#64748b;text-transform:uppercase;letter-spacing:.06em}
    .snapGo{width:26px;height:26px;border-radius:999px;border:1px solid rgba(226,232,240,.95);display:flex;align-items:center;justify-content:center;color:#0f172a;background:#fff;flex:0 0 auto}
    .snapVal{font-size:32px;font-weight:900;color:#0f172a;line-height:1}
    .snapVal.note{font-size:14px;font-weight:800;line-height:1.25}
    .snapMeta{font-size:12px;color:#64748b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .snapMeta.wrap{white-space:normal}
    .anaTopSplit{grid-column:1/-1;display:grid;grid-template-columns:2fr 1fr;gap:14px;align-items:stretch}
    @media (max-width: 900px){.anaTopSplit{grid-template-columns:1fr}}
    .anaLeftStack{display:grid;grid-template-rows:1fr auto;gap:14px;min-width:0;height:100%}
    .anaLeftStack>.card{height:100%}
    .anaRightStack{display:grid;grid-template-rows:1fr auto;gap:14px;min-width:0;height:100%}
    .anaRightStack>.card{height:100%}
    .anaSysCard{height:100%;display:flex;flex-direction:column}
    .anaSysCard .miniKpisV{flex:0 0 auto}
    .anaSysCard .anaAlertsWrap{margin-top:auto}
    .anaFillCard{height:100%;display:flex;flex-direction:column}
    .anaFillCard .chartHost{flex:1 1 auto;min-height:260px}
    .anaFillCard .chartHost .groupGrid{height:200px}
    .anaFillCard .chartHost .gBars{height:160px}
    .anaFillCard .hBars{flex:1 1 auto}

    .empList{display:flex;flex-direction:column;gap:10px}
    .empCard{border:1px solid rgba(226,232,240,.95);border-radius:14px;background:linear-gradient(180deg,#ffffff,#f8fafc);padding:10px 12px;display:grid;grid-template-columns:minmax(0,1fr) 92px;gap:10px;align-items:center}
    .empName{font-size:12px;font-weight:800;color:#0f172a;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .empDept{font-size:12px;color:#64748b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-top:2px}
    .empPct{font-size:12px;color:#0f172a;font-weight:800;text-align:right}
    .empTrack{height:10px;border-radius:999px;background:rgba(226,232,240,.75);border:1px solid rgba(226,232,240,.95);overflow:hidden;margin-top:8px}
    .empFill{height:100%;border-radius:999px;background:linear-gradient(90deg,#163A70,#2BB7A9)}
    #anaTopEmployees{max-height:260px;overflow:auto;padding-right:2px}

    .gNum{font-size:10px;color:rgba(100,116,139,.95);text-align:center;margin-bottom:6px;font-variant-numeric:tabular-nums}
    .calHead{display:flex;align-items:flex-end;justify-content:space-between;gap:10px;margin-bottom:10px}
    .calTitle{font-weight:900;color:#0f172a;font-size:14px;letter-spacing:.02em}
    .calSub{color:#64748b;font-size:12px;margin-top:2px}
    .monthInput{padding:6px 10px;border-radius:999px;border:1px solid rgba(226,232,240,.95);background:#fff;color:#0f172a;font-weight:800;font-size:12px;font-family:inherit}
    .calGrid{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:6px}
    .calDow{color:#64748b;font-size:11px;text-transform:uppercase;letter-spacing:.08em;text-align:center;padding:6px 0}
    .calGrid{display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:6px;grid-auto-rows:1fr}
    .calCell{border:1px solid rgba(226,232,240,.9);border-radius:10px;background:#f8fafc;min-height:82px;position:relative;overflow:hidden}
    .calCell.off{background:transparent;border-color:transparent}
    .calCell .d{position:absolute;left:8px;top:6px;font-size:11px;color:#64748b}
    .calCell .v{position:absolute;left:0;right:0;top:50%;transform:translateY(-45%);text-align:center;font-weight:900;font-variant-numeric:tabular-nums;color:#0f172a}
    .calCell.z0{background:#f8fafc}
    .calCell.z1{background:#fef3c7}
    .calCell.z2{background:#fde68a}
    .calCell.z3{background:#fdba74}
    .calCell.z4{background:#fb923c;color:#0f172a}
    .calCell.z5{background:#f97316;color:#0f172a}
    .calCell.future{background:linear-gradient(180deg,#f8fafc,#f1f5f9);opacity:.55}
    .miniKpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
    @media (max-width: 900px){.miniKpis{grid-template-columns:repeat(2,minmax(0,1fr))}}
    @media (max-width: 520px){.miniKpis{grid-template-columns:1fr}}
    .miniKpisV{display:flex;flex-direction:column;gap:12px}
    #subtab-summary-attendance .card{position:relative;border-radius:18px;border-color:rgba(148,163,184,.55);box-shadow:0 14px 40px rgba(2,6,23,.08);background:linear-gradient(180deg,#ffffff,#f8fafc)}
    #subtab-summary-attendance .card::before{content:'';position:absolute;left:0;right:0;top:0;height:4px;background:linear-gradient(90deg,rgba(22,58,112,.92),rgba(43,183,169,.86));opacity:.9}
    #tab-staffRecords table th,#tab-staffRecords table td{white-space:normal;overflow-wrap:anywhere;word-break:break-word}
    #subtab-staff-staff table th:nth-child(1),#subtab-staff-staff table td:nth-child(1){width:44px}
    #subtab-staff-staff table th:nth-child(2),#subtab-staff-staff table td:nth-child(2){width:90px;white-space:nowrap}
    #subtab-staff-staff table th:nth-child(6),#subtab-staff-staff table td:nth-child(6){white-space:nowrap}
    #subtab-staff-staff table th:nth-child(7),#subtab-staff-staff table td:nth-child(7){white-space:nowrap}
    #subtab-staff-staff table th:nth-child(9),#subtab-staff-staff table td:nth-child(9){width:96px;white-space:nowrap}
    #subtab-staff-shift table th:nth-child(1),#subtab-staff-shift table td:nth-child(1){width:44px}
    #subtab-staff-shift table th:nth-child(7),#subtab-staff-shift table td:nth-child(7){width:56px;white-space:nowrap}
    .wdBox{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
    .wdItem{display:inline-flex;align-items:center;gap:4px;border:1px solid rgba(226,232,240,.95);background:#fff;border-radius:999px;padding:4px 8px;font-size:11px;color:#334155}
    .wdItem input{width:14px;height:14px}
    .wdItem span{font-weight:800;letter-spacing:.01em}
    #subtab-summary-attendance .title{color:#0f172a}
    #subtab-summary-attendance .title::after{content:'';display:block;height:2px;width:52px;margin-top:8px;border-radius:999px;background:linear-gradient(90deg,rgba(22,58,112,.92),rgba(43,183,169,.86))}

    .miniKpi{--kpi:#2563eb;background:linear-gradient(180deg,#ffffff,var(--panel2));border:1px solid rgba(148,163,184,.55);border-left:5px solid var(--kpi);border-radius:16px;padding:14px;min-width:0;position:relative;overflow:hidden;box-shadow:0 10px 30px rgba(2,6,23,.06)}
    .miniKpi::after{content:'';position:absolute;right:-46px;top:-46px;width:140px;height:140px;border-radius:999px;background:radial-gradient(circle at 40% 40%,color-mix(in srgb,var(--kpi) 26%,transparent),transparent 62%);pointer-events:none}
    .miniKpi:nth-child(1){--kpi:#2563eb}
    .miniKpi:nth-child(2){--kpi:#0ea5e9}
    .miniKpi:nth-child(3){--kpi:#ef4444}
    .miniKpi:nth-child(4){--kpi:#8b5cf6}
    .miniKpi:nth-child(5){--kpi:#f59e0b}
    .miniKpi:nth-child(6){--kpi:#f97316}
    .miniKpi:nth-child(7){--kpi:#10b981}
    .miniKpi:nth-child(8){--kpi:#14b8a6}
    .miniKpiTitle{font-size:12px;font-weight:700;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}
    .miniKpiVal{font-size:26px;font-weight:800;margin-top:6px}
    .miniKpiMeta{font-size:12px;color:var(--muted);margin-top:6px}
    .split2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
    @media (max-width: 900px){.split2{grid-template-columns:1fr}}
    .segBtns{display:inline-flex;border:1px solid var(--border);border-radius:999px;overflow:hidden;background:linear-gradient(180deg,#ffffff,var(--panel2))}
    .segBtn{appearance:none;border:0;background:transparent;padding:8px 12px;font-size:12px;color:var(--muted);cursor:pointer}
    .segBtn:hover{background:rgba(15,23,42,.04)}
    .segBtn.active{background:linear-gradient(90deg,var(--accent),var(--accent2));color:#fff;font-weight:800}
    .hBars{display:flex;flex-direction:column;gap:8px}
    .hRow{display:grid;grid-template-columns:minmax(120px,1fr) 1fr minmax(44px,64px);gap:10px;align-items:center}
    .hLab{font-size:12px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .hTrack{height:12px;border-radius:999px;background:var(--panel2);border:1px solid var(--border);overflow:hidden}
    .hFill{height:100%;border-radius:999px;background:linear-gradient(90deg,var(--accent),var(--accent2))}
    .hVal{font-size:12px;color:var(--muted);text-align:right}
    .donutGrid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
    @media (max-width: 900px){.donutGrid{grid-template-columns:1fr}}
    .donutCard{border:1px dashed rgba(148,163,184,.80);border-radius:16px;background:linear-gradient(180deg,#ffffff,#f8fafc);padding:12px;box-shadow:0 10px 30px rgba(2,6,23,.05)}
    .donutTitle{font-size:12px;font-weight:800;letter-spacing:.06em;text-transform:uppercase;color:var(--text);margin:0 0 10px}
    .donutWrap{display:grid;grid-template-columns:120px minmax(0,1fr);gap:12px;align-items:center}
    .donut{width:120px;height:120px;border-radius:999px;background:conic-gradient(var(--accent) 0 25%, var(--accent2) 25% 50%, rgba(220,38,38,.55) 50% 75%, rgba(22,163,74,.55) 75% 100%);position:relative;box-shadow:0 14px 30px rgba(2,6,23,.10)}
    .donut::after{content:'';position:absolute;inset:22px;border-radius:999px;background:#fff;border:1px solid var(--border)}
    .donutCenter{position:absolute;inset:0;display:grid;place-items:center;font-weight:900;font-size:16px;color:var(--text)}
    .donutLegend{display:flex;flex-direction:column;gap:6px;min-width:0}
    .donutItem{display:flex;align-items:center;justify-content:space-between;gap:10px;font-size:12px;color:var(--muted)}
    .sw{width:10px;height:10px;border-radius:3px;flex:0 0 auto}
    .gaugeBox{display:flex;flex-direction:column;align-items:center;gap:8px}
    .gauge{position:relative;width:180px;height:124px;overflow:hidden}
    .gaugeArc{position:absolute;left:0;right:0;bottom:-72px;width:180px;height:180px;border-radius:999px;background:conic-gradient(from 180deg, #e5e7eb 0deg 180deg, transparent 180deg 360deg)}
    .gaugeArc::after{content:'';position:absolute;inset:22px;border-radius:999px;background:#fff;border:1px solid var(--border)}
    .gaugeNeedle{position:absolute;left:50%;bottom:0;width:4px;height:72px;background:#334155;border-radius:999px;transform-origin:50% 100%;transform:translateX(-50%) rotate(var(--gNeedle));box-shadow:0 1px 2px rgba(15,23,42,.25)}
    .gaugeHub{position:absolute;left:50%;bottom:0;width:12px;height:12px;background:#334155;border-radius:999px;transform:translate(-50%,50%)}
    .gaugeMin,.gaugeMax{position:absolute;bottom:0;font-size:11px;color:#64748b;font-variant-numeric:tabular-nums}
    .gaugeMin{left:2px}
    .gaugeMax{right:2px}
    .gaugeValue{font-weight:900;font-size:20px;color:var(--text);font-variant-numeric:tabular-nums}
    .sparkWrap{width:100%;height:220px;border:1px solid var(--border);border-radius:14px;background:linear-gradient(180deg,#ffffff,var(--panel2));padding:10px}
    .sparkTitle{font-size:12px;font-weight:800;letter-spacing:.06em;text-transform:uppercase;color:var(--text);margin:0 0 8px}
    .sparkSvg{width:100%;height:180px;display:block}
    @media (max-width: 900px){.charts{grid-template-columns:1fr}}
    @media (max-width: 900px){.grid{grid-template-columns:1fr}.formGrid{grid-template-columns:1fr}}
    @media (max-width: 700px){.summaryWrap .kpis{grid-template-columns:1fr}}

    .appShell{display:flex;min-height:100vh;background:var(--bg)}
    .sideBar{width:280px;flex:0 0 auto;display:flex;flex-direction:column;gap:14px;padding:16px 14px;background:#ffffff;border-right:1px solid var(--border);position:sticky;top:0;height:100vh;overflow:auto;transition:width .22s ease,padding .22s ease}
    body.sideCollapsed .sideBar{width:72px;padding:16px 10px}
    .sideBrand{display:flex;align-items:center;gap:10px;padding:8px 8px;border-radius:14px;background:linear-gradient(135deg,rgba(22,58,112,.08),rgba(43,183,169,.10))}
    .sideLogo{width:38px;height:38px;object-fit:contain;flex:0 0 auto}
    .sideBrandText{min-width:0}
    .sideBrandName{font-family:var(--fontHead);font-weight:900;color:var(--navy);font-size:13px;line-height:1.1}
    .sideBrandTag{color:var(--muted);font-size:11px;line-height:1.1;margin-top:3px}
    body.sideCollapsed .sideBrandText{display:none}
    .sideNav{display:flex;flex-direction:column;gap:4px}
    .navSection{margin-top:10px;padding-top:10px;border-top:1px solid var(--border)}
    .navBtn{width:100%;display:flex;align-items:center;gap:10px;padding:10px 10px;border-radius:12px;border:1px solid transparent;background:transparent;color:#1F2937;cursor:pointer;text-align:left;font-size:13px;font-weight:700;transition:background .12s ease,color .12s ease,border-color .12s ease}
    .navBtn:hover{background:rgba(15,23,42,.04)}
    .navBtn.active{background:var(--activeBg);color:var(--activeText);border-color:rgba(43,183,169,.30)}
    .navBtn svg{width:18px;height:18px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;flex:0 0 auto}
    .navLabel{min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    body.sideCollapsed .navLabel{display:none}
    .navChev{margin-left:auto;opacity:.75;transition:transform .18s ease}
    body.sideCollapsed .navChev{display:none}
    .navGroup.open>.navBtn .navChev{transform:rotate(90deg)}
    .navChildren{display:none;flex-direction:column;gap:2px;padding-left:30px;margin-top:2px}
    body.sideCollapsed .navChildren{padding-left:0}
    .navGroup.open .navChildren{display:flex}
    .navChild{font-size:12px;font-weight:700;padding:9px 10px}
    body.sideCollapsed .navChild{height:40px;padding:0;border:1px solid rgba(22,58,112,.16);background:rgba(22,58,112,.06);border-radius:12px;display:flex;align-items:center;justify-content:center}
    body.sideCollapsed .navChild::before{content:'';width:10px;height:10px;border-radius:999px;background:rgba(100,116,139,.85);box-shadow:0 0 0 4px rgba(22,58,112,.10)}
    body.sideCollapsed .navChild.active{background:var(--activeBg);border-color:rgba(43,183,169,.55)}
    body.sideCollapsed .navChild.active::before{background:rgba(43,183,169,.92);box-shadow:0 0 0 4px rgba(22,58,112,.10)}
    .mainCol{flex:1;min-width:0;display:flex;flex-direction:column}
    .topBar{position:sticky;top:0;z-index:2000;display:flex;align-items:center;gap:12px;padding:12px 16px;background:linear-gradient(90deg,var(--navy),var(--teal));color:#fff;border-bottom:1px solid rgba(15,23,42,.12)}
    .topBar .iconBtn{border-color:rgba(255,255,255,.35);background:rgba(255,255,255,.10);color:#fff}
    .topBar .iconBtn:hover{background:rgba(255,255,255,.16);border-color:rgba(255,255,255,.45)}
    .topTitleWrap{min-width:0}
    .topTitle{font-family:var(--fontHead);font-weight:900;font-size:14px;line-height:1.1}
    .topSub{font-size:12px;opacity:.9;line-height:1.1;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
    .topRight{margin-left:auto;display:flex;align-items:center;gap:10px}
    .content{max-width:1200px;margin:0 auto;padding:16px 16px 26px;min-width:0;width:100%}

    .syncCards{display:flex;gap:12px;align-items:stretch}
    @media (max-width: 900px){.syncCards{width:100%;justify-content:flex-start;flex-wrap:wrap}}
    .syncCard{min-width:170px;border:1px solid var(--border);background:linear-gradient(180deg,#ffffff,var(--panel2));border-radius:14px;padding:12px;box-shadow:0 10px 28px rgba(2,6,23,.06)}
    .syncCardTitle{font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--muted)}
    .syncCardVal{margin-top:8px;display:flex;align-items:center;gap:8px}
    .syncCardVal .pill{height:24px;padding:0 10px;border-radius:999px;font-weight:900}
    .syncCardVal .pill.ok{background:rgba(43,183,169,.12);border-color:rgba(43,183,169,.35);color:var(--navy)}
    .syncCardVal .pill.bad{background:rgba(220,38,38,.10);border-color:rgba(220,38,38,.25);color:#7f1d1d}

    .cardHead{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:-14px -14px 12px;padding:12px 14px;border-bottom:1px solid var(--border);background:linear-gradient(90deg,rgba(22,58,112,.10),rgba(43,183,169,.10))}
    .cardHeadTitle{font-family:var(--fontHead);font-weight:900;color:var(--navy);font-size:13px;letter-spacing:.08em;text-transform:uppercase}
    .filterBar{display:flex;align-items:center;gap:10px;flex-wrap:wrap;background:linear-gradient(180deg,#ffffff,var(--panel2));border:1px solid var(--border);border-radius:14px;padding:10px 12px;margin-bottom:10px}
    .filterBar label{font-size:11px;font-weight:800;letter-spacing:.08em;text-transform:uppercase;color:var(--muted)}
    .dateInput{height:38px;padding:8px 10px;border-radius:12px;border:1px solid var(--border2);background:#ffffff;font-family:inherit;font-size:12px}
    .filterBar .btn{height:38px}

    .prettyTable{width:100%;border-collapse:separate;border-spacing:0;font-size:12.5px}
    .prettyTable thead th{position:sticky;top:0;background:linear-gradient(90deg,rgba(22,58,112,.10),rgba(43,183,169,.10));border-bottom:1px solid var(--border);padding:12px 12px;text-align:left;color:#0f172a;font-weight:900}
    .prettyTable thead th:first-child{border-top-left-radius:14px}
    .prettyTable thead th:last-child{border-top-right-radius:14px}
    .prettyTable tbody td{padding:12px 12px;border-bottom:1px solid rgba(226,232,240,.9);background:#fff}
    .prettyTable tbody tr:hover td{background:rgba(22,58,112,.03)}
    .prettyTable tbody tr:last-child td:first-child{border-bottom-left-radius:14px}
    .prettyTable tbody tr:last-child td:last-child{border-bottom-right-radius:14px}
    .prettyTable th.sortable{cursor:pointer;user-select:none}
    .prettyTable th.sortable::after{content:'↕';margin-left:8px;color:rgba(15,23,42,.35);font-weight:900}
    .prettyTable th.sortable.sortedAsc::after{content:'▲';color:rgba(22,58,112,.75)}
    .prettyTable th.sortable.sortedDesc::after{content:'▼';color:rgba(22,58,112,.75)}

    .sectionHead{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:14px;margin-bottom:10px;padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:linear-gradient(90deg,rgba(22,58,112,.08),rgba(43,183,169,.08))}
    .sectionHeadTitle{font-family:var(--fontHead);font-weight:900;color:var(--navy);font-size:12px;letter-spacing:.08em;text-transform:uppercase}

    .formGrid.compactLabels{grid-template-columns:120px minmax(0,1fr)}
    .bulletList{margin:8px 0 0 18px;padding:0;color:var(--muted);font-size:12px}
    .bulletList li{margin:6px 0;overflow-wrap:anywhere;word-break:break-word}
    .timeRow{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
    .timeInput{height:38px;border-radius:12px;border:1px solid var(--border2);background:#ffffff;padding:8px 10px;font-family:inherit;font-size:12px}
    .timeCards{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}
    @media (max-width: 1100px){.timeCards{grid-template-columns:repeat(2,minmax(0,1fr))}}
    @media (max-width: 520px){.timeCards{grid-template-columns:1fr}}
    .timeCard{border:1px solid #e5e7eb;background:linear-gradient(180deg,#ffffff,#f8fafc);border-radius:12px;padding:10px 12px;display:flex;align-items:center;justify-content:space-between;gap:10px;min-width:0}
    .timeCardVal{font-weight:600;color:#0f172a;font-variant-numeric:tabular-nums;letter-spacing:.02em}
    .timeCardBtns{display:flex;gap:8px;align-items:center}

    .toast{position:fixed;right:18px;bottom:18px;z-index:5000;min-width:240px;max-width:360px;border-radius:14px;padding:12px 14px;border:1px solid rgba(226,232,240,.9);background:#ffffff;box-shadow:0 18px 48px rgba(2,6,23,.18);display:none}
    .toast.show{display:block;animation:toastIn .12s ease-out}
    .toast.ok{border-color:rgba(43,183,169,.38);background:linear-gradient(180deg,#ffffff,rgba(43,183,169,.08))}
    .toast.bad{border-color:rgba(220,38,38,.25);background:linear-gradient(180deg,#ffffff,rgba(220,38,38,.06))}
    .toastTitle{font-weight:900;color:var(--navy);font-size:12px;letter-spacing:.08em;text-transform:uppercase}
    .toastMsg{margin-top:6px;color:#0f172a;font-size:13px;line-height:1.25}
    @keyframes toastIn{from{transform:translateY(8px);opacity:.0}to{transform:translateY(0);opacity:1}}
  </style>
</head>
<body>
  <div class="appShell">
    <aside class="sideBar" id="sideBar">
      <div class="sideBrand">
        <img class="sideLogo" src="/assets/logo.png" alt="SHAB Attendance Dashboard" />
        <div class="sideBrandText">
          <div class="sideBrandName">SHAB Attendance System</div>
          <div class="sideBrandTag">Attendance Dashboard</div>
        </div>
      </div>

      <nav class="sideNav" id="sideNav" aria-label="Sidebar">
        <button class="navBtn" type="button" data-tab="summary" data-subtab-group="summary" data-subtab="attendance">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 10.5l8-6 8 6V20a1 1 0 0 1-1 1h-5v-7H10v7H5a1 1 0 0 1-1-1v-9.5z"/></svg>
          <span class="navLabel">Summary</span>
        </button>

        <div class="navGroup" data-group="sheet">
          <button class="navBtn" type="button" data-accordion="sheet">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 3h10a2 2 0 0 1 2 2v3H5V5a2 2 0 0 1 2-2z"/><path d="M5 8h14v13a1 1 0 0 1-1 1H6a1 1 0 0 1-1-1V8z"/><path d="M7 12h3M7 16h3M12 12h3M12 16h3"/></svg>
            <span class="navLabel">Attendance Spreadsheet</span>
            <svg class="navChev" viewBox="0 0 24 24" aria-hidden="true"><path d="M9 18l6-6-6-6"/></svg>
          </button>
          <div class="navChildren">
            <button class="navBtn navChild" type="button" data-tab="attendanceSpreadsheet" data-subtab-group="sheet" data-subtab="daily"><span class="navLabel">Daily</span></button>
            <button class="navBtn navChild" type="button" data-tab="attendanceSpreadsheet" data-subtab-group="sheet" data-subtab="weekly"><span class="navLabel">Weekly</span></button>
            <button class="navBtn navChild" type="button" data-tab="attendanceSpreadsheet" data-subtab-group="sheet" data-subtab="monthly"><span class="navLabel">Monthly</span></button>
          </div>
        </div>

        <div class="navGroup" data-group="staff">
          <button class="navBtn" type="button" data-accordion="staff">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><path d="M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8z"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
            <span class="navLabel">Staff Records</span>
            <svg class="navChev" viewBox="0 0 24 24" aria-hidden="true"><path d="M9 18l6-6-6-6"/></svg>
          </button>
          <div class="navChildren">
            <button class="navBtn navChild" type="button" data-tab="staffRecords" data-subtab-group="staff" data-subtab="staff"><span class="navLabel">Employee List</span></button>
            <button class="navBtn navChild" type="button" data-tab="staffRecords" data-subtab-group="staff" data-subtab="shift"><span class="navLabel">Shift Pattern</span></button>
          </div>
        </div>

        <div class="navGroup" data-group="raw">
          <button class="navBtn" type="button" data-accordion="raw">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 4h16v6H4z"/><path d="M4 14h16v6H4z"/><path d="M8 7h.01M8 17h.01"/></svg>
            <span class="navLabel">Raw Data</span>
            <svg class="navChev" viewBox="0 0 24 24" aria-hidden="true"><path d="M9 18l6-6-6-6"/></svg>
          </button>
          <div class="navChildren">
            <button class="navBtn navChild" type="button" data-tab="rawData" data-subtab-group="raw" data-subtab="device"><span class="navLabel">Device Records</span></button>
            <button class="navBtn navChild" type="button" data-tab="rawData" data-subtab-group="raw" data-subtab="db"><span class="navLabel">Database Records</span></button>
          </div>
        </div>

        <div class="navSection"></div>

        <div class="navGroup" data-group="settings">
          <button class="navBtn" type="button" data-accordion="settings">
            <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 15.5a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7z"/><path d="M19.4 15a7.8 7.8 0 0 0 .1-1 7.8 7.8 0 0 0-.1-1l2.1-1.6-2-3.4-2.5 1a7.3 7.3 0 0 0-1.7-1L15 2h-6l-.4 2.9a7.3 7.3 0 0 0-1.7 1l-2.5-1-2 3.4L4.6 12a7.8 7.8 0 0 0-.1 1 7.8 7.8 0 0 0 .1 1L2.5 15.6l2 3.4 2.5-1a7.3 7.3 0 0 0 1.7 1L9 22h6l.4-2.9a7.3 7.3 0 0 0 1.7-1l2.5 1 2-3.4L19.4 15z"/></svg>
            <span class="navLabel">Settings</span>
            <svg class="navChev" viewBox="0 0 24 24" aria-hidden="true"><path d="M9 18l6-6-6-6"/></svg>
          </button>
          <div class="navChildren">
            <button class="navBtn navChild" type="button" data-tab="settings" data-subtab-group="settings" data-subtab="connection"><span class="navLabel">Connection</span></button>
            <button class="navBtn navChild" type="button" data-tab="settings" data-subtab-group="settings" data-subtab="logs"><span class="navLabel">Logs</span></button>
          </div>
        </div>
      </nav>
    </aside>

    <div class="mainCol">
      <header class="topBar">
        <button class="iconBtn" id="sideToggle" type="button" title="Toggle sidebar">
          <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 6h16M4 12h16M4 18h16"/></svg>
        </button>
        <div class="topTitleWrap">
          <div class="topTitle" id="topTitle">Summary</div>
          <div class="topSub" id="topSub">Attendance Analysis</div>
        </div>
        <div class="topRight">
          <form method="post" action="/logout"><button class="btn iconBtn" type="submit" title="Logout"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M10 17l1 1h8a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-8l-1 1"/><path d="M15 12H3"/><path d="M6 9l-3 3 3 3"/></svg></button></form>
        </div>
      </header>

      <main class="content">
    <div class="tabs" role="tablist" aria-label="Dashboard tabs">
      <button class="tabBtn active" id="tabBtn-summary" type="button" data-tab="summary">Summary</button>
      <button class="tabBtn" id="tabBtn-attendanceSpreadsheet" type="button" data-tab="attendanceSpreadsheet">Attendance Spreadsheet</button>
      <button class="tabBtn" id="tabBtn-staffRecords" type="button" data-tab="staffRecords">Staff Records</button>
      <button class="tabBtn" id="tabBtn-rawData" type="button" data-tab="rawData">Raw Data</button>
      <button class="tabBtn" id="tabBtn-settings" type="button" data-tab="settings">Settings</button>
    </div>

    <section class="tabPanel active" id="tab-summary" role="tabpanel" aria-labelledby="tabBtn-summary">
      <div class="summaryWrap">
        <div class="summaryHeader">
          <div>
            <div class="summaryTitle">Device &amp; Sync Summary</div>
            <div class="summarySub">Attendance and database overview</div>
          </div>
          <div class="syncCards">
            <div class="syncCard">
              <div class="syncCardTitle">Device Sync</div>
              <div class="syncCardVal"><span class="pill" id="sumDeviceSyncPill">Unknown</span></div>
            </div>
            <div class="syncCard">
              <div class="syncCardTitle">Database Sync</div>
              <div class="syncCardVal"><span class="pill" id="sumDbSyncPill">Unknown</span></div>
            </div>
          </div>
        </div>
        <div class="subTabs" role="tablist" aria-label="Summary sub-tabs">
          <button class="subTabBtn active" type="button" data-subtab-group="summary" data-subtab="attendance">Attendance Analysis</button>
        </div>

        <div class="subTabPanel active" data-subtab-group="summary" id="subtab-summary-attendance">
          <div class="grid" style="margin:0">
            <div class="card" style="grid-column:1/-1">
              <div class="title">Executive Snapshot</div>
              <div class="analysisFilters">
                <label for="anaDate">Date</label>
                <input id="anaDate" type="date" lang="en-GB" />
                <label for="anaDept">Department</label>
                <select id="anaDept"><option value="All">All</option></select>
                <button class="btn" id="anaRefresh" type="button">Refresh</button>
              </div>
              <div class="muted" id="anaNote" style="display:none"></div>
              <div class="snapGrid" id="anaSnapGrid">
                <button class="snapCard" type="button" data-go="sheetDaily">
                  <div class="snapTop"><div class="snapTitle">Employee</div><div class="snapGo">↗</div></div>
                  <div class="snapVal" id="anaSnapTotal">-</div>
                  <div class="snapMeta" id="anaSnapScope">-</div>
                </button>
                <button class="snapCard" type="button" data-go="sheetDaily">
                  <div class="snapTop"><div class="snapTitle">Present</div><div class="snapGo">↗</div></div>
                  <div class="snapVal" id="anaSnapPresent">-</div>
                  <div class="snapMeta" id="anaSnapPresentMeta">Today</div>
                </button>
                <button class="snapCard" type="button" data-go="sheetDaily">
                  <div class="snapTop"><div class="snapTitle">Absent</div><div class="snapGo">↗</div></div>
                  <div class="snapVal" id="anaSnapAbsent">-</div>
                  <div class="snapMeta" id="anaSnapAbsentMeta">Today</div>
                </button>
                <button class="snapCard" type="button" data-go="sheetDaily">
                  <div class="snapTop"><div class="snapTitle">Late Comers</div><div class="snapGo">↗</div></div>
                  <div class="snapVal" id="anaSnapLate">-</div>
                  <div class="snapMeta">After 09:15</div>
                </button>

                <button class="snapCard" type="button" data-go="sheetDaily">
                  <div class="snapTop"><div class="snapTitle">Attendance %</div><div class="snapGo">↗</div></div>
                  <div class="snapVal" id="anaSnapAttPct">-</div>
                  <div class="snapMeta">Rate</div>
                </button>
                <button class="snapCard" type="button" data-go="sheetDaily">
                  <div class="snapTop"><div class="snapTitle">Absenteeism %</div><div class="snapGo">↗</div></div>
                  <div class="snapVal" id="anaSnapAbsPct">-</div>
                  <div class="snapMeta">Rate</div>
                </button>
                <button class="snapCard" type="button" data-go="sheetDaily">
                  <div class="snapTop"><div class="snapTitle">Average Work Hours</div><div class="snapGo">↗</div></div>
                  <div class="snapVal" id="anaSnapAvgWork">-</div>
                  <div class="snapMeta">Per present employee</div>
                </button>
                <button class="snapCard" type="button" data-go="sheetDaily">
                  <div class="snapTop"><div class="snapTitle">Expected Work Hours</div><div class="snapGo">↗</div></div>
                  <div class="snapVal" id="anaSnapExpHours">-</div>
                  <div class="snapMeta">Present / 100% attendance</div>
                </button>
              </div>
            </div>

            <div class="anaTopSplit">
              <div class="anaLeftStack">
                <div class="card">
                  <div class="cardHead">
                    <div class="cardHeadTitle">Calendar</div>
                    <div class="row" style="gap:8px">
                      <input id="anaCalMonth" class="monthInput" type="month" />
                    </div>
                  </div>
                  <div class="hint" id="anaCalSub" style="margin-bottom:8px">Count of staff present for each day</div>
                  <div class="calGrid" id="anaCalDow">
                    <div class="calDow">Sun</div><div class="calDow">Mon</div><div class="calDow">Tue</div><div class="calDow">Wed</div><div class="calDow">Thu</div><div class="calDow">Fri</div><div class="calDow">Sat</div>
                  </div>
                  <div style="height:6px"></div>
                  <div class="calGrid" id="anaCalGrid"></div>
                </div>

                <div class="card anaFillCard">
                  <div class="title">Last 7 Days Attendance</div>
                  <div class="chartHost" id="anaAttendDays"></div>
                </div>
              </div>

              <div class="anaRightStack">
                <div class="card anaSysCard">
                  <div class="title">System &amp; Alerts</div>
                  <div class="snapCol">
                    <button class="snapCard teal" type="button" data-go="sheetDaily">
                      <div class="snapTop"><div class="snapTitle">Database Punches</div><div class="snapGo">↗</div></div>
                      <div class="snapVal" id="anaSysDbPunches">-</div>
                      <div class="snapMeta">Today</div>
                    </button>
                    <button class="snapCard teal" type="button" data-go="sheetDaily">
                      <div class="snapTop"><div class="snapTitle">Flagged Punches</div><div class="snapGo">↗</div></div>
                      <div class="snapVal" id="anaSysFlagged">-</div>
                      <div class="snapMeta">Missing OUT + duplicates</div>
                    </button>
                    <button class="snapCard teal" type="button" data-go="sheetDaily">
                      <div class="snapTop"><div class="snapTitle">Last Punch</div><div class="snapGo">↗</div></div>
                      <div class="snapVal" id="anaSysLastPunch">-</div>
                      <div class="snapMeta">Selected day</div>
                    </button>
                    <button class="snapCard teal" type="button" data-go="sheetDaily">
                      <div class="snapTop"><div class="snapTitle">Notification</div><div class="snapGo">↗</div></div>
                      <div class="snapVal note" id="anaSysNotifVal">-</div>
                    </button>
                  </div>
                </div>

                <div class="card anaFillCard">
                  <div class="title">Top Employees</div>
                  <div id="anaTopEmployees" class="hBars"></div>
                </div>
              </div>
            </div>

            <div class="card" style="grid-column:1/-1">
              <div class="title">KPI Status</div>
              <div class="donutGrid">
                <div class="donutCard">
                  <div class="donutTitle">Attendance Rate</div>
                  <div class="gaugeBox">
                    <div class="gauge" id="anaGaugeAttendance"></div>
                    <div class="gaugeValue" id="anaGaugeAttendanceVal">-</div>
                  </div>
                </div>
                <div class="donutCard">
                  <div class="donutTitle">Working Hours (Avg)</div>
                  <div class="gaugeBox">
                    <div class="gauge" id="anaGaugeAvgHours"></div>
                    <div class="gaugeValue" id="anaGaugeAvgHoursVal">-</div>
                  </div>
                </div>
                <div class="donutCard">
                  <div class="donutTitle">Present Month Working Hours</div>
                  <div class="gaugeBox">
                    <div class="gauge" id="anaGaugeMonthHours"></div>
                    <div class="gaugeValue" id="anaGaugeMonthHoursVal">-</div>
                  </div>
                </div>
              </div>
            </div>

            <div class="card" style="grid-column:1/-1">
              <div class="title">Today Per-Hour</div>
              <div class="legend">
                <span class="legItem"><span class="dot db"></span>Database</span>
              </div>
              <div class="chartHost" id="barsDb"></div>
            </div>

            <div class="card" style="grid-column:1/-1">
              <div class="title">Department</div>
              <div class="analysisFilters" style="margin-bottom:10px">
                <div class="segBtns" id="anaDeptPeriod">
                  <button class="segBtn active" type="button" data-period="day">Day</button>
                  <button class="segBtn" type="button" data-period="week">Week</button>
                  <button class="segBtn" type="button" data-period="month">Month</button>
                </div>
                <div class="muted" id="anaDeptPeriodHint" style="font-size:12px"></div>
              </div>
              <div class="split2">
                <div>
                  <div class="hint" id="anaDeptCountHint" style="margin-bottom:8px">Attendance by department (count)</div>
                  <div id="anaDeptCount" class="hBars"></div>
                </div>
                <div>
                  <div class="hint" id="anaDeptRateHint" style="margin-bottom:8px">Attendance rate by department (%)</div>
                  <div id="anaDeptRate" class="hBars"></div>
                </div>
              </div>
            </div>

            <div class="card" style="grid-column:1/-1">
              <div class="title">Absence &amp; Late</div>
              <div class="donutGrid">
                <div class="donutCard">
                  <div class="donutTitle">Absentees — Last 7 Days</div>
                  <div class="donutWrap">
                    <div class="donut" id="anaDonutAbsent7"><div class="donutCenter" id="anaDonutAbsent7Center">-</div></div>
                    <div class="donutLegend" id="anaDonutAbsent7Legend"></div>
                  </div>
                </div>
                <div class="donutCard">
                  <div class="donutTitle">Late Comers — Last 7 Days</div>
                  <div class="donutWrap">
                    <div class="donut" id="anaDonutLate7"><div class="donutCenter" id="anaDonutLate7Center">-</div></div>
                    <div class="donutLegend" id="anaDonutLate7Legend"></div>
                  </div>
                </div>
                <div class="donutCard">
                  <div class="donutTitle">Absentees — By Month</div>
                  <div class="donutWrap">
                    <div class="donut" id="anaDonutAbsentM"><div class="donutCenter" id="anaDonutAbsentMCenter">-</div></div>
                    <div class="donutLegend" id="anaDonutAbsentMLegend"></div>
                  </div>
                </div>
              </div>
            </div>

            <div class="card" id="anaTrendsCard" style="grid-column:1/-1;margin-bottom:28px">
              <div class="title">Trends</div>
              <div class="split2">
                <div class="sparkWrap">
                  <div class="sparkTitle">Overall Attendance — Last 6 Months</div>
                  <svg class="sparkSvg" id="anaLineAttendance" viewBox="0 0 600 180" preserveAspectRatio="none"></svg>
                </div>
                <div class="sparkWrap">
                  <div class="sparkTitle">Absenteeism Rate — Last 6 Months</div>
                  <svg class="sparkSvg" id="anaAreaAbsentee" viewBox="0 0 600 180" preserveAspectRatio="none"></svg>
                </div>
              </div>
            </div>
          </div>
        </div>

      </div>
    </section>

    <section class="tabPanel" id="tab-attendanceSpreadsheet" role="tabpanel" aria-labelledby="tabBtn-attendanceSpreadsheet">
      <div class="summaryHeader">
        <div>
          <div class="summaryTitle">Attendance Spreadsheet</div>
          <div class="summarySub">Daily, weekly, and monthly views derived from punch logs.</div>
        </div>
      </div>
      <div class="subTabs" role="tablist" aria-label="Attendance spreadsheet sub-tabs">
        <button class="subTabBtn active" type="button" data-subtab-group="sheet" data-subtab="daily">Daily</button>
        <button class="subTabBtn" type="button" data-subtab-group="sheet" data-subtab="weekly">Weekly</button>
        <button class="subTabBtn" type="button" data-subtab-group="sheet" data-subtab="monthly">Monthly</button>
      </div>

      <div class="subTabPanel active" data-subtab-group="sheet" id="subtab-sheet-daily">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="cardHead">
              <div class="cardHeadTitle">Daily</div>
            </div>
            <div class="filterBar">
              <label for="sheetDailyFrom">From</label><input class="dateInput" id="sheetDailyFrom" type="date" lang="en-GB" />
              <label for="sheetDailyTo">To</label><input class="dateInput" id="sheetDailyTo" type="date" lang="en-GB" />
              <button class="btn primary" id="sheetDailyRefresh" type="button">Refresh</button>
              <button class="btn" id="sheetDailyDownload" type="button" style="display:none">Download Table</button>
            </div>
            <table class="prettyTable" id="sheetDailyTable">
              <thead>
                <tr>
                  <th class="sortable" data-sort="name">Name</th>
                  <th class="sortable" data-sort="date">Date</th>
                  <th class="sortable" data-sort="shift">Shift</th>
                  <th class="sortable" data-sort="first_in">First In</th>
                  <th class="sortable" data-sort="last_out">Last Out</th>
                  <th class="sortable" data-sort="total_hours">Total Hours</th>
                  <th class="sortable" data-sort="ot_hours">OT Hours</th>
                  <th class="sortable" data-sort="status">Status</th>
                  <th class="sortable" data-sort="flagged_punches">Flagged Punch</th>
                </tr>
              </thead>
              <tbody id="sheetDailyBody"></tbody>
            </table>
            <div class="hint">Click a row to view punch details.</div>
          </div>
        </div>
      </div>

      <div class="subTabPanel" data-subtab-group="sheet" id="subtab-sheet-weekly">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="cardHead">
              <div class="cardHeadTitle">Weekly</div>
            </div>
            <div class="filterBar">
              <label for="sheetWeeklyFrom">From</label><input class="dateInput" id="sheetWeeklyFrom" type="date" lang="en-GB" />
              <label for="sheetWeeklyTo">To</label><input class="dateInput" id="sheetWeeklyTo" type="date" lang="en-GB" />
              <button class="btn primary" id="sheetWeeklyRefresh" type="button">Refresh</button>
              <button class="btn" id="sheetWeeklyDownload" type="button" style="display:none">Download Table</button>
            </div>
            <table class="prettyTable" id="sheetWeeklyTable">
              <thead>
                <tr>
                  <th class="sortable" data-sort="name">Name</th>
                  <th class="sortable" data-sort="week_start">Week</th>
                  <th class="sortable" data-sort="flagged_punches">Flagged Punches</th>
                  <th class="sortable" data-sort="total_hours">Total Hours</th>
                  <th class="sortable" data-sort="ot_hours">OT Hours</th>
                  <th class="sortable" data-sort="days_present">Days Present</th>
                  <th class="sortable" data-sort="days_absent">Days Absent</th>
                  <th class="sortable" data-sort="attendance_pct">Attendance %</th>
                </tr>
              </thead>
              <tbody id="sheetWeeklyBody"></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="subTabPanel" data-subtab-group="sheet" id="subtab-sheet-monthly">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="cardHead">
              <div class="cardHeadTitle">Monthly</div>
            </div>
            <div class="filterBar">
              <label for="sheetMonthlyFrom">From</label><input class="dateInput" id="sheetMonthlyFrom" type="month" />
              <label for="sheetMonthlyTo">To</label><input class="dateInput" id="sheetMonthlyTo" type="month" />
              <button class="btn primary" id="sheetMonthlyRefresh" type="button">Refresh</button>
              <button class="btn" id="sheetMonthlyDownload" type="button" style="display:none">Download Table</button>
              <button class="btn" id="sheetMonthlyBulkDownload" type="button" style="display:none">Download Report</button>
            </div>
            <table class="prettyTable" id="sheetMonthlyTable">
              <thead>
                <tr>
                  <th style="width:44px"><input id="sheetMonthlySelectAll" type="checkbox" style="display:none" /></th>
                  <th class="sortable" data-sort="name">Name</th>
                  <th class="sortable" data-sort="month">Month</th>
                  <th class="sortable" data-sort="flagged_punches">Flagged Punches</th>
                  <th class="sortable" data-sort="total_hours">Total Hours</th>
                  <th class="sortable" data-sort="ot_hours">OT Hours</th>
                  <th class="sortable" data-sort="days_present">Days Present</th>
                  <th class="sortable" data-sort="days_absent">Days Absent</th>
                  <th class="sortable" data-sort="attendance_pct">Attendance %</th>
                  <th style="width:44px"></th>
                </tr>
              </thead>
              <tbody id="sheetMonthlyBody"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section class="tabPanel" id="tab-rawData" role="tabpanel" aria-labelledby="tabBtn-rawData">
      <div class="summaryHeader">
        <div>
          <div class="summaryTitle">Raw Data</div>
          <div class="summarySub">Inspect, filter, and export raw attendance records from device files and Supabase.</div>
        </div>
      </div>
      <div class="row toolbarRight" style="margin-bottom:10px">
        <select id="rawDevicePick" style="width:260px;flex:0 0 auto">
          <option value="all">All devices</option>
        </select>
      </div>
      <div class="subTabs" role="tablist" aria-label="Raw data sub-tabs">
        <button class="subTabBtn active" type="button" data-subtab-group="raw" data-subtab="device">Device Records</button>
        <button class="subTabBtn" type="button" data-subtab-group="raw" data-subtab="db">Database Records</button>
      </div>

      <div class="subTabPanel active" data-subtab-group="raw" id="subtab-raw-device">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="cardHead"><div class="cardHeadTitle">Device Records</div></div>
            <div class="row toolbarRight" style="margin-bottom:8px">
              <button class="btn" id="devCsv" type="button">Download Table</button>
              <button class="btn primary" id="devRefresh" type="button">Sync Device</button>
            </div>
            <div id="deviceLoadBox" class="loadRow" style="display:none">
              <div class="ring" id="deviceLoadRing" style="--pct:0%"><div class="ringText" id="deviceLoadPct">0%</div></div>
              <div class="loadText" id="deviceLoadText"><strong>Loading</strong></div>
            </div>
            <table class="prettyTable">
              <thead>
                <tr>
                  <th>Device</th>
                  <th>Staff ID</th>
                  <th>Date &amp; Time</th>
                  <th>Verified</th>
                  <th>Status</th>
                  <th>Work Code</th>
                  <th>Reserved</th>
                </tr>
              </thead>
              <tbody id="deviceBody"></tbody>
            </table>
            <div class="hint">WL10 reads 1_attlog.dat (6 columns). W30 reads AttendanceLog*.dat (4 columns). Use Sync Device (WL10) or Sync Database (W30 file import) to update.</div>
          </div>
        </div>
      </div>

      <div class="subTabPanel" data-subtab-group="raw" id="subtab-raw-db">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="cardHead"><div class="cardHeadTitle">Database Records (Supabase)</div></div>
            <div class="row toolbarRight" style="margin-bottom:8px">
              <button class="btn" id="dbCsv" type="button">Download Table</button>
              <button class="btn primary" id="dbUpdateSupabase" type="button">Sync Database</button>
            </div>
            <div id="dbUpdateBox" class="loadRow" style="display:none">
              <div class="ring" id="dbUpdateRing" style="--pct:0%"><div class="ringText" id="dbUpdatePct">0%</div></div>
              <div class="loadText" id="dbUpdateText"><strong>Updating</strong> -</div>
            </div>
            <table class="prettyTable">
              <thead>
                <tr>
                  <th>Staff ID</th>
                  <th>Date &amp; Time</th>
                  <th>Verified</th>
                  <th>Status</th>
                  <th>Work Code</th>
                  <th>Reserved</th>
                </tr>
              </thead>
              <tbody id="dbBody"></tbody>
            </table>
            <div class="hint">Shows rows from Supabase (read-only).</div>
          </div>
        </div>
      </div>
    </section>

    <section class="tabPanel" id="tab-staffRecords" role="tabpanel" aria-labelledby="tabBtn-staffRecords">
      <div class="summaryHeader">
        <div>
          <div class="summaryTitle">Staff Records</div>
          <div class="summarySub">Manage employee list and shift patterns used for attendance analytics.</div>
        </div>
      </div>
      <div class="subTabs" role="tablist" aria-label="Staff sub-tabs">
        <button class="subTabBtn active" type="button" data-subtab-group="staff" data-subtab="staff">Employee List</button>
        <button class="subTabBtn" type="button" data-subtab-group="staff" data-subtab="shift">Shift Pattern</button>
      </div>

      <div class="subTabPanel active" data-subtab-group="staff" id="subtab-staff-staff">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="cardHead"><div class="cardHeadTitle">Employee List</div></div>
            <div class="row toolbarRight" style="margin-bottom:8px">
              <button class="btn" id="staffImport" type="button" disabled>Import</button>
              <button class="btn" id="staffExport" type="button">Download</button>
              <button class="btn" id="staffAdd" type="button">Add</button>
              <button class="btn" id="staffProvisionSelected" type="button" style="display:none" disabled>Update to Device</button>
              <button class="btn" id="staffDelete" type="button" style="display:none" disabled>Delete</button>
            </div>
            <table class="prettyTable">
              <thead>
                <tr>
                  <th><input id="staffSelectAll" type="checkbox" style="display:none" /></th>
                  <th data-sort="user_id">User ID</th>
                  <th data-sort="full_name">First Name</th>
                  <th data-sort="role">Role</th>
                  <th data-sort="department">Department</th>
                  <th data-sort="status">Status</th>
                  <th data-sort="date_joined">Date Joined</th>
                  <th data-sort="shift_pattern">Shift Pattern</th>
                  <th></th>
                </tr>
              </thead>
              <tbody id="staffBody"></tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="subTabPanel" data-subtab-group="staff" id="subtab-staff-shift">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="cardHead"><div class="cardHeadTitle">Shift Pattern</div></div>
            <div class="row toolbarRight" style="margin-bottom:8px">
              <button class="btn" id="shiftImport" type="button" disabled>Import</button>
              <button class="btn" id="shiftExport" type="button" disabled>Download</button>
              <button class="btn" id="shiftAdd" type="button">Add</button>
              <button class="btn" id="shiftDelete" type="button" disabled>Delete</button>
            </div>
            <table class="prettyTable">
              <thead>
                <tr>
                  <th></th>
                  <th>Pattern</th>
                  <th>Working Days</th>
                  <th>Working Hours</th>
                  <th>Break</th>
                  <th>Notes</th>
                  <th></th>
                </tr>
              </thead>
              <tbody id="shiftBody"></tbody>
            </table>
            <div class="hint">Shift patterns are stored in Supabase when configured (fallback to local state if Supabase is not configured).</div>
          </div>
        </div>
      </div>
    </section>

    <section class="tabPanel" id="tab-settings" role="tabpanel" aria-labelledby="tabBtn-settings">
      <div class="summaryHeader">
        <div>
          <div class="summaryTitle">Settings</div>
          <div class="summarySub">Configure device connection, sync behavior, and Supabase integration for this middleware.</div>
        </div>
      </div>
      <div class="subTabs" role="tablist" aria-label="Settings sub-tabs">
        <button class="subTabBtn active" type="button" data-subtab-group="settings" data-subtab="connection">Connection</button>
        <button class="subTabBtn" type="button" data-subtab-group="settings" data-subtab="logs">Logs</button>
      </div>

      <div class="subTabPanel active" data-subtab-group="settings" id="subtab-settings-connection">
        <div class="grid" style="margin:0">
          <div class="card">
            <div class="cardHead"><div class="cardHeadTitle">Device Settings</div></div>
            <div class="hint">Shows all configured devices saved on this PC. Active device is the one used for Sync Device and live connection checks.</div>
            <div style="height:10px"></div>
            <div style="overflow-x:auto;max-width:100%">
              <table class="prettyTable settingsDeviceTable" style="width:100%;table-layout:fixed">
                <thead>
                  <tr>
                    <th style="width:240px">Device</th>
                    <th style="width:180px">IP:Port</th>
                    <th style="width:120px">Reader</th>
                    <th>W30 Log Folder</th>
                    <th style="width:180px">Last OK</th>
                  </tr>
                </thead>
                <tbody id="settingsDevicesBody"></tbody>
              </table>
            </div>

            <div class="sectionHead"><div class="sectionHeadTitle">Device Sync Summary</div></div>
            <div style="overflow-x:auto;max-width:100%">
              <table class="prettyTable settingsDeviceTable" style="width:100%;table-layout:fixed">
                <thead>
                  <tr>
                    <th style="width:240px">Device</th>
                    <th style="width:120px">Mode</th>
                    <th style="width:180px">Last Sync</th>
                    <th style="width:140px">Last Read</th>
                    <th>Notes</th>
                  </tr>
                </thead>
                <tbody id="settingsDeviceSyncBody"></tbody>
              </table>
            </div>
            <div style="height:8px"></div>
            <div class="muted" id="lastErrBrief"></div>
            <details id="lastErrBox" style="display:none"><summary>Error details</summary><pre id="lastErr"></pre></details>

            <div class="sectionHead"><div class="sectionHeadTitle">Device Connection</div></div>
            <div class="formGrid" style="display:none">
              <label for="setIp">Device IP</label><input id="setIp" placeholder="Device IP" />
              <label for="setPort">Device Port</label><input id="setPort" placeholder="Port" inputmode="numeric" />
              <label for="setReader">Reader Mode</label>
              <select id="setReader">
                <option value="auto">Auto</option>
                <option value="native">Native</option>
                <option value="com">COM</option>
              </select>
            </div>
            <div class="row toolbarRight" style="margin:10px 0;gap:8px">
              <select id="devAddType" style="width:140px;flex:0 0 auto">
                <option value="WL10">WL10</option>
                <option value="W30">W30</option>
              </select>
              <button class="btn" id="devAddDevice" type="button">Add Device</button>
            </div>
            <div style="overflow-x:auto;max-width:100%">
              <table class="prettyTable devConnTable" style="width:100%;table-layout:fixed">
                <thead>
                  <tr>
                    <th style="width:260px">Device</th>
                    <th style="width:180px">IP:Port</th>
                    <th style="width:120px">Reader</th>
                    <th>W30 Log Folder</th>
                    <th style="width:170px">Actions</th>
                  </tr>
                </thead>
                <tbody id="devConnBody"></tbody>
              </table>
            </div>
            <div style="height:8px"></div>
            <div class="hint" id="actionOut"></div>
            <div class="sectionHead"><div class="sectionHeadTitle">Desktop Info</div></div>
            <ul class="bulletList" id="desktopInfo"></ul>

            <div class="sectionHead"><div class="sectionHeadTitle">Sync &amp; Dashboard</div></div>
            <div class="hint">Mandatory Sync times are fixed times when a sync run is required.</div>
            <input id="schedTimes" type="hidden" />
            <div class="timeRow" style="margin-top:10px">
              <input class="timeInput" id="schedTimeNew" type="time" step="60" />
              <button class="btn" id="schedTimeAdd" type="button">Add Time</button>
            </div>
            <div style="height:10px"></div>
            <div class="timeCards" id="schedTimeCards"></div>

            <div class="sectionHead"><div class="sectionHeadTitle">Periodic Sync</div></div>
            <div class="formGrid">
              <label for="setAuto">Periodic Sync</label>
              <select id="setAuto">
                <option value="true">Enable</option>
                <option value="false">Disabled</option>
              </select>
              <input id="setDashRefresh" type="hidden" />
              <label id="refreshPeriodLabel" for="pollEvery" style="display:none">Refresh Period</label>
              <div id="refreshPeriodRow" class="row noWrap" style="gap:10px;display:none">
                <input id="pollEvery" inputmode="numeric" placeholder="1" style="width:120px;flex:0 0 auto" />
                <select id="pollUnit" style="width:140px;flex:0 0 auto">
                  <option value="hr">hours</option>
                  <option value="min">minutes</option>
                </select>
              </div>
            </div>
            <div style="height:10px"></div>
            <div class="row">
              <button class="btn" id="saveSync" type="button">Save Sync</button>
              <button class="btn" id="restartDashboard" type="button">Restart Dashboard</button>
            </div>
          </div>

          <div class="card">
            <div class="cardHead"><div class="cardHeadTitle">Database Settings</div></div>

            <div class="kpis kpis1">
              <div class="kpi">
                <div class="summaryRow">
                  <div class="kTitle">Database Sync</div>
                  <div id="dbSyncKpiPill" class="pill">Unknown</div>
                </div>
                <div class="kMeta">Database Clouds <strong>Supabase</strong></div>
                <div class="kMeta">Table <strong id="dbSyncKpiTable">-</strong></div>
              </div>
            </div>

            <div class="kpis kpis2" style="margin-top:14px">
              <div class="kpi">
                <div class="kTitle">Today Database</div>
                <div class="kVal" id="totalTodayDb">-</div>
                <div class="kMeta">Unique staff <strong id="uniqueTodayDb">-</strong></div>
              </div>
              <div class="kpi">
                <div class="kTitle">Last Sync</div>
                <div class="kVal" id="supaLastSyncKpi">-</div>
                <div class="kMeta" id="supaLastSyncDateKpi">Update on -</div>
                <div class="kMeta">Upserted <strong id="supaLastResKpi">-</strong></div>
              </div>
            </div>

            <div class="sectionHead"><div class="sectionHeadTitle">Database Sync Summary</div></div>
            <div class="subCards">
              <div class="subCard">
                <div class="subTitle">Status</div>
                <div class="subVal" id="supaPillWrap"><span id="supaPill" class="pill">Unknown</span></div>
              </div>
              <div class="subCard">
                <div class="subTitle">Table</div>
                <div class="subVal mono" id="supaTableState"></div>
              </div>
              <div class="subCard">
                <div class="subTitle">Auto Sync</div>
                <div class="subVal" id="supaAuto"></div>
              </div>
              <div class="subCard">
                <div class="subTitle">Next Sync</div>
                <div class="subVal" id="supaInterval"></div>
              </div>
              <div class="subCard">
                <div class="subTitle">Last Upserted</div>
                <div class="subVal" id="supaLastRes">-</div>
              </div>
              <div class="subCard">
                <div class="subTitle">Last Sync</div>
                <div class="subVal mono" id="supaLastSyncAt">-</div>
              </div>
              <div class="subCard">
                <div class="subTitle">Total Records</div>
                <div class="subVal" id="supaTotalRecords"></div>
              </div>
              <div class="subCard">
                <div class="subTitle">Online Database Date</div>
                <div class="subVal mono" id="dbWm"></div>
              </div>
            </div>
            <div style="height:10px"></div>
            <div class="hint">
              Online Database Date is the newest datetime found in Supabase. When Supabase sync is disabled, this value is shown as disabled.
            </div>
            <div class="hint" id="supaUrlState" style="display:none"></div>

            <div class="sectionHead"><div class="sectionHeadTitle">Database Connection</div></div>
            <div class="formGrid compactLabels">
              <label for="supaUrl">URL</label><input id="supaUrl" placeholder="https://lmssdqnduaahmqmvpuvn.supabase.co" readonly />
              <label for="supaProjectId">Project ID</label><input id="supaProjectId" placeholder="lmssdqnduaahmqmvpuvn" readonly />
              <label for="supaTable">Database Table</label><input id="supaTable" placeholder="attendance_events" />
              <label for="supaSyncMode">Sync to Database</label>
              <select id="supaSyncMode">
              <option value="enabled">enabled</option>
              <option value="disabled">disabled</option>
              </select>
              <label for="supaPubKey">Publishable Key</label><textarea id="supaPubKey" rows="4" placeholder="sb_publishable_..." autocomplete="off" readonly></textarea>
              <label for="supaKey">Service Role Key</label><textarea id="supaKey" rows="6" placeholder="service role key" autocomplete="off"></textarea>
              <label for="supaJwt">JWT Key</label><textarea id="supaJwt" rows="4" placeholder="JWT key" autocomplete="off"></textarea>
            </div>
            <div class="hint">ADMIN ONLY: These secrets grant privileged access. Do not share screenshots, logs, or exports containing these values.</div>
            <div style="height:10px"></div>
            <div class="row">
              <button class="btn" id="saveSupabase" type="button">Save Settings</button>
              <button class="btn primary" id="testSupabase" type="button">Test Connection</button>
              <span class="muted" id="supaTestResult"></span>
            </div>
            <div style="height:10px"></div>
            <div class="hint">Save Settings also writes .env.local for this PC so the middleware can reconnect on restart.</div>
          </div>
        </div>
      </div>

      <div class="subTabPanel" data-subtab-group="settings" id="subtab-settings-logs">
        <div class="grid">
          <div class="card">
            <div class="cardHead">
              <div class="cardHeadTitle">Activity Log</div>
              <div class="row" style="gap:8px">
                <button class="btn iconBtn" id="activityDownload" type="button" title="Download activity log">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3v10"/><path d="M8 11l4 4 4-4"/><path d="M4 17v4h16v-4"/></svg>
                </button>
                <button class="btn" id="activityRefresh" type="button">Refresh</button>
                <button class="btn" id="activityClear" type="button">Clear</button>
              </div>
            </div>
            <div class="hint">Actions you performed in the dashboard (buttons, imports, sync actions). Use this to quickly trace what happened.</div>
            <pre id="activityLogs"></pre>
          </div>

          <div class="card">
            <div class="cardHead">
              <div class="cardHeadTitle">System Log</div>
              <div class="row" style="gap:8px">
                <button class="btn iconBtn" id="systemDownload" type="button" title="Download system log">
                  <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3v10"/><path d="M8 11l4 4 4-4"/><path d="M4 17v4h16v-4"/></svg>
                </button>
                <button class="btn" id="logsRefresh" type="button">Refresh</button>
              </div>
            </div>
            <div class="hint">Middleware output (device connectivity, sync operations, Supabase requests). Use this for deeper troubleshooting.</div>
            <pre id="logs"></pre>
          </div>
        </div>
      </div>
    </section>
  </main>

  <div id="modalBack" class="modalBack" role="dialog" aria-modal="true">
    <div class="modalCard">
      <div class="modalHead">
        <div class="modalTitle" id="modalTitle"></div>
        <button class="iconBtn" id="modalClose" type="button">X</button>
      </div>
      <div class="modalBody modalGrid" id="modalBody"></div>
      <div class="modalFoot">
        <button class="btn" id="modalCancel" type="button">Cancel</button>
        <button class="btn primary" id="modalSave" type="button">Save</button>
      </div>
      <div class="hint" id="modalError" style="margin-top:10px;color:#fca5a5"></div>
    </div>
  </div>

  <div id="toast" class="toast" role="status" aria-live="polite" aria-atomic="true">
    <div class="toastTitle" id="toastTitle"></div>
    <div class="toastMsg" id="toastMsg"></div>
  </div>

    </div>
  </div>

  <script>
    const el = (id) => document.getElementById(id);
    const fmt = (s) => {
      if (!s) return '-';
      try {
        const d = new Date(s);
        if (!Number.isFinite(d.getTime())) return '-';
        const parts = new Intl.DateTimeFormat('en-GB', {
          timeZone: 'Asia/Kuala_Lumpur',
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hour12: false,
        }).formatToParts(d);
        const get = (t) => (parts.find(p => p.type === t) || {}).value || '';
        return get('year') + '-' + get('month') + '-' + get('day') + ' ' + get('hour') + ':' + get('minute') + ':' + get('second');
      } catch {
        return '-';
      }
    };
    const fmtShort = (s) => {
      if (!s) return '-';
      try {
        const d = new Date(s);
        if (!Number.isFinite(d.getTime())) return '-';
        const parts = new Intl.DateTimeFormat('en-GB', {
          timeZone: 'Asia/Kuala_Lumpur',
          hour: '2-digit',
          minute: '2-digit',
          hour12: false,
        }).formatToParts(d);
        const get = (t) => (parts.find(p => p.type === t) || {}).value || '';
        const hh = get('hour');
        const mm = get('minute');
        return (hh && mm) ? (hh + ':' + mm) : '-';
      } catch {
        return '-';
      }
    };
    const fmtDateOnly = (s) => {
      if (!s) return '-';
      try {
        const d = new Date(s);
        if (!Number.isFinite(d.getTime())) return '-';
        const parts = new Intl.DateTimeFormat('en-GB', {
          timeZone: 'Asia/Kuala_Lumpur',
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
        }).formatToParts(d);
        const get = (t) => (parts.find(p => p.type === t) || {}).value || '';
        return get('year') + '-' + get('month') + '-' + get('day');
      } catch {
        return '-';
      }
    };

    const normText = (s) => {
      const raw = String(s ?? '').trim();
      if (!raw) return '-';
      const low = raw.toLowerCase();
      if (low === '(none)') return '-';
      if (low === '(disabled)') return 'Disabled';
      if (low === 'on') return 'On';
      if (low === 'off') return 'Off';
      if (low === 'never') return 'None';
      if (low === 'ok') return 'OK';
      if (low === 'error') return 'Error';
      if (low === 'running') return 'Running';
      if (low === 'enabled') return 'Enabled';
      if (low === 'disabled') return 'Disabled';
      return raw;
    };
    let activeTab = 'summary';
    let activeSummarySubTab = 'attendance';
    let activeRawSubTab = 'device';
    let activeStaffSubTab = 'staff';
    let activeSettingsSubTab = 'connection';
    let activeSheetSubTab = 'daily';
    let sheetDailyByKey = new Map();
    let isSuperadmin = false;
    let lastSheetDailyRows = [];
    let lastSheetWeeklyRows = [];
    let lastSheetMonthlyRows = [];
    let monthlySelectedStaff = new Set();
    let sheetSort = {
      daily: { key: 'date', dir: 'asc' },
      weekly: { key: 'week_start', dir: 'desc' },
      monthly: { key: 'month', dir: 'desc' },
    };

    function parseYm(ym) {
      const m = String(ym || '').trim().match(/^(\d{4})-(\d{2})$/);
      if (!m) return null;
      const y = Number(m[1]);
      const mo = Number(m[2]);
      if (!Number.isFinite(y) || !Number.isFinite(mo)) return null;
      return y * 12 + (mo - 1);
    }

    function sortKeyVal(row, key) {
      const k = String(key || '').trim();
      const v = row && (row[k] ?? row[k.toLowerCase()] ?? row[k.toUpperCase()]);
      if (v === null || v === undefined) return '';
      const s = String(v).trim();
      if (!s) return '';
      if (k === 'month') return parseYm(s) ?? s;
      if (k === 'week_start' || k === 'week_end' || k === 'date') {
        const ms = Date.parse(s);
        return Number.isFinite(ms) ? ms : s;
      }
      if (k.endsWith('_hours') || k.endsWith('_pct') || k.includes('days_') || k.includes('flagged')) {
        const n = Number(String(s).replace('%', ''));
        return Number.isFinite(n) ? n : s;
      }
      return s.toLowerCase();
    }

    function sortRows(rows, scope) {
      const sc = String(scope || '').trim();
      const st = sheetSort && sheetSort[sc] ? sheetSort[sc] : null;
      if (!st || !st.key) return rows;
      const dir = (st.dir === 'desc') ? -1 : 1;
      const key = st.key;
      return rows.slice().sort((a, b) => {
        const av = sortKeyVal(a, key);
        const bv = sortKeyVal(b, key);
        if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
        return String(av).localeCompare(String(bv), undefined, { numeric: true, sensitivity: 'base' }) * dir;
      });
    }

    function updateSheetSortUi() {
      const map = { daily: 'sheetDailyTable', weekly: 'sheetWeeklyTable', monthly: 'sheetMonthlyTable' };
      const tid = map[activeSheetSubTab] || '';
      const table = tid ? document.getElementById(tid) : null;
      if (!table) return;
      const cfg = sheetSort && sheetSort[activeSheetSubTab] ? sheetSort[activeSheetSubTab] : null;
      for (const th of table.querySelectorAll('th.sortable')) {
        const k = th.getAttribute('data-sort') || '';
        th.classList.toggle('sortedAsc', !!cfg && cfg.key === k && cfg.dir === 'asc');
        th.classList.toggle('sortedDesc', !!cfg && cfg.key === k && cfg.dir === 'desc');
      }
    }

    function updateSheetMonthlyBulkUi() {
      const btn = el('sheetMonthlyBulkDownload');
      if (btn) btn.style.display = (isSuperadmin && monthlySelectedStaff && monthlySelectedStaff.size > 0) ? '' : 'none';
    }

    function renderSheetDailyRows(rows, errorText) {
      const body = el('sheetDailyBody');
      if (!body) return;
      body.innerHTML = '';
      sheetDailyByKey = new Map();
      if (errorText) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="9" class="muted">' + escHtml(errorText) + '</td>';
        body.appendChild(tr);
        updateSheetSortUi();
        return;
      }
      if (!rows || !rows.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="9" class="muted">(none)</td>';
        body.appendChild(tr);
        updateSheetSortUi();
        return;
      }
      for (const r of rows) {
        const key = String(r.staff_id || '') + '|' + String(r.date || '');
        sheetDailyByKey.set(key, r);
        const tr = document.createElement('tr');
        tr.style.cursor = 'pointer';
        tr.dataset.key = key;
        const flagged = Array.isArray(r.flagged_punches) ? r.flagged_punches.join(', ') : String(r.flagged_punches || '');
        const flagsText = String(r.flags || '').trim();
        const flaggedText = String(flagged || '').trim() || '';
        const shortFlags = compactFlags(flagsText);
        const parts = [];
        if (shortFlags) parts.push(shortFlags);
        if (flaggedText) parts.push(flaggedText);
        const flaggedHtml = '<span class="mono" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:block;max-width:100%">' + escHtml(parts.length ? parts.join(' • ') : '-') + '</span>';
        tr.innerHTML =
          '<td>' + escHtml(r.name || r.staff_id || '') + '</td>' +
          '<td class="mono">' + escHtml(r.date || '') + '</td>' +
          '<td>' + escHtml(r.shift || '') + '</td>' +
          '<td class="mono">' + escHtml(r.first_in || '-') + '</td>' +
          '<td class="mono">' + escHtml(r.last_out || '-') + '</td>' +
          '<td class="mono">' + escHtml(String(r.total_hours ?? '')) + '</td>' +
          '<td class="mono">' + escHtml(String(r.ot_hours ?? '')) + '</td>' +
          '<td>' + escHtml(r.status || '') + '</td>' +
          '<td>' + flaggedHtml + '</td>';
        body.appendChild(tr);
      }
      updateSheetSortUi();
    }

    function renderSheetWeeklyRows(rows, errorText) {
      const body = el('sheetWeeklyBody');
      if (!body) return;
      body.innerHTML = '';
      if (errorText) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="8" class="muted">' + escHtml(errorText) + '</td>';
        body.appendChild(tr);
        updateSheetSortUi();
        return;
      }
      if (!rows || !rows.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="8" class="muted">(none)</td>';
        body.appendChild(tr);
        updateSheetSortUi();
        return;
      }
      for (const r of rows) r.week_display = fmtWeekShortRange(r.week_start, r.week_end);
      for (const r of rows) {
        const tr = document.createElement('tr');
        tr.dataset.staff = String(r.staff_id || '');
        tr.dataset.weekStart = String(r.week_start || '');
        tr.style.cursor = 'pointer';
        tr.innerHTML =
          '<td>' + escHtml(r.name || r.staff_id || '') + '</td>' +
          '<td class="mono">' + escHtml(r.week_display || r.week || '') + '</td>' +
          '<td class="mono">' + escHtml(String(r.flagged_punches ?? 0)) + '</td>' +
          '<td class="mono">' + escHtml(String(r.total_hours ?? '')) + '</td>' +
          '<td class="mono">' + escHtml(String(r.ot_hours ?? '')) + '</td>' +
          '<td class="mono">' + escHtml(String(r.days_present ?? '')) + '</td>' +
          '<td class="mono">' + escHtml(String(r.days_absent ?? '')) + '</td>' +
          '<td class="mono">' + escHtml(String(r.attendance_pct ?? '')) + '%</td>';
        body.appendChild(tr);
      }
      updateSheetSortUi();
    }

    function renderSheetMonthlyRows(rows, errorText) {
      const body = el('sheetMonthlyBody');
      if (!body) return;
      body.innerHTML = '';
      if (errorText) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="10" class="muted">' + escHtml(errorText) + '</td>';
        body.appendChild(tr);
        updateSheetMonthlyBulkUi();
        updateSheetSortUi();
        return;
      }
      if (!rows || !rows.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="10" class="muted">(none)</td>';
        body.appendChild(tr);
        updateSheetMonthlyBulkUi();
        updateSheetSortUi();
        return;
      }
      for (const r of rows) r.month_display = fmtMonthShort(r.month);
      for (const r of rows) {
        const tr = document.createElement('tr');
        const sid = String(r.staff_id || '').trim();
        tr.dataset.staff = sid;
        tr.dataset.month = String(r.month || '');
        tr.style.cursor = 'pointer';
        const checked = monthlySelectedStaff.has(sid);
        const cbHtml = isSuperadmin
          ? ('<input type="checkbox" class="sheetMonthlyPick" data-staff="' + escHtml(sid) + '"' + (checked ? ' checked' : '') + ' />')
          : '';
        const dlHtml = isSuperadmin
          ? ('<button class="dlIconBtn" type="button" data-sheet-download="' + escHtml(sid) + '" data-sheet-month="' + escHtml(String(r.month || '')) + '" title="Download report" aria-label="Download report"><svg viewBox="0 0 24 24"><path d="M12 3v10"/><path d="M8 11l4 4 4-4"/><path d="M4 17v4h16v-4"/></svg></button>')
          : '';
        tr.innerHTML =
          '<td>' + cbHtml + '</td>' +
          '<td>' + escHtml(r.name || r.staff_id || '') + '</td>' +
          '<td class="mono">' + escHtml(r.month_display || r.month || '') + '</td>' +
          '<td class="mono">' + escHtml(String(r.flagged_punches ?? 0)) + '</td>' +
          '<td class="mono">' + escHtml(String(r.total_hours ?? '')) + '</td>' +
          '<td class="mono">' + escHtml(String(r.ot_hours ?? '')) + '</td>' +
          '<td class="mono">' + escHtml(String(r.days_present ?? '')) + '</td>' +
          '<td class="mono">' + escHtml(String(r.days_absent ?? '')) + '</td>' +
          '<td class="mono">' + escHtml(String(r.attendance_pct ?? '')) + '%</td>' +
          '<td>' + dlHtml + '</td>';
        body.appendChild(tr);
      }
      updateSheetMonthlyBulkUi();
      updateSheetSortUi();
    }

    function setSidebarCollapsed(collapsed) {
      document.body.classList.toggle('sideCollapsed', !!collapsed);
      try { localStorage.setItem('wl10dash.sidebar.collapsed', collapsed ? '1' : '0'); } catch { }
    }

    function setOpenNavGroup(groupId) {
      const g = String(groupId || '').trim();
      for (const grp of document.querySelectorAll('.navGroup')) {
        grp.classList.toggle('open', grp.dataset.group === g && !!g);
      }
      try { localStorage.setItem('wl10dash.nav.open', g); } catch { }
    }

    function syncNavUi() {
      let group = '';
      let sub = '';
      if (activeTab === 'attendanceSpreadsheet') { group = 'sheet'; sub = activeSheetSubTab; }
      else if (activeTab === 'staffRecords') { group = 'staff'; sub = activeStaffSubTab; }
      else if (activeTab === 'rawData') { group = 'raw'; sub = activeRawSubTab; }
      else if (activeTab === 'settings') { group = 'settings'; sub = activeSettingsSubTab; }
      else { group = ''; sub = activeSummarySubTab; }

      setOpenNavGroup(group);

      for (const btn of document.querySelectorAll('.navBtn')) {
        const tab = btn.dataset.tab || '';
        const sg = btn.dataset.subtabGroup || '';
        const st = btn.dataset.subtab || '';
        const isActive = (tab === activeTab) && (!st || (sg === group && st === sub));
        btn.classList.toggle('active', isActive);
      }

      const topTitle = el('topTitle');
      const topSub = el('topSub');
      if (topTitle) {
        topTitle.textContent =
          activeTab === 'summary' ? 'Summary'
          : activeTab === 'attendanceSpreadsheet' ? 'Attendance Spreadsheet'
          : activeTab === 'staffRecords' ? 'Staff Records'
          : activeTab === 'rawData' ? 'Raw Data'
          : activeTab === 'settings' ? 'Settings'
          : 'Dashboard';
      }
      if (topSub) {
        const label =
          activeTab === 'summary' ? (activeSummarySubTab === 'attendance' ? 'Attendance Analysis' : 'Summary')
          : activeTab === 'attendanceSpreadsheet' ? (activeSheetSubTab === 'daily' ? 'Daily' : (activeSheetSubTab === 'weekly' ? 'Weekly' : 'Monthly'))
          : activeTab === 'staffRecords' ? (activeStaffSubTab === 'shift' ? 'Shift Pattern' : 'Employee List')
          : activeTab === 'rawData' ? (activeRawSubTab === 'db' ? 'Database Records' : 'Device Records')
          : activeTab === 'settings' ? (activeSettingsSubTab === 'logs' ? 'Logs' : 'Connection')
          : '';
        topSub.textContent = label;
      }
    }

    function setActiveSubTab(group, name) {
      const g = String(group || '').trim();
      const n = String(name || '').trim();
      if (!g || !n) return;
      const panelId = 'subtab-' + g + '-' + n;
      if (!document.getElementById(panelId)) return;
      for (const btn of document.querySelectorAll('.subTabBtn[data-subtab-group="' + g + '"]')) btn.classList.toggle('active', btn.dataset.subtab === n);
      for (const panel of document.querySelectorAll('.subTabPanel[data-subtab-group="' + g + '"]')) panel.classList.toggle('active', panel.id === panelId);
      try { localStorage.setItem('wl10dash.subtab.' + g, n); } catch { }

      if (g === 'summary') {
        activeSummarySubTab = n;
        if (activeTab === 'summary') {
          refreshAnalytics().catch(() => {});
          if (n === 'attendance') refreshAttendance().catch(() => {});
        }
      }
      if (g === 'raw') {
        activeRawSubTab = n;
        if (activeTab === 'rawData') {
          if (n === 'db') refreshDbRecords().catch(() => {});
          else refreshDeviceRecords().catch(() => {});
        }
      }
      if (g === 'staff') {
        activeStaffSubTab = n;
        if (activeTab === 'staffRecords') {
          refreshStaffRecords().catch(() => {});
          if (n === 'shift') refreshShiftPatterns().catch(() => {});
        }
      }
      if (g === 'settings') {
        activeSettingsSubTab = n;
        if (activeTab === 'settings' && n === 'logs') refreshLogs().catch(() => {});
      }
      if (g === 'sheet') {
        activeSheetSubTab = n;
        if (activeTab === 'attendanceSpreadsheet') refreshAttendanceSpreadsheet().catch(() => {});
        updateSheetSortUi();
      }
      syncNavUi();
    }

    function out(msg) {
      const x =
        (activeTab === 'settings' && activeSettingsSubTab !== 'logs' ? el('actionOut') : null) ||
        el('logs');
      if (x) x.textContent = String(msg || '');
    }

    function outAppend(msg) {
      const x =
        (activeTab === 'settings' && activeSettingsSubTab !== 'logs' ? el('actionOut') : null) ||
        el('logs');
      if (!x) return;
      const next = String(msg || '');
      x.textContent = (x.textContent ? (x.textContent + '\n') : '') + next;
      if (x.textContent.length > 12000) x.textContent = x.textContent.slice(-12000);
    }

    let toastTimer = 0;
    function notify(title, msg, kind) {
      const box = el('toast');
      const t = el('toastTitle');
      const m = el('toastMsg');
      if (!box || !t || !m) return;
      const k = String(kind || '').toLowerCase();
      box.className = 'toast show' + (k === 'bad' || k === 'err' || k === 'error' ? ' bad' : (k === 'ok' || k === 'success' ? ' ok' : ''));
      t.textContent = String(title || 'Update');
      m.textContent = String(msg || '');
      if (toastTimer) { try { clearTimeout(toastTimer); } catch { } }
      toastTimer = setTimeout(() => { box.className = 'toast'; }, 3200);
    }

    function downloadTextFile(fileName, text) {
      const name = String(fileName || 'download.txt').trim() || 'download.txt';
      const content = String(text || '');
      const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = name;
      document.body.appendChild(a);
      a.click();
      try { a.remove(); } catch { }
      try { URL.revokeObjectURL(url); } catch { }
    }

    function normTimeHHmm(raw) {
      const s = String(raw || '').trim();
      const m = s.match(/^(\d{1,2}):(\d{2})$/);
      if (!m) return null;
      const hh = Number(m[1]);
      const mm = Number(m[2]);
      if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
      if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return null;
      return String(hh).padStart(2, '0') + ':' + String(mm).padStart(2, '0');
    }

    function setMandatoryTimes(list) {
      const uniq = new Map();
      for (const t of (list || [])) {
        const v = normTimeHHmm(t);
        if (!v) continue;
        uniq.set(v, true);
      }
      mandatorySyncTimes = Array.from(uniq.keys()).sort((a, b) => a.localeCompare(b));
      const raw = mandatorySyncTimes.join(',');
      const x = el('schedTimes');
      if (x) x.value = raw;
      renderMandatorySyncTimes();
    }

    function renderMandatorySyncTimes() {
      const host = el('schedTimeCards');
      if (!host) return;
      host.innerHTML = '';
      const list = Array.isArray(mandatorySyncTimes) ? mandatorySyncTimes.slice() : [];
      if (!list.length) {
        host.innerHTML = '<div class="muted">No mandatory sync times set.</div>';
        return;
      }
      for (const t of list) {
        const card = document.createElement('div');
        card.className = 'timeCard';
        card.innerHTML =
          '<div class="timeCardVal">' + escHtml(t) + '</div>' +
          '<div class="timeCardBtns">' +
            '<button class="btn iconBtn" type="button" data-edit-time="' + escHtml(t) + '" title="Edit time"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/></svg></button>' +
            '<button class="btn iconBtn" type="button" data-del-time="' + escHtml(t) + '" title="Delete time"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 6h18"/><path d="M8 6V4h8v2"/><path d="M19 6l-1 14H6L5 6"/></svg></button>' +
          '</div>';
        host.appendChild(card);
      }
    }

    function klDateParts(d) {
      const dt = d instanceof Date ? d : new Date();
      const fmt = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Kuala_Lumpur', year: 'numeric', month: '2-digit', day: '2-digit' });
      const parts = fmt.formatToParts(dt);
      const y = parts.find(p => p.type === 'year')?.value || '';
      const m = parts.find(p => p.type === 'month')?.value || '';
      const da = parts.find(p => p.type === 'day')?.value || '';
      return { y, m, d: da };
    }

    function todayKlIso() {
      const p = klDateParts(new Date());
      if (!p.y || !p.m || !p.d) return new Date().toISOString().slice(0, 10);
      return p.y + '-' + p.m + '-' + p.d;
    }

    function isoToDmy(iso) {
      const s = String(iso || '').trim().slice(0, 10);
      const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (!m) return '';
      return m[3] + '/' + m[2] + '/' + m[1];
    }

    function dmyToIso(dmy) {
      const s = String(dmy || '').trim();
      const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
      if (!m) return null;
      const dd = Number(m[1]);
      const mm = Number(m[2]);
      const yy = Number(m[3]);
      if (!Number.isFinite(dd) || !Number.isFinite(mm) || !Number.isFinite(yy)) return null;
      if (yy < 1900 || yy > 2100) return null;
      if (mm < 1 || mm > 12) return null;
      if (dd < 1 || dd > 31) return null;
      return String(yy) + '-' + String(mm).padStart(2, '0') + '-' + String(dd).padStart(2, '0');
    }

    function parseUserDateToIso(raw) {
      const s = String(raw || '').trim();
      if (!s) return null;
      if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
      return dmyToIso(s);
    }

    function setDmyInputIfEmpty(id, iso) {
      const x = el(id);
      if (!x) return;
      if (String(x.value || '').trim()) return;
      const dmy = isoToDmy(iso);
      if (dmy) x.value = dmy;
    }

    function normalizeDmyInput(id) {
      const x = el(id);
      if (!x) return;
      if (String(x.type || '').toLowerCase() === 'date') return;
      const iso = parseUserDateToIso(x.value);
      if (!iso) return;
      const dmy = isoToDmy(iso);
      if (dmy) x.value = dmy;
    }

    function nowKlStamp() {
      const dt = new Date();
      const fmt = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Kuala_Lumpur', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' });
      const parts = fmt.formatToParts(dt);
      const y = parts.find(p => p.type === 'year')?.value || '';
      const m = parts.find(p => p.type === 'month')?.value || '';
      const da = parts.find(p => p.type === 'day')?.value || '';
      const hh = parts.find(p => p.type === 'hour')?.value || '00';
      const mm = parts.find(p => p.type === 'minute')?.value || '00';
      const ss = parts.find(p => p.type === 'second')?.value || '00';
      if (!y || !m || !da) return new Date().toISOString().replace('T', ' ').slice(0, 19);
      return y + '-' + m + '-' + da + ' ' + hh + ':' + mm + ':' + ss;
    }

    function nowKlIsoOffset() {
      const dt = new Date();
      const fmt = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Kuala_Lumpur', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' });
      const parts = fmt.formatToParts(dt);
      const y = parts.find(p => p.type === 'year')?.value || '';
      const m = parts.find(p => p.type === 'month')?.value || '';
      const da = parts.find(p => p.type === 'day')?.value || '';
      const hh = parts.find(p => p.type === 'hour')?.value || '00';
      const mm = parts.find(p => p.type === 'minute')?.value || '00';
      const ss = parts.find(p => p.type === 'second')?.value || '00';
      if (!y || !m || !da) return new Date().toISOString();
      return y + '-' + m + '-' + da + 'T' + hh + ':' + mm + ':' + ss + '+08:00';
    }

    function nowKlFileStamp() {
      const dt = new Date();
      const fmt = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Kuala_Lumpur', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hourCycle: 'h23' });
      const parts = fmt.formatToParts(dt);
      const y = parts.find(p => p.type === 'year')?.value || '';
      const m = parts.find(p => p.type === 'month')?.value || '';
      const da = parts.find(p => p.type === 'day')?.value || '';
      const hh = parts.find(p => p.type === 'hour')?.value || '00';
      const mm = parts.find(p => p.type === 'minute')?.value || '00';
      if (!y || !m || !da) return new Date().toISOString().replace(/[:]/g, '').replace('T', '_').slice(0, 15);
      return y + '-' + m + '-' + da + '_' + hh + mm;
    }

    function logActivity(scope, msg, level) {
      const elx = el('activityLogs');
      if (!elx) return;
      const s = String(scope || '').trim() || 'System';
      const m = String(msg || '').trim();
      if (!m) return;
      const ts = nowKlStamp();
      const lvl = String(level || '').trim().toUpperCase();
      const tag = lvl ? ('[' + lvl + '] ') : '';
      const line = ts + ' [' + s + '] ' + tag + m;
      elx.textContent = (elx.textContent ? (elx.textContent + '\n') : '') + line;
      const max = 2000;
      const parts = elx.textContent.split('\n');
      if (parts.length > max) elx.textContent = parts.slice(parts.length - max).join('\n');

      try {
        fetch('/api/activity/append', {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ ts: nowKlIsoOffset(), scope: s, level: (lvl || 'INFO'), message: m })
        }).catch(() => {});
      } catch { }
    }

    let autoSyncInFlight = false;
    let lastAutoSyncAtMs = 0;

    async function triggerAutoSync(reason, force) {
      const now = Date.now();
      if (autoSyncInFlight) return;
      if (!force && now - lastAutoSyncAtMs < 60_000) return;
      autoSyncInFlight = true;
      lastAutoSyncAtMs = now;

      try {
        const statusOk = await refreshStatus();
        if (!statusOk) {
          logActivity('System', 'Auto sync skipped: dashboard status unavailable.', 'WARN');
          return;
        }

        const st = lastStatus || {};
        const deviceOk = !!(st.device && st.device.reachable);
        const supaOk = !!(st.supabase && st.supabase.configured && st.supabase.syncEnabled);

        let url = '';
        let label = '';
        if (deviceOk && supaOk) { url = '/api/sync?today=1'; label = 'device + supabase'; }
        else if (deviceOk && !supaOk) { url = '/api/sync?today=1&supabase=0'; label = 'device-only'; }
        else if (!deviceOk && supaOk) { url = '/api/supabase/update'; label = 'supabase-only'; }
        else {
          logActivity('System', 'Auto sync skipped (' + String(reason || 'auto') + '): no device and no supabase.', 'WARN');
          return;
        }

        logActivity('System', 'Auto sync started (' + label + ', ' + String(reason || 'auto') + ')...');
        const res = await fetch(url, { method: 'POST', credentials: 'same-origin' });
        if (res.status === 401) {
          logActivity('System', 'Auto sync failed (' + label + '): Unauthorized.', 'ERR');
        } else if (!res.ok) {
          logActivity('System', 'Auto sync failed (' + label + '): ' + (await res.text()), 'ERR');
        } else {
          logActivity('System', 'Auto sync completed (' + label + ').', 'OK');
        }
      } catch (e) {
        logActivity('System', 'Auto sync failed: ' + String(e), 'ERR');
      } finally {
        autoSyncInFlight = false;
      }

      refreshStatus().catch(() => {});
      refreshLogs().catch(() => {});
      refreshDeviceRecords().catch(() => {});
      refreshDbRecords().catch(() => {});
      refreshStaffRecords().catch(() => {});
      refreshShiftPatterns().catch(() => {});
      refreshAnalytics().catch(() => {});
    }

    function setActiveTab(name) {
      if (name === 'attendance') {
        name = 'summary';
        setActiveSubTab('summary', 'attendance');
      }
      if (name === 'deviceRecords') {
        name = 'rawData';
        setActiveSubTab('raw', 'device');
      }
      if (name === 'dbRecords') {
        name = 'rawData';
        setActiveSubTab('raw', 'db');
      }
      if (name === 'shiftPatterns') {
        name = 'staffRecords';
        setActiveSubTab('staff', 'staff');
      }
      if (name === 'logs') {
        name = 'settings';
        setActiveSubTab('settings', 'logs');
      }

      if (!document.getElementById('tab-' + name)) name = 'summary';
      for (const btn of document.querySelectorAll('.tabBtn')) btn.classList.toggle('active', btn.dataset.tab === name);
      for (const panel of document.querySelectorAll('.tabPanel')) panel.classList.toggle('active', panel.id === 'tab-' + name);
      try { localStorage.setItem('wl10dash.tab', name); } catch { }
      activeTab = name;
      if (name === 'summary') {
        refreshAnalytics().catch(() => {});
        if (activeSummarySubTab === 'attendance') refreshAttendance().catch(() => {});
      }
      if (name === 'rawData') {
        if (activeRawSubTab === 'db') refreshDbRecords().catch(() => {});
        else refreshDeviceRecords().catch(() => {});
      }
      if (name === 'staffRecords') {
        staffSortKey = 'user_id';
        staffSortDir = 'asc';
        setActiveSubTab('staff', 'staff');
        updateStaffSortUi();
      }
      if (name === 'settings') {
        if (activeSettingsSubTab === 'logs') refreshLogs().catch(() => {});
      }
      if (name === 'attendanceSpreadsheet') {
        refreshAttendanceSpreadsheet().catch(() => {});
      }
      syncNavUi();
    }

    for (const btn of document.querySelectorAll('.tabBtn')) {
      btn.addEventListener('click', () => setActiveTab(btn.dataset.tab));
    }

    for (const btn of document.querySelectorAll('.subTabBtn')) {
      btn.addEventListener('click', () => setActiveSubTab(btn.dataset.subtabGroup, btn.dataset.subtab));
    }

    const sideToggle = el('sideToggle');
    if (sideToggle) sideToggle.addEventListener('click', () => {
      setSidebarCollapsed(!document.body.classList.contains('sideCollapsed'));
    });

    for (const btn of document.querySelectorAll('.navBtn')) {
      if (!btn.title) {
        const t = String(btn.textContent || '').replace(/\s+/g, ' ').trim();
        if (t) btn.title = t;
      }
      btn.addEventListener('click', () => {
        const acc = btn.dataset.accordion;
        if (acc) {
          if (document.body.classList.contains('sideCollapsed')) {
            setOpenNavGroup(acc);
            const next =
              acc === 'sheet' ? { tab: 'attendanceSpreadsheet', g: 'sheet', st: 'daily' }
              : acc === 'staff' ? { tab: 'staffRecords', g: 'staff', st: 'staff' }
              : acc === 'raw' ? { tab: 'rawData', g: 'raw', st: 'device' }
              : acc === 'settings' ? { tab: 'settings', g: 'settings', st: 'connection' }
              : null;
            if (next) {
              setActiveTab(next.tab);
              setActiveSubTab(next.g, next.st);
              return;
            }
          }
          const grp = btn.closest('.navGroup');
          const isOpen = grp && grp.classList.contains('open');
          setOpenNavGroup(isOpen ? '' : acc);
          return;
        }

        const tab = btn.dataset.tab;
        if (!tab) return;
        setActiveTab(tab);
        const sg = btn.dataset.subtabGroup;
        const st = btn.dataset.subtab;
        if (sg && st) setActiveSubTab(sg, st);
      });
    }

    try {
      const collapsed = localStorage.getItem('wl10dash.sidebar.collapsed') === '1';
      if (collapsed) setSidebarCollapsed(true);
    } catch { }

    try {
      const open = localStorage.getItem('wl10dash.nav.open');
      if (open) setOpenNavGroup(open);
    } catch { }

    try {
      const saved = localStorage.getItem('wl10dash.tab');
      if (saved) setActiveTab(saved);
    } catch { }

    try {
      const savedSummary = localStorage.getItem('wl10dash.subtab.summary');
      if (savedSummary) setActiveSubTab('summary', savedSummary);
      else setActiveSubTab('summary', 'attendance');
    } catch { setActiveSubTab('summary', 'attendance'); }

    try {
      const savedRaw = localStorage.getItem('wl10dash.subtab.raw');
      if (savedRaw) setActiveSubTab('raw', savedRaw);
      else setActiveSubTab('raw', 'device');
    } catch { setActiveSubTab('raw', 'device'); }

    try { setActiveSubTab('staff', 'staff'); } catch { setActiveSubTab('staff', 'staff'); }

    try {
      const savedSettings = localStorage.getItem('wl10dash.subtab.settings');
      if (savedSettings === 'deviceSync' || savedSettings === 'databaseSync') setActiveSubTab('settings', 'connection');
      else if (savedSettings) setActiveSubTab('settings', savedSettings);
      else setActiveSubTab('settings', 'connection');
    } catch { setActiveSubTab('settings', 'connection'); }

    try {
      const q = new URLSearchParams(window.location.search || '');
      const tab = (q.get('tab') || '').trim();
      const sub = (q.get('subtab') || '').trim();
      if (tab) setActiveTab(tab);
      if (sub && sub.includes(':')) {
        const parts = sub.split(':');
        const g = (parts[0] || '').trim();
        const n = (parts[1] || '').trim();
        if (g && n) setActiveSubTab(g, n);
      }
      if (tab || sub) history.replaceState(null, '', window.location.pathname);
    } catch { }

    try {
      const savedSheet = localStorage.getItem('wl10dash.subtab.sheet');
      if (savedSheet) setActiveSubTab('sheet', savedSheet);
      else setActiveSubTab('sheet', 'daily');
    } catch { setActiveSubTab('sheet', 'daily'); }

    let nextPollMs = 3000;
    let uiPollMs = 3000;

    function setApiOfflineUi() {
      const reach = el('reach');
      reach.className = 'pill bad';
      reach.textContent = 'Dashboard API offline';
      const settingsReach = document.getElementById('settingsReach');
      if (settingsReach) {
        settingsReach.className = 'pill bad';
        settingsReach.textContent = 'Dashboard API offline';
      }
      const savedDeviceHint = document.getElementById('savedDeviceHint');
      if (savedDeviceHint) savedDeviceHint.textContent = 'Saved Device: -';
      showQuickFix();
    }

    function getQuickFixCommand() {
      return [
        'cd \".\\\\SHAB Attendance System\\\\SHAB Attendance Middleware\"',
        'dotnet build -c Release',
        'dotnet run -c Release -- --dashboard --dashboard-port 5099',
      ].join('\r\n');
    }

    function showQuickFix() {
      const box = el('quickFixBox');
      const cmd = el('quickFixCmd');
      if (cmd) cmd.textContent = getQuickFixCommand();
      const hint = el('quickFixLanHint');
      if (hint) {
        let ip = '';
        try { ip = (localStorage.getItem('wl10dash.bestIpv4') || '').trim(); } catch { }
        hint.textContent = ip ? ('LAN URL: http://' + ip + ':5099/') : 'LAN URL: http://<this-pc-ip>:5099/  (example: http://10.136.8.126:5099/)';
      }
      if (box) box.style.display = '';
    }

    function hideQuickFix() {
      const box = el('quickFixBox');
      if (box) box.style.display = 'none';
    }

    async function copyText(s) {
      const text = String(s || '');
      try {
        if (navigator && navigator.clipboard && navigator.clipboard.writeText) {
          await navigator.clipboard.writeText(text);
          return true;
        }
      } catch { }
      try {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.setAttribute('readonly', 'readonly');
        ta.style.position = 'fixed';
        ta.style.left = '-9999px';
        document.body.appendChild(ta);
        ta.select();
        const ok = document.execCommand('copy');
        ta.remove();
        return ok;
      } catch {
        return false;
      }
    }

    function autoGrowTextarea(x) {
      if (!x || !x.tagName) return;
      if (String(x.tagName).toLowerCase() !== 'textarea') return;
      try {
        x.style.height = 'auto';
        x.style.height = Math.max(40, x.scrollHeight + 2) + 'px';
      } catch { }
    }

    function setValIfNotFocused(id, value) {
      const x = el(id);
      if (!x) return;
      if (document.activeElement === x) return;
      x.value = value;
      autoGrowTextarea(x);
    }

    async function getJson(url, withError) {
      try {
        const res = await fetch(url, { cache: 'no-store', credentials: 'same-origin' });
        if (res.status === 401 || (res.redirected && String(res.url || '').includes('/login'))) {
          window.location.href = '/login';
          return null;
        }
        if (!res.ok) {
          if (withError) {
            let body = '';
            try { body = await res.text(); } catch { }
            return { ok: false, error: body || (String(res.status) + ' ' + String(res.statusText || '')) };
          }
          return null;
        }
        const j = await res.json();
        if (withError && j && typeof j === 'object' && !('ok' in j)) j.ok = true;
        return j;
      } catch (e) {
        return withError ? { ok: false, error: String(e || 'fetch failed') } : null;
      }
    }

    function getDeviceTarget() {
      const ip = (el('setIp')?.value || '').trim();
      const portRaw = (el('setPort')?.value || '').trim();
      const port = parseInt(portRaw, 10);
      return { ip, port: Number.isFinite(port) ? port : null };
    }

    async function postJson(url, body) {
      try {
        const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body), credentials: 'same-origin' });
        if (res.status === 401) { return { ok: false, error: 'unauthorized' }; }
        if (!res.ok) return { ok: false, error: await res.text() };
        try { return await res.json(); } catch { return { ok: true }; }
      } catch (e) {
        return { ok: false, error: String(e) };
      }
    }

    let lastStatus = null;
    let lastSystemLogLines = [];
    let mandatorySyncTimes = [];
    let anaCalendarMonth = '';
    let rawDevicePickWired = false;

    function getRawDevicePick() {
      const x = el('rawDevicePick');
      const v = (x && x.value) ? String(x.value || '').trim() : '';
      return v || 'all';
    }

    function setRawDevicePick(value) {
      const x = el('rawDevicePick');
      if (!x) return;
      x.value = String(value || 'all');
    }

    function syncRawDevicePickOptions(status) {
      const x = el('rawDevicePick');
      if (!x) return;
      const devices = (status && status.devices && Array.isArray(status.devices)) ? status.devices : [];

      let selected = 'all';
      try { selected = String(localStorage.getItem('wl10dash.rawDevicePick') || 'all').trim() || 'all'; } catch { selected = 'all'; }

      const opts = ['<option value="all">All devices</option>'];
      const ids = new Set(['all']);
      for (const d of devices) {
        const id = String((d && d.deviceId) || '').trim();
        const type = String((d && d.type) || '').trim();
        const ip = String((d && d.ip) || '').trim();
        if (!id) continue;
        ids.add(id);
        const label = (type && ip) ? (type + ' - ' + ip) : (type ? type : id);
        opts.push('<option value="' + escHtml(id) + '">' + escHtml(label) + '</option>');
      }

      x.innerHTML = opts.join('');
      if (!ids.has(selected)) selected = 'all';
      setRawDevicePick(selected);

      if (!rawDevicePickWired) {
        rawDevicePickWired = true;
        x.addEventListener('change', async () => {
          const v = getRawDevicePick();
          try { localStorage.setItem('wl10dash.rawDevicePick', v); } catch { }
          updateRawDeviceActions();
          if (typeof activeTab !== 'undefined' && activeTab === 'rawData') {
            if (typeof activeRawSubTab !== 'undefined' && activeRawSubTab === 'device') await refreshDeviceRecords();
            if (typeof activeRawSubTab !== 'undefined' && activeRawSubTab === 'db') await refreshDbRecords();
          }
        });
      }
    }

    function updateRawDeviceActions() {
      const pick = getRawDevicePick();
      const devRefreshBtn = el('devRefresh');
      if (devRefreshBtn) {
        const devs = (lastStatus && Array.isArray(lastStatus.devices)) ? lastStatus.devices : [];
        const match = (pick && pick !== 'all') ? devs.find(d => String((d && d.deviceId) || '').trim() === pick) : null;
        const t = match ? String((match && match.type) || '').trim().toUpperCase() : '';
        const isW30 = t === 'W30';
        devRefreshBtn.disabled = isW30;
        devRefreshBtn.title = isW30 ? 'W30 uses file import. Copy AttendanceLog*.dat then use Sync Database.' : '';
      }
    }

    let settingsDevices = [];
    let settingsDevicesActiveId = '';
    let settingsDevicesWired = false;

    function renderSettingsDevices() {
      const body = el('devConnBody');
      if (!body) return;
      const rows = Array.isArray(settingsDevices) ? settingsDevices : [];
      if (rows.length === 0) {
        body.innerHTML = '<tr><td colspan="5" class="muted">No configured devices.</td></tr>';
        return;
      }

      const parts = [];
      for (const d of rows) {
        const id = String(d.deviceId || '').trim();
        const type = String(d.deviceType || '').trim();
        const ip = String(d.deviceIp || '').trim();
        const port = String(d.devicePort ?? '').trim();
        const rm = String(d.readerMode || '').trim();
        const logDir = String(d.logDir || '').trim();
        const active = id && settingsDevicesActiveId && id === settingsDevicesActiveId;
        const ipPort = (ip && port) ? (ip + ':' + port) : (ip || port || '-');
        const w30Dir = (String(type || '').trim().toUpperCase() === 'W30') ? (logDir || '(Reference)') : '-';
        parts.push(
          '<tr>' +
          '<td>' + escHtml((type ? type + ' ' : '') + (id || '-')) + (active ? ' <span class="pill">Active</span>' : '') + '</td>' +
          '<td class="mono">' + escHtml(ipPort) + '</td>' +
          '<td>' + escHtml(rm || '-') + '</td>' +
          '<td class="mono">' + escHtml(w30Dir) + '</td>' +
          '<td><div class="actionIcons">' +
          '<button class="iconBtn sm" type="button" data-dev-action="connect" data-dev-id="' + escHtml(id) + '" title="Connect" aria-label="Connect"' + (active ? ' disabled' : '') + '><svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M8 12l2.5 2.5L16 9"/></svg></button>' +
          '<button class="iconBtn sm" type="button" data-dev-action="disconnect" data-dev-id="' + escHtml(id) + '" title="Disconnect" aria-label="Disconnect"' + (!active ? ' disabled' : '') + '><svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="9"/><path d="M9 9l6 6"/><path d="M15 9l-6 6"/></svg></button>' +
          '<span class="actionSep"></span>' +
          '<button class="iconBtn sm" type="button" data-dev-action="edit" data-dev-id="' + escHtml(id) + '" title="Edit" aria-label="Edit"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4 12.5-12.5z"/></svg></button>' +
          '<button class="iconBtn sm danger" type="button" data-dev-action="delete" data-dev-id="' + escHtml(id) + '" title="Remove" aria-label="Remove"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 6h18"/><path d="M8 6V4h8v2"/><path d="M19 6l-1 14H6L5 6"/></svg></button>' +
          '</div></td>' +
          '</tr>'
        );
      }
      body.innerHTML = parts.join('');
    }

    async function refreshSettingsDevices() {
      const j = await getJson('/api/devices/list', true);
      if (!j || j.ok === false) return false;
      settingsDevices = Array.isArray(j.devices) ? j.devices : [];
      settingsDevicesActiveId = String(j.activeDeviceId || '').trim();
      renderSettingsDevices();
      if (!settingsDevicesWired) {
        settingsDevicesWired = true;
        const addBtn = el('devAddDevice');
        if (addBtn) addBtn.addEventListener('click', () => {
          const addType = String(el('devAddType')?.value || 'WL10').trim() || 'WL10';
          openDeviceModal(null, addType);
        });
        const body = el('devConnBody');
        if (body) body.addEventListener('click', async (ev) => {
          const btn = ev.target && ev.target.closest ? ev.target.closest('button[data-dev-action]') : null;
          if (!btn) return;
          const act = String(btn.getAttribute('data-dev-action') || '').trim();
          const id = String(btn.getAttribute('data-dev-id') || '').trim();
          if (!act || !id) return;
          ev.preventDefault();

          if (act === 'edit') {
            const d = (settingsDevices || []).find(x => String(x.deviceId || '').trim() === id) || null;
            openDeviceModal(d, '');
            return;
          }
          if (act === 'delete') {
            if (!confirm('Remove device ' + id + '?')) return;
            const res = await postJson('/api/devices/delete', { deviceId: id });
            if (!res || res.ok === false) { notify('Error', 'Remove failed', String((res && res.error) || '')); return; }
            await refreshSettingsDevices();
            await refreshStatus();
            return;
          }
          if (act === 'connect') {
            const res = await postJson('/api/devices/connect', { deviceId: id });
            if (!res || res.ok === false) { notify('Error', 'Connect failed', String((res && res.error) || '')); return; }
            await refreshSettingsDevices();
            await refreshStatus();
            return;
          }
          if (act === 'disconnect') {
            const res = await postJson('/api/devices/disconnect', { deviceId: id });
            if (!res || res.ok === false) { notify('Error', 'Disconnect failed', String((res && res.error) || '')); return; }
            await refreshSettingsDevices();
            await refreshStatus();
            return;
          }
        });
      }
      return true;
    }

    function openDeviceModal(existing, preferType) {
      const e = existing || {};
      const isEdit = !!(e && e.deviceId);
      const title = isEdit ? 'Edit Device' : 'Add Device';
      const initialType = String(e.deviceType || preferType || 'WL10').trim() || 'WL10';
      const init = {
        deviceType: initialType,
        deviceId: String(e.deviceId || '').trim(),
        deviceIp: String(e.deviceIp || '').trim(),
        devicePort: String((e.devicePort ?? '') || '').trim(),
        readerMode: String(e.readerMode || '').trim(),
        logDir: String(e.logDir || '').trim(),
        filePattern: String(e.filePattern || '').trim(),
      };
      if (!init.readerMode) init.readerMode = (initialType.toUpperCase() === 'W30') ? 'file' : 'auto';
      if (!init.devicePort && initialType.toUpperCase() === 'WL10') init.devicePort = '4370';
      if (!init.filePattern && initialType.toUpperCase() === 'W30') init.filePattern = 'AttendanceLog*.dat';

      const fields = [
        { key: 'deviceType', label: 'Device Type', kind: 'select', options: ['WL10', 'W30'], default: initialType },
        { key: 'deviceId', label: 'Device ID', placeholder: 'WL10-192.168.1.170', default: init.deviceId },
        { key: 'deviceIp', label: 'Device IP', placeholder: '192.168.1.170', default: init.deviceIp },
        { key: 'devicePort', label: 'Device Port', type: 'number', placeholder: '4370', default: init.devicePort },
        { key: 'readerMode', label: 'Reader Mode', kind: 'select', options: ['auto', 'native', 'com', 'file'], default: init.readerMode },
        { key: 'logDir', label: 'W30 Log Folder (blank = Reference)', placeholder: '', default: init.logDir },
        { key: 'filePattern', label: 'W30 File Pattern', placeholder: 'AttendanceLog*.dat', default: init.filePattern },
      ];

      let autoId = !init.deviceId;
      if (!autoId && init.deviceIp && init.deviceType) {
        const expected = String(init.deviceType || '').trim().toUpperCase() + '-' + init.deviceIp;
        if (init.deviceId.toUpperCase() === expected.toUpperCase()) autoId = true;
      }

      openModal(title, fields, init, async (obj) => {
        const payload = {
          deviceId: String(obj.deviceId || '').trim(),
          deviceType: String(obj.deviceType || 'WL10').trim(),
          deviceIp: String(obj.deviceIp || '').trim(),
          devicePort: obj.devicePort === '' || obj.devicePort === null || obj.devicePort === undefined ? 0 : parseInt(String(obj.devicePort), 10),
          readerMode: String(obj.readerMode || '').trim(),
          logDir: String(obj.logDir || '').trim(),
          filePattern: String(obj.filePattern || '').trim(),
        };
        if (isEdit && !payload.deviceId) payload.deviceId = String(e.deviceId || '').trim();
        const res = await postJson('/api/devices/upsert', payload);
        if (!res || res.ok === false) throw new Error(String((res && res.error) || 'Save failed'));
        await refreshSettingsDevices();
        await refreshStatus();
      });

      const typeEl = el('modal_deviceType');
      const idEl = el('modal_deviceId');
      const ipEl = el('modal_deviceIp');
      const portEl = el('modal_devicePort');
      const rmEl = el('modal_readerMode');
      const dirEl = el('modal_logDir');
      const patEl = el('modal_filePattern');

      function refreshDeviceModalDefaults() {
        const type = String(typeEl && typeEl.value ? typeEl.value : 'WL10').trim().toUpperCase();
        const ip = String(ipEl && ipEl.value ? ipEl.value : '').trim();
        if (rmEl) {
          if (type === 'W30') rmEl.value = 'file';
          else if (!rmEl.value || String(rmEl.value).trim().toLowerCase() === 'file') rmEl.value = 'auto';
        }
        if (portEl) {
          const raw = String(portEl.value || '').trim();
          if (!raw) {
            if (type === 'WL10') portEl.value = '4370';
          }
        }
        if (patEl) {
          const raw = String(patEl.value || '').trim();
          if (type === 'W30' && !raw) patEl.value = 'AttendanceLog*.dat';
        }
        if (dirEl) {
          dirEl.disabled = type !== 'W30';
        }
        if (patEl) {
          patEl.disabled = type !== 'W30';
        }
        if (idEl) {
          const cur = String(idEl.value || '').trim();
          if (!cur) autoId = true;
          if (autoId) {
            const next = (type || 'WL10') + '-' + (ip || 'unknown');
            idEl.value = next;
          }
        }
      }

      if (idEl) idEl.addEventListener('input', () => { autoId = false; });
      if (typeEl) typeEl.addEventListener('change', refreshDeviceModalDefaults);
      if (ipEl) ipEl.addEventListener('input', refreshDeviceModalDefaults);
      refreshDeviceModalDefaults();
    }

    async function refreshStatus() {
      const j = await getJson('/api/status');
      if (!j) { setApiOfflineUi(); return false; }
      lastStatus = j;
      hideQuickFix();

      syncRawDevicePickOptions(j);
      updateRawDeviceActions();
      if (activeTab === 'settings') { await refreshSettingsDevices(); }

      isSuperadmin = !!j.isSuperadmin;
      const staffImportBtn = el('staffImport');
      if (staffImportBtn) staffImportBtn.disabled = !isSuperadmin;
      const staffSelectAll = el('staffSelectAll');
      if (staffSelectAll) staffSelectAll.style.display = isSuperadmin ? '' : 'none';
      updateStaffDeleteEnabled();
      updateStaffSortUi();
      const shiftImportBtn = el('shiftImport');
      if (shiftImportBtn) shiftImportBtn.disabled = !isSuperadmin;
      const shiftExportBtn = el('shiftExport');
      if (shiftExportBtn) shiftExportBtn.disabled = !isSuperadmin;

      const sheetDailyDownloadBtn = el('sheetDailyDownload');
      if (sheetDailyDownloadBtn) sheetDailyDownloadBtn.style.display = isSuperadmin ? '' : 'none';
      const sheetWeeklyDownloadBtn = el('sheetWeeklyDownload');
      if (sheetWeeklyDownloadBtn) sheetWeeklyDownloadBtn.style.display = isSuperadmin ? '' : 'none';
      const sheetMonthlyDownloadBtn = el('sheetMonthlyDownload');
      if (sheetMonthlyDownloadBtn) sheetMonthlyDownloadBtn.style.display = isSuperadmin ? '' : 'none';
      const sheetMonthlyBulkDownloadBtn = el('sheetMonthlyBulkDownload');
      if (sheetMonthlyBulkDownloadBtn) sheetMonthlyBulkDownloadBtn.style.display = (isSuperadmin && monthlySelectedStaff && monthlySelectedStaff.size > 0) ? '' : 'none';
      const sheetMonthlySelectAll = el('sheetMonthlySelectAll');
      if (sheetMonthlySelectAll) sheetMonthlySelectAll.style.display = isSuperadmin ? '' : 'none';

      try {
        const devBody = el('settingsDevicesBody');
        if (devBody) {
          const devs = (j && Array.isArray(j.devices)) ? j.devices : [];
          if (!devs.length) {
            devBody.innerHTML = '<tr><td colspan="5" class="muted">No configured devices.</td></tr>';
          } else {
            devBody.innerHTML = devs.map(d => {
              const id = String((d && d.deviceId) || '').trim();
              const type = String((d && d.type) || '').trim();
              const ip = String((d && d.ip) || '').trim();
              const port = String((d && d.port) ?? '').trim();
              const rm = String((d && d.readerMode) || '').trim();
              const active = !!(d && d.active);
              const ipPort = (ip && port) ? (ip + ':' + port) : (ip || port || '-');
              const logDir = String((d && d.logDir) || '').trim();
              const w30Dir = (type && type.toUpperCase() === 'W30') ? (logDir || '(Reference)') : '-';
              const okAt = (d && d.lastOkAtUtc) ? fmt(String(d.lastOkAtUtc || '')) : '-';
              return '<tr>' +
                '<td>' + escHtml((type ? type + ' ' : '') + (id || '-')) + (active ? ' <span class="pill">Active</span>' : '') + '</td>' +
                '<td class="mono">' + escHtml(ipPort) + '</td>' +
                '<td>' + escHtml(rm || '-') + '</td>' +
                '<td class="mono">' + escHtml(w30Dir) + '</td>' +
                '<td class="mono">' + escHtml(okAt) + '</td>' +
              '</tr>';
            }).join('');
          }
        }

        const syncBody = el('settingsDeviceSyncBody');
        if (syncBody) {
          const devs = (j && Array.isArray(j.devices)) ? j.devices : [];
          if (!devs.length) {
            syncBody.innerHTML = '<tr><td colspan="5" class="muted">No configured devices.</td></tr>';
          } else {
            const activeId = String((j && j.device && j.device.deviceId) || '').trim();
            const lastSyncAt = (j && j.sync) ? (j.sync.lastSyncFinishedAtUtc || j.sync.lastSyncStartedAtUtc || '') : '';
            const lastReadAt = (j && j.sync) ? (j.sync.lastLocalWatermarkUtc || '') : '';
            syncBody.innerHTML = devs.map(d => {
              const id = String((d && d.deviceId) || '').trim();
              const type = String((d && d.type) || '').trim().toUpperCase();
              const active = id && activeId && id === activeId;
              const mode = type === 'W30' ? 'Import' : (type === 'WL10' ? 'Sync Device' : '-');
              const lastSync = type === 'W30'
                ? ((d && d.lastProcessedAtUtc) ? fmt(String(d.lastProcessedAtUtc || '')) : '-')
                : (active ? fmt(String(lastSyncAt || '')) : '-');
              const lastRead = (type === 'WL10' && active) ? fmt(String(lastReadAt || '')) : '-';
              let notes = '';
              if (type === 'W30') {
                const c = (d && Number.isFinite(Number(d.processedFilesCount))) ? Number(d.processedFilesCount) : 0;
                const pat = String((d && d.filePattern) || '').trim();
                notes = 'Processed files: ' + String(c) + (pat ? (' • ' + pat) : '');
              } else if (type === 'WL10' && active) {
                notes = 'Read punches: ' + String((j && j.sync && (j.sync.lastRunPunchCount ?? 0)) ?? 0) + ' • Total cached: ' + String((j && j.sync && (j.sync.deviceRecordsTotal ?? 0)) ?? 0);
              }
              return '<tr>' +
                '<td>' + escHtml((type ? type + ' ' : '') + (id || '-')) + (active ? ' <span class="pill">Active</span>' : '') + '</td>' +
                '<td>' + escHtml(mode) + '</td>' +
                '<td class="mono">' + escHtml(lastSync) + '</td>' +
                '<td class="mono">' + escHtml(lastRead) + '</td>' +
                '<td class="mono">' + escHtml(notes || '-') + '</td>' +
              '</tr>';
            }).join('');
          }
        }
      } catch { }

      const ipEl = el('ip'); if (ipEl) ipEl.textContent = j.device.ip;
      const portEl = el('port'); if (portEl) portEl.textContent = j.device.port;
      const deviceIdEl = el('deviceId'); if (deviceIdEl) deviceIdEl.textContent = j.device.deviceId;
      const readerModeEl = el('readerMode'); if (readerModeEl) {
        const raw = String(j.device.readerMode || '').trim();
        if (!raw) readerModeEl.textContent = '-';
        else if (raw.toLowerCase() === 'com') readerModeEl.textContent = 'COM';
        else readerModeEl.textContent = raw.charAt(0).toUpperCase() + raw.slice(1);
      }

      const reach = el('reach');
      if (reach) {
        if (j.device.reachable) {
          reach.className = 'pill ok';
          reach.textContent = 'Connected (TCP) ' + (j.device.rttMs ? (j.device.rttMs + 'ms') : '');
        } else {
          reach.className = 'pill bad';
          reach.textContent = 'Disconnected ' + (j.device.reachError ? ('(' + j.device.reachError + ')') : '');
        }
      }

      const sumDeviceSyncPill = el('sumDeviceSyncPill');
      if (sumDeviceSyncPill) {
        if (j.device.reachable) { sumDeviceSyncPill.className = 'pill ok'; sumDeviceSyncPill.textContent = 'Online'; }
        else { sumDeviceSyncPill.className = 'pill bad'; sumDeviceSyncPill.textContent = 'Offline'; }
      }
      const sumDbSyncPill = el('sumDbSyncPill');
      if (sumDbSyncPill) {
        const ok = !!(j.supabase && j.supabase.configured && j.supabase.syncEnabled);
        if (ok) { sumDbSyncPill.className = 'pill ok'; sumDbSyncPill.textContent = 'Online'; }
        else { sumDbSyncPill.className = 'pill bad'; sumDbSyncPill.textContent = 'Offline'; }
      }

      const autoEl = el('auto'); if (autoEl) autoEl.textContent = normText(j.sync.autoSyncEnabled ? 'on' : 'off');
      const intervalWithUnitEl = el('intervalWithUnit'); if (intervalWithUnitEl) intervalWithUnitEl.textContent = (j.sync && j.sync.nextSyncAtUtc) ? fmt(j.sync.nextSyncAtUtc) : '-';
      const dashSec = (j.dashboard && Number.isFinite(Number(j.dashboard.refreshSeconds))) ? Number(j.dashboard.refreshSeconds) : 600;
      const dashMin = (j.dashboard && Number.isFinite(Number(j.dashboard.refreshMinutes))) ? Number(j.dashboard.refreshMinutes) : Math.max(1, Math.round(dashSec / 60));
      const autoOn = !!(j.sync && j.sync.autoSyncEnabled);
      if (autoOn && j.sync && j.sync.nextSyncAtUtc) {
        const targetMs = new Date(j.sync.nextSyncAtUtc).getTime() - Date.now() + 2000;
        uiPollMs = Math.max(1000, Math.min(30 * 60 * 1000, Math.round(targetMs)));
      } else {
        uiPollMs = Math.max(1000, Math.round(dashSec * 1000));
      }
      const runCountEl = el('runCount'); if (runCountEl) runCountEl.textContent = String(j.sync.deviceRecordsTotal ?? 0);
      const sentCountEl = el('sentCount'); if (sentCountEl) sentCountEl.textContent = String(j.sync.lastRunUpsertedCount ?? 0);
      const localWmEl = el('localWm'); if (localWmEl) localWmEl.textContent = fmt(j.sync.lastLocalWatermarkUtc);
      const dbWmEl = el('dbWm'); if (dbWmEl) dbWmEl.textContent = j.supabase.syncEnabled ? fmt(j.sync.lastDbWatermarkUtc) : 'Disabled';
      const lastResEl = el('lastRes'); if (lastResEl) lastResEl.textContent = String(j.sync.lastRunPunchCount ?? 0);
      const deviceSyncTableEl = el('deviceSyncTable'); if (deviceSyncTableEl) deviceSyncTableEl.textContent = 'Device Records';
      const deviceSyncStatusEl = el('deviceSyncStatus');
      if (deviceSyncStatusEl) {
        if (!j.device.reachable) {
          deviceSyncStatusEl.className = 'pill bad';
          deviceSyncStatusEl.textContent = 'Offline';
        } else {
          deviceSyncStatusEl.className = 'pill ok';
          deviceSyncStatusEl.textContent = 'Online';
        }
      }
      const lastSyncAt = j.sync.lastSyncFinishedAtUtc || j.sync.lastSyncStartedAtUtc || '';
      const lastSyncAtEl = el('lastSyncAt'); if (lastSyncAtEl) lastSyncAtEl.textContent = fmt(lastSyncAt);
      const lastSyncKpiEl = el('lastSyncKpi'); if (lastSyncKpiEl) lastSyncKpiEl.textContent = fmtShort(lastSyncAt);
      const lastSyncDateKpiEl = el('lastSyncDateKpi'); if (lastSyncDateKpiEl) lastSyncDateKpiEl.innerHTML = 'Update on <strong>' + escHtml(fmtDateOnly(lastSyncAt)) + '</strong>';
      const lastResKpiEl = el('lastResKpi'); if (lastResKpiEl) lastResKpiEl.textContent = String(j.sync.lastRunPunchCount ?? 0);
      const err = j.sync.lastSyncError || '';
      const lastErrBriefEl = el('lastErrBrief'); if (lastErrBriefEl) lastErrBriefEl.textContent = (err.split('\n')[0] || '');
      const box = el('lastErrBox');
      const lastErrEl = el('lastErr');
      if (box && lastErrEl) {
        if (err) { box.style.display = ''; lastErrEl.textContent = err; } else { box.style.display = 'none'; lastErrEl.textContent = ''; }
      }

      setValIfNotFocused('setIp', j.device.ip || '');
      setValIfNotFocused('setPort', String(j.device.port || ''));
      setValIfNotFocused('setReader', j.device.readerMode || 'native');
      setValIfNotFocused('setDashRefresh', String(dashMin || 10));
      setValIfNotFocused('setAuto', j.sync.autoSyncEnabled ? 'true' : 'false');
      const timesRaw = (j.sync && Array.isArray(j.sync.scheduleLocalTimes)) ? j.sync.scheduleLocalTimes.join(',') : '';
      setValIfNotFocused('schedTimes', timesRaw);

      try {
        const intervalSec = (j.sync && Number.isFinite(Number(j.sync.pollIntervalSeconds))) ? Number(j.sync.pollIntervalSeconds) : 3600;
        let unit = 'min';
        let every = Math.max(1, Math.round(intervalSec / 60));
        if (intervalSec % 3600 === 0 && intervalSec >= 3600) { unit = 'hr'; every = Math.max(1, Math.round(intervalSec / 3600)); }
        setValIfNotFocused('pollEvery', String(every));
        setValIfNotFocused('pollUnit', unit);
      } catch { }

      try {
        const pollEvery = el('pollEvery');
        const pollUnit = el('pollUnit');
        if (pollEvery) pollEvery.disabled = !autoOn;
        if (pollUnit) pollUnit.disabled = !autoOn;
        const refreshPeriodLabel = el('refreshPeriodLabel');
        const refreshPeriodRow = el('refreshPeriodRow');
        if (refreshPeriodLabel) refreshPeriodLabel.style.display = autoOn ? '' : 'none';
        if (refreshPeriodRow) refreshPeriodRow.style.display = autoOn ? '' : 'none';
      } catch { }

      try {
        mandatorySyncTimes = (timesRaw ? timesRaw.split(',') : []).map(s => String(s || '').trim()).filter(s => !!s);
        renderMandatorySyncTimes();
      } catch { }
      setValIfNotFocused('supaUrl', (j.supabase && j.supabase.url) ? j.supabase.url : '');
      setValIfNotFocused('supaProjectId', (j.supabase && j.supabase.projectId) ? j.supabase.projectId : '');
      setValIfNotFocused('supaTable', (j.supabase && j.supabase.attendanceTable) ? j.supabase.attendanceTable : '');
      setValIfNotFocused('supaSyncMode', (j.supabase && j.supabase.syncEnabled) ? 'enabled' : 'disabled');
      setValIfNotFocused('supaPubKey', (j.supabase && j.supabase.anonKey) ? j.supabase.anonKey : '');
      setValIfNotFocused('supaKey', (j.supabase && j.supabase.serviceRoleKey) ? j.supabase.serviceRoleKey : '');
      setValIfNotFocused('supaJwt', '');
      const desktopInfo = el('desktopInfo');
      if (desktopInfo) {
        const bestLabel = (j.pc && j.pc.bestLabel) ? String(j.pc.bestLabel || '').trim() : '';
        const bestName = (j.pc && j.pc.bestInterfaceName) ? String(j.pc.bestInterfaceName || '').trim() : '';
        const best = (j.pc && j.pc.bestIpv4) ? String(j.pc.bestIpv4) : '';
        const iface = bestName || (bestLabel.includes(':') ? bestLabel.split(':')[0] : '');
        const desktopLine = 'Desktop IP: ' + (bestLabel || best || '-');
        const ifaceLine = 'Active Network: ' + (iface || '-');
        const dashPort = (j.dashboard && j.dashboard.port) ? String(j.dashboard.port) : (window.location.port || '5099');
        const dashHost = best || '127.0.0.1';
        const dashLine = 'Dashboard URL: http://' + dashHost + ':' + dashPort + '/';
        try { if (best) localStorage.setItem('wl10dash.bestIpv4', best); } catch { }
        desktopInfo.innerHTML =
          '<li>' + escHtml(ifaceLine) + '</li>' +
          '<li>' + escHtml(desktopLine) + '</li>' +
          '<li>' + escHtml(dashLine) + '</li>';
      }

      const supaPill = el('supaPill');
      if (!j.supabase.configured) {
        supaPill.className = 'pill bad';
        supaPill.textContent = 'Offline';
      } else if (j.supabase.syncEnabled) {
        supaPill.className = 'pill ok';
        supaPill.textContent = 'Online';
      } else {
        supaPill.className = 'pill bad';
        supaPill.textContent = 'Offline';
      }
      el('supaTableState').textContent = normText(j.supabase.attendanceTable);
      const dbSyncKpiPill = el('dbSyncKpiPill');
      if (dbSyncKpiPill) {
        if (!j.supabase.configured) { dbSyncKpiPill.className = 'pill bad'; dbSyncKpiPill.textContent = 'Offline'; }
        else if (j.supabase.syncEnabled) { dbSyncKpiPill.className = 'pill ok'; dbSyncKpiPill.textContent = 'Online'; }
        else { dbSyncKpiPill.className = 'pill bad'; dbSyncKpiPill.textContent = 'Offline'; }
      }
      const dbSyncKpiTable = el('dbSyncKpiTable'); if (dbSyncKpiTable) dbSyncKpiTable.textContent = normText(j.supabase.attendanceTable);
      const supaAutoEl = el('supaAuto'); if (supaAutoEl) supaAutoEl.textContent = normText(j.supabase.syncEnabled ? 'on' : 'off');
      const supaIntervalEl = el('supaInterval'); if (supaIntervalEl) supaIntervalEl.textContent = (j.sync && j.sync.nextSyncAtUtc) ? fmt(j.sync.nextSyncAtUtc) : '-';
      const supaLastResEl = el('supaLastRes'); if (supaLastResEl) supaLastResEl.textContent = String(j.sync.lastSupabaseUpsertedCount ?? 0);
      const supaLastSyncAt = j.sync.lastSupabaseSyncFinishedAtUtc || j.sync.lastSupabaseSyncStartedAtUtc || '';
      const supaLastSyncAtEl = el('supaLastSyncAt'); if (supaLastSyncAtEl) supaLastSyncAtEl.textContent = fmt(supaLastSyncAt);
      const supaLastSyncKpiEl = el('supaLastSyncKpi'); if (supaLastSyncKpiEl) supaLastSyncKpiEl.textContent = fmtShort(supaLastSyncAt);
      const supaLastSyncDateKpiEl = el('supaLastSyncDateKpi'); if (supaLastSyncDateKpiEl) supaLastSyncDateKpiEl.innerHTML = 'Update on <strong>' + escHtml(fmtDateOnly(supaLastSyncAt)) + '</strong>';
      const supaLastResKpiEl = el('supaLastResKpi'); if (supaLastResKpiEl) supaLastResKpiEl.textContent = String(j.sync.lastSupabaseUpsertedCount ?? 0);
      const supaTotalRecordsEl = el('supaTotalRecords'); if (supaTotalRecordsEl) supaTotalRecordsEl.textContent = String(j.sync.dbRecordsTotal ?? 0);
      const supaUrlState = el('supaUrlState');
      if (supaUrlState) supaUrlState.textContent = '';

      setValIfNotFocused('supaUrl', j.supabase.url || '');
      setValIfNotFocused('supaProjectId', j.supabase.projectId || '');
      setValIfNotFocused('supaTable', j.supabase.attendanceTable || '');
      setValIfNotFocused('supaSyncMode', j.supabase.syncEnabled ? 'enabled' : 'disabled');
      setValIfNotFocused('supaPubKey', j.supabase.anonKey || '');
      setValIfNotFocused('supaKey', j.supabase.serviceRoleKey || '');
      setValIfNotFocused('supaJwt', '');

      const settingsReach = el('settingsReach');
      if (settingsReach) {
        if (j.device.reachable) {
          settingsReach.className = 'pill ok';
          settingsReach.textContent = 'Connected (TCP) ' + (j.device.rttMs ? (j.device.rttMs + 'ms') : '');
        } else {
          settingsReach.className = 'pill bad';
          settingsReach.textContent = 'Disconnected ' + (j.device.reachError ? ('(' + j.device.reachError + ')') : '');
        }
      }

      try {
        const did = sessionStorage.getItem('wl10dash.autosync.login');
        const can = !!(j.device && j.device.reachable) || !!(j.supabase && j.supabase.configured && j.supabase.syncEnabled);
        if (!did && can) {
          sessionStorage.setItem('wl10dash.autosync.login', '1');
          triggerAutoSync('login', true);
        }
      } catch { }

      return true;
    }

    function escHtml(s) {
      return String(s ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }
    function compactFlags(s) {
      const raw = String(s ?? '').trim();
      if (!raw) return '';
      const parts = raw.split('|').map(x => String(x || '').trim()).filter(Boolean);
      const out = [];
      for (const p of parts) {
        const up = p.toUpperCase();
        if (up === 'MISSING_OUT') { out.push('Miss Out'); continue; }
        if (up === 'MISSING_IN') { out.push('Miss In'); continue; }
        if (up === 'ODD') { out.push('Odd'); continue; }
        if (up.startsWith('DUPLICATE')) {
          const m = p.match(/x\s*(\d+)/i);
          out.push('Dup×' + (m ? m[1] : ''));
          continue;
        }
        out.push(p.replaceAll('_', ' '));
      }
      return out.join(' | ');
    }

    let modalOnSave = null;
    function closeModal() {
      const back = el('modalBack');
      if (back) back.style.display = 'none';
      const body = el('modalBody');
      if (body) { body.innerHTML = ''; body.className = 'modalBody modalGrid'; }
      const err = el('modalError');
      if (err) err.textContent = '';
      const saveBtn = el('modalSave');
      if (saveBtn) saveBtn.style.display = '';
      const cancelBtn = el('modalCancel');
      if (cancelBtn) cancelBtn.textContent = 'Cancel';
      modalOnSave = null;
    }

    function openModal(title, fields, initialValues, onSaveAsync) {
      const back = el('modalBack');
      const body = el('modalBody');
      const titleEl = el('modalTitle');
      const err = el('modalError');
      if (!back || !body || !titleEl || !err) return;
      titleEl.textContent = title || '';
      err.textContent = '';
      body.className = 'modalBody modalGrid';
      body.innerHTML = '';

      for (const f of (fields || [])) {
        const lab = document.createElement('label');
        lab.textContent = f.label || f.key || '';
        let ctrl = null;
        const kind = String(f.kind || '').trim().toLowerCase();
        if (kind === 'select') {
          const sel = document.createElement('select');
          sel.id = 'modal_' + f.key;
          const opts = Array.isArray(f.options) ? f.options : [];
          for (const o of opts) {
            const opt = document.createElement('option');
            opt.value = String(o ?? '');
            opt.textContent = String(o ?? '');
            sel.appendChild(opt);
          }
          const raw = (initialValues && (initialValues[f.key] ?? null) !== null) ? String(initialValues[f.key]) : '';
          const v = raw || String(f.default || '');
          if (v) sel.value = v;
          if (f.disabled) sel.disabled = true;
          ctrl = sel;
        } else if (kind === 'datalist') {
          const inp = document.createElement('input');
          const listId = 'modal_list_' + f.key;
          inp.id = 'modal_' + f.key;
          inp.type = f.type || 'text';
          inp.placeholder = f.placeholder || '';
          inp.setAttribute('list', listId);
          inp.value = (initialValues && (initialValues[f.key] ?? null) !== null) ? String(initialValues[f.key]) : '';
          if (f.disabled) inp.disabled = true;
          const dl = document.createElement('datalist');
          dl.id = listId;
          const opts = Array.isArray(f.options) ? f.options : [];
          for (const o of opts) {
            const opt = document.createElement('option');
            opt.value = String(o ?? '');
            dl.appendChild(opt);
          }
          body.appendChild(lab);
          body.appendChild(inp);
          body.appendChild(dl);
          continue;
        } else {
          const input = document.createElement('input');
          input.id = 'modal_' + f.key;
          input.type = f.type || (kind === 'checkbox' ? 'checkbox' : 'text');
          input.placeholder = f.placeholder || '';
          if (kind === 'checkbox') {
            const raw = (initialValues && (initialValues[f.key] ?? null) !== null) ? String(initialValues[f.key]) : '';
            const def = String(f.default || '').toLowerCase();
            input.checked = (raw === 'true' || raw === '1' || raw === 'yes') || (raw === '' && (def === 'true' || def === '1' || def === 'yes'));
          } else {
            input.value = (initialValues && (initialValues[f.key] ?? null) !== null) ? String(initialValues[f.key]) : (String(f.default || '') || '');
          }
          if (f.disabled) input.disabled = true;
          ctrl = input;
        }
        body.appendChild(lab);
        body.appendChild(ctrl);
      }

      modalOnSave = async () => {
        const values = {};
        for (const f of (fields || [])) {
          const inp = el('modal_' + f.key);
          const kind = String(f.kind || '').trim().toLowerCase();
          if (kind === 'checkbox') values[f.key] = !!(inp && inp.checked);
          else values[f.key] = inp ? String(inp.value ?? '').trim() : '';
        }
        const saveBtn = el('modalSave');
        const cancelBtn = el('modalCancel');
        const closeBtn = el('modalClose');
        if (saveBtn) saveBtn.disabled = true;
        if (cancelBtn) cancelBtn.disabled = true;
        if (closeBtn) closeBtn.disabled = true;
        try {
          await onSaveAsync(values);
          closeModal();
        } catch (e) {
          err.textContent = String(e || 'Save failed');
        } finally {
          if (saveBtn) saveBtn.disabled = false;
          if (cancelBtn) cancelBtn.disabled = false;
          if (closeBtn) closeBtn.disabled = false;
        }
      };

      back.style.display = 'flex';
      const first = body.querySelector('input,select');
      if (first) first.focus();
    }

    function openModalHtml(title, html, options) {
      const back = el('modalBack');
      const body = el('modalBody');
      const titleEl = el('modalTitle');
      const err = el('modalError');
      if (!back || !body || !titleEl || !err) return;
      const card = back.querySelector('.modalCard');
      titleEl.textContent = title || '';
      err.textContent = '';
      body.className = 'modalBody' + ((options && options.bodyClass) ? (' ' + String(options.bodyClass)) : '');
      body.innerHTML = html || '';
      if (card) card.className = 'modalCard' + ((options && options.cardClass) ? (' ' + String(options.cardClass)) : '');

      const saveBtn = el('modalSave');
      const cancelBtn = el('modalCancel');
      if (saveBtn) saveBtn.style.display = (options && options.hideSave) ? 'none' : '';
      if (cancelBtn) cancelBtn.textContent = (options && options.cancelText) ? String(options.cancelText) : 'Cancel';
      modalOnSave = (options && options.onSaveAsync) ? options.onSaveAsync : null;

      back.style.display = 'flex';
    }

    function staffInitials(name) {
      const s = String(name || '').trim();
      if (!s) return '?';
      const parts = s.split(/\s+/).filter(Boolean);
      const a = parts[0] ? parts[0][0] : s[0];
      const b = parts.length > 1 ? parts[parts.length - 1][0] : '';
      return (String(a || '') + String(b || '')).toUpperCase();
    }

    function fmtMinToHrs(min) {
      const m = Number(min) || 0;
      if (m <= 0) return '-';
      const h = Math.floor(m / 60);
      const r = m % 60;
      if (h <= 0) return r + 'm';
      return h + 'h ' + String(r).padStart(2, '0') + 'm';
    }

    function renderStaffMonthModal(data, staffId, month) {
      const ok = !!(data && data.ok);
      if (!ok) {
        openModalHtml('Attendance Details', '<div class="muted">' + escHtml((data && data.error) ? data.error : 'Failed to load.') + '</div>', { hideSave: true, cancelText: 'Close', cardClass: 'wide' });
        return;
      }
      const st = data.staff || {};
      const stats = data.stats || {};
      const sched = data.schedule || {};
      const days = Array.isArray(data.days) ? data.days : [];

      const header =
        '<div class="staffHead">' +
          '<div class="staffLeft">' +
            '<div class="staffAvatar">' + escHtml(staffInitials(st.name || staffId)) + '</div>' +
            '<div style="min-width:0">' +
              '<div class="staffName">' + escHtml(st.name || staffId) + '</div>' +
              '<div class="staffMeta">' + escHtml([st.id || staffId, st.department || '', st.shiftPattern || ''].filter(Boolean).join(' • ')) + '</div>' +
            '</div>' +
          '</div>' +
          '<div class="row" style="gap:8px">' +
            '<input id="staffMonthPicker" class="monthInput" type="month" value="' + escHtml(month || '') + '"/>' +
          '</div>' +
        '</div>';

      const statHtml =
        '<div class="staffStats">' +
          '<div class="staffStat"><div class="staffStatT">Present</div><div class="staffStatV">' + escHtml(String(stats.presentDays ?? '-')) + '</div></div>' +
          '<div class="staffStat"><div class="staffStatT">Absent</div><div class="staffStatV">' + escHtml(String(stats.absentDays ?? '-')) + '</div></div>' +
          '<div class="staffStat"><div class="staffStatT">Late</div><div class="staffStatV">' + escHtml(String(stats.lateDays ?? '-')) + '</div></div>' +
          '<div class="staffStat"><div class="staffStatT">Missing OUT</div><div class="staffStatV">' + escHtml(String(stats.missingOutDays ?? '-')) + '</div></div>' +
          '<div class="staffStat"><div class="staffStatT">Duplicates</div><div class="staffStatV">' + escHtml(String(stats.duplicatePunches ?? '-')) + '</div></div>' +
        '</div>';

      const axis =
        '<div class="axis">' +
          '<span>00:00</span><span>06:00</span><span>12:00</span><span>18:00</span><span>24:00</span>' +
        '</div>';

      function minToHm(m) {
        const mm = Math.max(0, Math.min(1440, Number(m) || 0));
        const h = Math.floor(mm / 60);
        const r = mm % 60;
        return String(h).padStart(2, '0') + ':' + String(r).padStart(2, '0');
      }

      const cutoffIso = todayKlIso();
      const cutoffMonth = cutoffIso.slice(0, 7);
      const list = days
        .slice()
        .filter(d => {
          const iso = String(d && d.date ? d.date : '').slice(0, 10);
          if (!iso) return false;
          if (String(month || '').trim() !== cutoffMonth) return true;
          return iso <= cutoffIso;
        })
        .sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')))
        .map(d => {
        const segs = Array.isArray(d.segments) ? d.segments : [];
        const segHtml = segs.map(s => {
          const a = Math.max(0, Math.min(1440, Number(s.start) || 0));
          const b = Math.max(0, Math.min(1440, Number(s.end) || 0));
          if (b <= a) return '';
          const left = (a / 1440) * 100;
          const width = ((b - a) / 1440) * 100;
        const kind = String(s.kind || 'work');
        const label = kind === 'extra' ? 'Extra time' : (kind === 'break' ? 'Break' : 'Working time');
          const tip = label + '\n' + minToHm(a) + '–' + minToHm(b) + ' (' + fmtMinToHrs(b - a) + ')';
          return '<div class="seg ' + escHtml(kind) + '" style="left:' + left.toFixed(3) + '%;width:' + width.toFixed(3) + '%"><div class="segTip">' + escHtml(tip).replace(/\n/g, '<br/>') + '</div></div>';
        }).join('');
        const dateIso = String(d.date || '').slice(0, 10);
        const title = isoToDmy(dateIso) + ' (' + String(d.dow || '').slice(0, 3) + ')';
        const times = (d.clockIn || '-') + ' → ' + (d.clockOut || '-');
        const dur = fmtMinToHrs(d.durationMin || 0);
        const hint = (sched && sched.start && sched.end) ? ('Shift ' + String(sched.start) + '–' + String(sched.end)) : '';
        const wd = !!d.workingDay;
        const pCount = Array.isArray(d.punches) ? d.punches.length : 0;
        let badge = '';
        let badgeCls = 'dayBadge';
        if (!wd) { badge = 'Off day'; badgeCls += ' off'; }
        else if (pCount >= 1) { badge = 'Present'; if (pCount % 2 === 1) badgeCls += ' warn'; }
        else { badge = 'Absent'; badgeCls += ' bad'; }
        const missAt = Number(d.missingOutAtMin) || 0;
        const missDot = (wd && pCount % 2 === 1 && missAt > 0) ? ('<div class="missDot" style="left:' + ((Math.max(0, Math.min(1440, missAt)) / 1440) * 100).toFixed(3) + '%" title="No clock-out"></div>') : '';
        return (
          '<div class="dayCard">' +
            '<div class="dayTop">' +
              '<div style="min-width:0">' +
                '<div class="dayTitle">' + escHtml(title) + '</div>' +
                '<div class="staffMeta">' + escHtml(hint) + '</div>' +
              '</div>' +
              '<div style="text-align:right">' +
                '<div class="' + escHtml(badgeCls) + '">' + escHtml(badge) + '</div>' +
                '<div class="dayTimes">' + escHtml(times) + '</div>' +
                '<div class="staffMeta">' + escHtml(dur) + '</div>' +
              '</div>' +
            '</div>' +
            '<div class="tl">' + axis + '<div class="track">' + segHtml + missDot + '</div></div>' +
          '</div>'
        );
      }).join('');

      const legend =
        '<div class="tlLegend">' +
          '<span class="tlLeg"><span class="tlDot work"></span>Working time</span>' +
          '<span class="tlLeg"><span class="tlDot break"></span>Break time</span>' +
          '<span class="tlLeg"><span class="tlDot extra"></span>Extra time</span>' +
          '<span class="tlLeg"><span class="tlDot miss"></span>No clock-out</span>' +
        '</div>';

      const body =
        header +
        statHtml +
        legend +
        '<div class="dayList">' + list + '</div>';

      openModalHtml('Attendance Details', body, { hideSave: true, cancelText: 'Close', cardClass: 'wide' });

      const picker = el('staffMonthPicker');
      if (picker) picker.addEventListener('change', async () => {
        const v = String(picker.value || '').trim();
        if (!/^\d{4}-\d{2}$/.test(v)) return;
        try {
          const j = await getJson('/api/staff/attendance/month?staffId=' + encodeURIComponent(staffId) + '&month=' + encodeURIComponent(v));
          renderStaffMonthModal(j, staffId, v);
        } catch { }
      });
    }

    async function openStaffAttendanceModal(staffId, dateIso) {
      const sid = String(staffId || '').trim();
      const d = String(dateIso || '').slice(0, 10);
      const m = (d && d.length >= 7) ? d.slice(0, 7) : todayKlIso().slice(0, 7);
      openModalHtml('Attendance Details', '<div class="muted">Loading…</div>', { hideSave: true, cancelText: 'Close', cardClass: 'wide' });
      const j = await getJson('/api/staff/attendance/month?staffId=' + encodeURIComponent(sid) + '&month=' + encodeURIComponent(m));
      renderStaffMonthModal(j, sid, m);
    }

    async function refreshLogs() {
      const j = await getJson('/api/logs');
      if (!j) return;
      const logsEl = el('logs');
      if (!logsEl) return;
      const lines = Array.isArray(j.lines) ? j.lines.slice() : [];
      lastSystemLogLines = lines.slice(-500);
      logsEl.textContent = lastSystemLogLines.join('\n');
    }

    async function renderTable(tbodyId, rows, emptyText) {
      const body = el(tbodyId);
      body.innerHTML = '';
      if (!rows || !rows.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="6" class="muted">' + escHtml(emptyText) + '</td>';
        body.appendChild(tr);
        return;
      }
      for (const row of rows) {
        const tr = document.createElement('tr');
        tr.innerHTML =
          '<td>' + escHtml(row.staff_id ?? '') + '</td>' +
          '<td>' + escHtml(row.datetime ?? '') + '</td>' +
          '<td>' + escHtml(row.verified ?? '') + '</td>' +
          '<td>' + escHtml(row.status ?? '') + '</td>' +
          '<td>' + escHtml(row.workcode ?? '') + '</td>' +
          '<td>' + escHtml(row.reserved ?? '') + '</td>';
        body.appendChild(tr);
      }
    }

    async function renderAttlogTable(rows, emptyText) {
      const body = el('deviceBody');
      body.innerHTML = '';
      if (!rows || !rows.length) {
        const box = el('deviceLoadBox');
        if (box) box.style.display = 'none';
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="7" class="muted">' + escHtml(emptyText) + '</td>';
        body.appendChild(tr);
        return;
      }
      const box = el('deviceLoadBox');
      const ring = el('deviceLoadRing');
      const pctEl = el('deviceLoadPct');
      const txtEl = el('deviceLoadText');
      function setLoad(pct, text) {
        if (box) box.style.display = '';
        if (ring) ring.style.setProperty('--pct', Math.max(0, Math.min(100, pct)) + '%');
        if (pctEl) pctEl.textContent = Math.max(0, Math.min(100, pct)) + '%';
        if (txtEl) txtEl.innerHTML = '<strong>Loading</strong> ' + escHtml(text || '');
      }
      setLoad(0, 'Preparing table...');

      const total = rows.length;
      const chunk = 300;
      for (let i = 0; i < total; i += chunk) {
        const frag = document.createDocumentFragment();
        const end = Math.min(total, i + chunk);
        for (let k = i; k < end; k++) {
          const row = rows[k];
          const tr = document.createElement('tr');
          tr.innerHTML =
            '<td>' + escHtml(row.device_id ?? '') + '</td>' +
            '<td>' + escHtml(row.staff_id ?? '') + '</td>' +
            '<td>' + escHtml(row.datetime ?? '') + '</td>' +
            '<td>' + escHtml(row.verified ?? '') + '</td>' +
            '<td>' + escHtml(row.status ?? '') + '</td>' +
            '<td>' + escHtml(row.workcode ?? '') + '</td>' +
            '<td>' + escHtml(row.reserved ?? '') + '</td>';
          frag.appendChild(tr);
        }
        body.appendChild(frag);
        const pct = Math.max(1, Math.round((end / total) * 100));
        setLoad(pct, end + ' / ' + total + ' rows');
        await new Promise(r => setTimeout(r, 0));
      }

      setLoad(100, total + ' rows');
      setTimeout(() => { if (box) box.style.display = 'none'; }, 350);
    }

    function applyFilters(rows, staffQuery, fromDate, toDate) {
      const q = (staffQuery || '').trim().toLowerCase();
      const from = (fromDate || '').trim();
      const to = (toDate || '').trim();
      return (rows || []).filter(r => {
        if (q) {
          const s = String(r.staff_id ?? '').toLowerCase();
          if (!s.includes(q)) return false;
        }
        const d = String(r.datetime ?? '').slice(0, 10);
        if (from && (!d || d < from)) return false;
        if (to && (!d || d > to)) return false;
        return true;
      });
    }

    function applyAttlogFilters(rows, staffQuery, fromDate, toDate) {
      const q = (staffQuery || '').trim().toLowerCase();
      const from = (fromDate || '').trim();
      const to = (toDate || '').trim();
      return (rows || []).filter(r => {
        if (q) {
          const s = String(r.staff_id ?? '').toLowerCase();
          if (!s.includes(q)) return false;
        }
        const d = String(r.datetime ?? '').slice(0, 10);
        if (from && (!d || d < from)) return false;
        if (to && (!d || d > to)) return false;
        return true;
      });
    }

    function toCsv(rows) {
      const header = ['staff_id', 'datetime', 'verified', 'status', 'workcode', 'reserved'];
      const out = [header.join(',')];
      for (const r of (rows || [])) {
        const vals = [
          r.staff_id ?? '',
          r.datetime ?? '',
          r.verified ?? '',
          r.status ?? '',
          r.workcode ?? '',
          r.reserved ?? '',
        ].map(v => {
          const s = String(v ?? '');
          return '"' + s.replace(/"/g, '""') + '"';
        });
        out.push(vals.join(','));
      }
      return out.join('\r\n');
    }

    function toAttlogCsv(rows) {
      const header = ['device_id', 'staff_id', 'datetime', 'verified', 'status', 'workcode', 'reserved'];
      const out = [header.join(',')];
      for (const r of (rows || [])) {
        const vals = [
          r.device_id ?? '',
          r.staff_id ?? '',
          r.datetime ?? '',
          r.verified ?? '',
          r.status ?? '',
          r.workcode ?? '',
          r.reserved ?? '',
        ].map(v => {
          const s = String(v ?? '');
          return '"' + s.replace(/"/g, '""') + '"';
        });
        out.push(vals.join(','));
      }
      return out.join('\r\n');
    }

    function toStaffCsv(rows) {
      function isoToMdY(raw) {
        const s = String(raw ?? '').trim();
        if (!s) return '';
        const iso = s.length >= 10 ? s.slice(0, 10) : s;
        const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (m) {
          const y = Number(m[1]);
          const mo = Number(m[2]);
          const d = Number(m[3]);
          if (Number.isFinite(y) && Number.isFinite(mo) && Number.isFinite(d)) return String(mo) + '/' + String(d) + '/' + String(y);
        }
        return s;
      }

      const header = ['User ID', 'First Name', 'Role', 'Department', 'Status', 'Date Joined', 'Shift Pattern'];
      const out = [header.join(',')];
      for (const r of (rows || [])) {
        const vals = [
          r.user_id ?? '',
          r.full_name ?? '',
          r.role ?? '',
          r.department ?? '',
          r.status ?? '',
          isoToMdY(r.date_joined ?? ''),
          (String(r.shift_pattern ?? '').trim() || 'Normal'),
        ].map(v => {
          const s = String(v ?? '');
          return '"' + s.replace(/"/g, '""') + '"';
        });
        out.push(vals.join(','));
      }
      return out.join('\r\n');
    }

    function toShiftCsv(rows) {
      const header = ['Pattern', 'Working Days', 'Working Hours', 'Break', 'Notes'];
      const out = [header.join(',')];
      for (const r of (rows || [])) {
        const vals = [
          r.pattern ?? '',
          r.workingDays ?? '',
          r.workingHours ?? '',
          r.break ?? '',
          r.notes ?? '',
        ].map(v => {
          const s = String(v ?? '');
          return '"' + s.replace(/"/g, '""') + '"';
        });
        out.push(vals.join(','));
      }
      return out.join('\r\n');
    }

    function downloadCsv(filename, rows) {
      const csv = toCsv(rows);
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 5000);
    }

    function downloadAttlogCsv(filename, rows) {
      const csv = toAttlogCsv(rows);
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 5000);
    }

    function downloadStaffCsv(filename, rows) {
      const csv = toStaffCsv(rows);
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 5000);
    }

    function downloadShiftCsv(filename, rows) {
      const csv = toShiftCsv(rows);
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 5000);
    }

    function parseCsv(text) {
      const s = String(text ?? '');
      const rows = [];
      let row = [];
      let field = '';
      let inQuotes = false;
      for (let i = 0; i < s.length; i++) {
        const ch = s[i];
        if (inQuotes) {
          if (ch === '"') {
            const next = s[i + 1];
            if (next === '"') { field += '"'; i++; continue; }
            inQuotes = false;
            continue;
          }
          field += ch;
          continue;
        }
        if (ch === '"') { inQuotes = true; continue; }
        if (ch === ',') { row.push(field); field = ''; continue; }
        if (ch === '\r') continue;
        if (ch === '\n') {
          row.push(field);
          field = '';
          const nonEmpty = row.some(v => String(v ?? '').trim().length > 0);
          if (nonEmpty) rows.push(row);
          row = [];
          continue;
        }
        field += ch;
      }
      row.push(field);
      if (row.some(v => String(v ?? '').trim().length > 0)) rows.push(row);
      return rows;
    }

    function normalizeDateToIso(raw) {
      const s = String(raw ?? '').trim();
      if (!s) return '';
      const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
      if (iso) return iso[1] + '-' + iso[2] + '-' + iso[3];
      const mdy = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
      if (mdy) {
        const mm = String(Number(mdy[1]));
        const dd = String(Number(mdy[2]));
        const yyyy = String(Number(mdy[3]));
        const m2 = String(mm).padStart(2, '0');
        const d2 = String(dd).padStart(2, '0');
        return yyyy + '-' + m2 + '-' + d2;
      }
      return s;
    }

    function normalizeStaffRow(raw) {
      const userId = String(raw.user_id ?? '').trim();
      if (!userId) return null;
      const role = String(raw.role ?? '').trim();
      const department = String(raw.department ?? '').trim();
      const statusRaw = String(raw.status ?? '').trim();
      const status = statusRaw ? (statusRaw.charAt(0).toUpperCase() + statusRaw.slice(1).toLowerCase()) : 'Active';
      const dateJoined = normalizeDateToIso(raw.date_joined ?? '');
      const shiftPattern = String(raw.shift_pattern ?? '').trim() || 'Normal';
      return {
        user_id: userId,
        full_name: String(raw.full_name ?? '').trim(),
        role,
        department,
        status,
        date_joined: dateJoined,
        shift_pattern: shiftPattern,
      };
    }

    let lastDeviceRows = [];
    let lastDbRows = [];
    let lastStaffRows = [];
    let lastShiftRows = [];
    let staffSelectedIds = new Set();
    let shiftSelectedKeys = new Set();
    const fallbackShiftRows = [
      { pattern: 'Normal', workingDays: 'Mon–Fri', workingHours: '09:00–18:00', break: '13:00–14:00', notes: 'Default' },
      { pattern: 'Shift 1', workingDays: 'Mon–Fri', workingHours: '08:00–16:00', break: '12:00–13:00', notes: 'Default' },
      { pattern: 'Shift 2', workingDays: 'Mon–Fri', workingHours: '16:00–00:00', break: '20:00–20:30', notes: 'Default' },
      { pattern: 'Shift 3', workingDays: 'Mon–Fri', workingHours: '00:00–08:00', break: '04:00–04:30', notes: 'Default' },
    ];

    async function refreshDeviceRecords() {
      const pick = getRawDevicePick();
      const url = '/api/records/file?deviceId=' + encodeURIComponent(pick);
      const j = await getJson(url);
      lastDeviceRows = j ? (j.rows || []) : [];
      const err = (j && j.error) ? String(j.error || '') : '';
      const rows = (lastDeviceRows || [])
        .slice()
        .sort((a, b) => String(b.datetime ?? '').localeCompare(String(a.datetime ?? '')));
      const msg = err ? ('No device records. ' + err) : 'No device records.';
      await renderAttlogTable(rows, msg);
    }

    async function refreshDbRecords() {
      let j = null;
      const pick = getRawDevicePick();
      const url = '/api/db/records?deviceId=' + encodeURIComponent(pick);
      for (let attempt = 0; attempt < 3; attempt++) {
        j = await getJson(url, true);
        if (j && j.ok !== false) break;
        await new Promise(r => setTimeout(r, 250));
      }
      lastDbRows = (j && j.ok !== false) ? (j.rows || []) : [];
      const err = (j && j.ok === false && j.error) ? String(j.error || '') : (j && j.error) ? String(j.error || '') : '';
      const filtered = (lastDbRows || [])
        .slice()
        .sort((a, b) => String(b.datetime ?? '').localeCompare(String(a.datetime ?? '')));
      const msg = err ? ('No database records. ' + err) : 'No database records.';
      await renderTable('dbBody', filtered, msg);
      return { ok: !err, total: lastDbRows.length, shown: filtered.length };
    }

    function applyStaffFilters(rows, query) {
      const q = (query || '').trim().toLowerCase();
      return (rows || []).filter(r => {
        if (!q) return true;
        const s = [
          r.user_id ?? '',
          r.full_name ?? '',
          r.role ?? '',
          r.department ?? '',
          r.status ?? '',
          r.date_joined ?? '',
          r.shift_pattern ?? '',
        ].join(' ').toLowerCase();
        return s.includes(q);
      });
    }

    let staffSortKey = 'user_id';
    let staffSortDir = 'asc';

    function parseIsoDate(raw) {
      const s = String(raw ?? '').trim();
      const iso = s.length >= 10 ? s.slice(0, 10) : s;
      const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (!m) return null;
      const y = Number(m[1]);
      const mo = Number(m[2]);
      const d = Number(m[3]);
      if (!Number.isFinite(y) || !Number.isFinite(mo) || !Number.isFinite(d)) return null;
      return { y, mo, d, key: y * 10000 + mo * 100 + d };
    }

    function toSortText(raw) {
      return String(raw ?? '').trim().toLowerCase();
    }

    function sortStaffRows(rows) {
      const key = String(staffSortKey || '').trim();
      const dir = staffSortDir === 'desc' ? -1 : 1;
      const copy = (rows || []).slice();
      copy.sort((a, b) => {
        if (key === 'user_id') {
          const an = Number(String(a.user_id ?? '').trim());
          const bn = Number(String(b.user_id ?? '').trim());
          if (Number.isFinite(an) && Number.isFinite(bn) && an !== bn) return (an < bn ? -1 : 1) * dir;
          const as = toSortText(a.user_id);
          const bs = toSortText(b.user_id);
          if (as !== bs) return (as < bs ? -1 : 1) * dir;
          return 0;
        }
        if (key === 'date_joined') {
          const ad = parseIsoDate(a.date_joined);
          const bd = parseIsoDate(b.date_joined);
          const ak = ad ? ad.key : null;
          const bk = bd ? bd.key : null;
          if (ak !== null && bk !== null && ak !== bk) return (ak < bk ? -1 : 1) * dir;
          if (ak !== null && bk === null) return -1 * dir;
          if (ak === null && bk !== null) return 1 * dir;
          const as = toSortText(a.date_joined);
          const bs = toSortText(b.date_joined);
          if (as !== bs) return (as < bs ? -1 : 1) * dir;
          return 0;
        }
        const as = toSortText(a[key]);
        const bs = toSortText(b[key]);
        if (as !== bs) return (as < bs ? -1 : 1) * dir;
        const aid = toSortText(a.user_id);
        const bid = toSortText(b.user_id);
        if (aid !== bid) return (aid < bid ? -1 : 1) * dir;
        return 0;
      });
      return copy;
    }

    function currentStaffViewRows() {
      return sortStaffRows(applyStaffFilters(lastStaffRows, ''));
    }

    function updateStaffSortUi() {
      const heads = document.querySelectorAll('#subtab-staff-staff thead th[data-sort]');
      for (const th of heads) {
        const k = String(th.getAttribute('data-sort') || '').trim();
        if (!k) continue;
        th.classList.toggle('sortable', !!isSuperadmin);
        const base = th.getAttribute('data-label') || th.textContent || '';
        if (!th.getAttribute('data-label')) th.setAttribute('data-label', base);
        if (!isSuperadmin) {
          th.textContent = base;
          continue;
        }
        const active = k === staffSortKey;
        th.textContent = active ? (base + (staffSortDir === 'desc' ? ' ▼' : ' ▲')) : base;
      }
    }

    function updateStaffDeleteEnabled() {
      const btn = el('staffDelete');
      const prov = el('staffProvisionSelected');
      const body = el('staffBody');
      const has = staffSelectedIds.size > 0;
      if (btn) {
        btn.disabled = !has || !isSuperadmin;
        btn.style.display = (has && isSuperadmin) ? '' : 'none';
      }
      if (prov) {
        prov.disabled = !has || !isSuperadmin;
        prov.style.display = (has && isSuperadmin) ? '' : 'none';
      }

      const selectAll = el('staffSelectAll');
      if (selectAll) {
        const all = !!body && !!body.querySelectorAll('input.staffChk[type="checkbox"]').length;
        const checked = !!body && !!body.querySelectorAll('input.staffChk[type="checkbox"]:checked').length;
        selectAll.checked = all && checked && body.querySelectorAll('input.staffChk[type="checkbox"]').length === checked;
      }
    }

    async function renderStaffTable(rows, emptyText) {
      function isoToMdY(raw) {
        const s = String(raw ?? '').trim();
        if (!s) return '';
        const iso = s.length >= 10 ? s.slice(0, 10) : s;
        const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})$/);
        if (m) {
          const y = Number(m[1]);
          const mo = Number(m[2]);
          const d = Number(m[3]);
          if (Number.isFinite(y) && Number.isFinite(mo) && Number.isFinite(d)) return String(mo) + '/' + String(d) + '/' + String(y);
        }
        return s;
      }

      const body = el('staffBody');
      if (!body) return;
      body.innerHTML = '';
      if (!rows || !rows.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="9" class="muted">' + escHtml(emptyText) + '</td>';
        body.appendChild(tr);
        updateStaffDeleteEnabled();
        return;
      }
      for (let i = 0; i < rows.length; i++) {
        const r = rows[i];
        const staffId = String(r.user_id ?? '');
        const shiftPattern = (String(r.shift_pattern ?? '').trim() || 'Normal');
        const checked = staffSelectedIds.has(staffId) ? ' checked' : '';
        const tr = document.createElement('tr');
        tr.innerHTML =
          '<td><input class="staffChk" type="checkbox" data-id="' + escHtml(staffId) + '"' + checked + '/></td>' +
          '<td>' + escHtml(staffId) + '</td>' +
          '<td>' + escHtml(r.full_name ?? '') + '</td>' +
          '<td>' + escHtml(r.role ?? '') + '</td>' +
          '<td>' + escHtml(r.department ?? '') + '</td>' +
          '<td>' + escHtml(r.status ?? '') + '</td>' +
          '<td>' + escHtml(isoToMdY(r.date_joined ?? '')) + '</td>' +
          '<td>' + escHtml(shiftPattern) + '</td>' +
          '<td>' +
            '<div class="actionIcons">' +
              '<button class="iconBtn staffProvisionRow" type="button" data-id="' + escHtml(staffId) + '" title="Create/Update on device" aria-label="Create/Update on device">' +
                '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 21V9"/><path d="m17 14-5-5-5 5"/><path d="M5 3h14"/></svg>' +
              '</button>' +
              '<span class="actionSep" aria-hidden="true"></span>' +
              '<button class="iconBtn staffEditRow" type="button" data-id="' + escHtml(staffId) + '" title="Edit" aria-label="Edit">' +
                '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4Z"/></svg>' +
              '</button>' +
            '</div>' +
          '</td>';
        body.appendChild(tr);
      }
      updateStaffDeleteEnabled();
    }

    async function refreshStaffRecords() {
      const j = await getJson('/api/staff/file');
      lastStaffRows = j ? (j.rows || []) : [];
      const err = (j && j.error) ? String(j.error || '') : '';
      const valid = new Set((lastStaffRows || []).map(r => String(r.user_id ?? '').trim()).filter(s => !!s));
      for (const id of Array.from(staffSelectedIds)) if (!valid.has(id)) staffSelectedIds.delete(id);

      const filtered = sortStaffRows(applyStaffFilters(lastStaffRows, ''));
      const msg = err ? ('No staff records. ' + err) : 'No staff records.';
      await renderStaffTable(filtered, msg);
      updateStaffSortUi();
    }

    async function provisionStaffToDevice(row, triggerEl) {
      const id = String(row && row.user_id ? row.user_id : '').trim();
      if (!id) { logActivity('Staff Records / Employee List', 'Missing user id.', 'ERR'); return; }
      const firstName = String(row && row.full_name ? row.full_name : '').trim();
      const role = String(row && row.role ? row.role : '').trim();
      logActivity('Staff Records / Employee List', 'Provisioning to device: user_id=' + id + ' first_name=' + firstName + ' ...');
      if (triggerEl && triggerEl.setAttribute) { try { triggerEl.setAttribute('disabled', 'disabled'); } catch { } }
      const r = await postJson('/api/device/user/create', { user_id: id, first_name: firstName, full_name: firstName, role });
      if (triggerEl && triggerEl.removeAttribute) { try { triggerEl.removeAttribute('disabled'); } catch { } }
      if (r && Array.isArray(r.trace)) {
        for (const line of r.trace) logActivity('Staff Records / Employee List', String(line), 'TRACE');
      }
      if (r && r.ok) {
        if (r.already_exists && r.updated) logActivity('Staff Records / Employee List', 'Updated user ' + id + ' on device.', 'OK');
        else if (r.already_exists) logActivity('Staff Records / Employee List', 'Device already has user ' + id + '.', 'OK');
        else if (r.created && r.verified === false) logActivity('Staff Records / Employee List', 'Created user ' + id + ' on device, but could not verify. Check device screen.', 'OK');
        else logActivity('Staff Records / Employee List', 'Created user ' + id + ' on device.', 'OK');
      } else {
        const base = String((r && r.error) ? r.error : 'unknown error');
        const le = (r && (r.last_error ?? null) !== null) ? (' last_error=' + String(r.last_error)) : '';
        const dev = (r && r.device_ip) ? (' device=' + String(r.device_ip) + ':' + String(r.device_port || '')) : '';
        const mn = (r && (r.machine_number ?? null) !== null) ? (' machine=' + String(r.machine_number)) : '';
        logActivity('Staff Records / Employee List', 'Device provision failed: ' + base + le + dev + mn, 'ERR');
        if (r && r.hint) logActivity('Staff Records / Employee List', String(r.hint), 'ERR');
      }
    }

    function applyShiftFilters(rows, query) {
      const q = (query || '').trim().toLowerCase();
      return (rows || []).filter(r => {
        if (!q) return true;
        const s = [r.pattern ?? '', r.workingDays ?? '', r.workingHours ?? '', r.break ?? '', r.notes ?? ''].join(' ').toLowerCase();
        return s.includes(q);
      });
    }

    function updateShiftDeleteEnabled() {
      const btn = el('shiftDelete');
      if (!btn) return;
      const has = shiftSelectedKeys.size > 0;
      btn.disabled = !has;
    }

    async function renderShiftTable(rows) {
      const body = el('shiftBody');
      if (!body) return;
      body.innerHTML = '';
      const list = rows || [];
      if (!list.length) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="7" class="muted">No shift patterns.</td>';
        body.appendChild(tr);
        updateShiftDeleteEnabled();
        return;
      }
      function parseWd(raw) {
        const s0 = String(raw || '').trim();
        const set = new Set();
        const order = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
        function normDayToken(x) {
          const k = String(x || '').trim().slice(0, 3).toLowerCase();
          if (k === 'mon') return 'Mon';
          if (k === 'tue') return 'Tue';
          if (k === 'wed') return 'Wed';
          if (k === 'thu') return 'Thu';
          if (k === 'fri') return 'Fri';
          if (k === 'sat') return 'Sat';
          if (k === 'sun') return 'Sun';
          return '';
        }
        function addRange(aRaw, bRaw) {
          const a = normDayToken(aRaw);
          const b = normDayToken(bRaw);
          const ai = order.indexOf(a);
          const bi = order.indexOf(b);
          if (ai < 0 || bi < 0) return false;
          let cur = ai;
          while (true) {
            set.add(order[cur]);
            if (cur === bi) break;
            cur = (cur + 1) % 7;
          }
          return true;
        }
        if (!s0) { for (const d of ['Mon','Tue','Wed','Thu','Fri']) set.add(d); return set; }
        const s = s0.replace(/to/ig, '–').replace(/-/g, '–');
        const parts = s.split(',').map(x => String(x || '').trim()).filter(Boolean);
        for (const p of parts) {
          if (p.includes('–')) {
            const rr = p.split('–').map(x => String(x || '').trim()).filter(Boolean);
            if (rr.length === 2 && addRange(rr[0], rr[1])) continue;
          }
          const d = normDayToken(p);
          if (d) set.add(d);
        }
        if (!set.size && s.includes('–')) {
          const rr = s.split('–').map(x => String(x || '').trim()).filter(Boolean);
          if (rr.length === 2) addRange(rr[0], rr[1]);
        }
        if (!set.size) { for (const d of ['Mon','Tue','Wed','Thu','Fri']) set.add(d); }
        return set;
      }

      function wdToStr(set) {
        const ordered = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'].filter(d => set.has(d));
        if (!ordered.length) return 'Mon–Fri';
        const monFri = ['Mon','Tue','Wed','Thu','Fri'];
        const monSun = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
        if (ordered.length === monFri.length && monFri.every(x => set.has(x))) return 'Mon–Fri';
        if (ordered.length === monSun.length) return 'Mon–Sun';
        return ordered.join(', ');
      }

      for (let i = 0; i < list.length; i++) {
        const r = list[i];
        const key = String(r.pattern ?? '');
        const checked = shiftSelectedKeys.has(key) ? ' checked' : '';
        const wdSet = parseWd(r.workingDays ?? '');
        const wdText = wdToStr(wdSet).replace('–', '-');
        const tr = document.createElement('tr');
        tr.innerHTML =
          '<td><input class="shiftChk" type="checkbox" data-id="' + escHtml(key) + '"' + checked + '/></td>' +
          '<td>' + escHtml(key) + '</td>' +
          '<td>' + escHtml(wdText) + '</td>' +
          '<td>' + escHtml(r.workingHours ?? '') + '</td>' +
          '<td>' + escHtml(r.break ?? '') + '</td>' +
          '<td>' + escHtml(r.notes ?? '') + '</td>' +
          '<td><button class="iconBtn shiftEditRow" type="button" data-id="' + escHtml(key) + '" title="Edit" aria-label="Edit"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L7 19l-4 1 1-4Z"/></svg></button></td>';
        body.appendChild(tr);
      }
      updateShiftDeleteEnabled();
    }

    async function refreshShiftPatterns() {
      const j = await getJson('/api/shifts');
      lastShiftRows = j ? (j.rows || []) : [];
      if (!lastShiftRows.length) lastShiftRows = fallbackShiftRows.slice();
      const valid = new Set((lastShiftRows || []).map(r => String(r.pattern ?? '').trim()).filter(s => !!s));
      for (const id of Array.from(shiftSelectedKeys)) if (!valid.has(id)) shiftSelectedKeys.delete(id);

      const filtered = applyShiftFilters(lastShiftRows, '');
      await renderShiftTable(filtered);
    }

    async function saveStaffRows(rows) {
      const r = await postJson('/api/staff/save', { rows: rows || [] });
      if (r && r.ok === false) throw new Error(String(r.error || 'Save failed'));
    }

    async function saveShiftRows(rows) {
      const r = await postJson('/api/shifts/save', { rows: rows || [] });
      if (r && r.ok === false) throw new Error(String(r.error || 'Save failed'));
    }

    function getCheckedDataIds(tbodyId, cls) {
      const body = el(tbodyId);
      if (!body) return [];
      return Array.from(body.querySelectorAll('input.' + cls + '[type="checkbox"]:checked'))
        .map(x => String(x.getAttribute('data-id') || '').trim())
        .filter(x => !!x);
    }

    function staffFields(mode) {
      const roId = mode === 'edit';
      const depts = Array.from(new Set((lastStaffRows || []).map(r => String(r.department ?? '').trim()).filter(s => !!s))).sort((a, b) => a.localeCompare(b));
      const shifts = Array.from(new Set((lastShiftRows || []).map(r => String(r.pattern ?? '').trim()).filter(s => !!s)));
      if (!shifts.includes('Normal')) shifts.unshift('Normal');
      shifts.sort((a, b) => a.localeCompare(b));
      const fields = [
        { key: 'user_id', label: 'User ID', placeholder: 'e.g. 10', disabled: roId },
        { key: 'full_name', label: 'First Name' },
        { key: 'role', label: 'Role', kind: 'select', options: ['Staff', 'Supervisor', 'Manager', 'Superadmin'], default: 'Staff' },
        { key: 'department', label: 'Department', kind: 'datalist', options: depts, placeholder: 'Select or type department' },
        { key: 'status', label: 'Status', kind: 'select', options: ['Active', 'Inactive'], default: 'Active' },
        { key: 'date_joined', label: 'Date Joined', type: 'date' },
        { key: 'shift_pattern', label: 'Shift Pattern', kind: 'select', options: shifts, default: 'Normal' },
      ];
      if (mode === 'add') {
        fields.push({ key: 'provision_to_device', label: 'Create user on device', kind: 'checkbox', default: 'true' });
      }
      return fields;
    }

    function shiftFields(mode) {
      const ro = mode === 'edit';
      return [
        { key: 'pattern', label: 'Pattern', disabled: ro },
        { key: 'workingDays', label: 'Working Days' },
        { key: 'workingHours', label: 'Working Hours' },
        { key: 'break', label: 'Break' },
        { key: 'notes', label: 'Notes' },
      ];
    }

    async function refreshAnalytics() {
      const j = await getJson('/api/analytics');
      if (!j) return;
      const d = j.device || {};
      const b = j.db || {};

      el('totalTodayDevice').textContent = d.ok ? String(d.totalPunches ?? 0) : '-';
      el('uniqueTodayDevice').textContent = d.ok ? String(d.uniqueStaff ?? 0) : '-';
      el('totalTodayDb').textContent = b.ok ? String(b.totalPunches ?? 0) : '-';
      el('uniqueTodayDb').textContent = b.ok ? String(b.uniqueStaff ?? 0) : '-';

      function renderHourly(hostId, series, kind) {
        const host = el(hostId);
        if (!host) return;
        host.innerHTML = '';
        const wrap = document.createElement('div');
        wrap.className = 'chartWrap';
        const y = document.createElement('div');
        y.className = 'yAxisLabel';
        y.textContent = 'Punches';
        const plot = document.createElement('div');
        plot.className = 'plot';
        const bars = document.createElement('div');
        bars.className = 'barsGrid';
        const x = document.createElement('div');
        x.className = 'xAxis';
        const a = Array.isArray(series) ? series.slice() : [];
        const max = a.reduce((m, z) => Math.max(m, z.count || 0), 0);
        const yNums = document.createElement('div');
        yNums.className = 'yNums';
        const ticks = (() => {
          const m = Number.isFinite(Number(max)) ? Number(max) : 0;
          if (m <= 3) return [3, 2, 1];
          if (m <= 6) return Array.from({ length: m }, (_, i) => m - i);
          const cand = [m, Math.ceil(m * 0.75), Math.ceil(m * 0.5), Math.ceil(m * 0.25), 1];
          const uniq = [];
          for (const v of cand) { const n = Math.max(1, Math.round(v)); if (!uniq.includes(n)) uniq.push(n); }
          return uniq.sort((a, b) => b - a);
        })();
        for (const t of ticks) {
          const s = document.createElement('span');
          s.textContent = String(t);
          yNums.appendChild(s);
        }
        for (let h = 0; h < 24; h++) {
          const f = a.find(z => z.hour === h) || { hour: h, count: 0 };
          const pct = max > 0 ? Math.max(2, Math.round((f.count / max) * 100)) : 2;
          const bar = document.createElement('div');
          bar.className = 'bar2 ' + (kind || 'device');
          bar.style.height = pct + '%';
          bar.title = String(h).padStart(2, '0') + ':00 — ' + f.count;
          bars.appendChild(bar);
          const lab = document.createElement('span');
          lab.textContent = String(h).padStart(2, '0');
          x.appendChild(lab);
        }
        plot.appendChild(yNums);
        plot.appendChild(bars);
        plot.appendChild(x);
        wrap.appendChild(y);
        wrap.appendChild(plot);
        host.appendChild(wrap);
      }

      function renderGrouped(hostId, deviceArr, dbArr, getLabel) {
        const host = el(hostId);
        if (!host) return;
        host.innerHTML = '';
        const wrap = document.createElement('div');
        wrap.className = 'chartWrap';
        const y = document.createElement('div');
        y.className = 'yAxisLabel';
        y.textContent = 'Punches';
        const plot = document.createElement('div');
        plot.className = 'plot';
        const grid = document.createElement('div');
        grid.className = 'groupGrid';
        const dev = Array.isArray(deviceArr) ? deviceArr.slice() : [];
        const db = Array.isArray(dbArr) ? dbArr.slice() : [];
        const n = Math.max(dev.length, db.length, 0);
        const max = Math.max(
          dev.reduce((m, z) => Math.max(m, z.count || 0), 0),
          db.reduce((m, z) => Math.max(m, z.count || 0), 0)
        );
        const yNums = document.createElement('div');
        yNums.className = 'yNums';
        const ticks = (() => {
          const m = Number.isFinite(Number(max)) ? Number(max) : 0;
          if (m <= 3) return [3, 2, 1];
          if (m <= 6) return Array.from({ length: m }, (_, i) => m - i);
          const cand = [m, Math.ceil(m * 0.75), Math.ceil(m * 0.5), Math.ceil(m * 0.25), 1];
          const uniq = [];
          for (const v of cand) { const n = Math.max(1, Math.round(v)); if (!uniq.includes(n)) uniq.push(n); }
          return uniq.sort((a, b) => b - a);
        })();
        for (const t of ticks) {
          const s = document.createElement('span');
          s.textContent = String(t);
          yNums.appendChild(s);
        }
        for (let i = 0; i < n; i++) {
          const dv = dev[i] || { count: 0 };
          const bv = db[i] || { count: 0 };
          const col = document.createElement('div');
          col.className = 'gCol';
          const bars = document.createElement('div');
          bars.className = 'gBars';
          const b1 = document.createElement('div');
          b1.className = 'gBar device';
          b1.style.height = (max > 0 ? Math.max(2, Math.round(((dv.count || 0) / max) * 100)) : 2) + '%';
          b1.title = 'Device — ' + (dv.count || 0);
          const b2 = document.createElement('div');
          b2.className = 'gBar db';
          b2.style.height = (max > 0 ? Math.max(2, Math.round(((bv.count || 0) / max) * 100)) : 2) + '%';
          b2.title = 'Database — ' + (bv.count || 0);
          bars.appendChild(b1);
          bars.appendChild(b2);
          const lbl = document.createElement('div');
          lbl.className = 'gLbl';
          lbl.textContent = getLabel(dv, bv, i);
          col.appendChild(bars);
          col.appendChild(lbl);
          grid.appendChild(col);
        }
        plot.appendChild(yNums);
        plot.appendChild(grid);
        wrap.appendChild(y);
        wrap.appendChild(plot);
        host.appendChild(wrap);
      }

      renderHourly('barsDb', b.ok ? (b.perHour || []) : [], 'db');
    }

    function fmtPct(v) {
      const n = Number(v);
      if (!Number.isFinite(n)) return '-';
      return (Math.round(n * 10) / 10).toFixed(1) + '%';
    }

    function setText(id, v) {
      const x = el(id);
      if (x) x.textContent = String(v ?? '-');
    }

    function renderHBars(hostId, items, getLabel, getValue, valueSuffix) {
      const host = el(hostId);
      if (!host) return;
      host.innerHTML = '';
      const arr = Array.isArray(items) ? items.slice() : [];
      if (!arr.length) {
        const d = document.createElement('div');
        d.className = 'muted';
        d.textContent = 'No data.';
        host.appendChild(d);
        return;
      }
      const max = arr.reduce((m, it) => Math.max(m, Number(getValue(it)) || 0), 0);
      for (const it of arr) {
        const row = document.createElement('div');
        row.className = 'hRow';
        const lab = document.createElement('div');
        lab.className = 'hLab';
        lab.textContent = String(getLabel(it) ?? '');
        const track = document.createElement('div');
        track.className = 'hTrack';
        const fill = document.createElement('div');
        fill.className = 'hFill';
        const v = Number(getValue(it)) || 0;
        fill.style.width = (max > 0 ? Math.max(2, Math.round((v / max) * 100)) : 2) + '%';
        track.appendChild(fill);
        const val = document.createElement('div');
        val.className = 'hVal';
        val.textContent = (Number.isFinite(v) ? String(v) : '-') + (valueSuffix || '');
        row.appendChild(lab);
        row.appendChild(track);
        row.appendChild(val);
        host.appendChild(row);
      }
    }

    function renderTopEmployees(hostId, items) {
      const host = el(hostId);
      if (!host) return;
      host.innerHTML = '';
      const arr = Array.isArray(items) ? items.slice() : [];
      if (!arr.length) { host.innerHTML = '<div class="muted">No data.</div>'; return; }
      const wrap = document.createElement('div');
      wrap.className = 'empList';
      for (const x of arr.slice(0, 8)) {
        const name = String(x.full_name || x.staff_id || '').trim();
        const dept = String(x.department || '').trim();
        const pct = Math.max(0, Math.min(100, Number(x.attendancePct) || 0));
        const card = document.createElement('div');
        card.className = 'empCard';
        const left = document.createElement('div');
        left.innerHTML =
          '<div class="empName">' + escHtml(name || '-') + '</div>' +
          '<div class="empDept">' + escHtml(dept || '') + '</div>' +
          '<div class="empTrack"><div class="empFill" style="width:' + pct.toFixed(1) + '%"></div></div>';
        const right = document.createElement('div');
        right.className = 'empPct';
        right.textContent = pct.toFixed(1) + '%';
        card.appendChild(left);
        card.appendChild(right);
        wrap.appendChild(card);
      }
      host.appendChild(wrap);
    }

    function renderDonut(donutId, centerId, legendId, parts, centerText) {
      const donut = el(donutId);
      const center = el(centerId);
      const legend = el(legendId);
      if (!donut || !center || !legend) return;

      const p = Array.isArray(parts) ? parts.filter(x => x && Number(x.value) > 0) : [];
      const total = p.reduce((m, x) => m + (Number(x.value) || 0), 0);
      center.textContent = String(centerText ?? (total ? total : '-'));

      legend.innerHTML = '';
      if (!p.length || total <= 0) {
        donut.style.background = 'conic-gradient(rgba(100,116,139,.25) 0 360deg)';
        const it = document.createElement('div');
        it.className = 'donutItem';
        it.textContent = 'No data.';
        legend.appendChild(it);
        return;
      }

      const segs = [];
      let acc = 0;
      for (const x of p) {
        const v = Number(x.value) || 0;
        const deg = (v / total) * 360;
        const from = acc;
        const to = acc + deg;
        acc = to;
        const color = (x.color || '').trim() || 'rgba(22,58,112,.85)';
        segs.push(color + ' ' + from.toFixed(2) + 'deg ' + to.toFixed(2) + 'deg');

        const row = document.createElement('div');
        row.className = 'donutItem';
        const left = document.createElement('div');
        left.style.display = 'flex';
        left.style.alignItems = 'center';
        left.style.gap = '8px';
        const sw = document.createElement('span');
        sw.className = 'sw';
        sw.style.background = color;
        const lab = document.createElement('span');
        lab.textContent = String(x.label ?? '');
        left.appendChild(sw);
        left.appendChild(lab);
        const right = document.createElement('div');
        right.textContent = String(x.value ?? 0);
        row.appendChild(left);
        row.appendChild(right);
        legend.appendChild(row);
      }
      donut.style.background = 'conic-gradient(' + segs.join(',') + ')';
    }

    function renderGauge(hostId, value, maxValue, paletteName, minLabel, maxLabel) {
      const host = el(hostId);
      if (!host) return;

      const palettes = {
        traffic: ['#ef4444', '#f97316', '#f59e0b', '#22c55e'],
        ocean: ['#0ea5e9', '#14b8a6', '#2563eb'],
        violet: ['#a855f7', '#3b82f6', '#14b8a6'],
      };

      const v = Number(value);
      const mx = Math.max(1e-9, Number(maxValue) || 0);
      const pct = Number.isFinite(v) ? Math.max(0, Math.min(1, v / mx)) : 0;
      const colors = palettes[String(paletteName || '').trim()] || palettes.ocean;
      const endColor = colors[colors.length - 1] || '#2563eb';

      const w = 180;
      const h = 124;
      const cx = 90;
      const cy = 104;
      const r = 78;
      const strokeW = 12;
      const arcLen = Math.PI * r;
      const filled = Math.max(0, Math.min(arcLen, arcLen * pct));

      const angle = Math.PI * (1 - pct);
      const nx = cx + (r - 14) * Math.cos(angle);
      const ny = cy - (r - 14) * Math.sin(angle);

      const isPct = (Math.abs(mx - 100) < 1e-6) && (String(paletteName || '').trim() === 'traffic');
      const valueText = isPct ? Math.round(pct * 100) + '%' : (Number.isFinite(v) ? String(Math.round(v)) : '-');
      const lx = Math.max(16, Math.min(w - 16, nx + 8));
      const ly = Math.max(20, Math.min(h - 14, ny - 10));

      host.innerHTML =
        '<svg viewBox="0 0 ' + w + ' ' + h + '" width="' + w + '" height="' + h + '" aria-hidden="true">' +
          '<path d="M ' + (cx - r) + ' ' + cy + ' A ' + r + ' ' + r + ' 0 0 1 ' + (cx + r) + ' ' + cy + '" fill="none" stroke="rgba(226,232,240,.95)" stroke-width="' + strokeW + '" stroke-linecap="round"/>' +
          '<path d="M ' + (cx - r) + ' ' + cy + ' A ' + r + ' ' + r + ' 0 0 1 ' + (cx + r) + ' ' + cy + '" fill="none" stroke="' + endColor + '" stroke-width="' + strokeW + '" stroke-linecap="round" stroke-dasharray="' + filled.toFixed(1) + ' ' + (arcLen - filled).toFixed(1) + '"/>' +
          '<line x1="' + cx + '" y1="' + cy + '" x2="' + nx.toFixed(1) + '" y2="' + ny.toFixed(1) + '" stroke="' + endColor + '" stroke-width="3" stroke-linecap="round"/>' +
          '<circle cx="' + cx + '" cy="' + cy + '" r="6" fill="' + endColor + '"/>' +
        '</svg>';

      host.dataset.endColor = String(endColor || '').trim() || '#2563eb';
      return host.dataset.endColor;
    }

    function renderLineSvg(svgId, points, valueKey, labelKey, mode) {
      const svg = el(svgId);
      if (!svg) return;
      const arr = Array.isArray(points) ? points.slice() : [];
      svg.innerHTML = '';
      if (!arr.length) return;

      const w = 600, h = 180, pad = 18;
      const vals = arr.map(p => Number(p[valueKey]) || 0);
      const min = Math.min(...vals);
      const max = Math.max(...vals);
      const span = Math.max(1e-9, max - min);

      const xStep = arr.length > 1 ? (w - pad * 2) / (arr.length - 1) : 0;
      const pts = arr.map((p, i) => {
        const v = Number(p[valueKey]) || 0;
        const x = pad + i * xStep;
        const y = pad + (h - pad * 2) * (1 - (v - min) / span);
        return { x, y, v, label: String(p[labelKey] ?? '') };
      });

      const grid = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      grid.setAttribute('stroke', 'rgba(100,116,139,.25)');
      grid.setAttribute('stroke-width', '1');
      for (let i = 0; i < 4; i++) {
        const y = pad + (h - pad * 2) * (i / 3);
        const ln = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        ln.setAttribute('x1', String(pad));
        ln.setAttribute('x2', String(w - pad));
        ln.setAttribute('y1', String(y));
        ln.setAttribute('y2', String(y));
        grid.appendChild(ln);
      }
      svg.appendChild(grid);

      const pathD = pts.map((p, i) => (i === 0 ? 'M' : 'L') + p.x.toFixed(2) + ',' + p.y.toFixed(2)).join(' ');
      if (mode === 'area') {
        const area = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        const baseY = h - pad;
        area.setAttribute('d', pathD + ' L' + pts[pts.length - 1].x.toFixed(2) + ',' + baseY.toFixed(2) + ' L' + pts[0].x.toFixed(2) + ',' + baseY.toFixed(2) + ' Z');
        area.setAttribute('fill', 'rgba(43,183,169,.18)');
        area.setAttribute('stroke', 'none');
        svg.appendChild(area);
      }

      const line = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      line.setAttribute('d', pathD);
      line.setAttribute('fill', 'none');
      line.setAttribute('stroke', 'rgba(22,58,112,.92)');
      line.setAttribute('stroke-width', '3');
      line.setAttribute('stroke-linecap', 'round');
      line.setAttribute('stroke-linejoin', 'round');
      svg.appendChild(line);

      for (const p of pts) {
        const c = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        c.setAttribute('cx', String(p.x));
        c.setAttribute('cy', String(p.y));
        c.setAttribute('r', '4');
        c.setAttribute('fill', '#ffffff');
        c.setAttribute('stroke', 'rgba(22,58,112,.92)');
        c.setAttribute('stroke-width', '2');
        svg.appendChild(c);
      }

      const xLabels = document.createElementNS('http://www.w3.org/2000/svg', 'g');
      for (const p of pts) {
        const t = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        t.setAttribute('x', String(p.x));
        t.setAttribute('y', String(h - 4));
        t.setAttribute('text-anchor', 'middle');
        t.setAttribute('font-size', '11');
        t.setAttribute('fill', 'rgba(100,116,139,.95)');
        t.textContent = p.label;
        xLabels.appendChild(t);
      }
      svg.appendChild(xLabels);
    }

    function renderGroupedBars(hostId, aArr, bArr, getLabel, getValueA, getValueB, kindA, kindB) {
      const host = el(hostId);
      if (!host) return;
      host.innerHTML = '';
      const wrap = document.createElement('div');
      wrap.className = 'chartWrap';
      const y = document.createElement('div');
      y.className = 'yAxisLabel';
      y.textContent = 'Count';
      const plot = document.createElement('div');
      plot.className = 'plot';
      const grid = document.createElement('div');
      grid.className = 'groupGrid';

      const aa = Array.isArray(aArr) ? aArr.slice() : [];
      const bb = Array.isArray(bArr) ? bArr.slice() : [];
      const n = Math.max(aa.length, bb.length, 0);
      const valsA = aa.map(x => Number(getValueA ? getValueA(x) : 0) || 0);
      const valsB = bb.map(x => Number(getValueB ? getValueB(x) : 0) || 0);
      const max = Math.max(0, ...valsA, ...valsB);

      const yNums = document.createElement('div');
      yNums.className = 'yNums';
      const ticks = (() => {
        const m = Number.isFinite(Number(max)) ? Number(max) : 0;
        if (m <= 3) return [3, 2, 1];
        if (m <= 6) return Array.from({ length: m }, (_, i) => m - i);
        const cand = [m, Math.ceil(m * 0.75), Math.ceil(m * 0.5), Math.ceil(m * 0.25), 1];
        const uniq = [];
        for (const v of cand) { const z = Math.max(1, Math.round(v)); if (!uniq.includes(z)) uniq.push(z); }
        return uniq.sort((x, y) => y - x);
      })();
      for (const t of ticks) {
        const s = document.createElement('span');
        s.textContent = String(t);
        yNums.appendChild(s);
      }

      for (let i = 0; i < n; i++) {
        const av = aa[i];
        const bv = bb[i];
        const col = document.createElement('div');
        col.className = 'gCol';
        const bars = document.createElement('div');
        bars.className = 'gBars';

        const aVal = av ? (Number(getValueA ? getValueA(av) : 0) || 0) : 0;
        const bVal = bv ? (Number(getValueB ? getValueB(bv) : 0) || 0) : 0;
        const pctA = (max > 0 ? Math.max(6, Math.round((aVal / max) * 100)) : 6);
        const pctB = (max > 0 ? Math.max(6, Math.round((bVal / max) * 100)) : 6);

        const b1 = document.createElement('div');
        b1.className = 'gBar ' + (kindA || 'device');
        b1.style.height = pctA + '%';
        b1.title = (kindA || 'A') + ': ' + aVal;
        const n1 = document.createElement('div');
        n1.className = 'gNum';
        n1.textContent = aVal ? String(aVal) : '';
        col.appendChild(n1);
        bars.appendChild(b1);

        if (bb.length) {
          const b2 = document.createElement('div');
          b2.className = 'gBar ' + (kindB || 'db');
          b2.style.height = pctB + '%';
          b2.title = (kindB || 'B') + ': ' + bVal;
          bars.appendChild(b2);
        }

        const lab = document.createElement('div');
        lab.className = 'gLbl';
        const rawLab = (av && getLabel) ? getLabel(av) : (bv && getLabel ? getLabel(bv) : '');
        lab.textContent = String(rawLab || '');

        col.appendChild(bars);
        col.appendChild(lab);
        grid.appendChild(col);
      }

      plot.appendChild(yNums);
      plot.appendChild(grid);
      wrap.appendChild(y);
      wrap.appendChild(plot);
      host.appendChild(wrap);
    }

    function renderCalendarHeatmap(cal) {
      const monthEl = el('anaCalMonth');
      const subEl = el('anaCalSub');
      const grid = el('anaCalGrid');
      if (!grid) return;
      grid.innerHTML = '';
      const month = cal && cal.month ? String(cal.month) : '';
      const label = cal && cal.label ? String(cal.label) : '';
      const days = cal && Array.isArray(cal.days) ? cal.days : [];
      const max = cal && Number.isFinite(Number(cal.maxPresent)) ? Number(cal.maxPresent) : Math.max(0, ...days.map(d => Number(d.present) || 0));

      if (monthEl && month) monthEl.value = month;
      if (subEl) subEl.textContent = 'Count of staff present for each day';
      if (month) anaCalendarMonth = month;

      if (!month || !days.length) return;
      const startIso = month + '-01';
      const start = new Date(startIso + 'T00:00:00');
      const startDow = Number.isFinite(start.getTime()) ? start.getDay() : 0;

      for (let i = 0; i < startDow; i++) {
        const x = document.createElement('div');
        x.className = 'calCell off';
        grid.appendChild(x);
      }

      for (const d of days) {
        const iso = String(d.date || '').slice(0, 10);
        const dayNum = Number(d.day) || 0;
        const present = Number(d.present) || 0;

        let z = 0;
        if (present > 0 && max > 0) {
          z = Math.min(5, Math.max(1, Math.ceil((present / max) * 5)));
        }

        const cell = document.createElement('div');
        cell.className = 'calCell z' + String(z);
        cell.title = isoToDmy(iso) + ': ' + String(present) + ' present';
        const countText = (present > 0) ? String(present) : '';
        cell.innerHTML = '<div class="d">' + String(dayNum || '') + '</div><div class="v">' + escHtml(countText) + '</div>';
        grid.appendChild(cell);
      }
    }

    async function refreshAttendance() {
      const dateEl = el('anaDate');
      const deptEl = el('anaDept');
      const date = (dateEl && dateEl.value) ? dateEl.value : '';
      const dept = (deptEl && deptEl.value) ? deptEl.value : 'All';

      const q = new URLSearchParams();
      if (date) q.set('date', date);
      if (dept) q.set('department', dept);
      if (anaCalendarMonth) q.set('calendarMonth', anaCalendarMonth);

      const j = await getJson('/api/attendance/insights?' + q.toString());
      if (!j) return;
      if (j.ok === false) {
        out(j.error || 'Attendance insights failed.');
        return;
      }

      const r = j.roster || {};
      const totalEmp = Number.isFinite(Number(r.totalEmployees)) ? Number(r.totalEmployees) : 0;
      const present = Number.isFinite(Number(r.present)) ? Number(r.present) : 0;
      const absent = Number.isFinite(Number(r.absent)) ? Number(r.absent) : 0;
      const late = Number.isFinite(Number(r.lateComers)) ? Number(r.lateComers) : 0;
      setText('anaSnapTotal', totalEmp ? String(totalEmp) : '-');
      setText('anaSnapScope', (j.department || 'All') + ' • ' + (j.date || ''));
      setText('anaSnapPresent', totalEmp ? String(present) : '-');
      setText('anaSnapAbsent', totalEmp ? String(absent) : '-');
      setText('anaSnapLate', totalEmp ? String(late) : '-');
      setText('anaSnapAttPct', fmtPct(r.attendanceRatePct));
      setText('anaSnapAbsPct', fmtPct(r.absenteeRatePct));
      const avgWork = Number(r.avgWorkingHours);
      setText('anaSnapAvgWork', Number.isFinite(avgWork) ? avgWork.toFixed(2) : '-');
      const expPerEmp = 8;
      const expPresent = present * expPerEmp;
      const expFull = totalEmp * expPerEmp;
      setText('anaSnapExpHours', (totalEmp > 0) ? (String(Math.round(expPresent)) + ' / ' + String(Math.round(expFull))) : '-');

      const note = el('anaNote');
      if (note) {
        const raw = String(j.note || '').trim();
        if (raw) { note.style.display = ''; note.textContent = raw; }
        else { note.style.display = 'none'; note.textContent = ''; }
      }

      renderCalendarHeatmap(j.calendar || null);

      const attRate = Number(r.attendanceRatePct);
      const attC = renderGauge('anaGaugeAttendance', Number.isFinite(attRate) ? attRate : 0, 100, 'traffic', '0', '100');
      setText('anaGaugeAttendanceVal', fmtPct(attRate));
      const attVal = el('anaGaugeAttendanceVal');
      if (attVal && attC) attVal.style.color = attC;

      const maxAvg = Math.max(6, Math.ceil((Number.isFinite(avgWork) ? avgWork : 0) + 2));
      const avgC = renderGauge('anaGaugeAvgHours', Number.isFinite(avgWork) ? avgWork : 0, maxAvg, 'ocean', '0', String(maxAvg));
      setText('anaGaugeAvgHoursVal', Number.isFinite(avgWork) ? (avgWork.toFixed(2) + ' hrs') : '-');
      const avgVal = el('anaGaugeAvgHoursVal');
      if (avgVal && avgC) avgVal.style.color = String(avgC);

      const monthWorked = Number(r.monthWorkingHours);
      const monthExpected = Number(r.monthExpectedHours);
      const maxMonth = Number.isFinite(monthExpected) && monthExpected > 0 ? monthExpected : Math.max(1, Number.isFinite(monthWorked) ? monthWorked : 0);
      const monthC = renderGauge('anaGaugeMonthHours', Number.isFinite(monthWorked) ? monthWorked : 0, maxMonth, 'violet', '0', Number.isFinite(maxMonth) ? String(Math.round(maxMonth)) : '');
      const mVal = el('anaGaugeMonthHoursVal');
      if (mVal) {
        const w = Number.isFinite(monthWorked) ? monthWorked.toFixed(1) : '-';
        const e = Number.isFinite(monthExpected) ? monthExpected.toFixed(1) : '-';
        const c = String(monthC || '').trim() || '#0ea5e9';
        mVal.innerHTML = '<span style="color:' + c + ';font-weight:900">' + w + '</span> / <span style="color:#64748b;font-weight:800">' + e + '</span> hrs';
      }

      const daysA = j.last7Days || [];
      renderGroupedBars(
        'anaAttendDays',
        daysA,
        [],
        x => x.dow || '',
        x => x.present || 0,
        x => x.present || 0,
        'db',
        ''
      );

      const deps = Array.isArray(j.departments) ? j.departments.slice() : [];
      if (deptEl) {
        const keep = deptEl.value || 'All';
        deptEl.innerHTML = '';
        const o0 = document.createElement('option');
        o0.value = 'All';
        o0.textContent = 'All';
        deptEl.appendChild(o0);
        for (const d of deps) {
          const opt = document.createElement('option');
          opt.value = String(d);
          opt.textContent = String(d);
          deptEl.appendChild(opt);
        }
        deptEl.value = keep;
        if (!deptEl.value) deptEl.value = 'All';
      }

      const anaDeptData = {
        day: Array.isArray(j.byDepartment) ? j.byDepartment.slice() : [],
        week: Array.isArray(j.byDepartmentWeek) ? j.byDepartmentWeek.slice() : [],
        month: Array.isArray(j.byDepartmentMonth) ? j.byDepartmentMonth.slice() : [],
      };
      window.__anaDeptData = anaDeptData;

      function renderDept(period) {
        const p = String(period || 'day');
        const hint = el('anaDeptPeriodHint');
        const countHint = el('anaDeptCountHint');
        const rateHint = el('anaDeptRateHint');
        const items = anaDeptData[p] || [];
        if (p === 'week') {
          if (hint) hint.textContent = 'Based on this week (Mon–Sun)';
          if (countHint) countHint.textContent = 'Avg present per day (count)';
          if (rateHint) rateHint.textContent = 'Attendance rate by department (%)';
          renderHBars('anaDeptCount', items, x => x.department, x => Math.round((Number(x.avgPresentPerDay) || 0) * 10) / 10, '');
        } else if (p === 'month') {
          if (hint) hint.textContent = 'Based on this month (to date)';
          if (countHint) countHint.textContent = 'Avg present per day (count)';
          if (rateHint) rateHint.textContent = 'Attendance rate by department (%)';
          renderHBars('anaDeptCount', items, x => x.department, x => Math.round((Number(x.avgPresentPerDay) || 0) * 10) / 10, '');
        } else {
          if (hint) hint.textContent = 'Based on selected day';
          if (countHint) countHint.textContent = 'Attendance by department (count)';
          if (rateHint) rateHint.textContent = 'Attendance rate by department (%)';
          renderHBars('anaDeptCount', items, x => x.department, x => x.present, '');
        }
        renderHBars('anaDeptRate', items, x => x.department, x => Math.round((Number(x.attendancePct) || 0) * 10) / 10, '%');
      }

      const seg = el('anaDeptPeriod');
      if (seg && !seg.__wired) {
        seg.__wired = true;
        seg.addEventListener('click', (ev) => {
          const btn = ev.target && ev.target.closest ? ev.target.closest('button[data-period]') : null;
          if (!btn) return;
          const period = btn.getAttribute('data-period') || 'day';
          for (const b of seg.querySelectorAll('button[data-period]')) b.classList.toggle('active', b === btn);
          renderDept(period);
        });
      }
      const activeBtn = seg ? seg.querySelector('button[data-period].active') : null;
      renderDept(activeBtn ? (activeBtn.getAttribute('data-period') || 'day') : 'day');

      const last7 = Array.isArray(j.last7Days) ? j.last7Days.slice() : [];
      const absent7Colors = ['#ef4444','#f97316','#f59e0b','#84cc16','#22c55e','#06b6d4','#3b82f6'];
      const late7Colors = ['#8b5cf6','#ec4899','#f43f5e','#fb7185','#a855f7','#0ea5e9','#14b8a6'];
      const absent7Parts = last7.map((d, i) => ({
        label: String(d.dow || ('D' + (i + 1))),
        value: Number(d.absent) || 0,
        color: absent7Colors[i % absent7Colors.length],
      }));
      renderDonut('anaDonutAbsent7', 'anaDonutAbsent7Center', 'anaDonutAbsent7Legend', absent7Parts, absent7Parts.reduce((m, x) => m + (x.value || 0), 0));

      const late7Parts = last7.map((d, i) => ({
        label: String(d.dow || ('D' + (i + 1))),
        value: Number(d.late) || 0,
        color: late7Colors[i % late7Colors.length],
      }));
      renderDonut('anaDonutLate7', 'anaDonutLate7Center', 'anaDonutLate7Legend', late7Parts, late7Parts.reduce((m, x) => m + (x.value || 0), 0));

      const months = Array.isArray(j.last6Months) ? j.last6Months.slice() : [];
      const absentMParts = months.map((m, i) => ({
        label: String(m.label || m.month || ('M' + (i + 1))),
        value: Math.round((Number(m.absenteePct) || 0) * 10) / 10,
        color: ['rgba(22,58,112,.86)','rgba(43,183,169,.80)','rgba(100,116,139,.70)','rgba(220,38,38,.66)','rgba(22,163,74,.60)','rgba(245,158,11,.64)'][i % 6],
      }));
      const avgAbsM = months.length ? (months.reduce((m, x) => m + (Number(x.absenteePct) || 0), 0) / months.length) : 0;
      renderDonut('anaDonutAbsentM', 'anaDonutAbsentMCenter', 'anaDonutAbsentMLegend', absentMParts, fmtPct(avgAbsM));

      renderLineSvg('anaLineAttendance', months, 'attendancePct', 'label', 'line');
      renderLineSvg('anaAreaAbsentee', months, 'absenteePct', 'label', 'area');

      const top = Array.isArray(j.topEmployees) ? j.topEmployees.slice() : [];
      renderTopEmployees('anaTopEmployees', top);

      const anaIntLastSync = el('anaIntLastSync');
      if (anaIntLastSync) {
        const s = await getJson('/api/status');
        if (s && s.ok !== false) {
          const lastSyncAt = (s.sync && (s.sync.lastSyncFinishedAtUtc || s.sync.lastSyncStartedAtUtc)) ? (s.sync.lastSyncFinishedAtUtc || s.sync.lastSyncStartedAtUtc) : '';
          anaIntLastSync.textContent = lastSyncAt ? fmtShort(lastSyncAt) : '-';
        } else {
          anaIntLastSync.textContent = '-';
        }
      }

      const a = await getJson('/api/analytics');
      if (a && a.ok !== false) {
        const db = a.db || {};
        setText('anaSysDbPunches', db.ok ? String(db.totalPunches ?? 0) : '-');
      }
      const missingOut = Number(r.missingOutCount) || 0;
      const duplicates = Number(r.duplicatePunches) || 0;
      setText('anaSysFlagged', String(missingOut + duplicates));
      setText('anaSysLastPunch', (j.lastPunchLocalTime && String(j.lastPunchLocalTime).trim()) ? String(j.lastPunchLocalTime).trim() : '-');

      const att = Number(r.attendanceRatePct) || 0;
      const abs = Number(r.absenteeRatePct) || 0;
      const notif = [];
      if (att > 0 && att < 85) notif.push('Attendance rate below 85%.');
      if (abs > 10) notif.push('Absenteeism above 10%.');
      setText('anaSysNotifVal', notif.length ? notif.join(' • ') : 'No notifications.');
    }

    function openReadOnlyModal(title, fields, values) {
      openModal(title, (fields || []).map(f => ({ ...f, disabled: true })), values || {}, async () => {});
      const saveBtn = el('modalSave');
      if (saveBtn) saveBtn.style.display = 'none';
      const cancelBtn = el('modalCancel');
      if (cancelBtn) cancelBtn.textContent = 'Close';
    }

    function mondayIso(d) {
      const x = new Date(d.getTime());
      const day = x.getDay();
      const diff = (day + 6) % 7;
      x.setDate(x.getDate() - diff);
      return x.toISOString().slice(0, 10);
    }

    function fmtDayShort(isoDate) {
      const raw = String(isoDate || '').trim();
      if (!raw) return '-';
      try {
        const d = new Date(raw + 'T00:00:00');
        if (!Number.isFinite(d.getTime())) return raw;
        const parts = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Kuala_Lumpur', day: '2-digit', month: 'short' }).formatToParts(d);
        const get = (t) => (parts.find(p => p.type === t) || {}).value || '';
        const dd = get('day');
        const mm = get('month');
        return (dd && mm) ? (dd + '-' + mm) : raw;
      } catch {
        return raw;
      }
    }

    function fmtWeekShortRange(weekStartIso, weekEndIso) {
      const a = fmtDayShort(weekStartIso);
      const b = fmtDayShort(weekEndIso);
      if (a === '-' || b === '-') return String(weekStartIso || '') + ' to ' + String(weekEndIso || '');
      return a + ' to ' + b;
    }

    function fmtMonthShort(ym) {
      const raw = String(ym || '').trim();
      if (!raw) return '-';
      const parts = raw.split('-');
      if (parts.length !== 2) return raw;
      const y = parseInt(parts[0], 10);
      const m = parseInt(parts[1], 10);
      if (!Number.isFinite(y) || !Number.isFinite(m) || y < 2000 || y > 2100 || m < 1 || m > 12) return raw;
      try {
        const d = new Date(Date.UTC(y, m - 1, 1));
        const p = new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Kuala_Lumpur', month: 'short', year: 'numeric' }).formatToParts(d);
        const get = (t) => (p.find(x => x.type === t) || {}).value || '';
        const mon = get('month');
        const yr = get('year');
        return (mon && yr) ? (mon + '-' + yr) : raw;
      } catch {
        return raw;
      }
    }

    function toSheetDailyCsv(rows) {
      const header = ['Name', 'Date', 'Shift', 'First In', 'Last Out', 'Total Hours', 'OT Hours', 'Status', 'Flagged Punch'];
      const out = [header.join(',')];
      for (const r of (rows || [])) {
        const flagged = Array.isArray(r.flagged_punches) ? r.flagged_punches.join('; ') : (r.flagged_punches || '');
        const flagsText = String(r.flags || '').trim();
        const flaggedText = String(flagged || '').trim();
        const combined = (flagsText || flaggedText) ? (flagsText + (flagsText && flaggedText ? '\n' : '') + flaggedText) : '';
        const vals = [
          r.name ?? '',
          r.date ?? '',
          r.shift ?? '',
          r.first_in ?? '',
          r.last_out ?? '',
          r.total_hours ?? '',
          r.ot_hours ?? '',
          r.status ?? '',
          combined,
        ].map(v => {
          const s = String(v ?? '');
          return '"' + s.replace(/"/g, '""') + '"';
        });
        out.push(vals.join(','));
      }
      return out.join('\r\n');
    }

    function toSheetWeeklyCsv(rows) {
      const header = ['Name', 'Week', 'Flagged Punches', 'Total Hours', 'OT Hours', 'Days Present', 'Days Absent', 'Attendance %'];
      const out = [header.join(',')];
      for (const r of (rows || [])) {
        const vals = [
          r.name ?? '',
          r.week_display ?? r.week ?? '',
          r.flagged_punches ?? '',
          r.total_hours ?? '',
          r.ot_hours ?? '',
          r.days_present ?? '',
          r.days_absent ?? '',
          String(r.attendance_pct ?? '') + '%',
        ].map(v => {
          const s = String(v ?? '');
          return '"' + s.replace(/"/g, '""') + '"';
        });
        out.push(vals.join(','));
      }
      return out.join('\r\n');
    }

    function toSheetMonthlyCsv(rows) {
      const header = ['Name', 'Month', 'Flagged Punches', 'Total Hours', 'OT Hours', 'Days Present', 'Days Absent', 'Attendance %'];
      const out = [header.join(',')];
      for (const r of (rows || [])) {
        const vals = [
          r.name ?? '',
          r.month_display ?? r.month ?? '',
          r.flagged_punches ?? '',
          r.total_hours ?? '',
          r.ot_hours ?? '',
          r.days_present ?? '',
          r.days_absent ?? '',
          String(r.attendance_pct ?? '') + '%',
        ].map(v => {
          const s = String(v ?? '');
          return '"' + s.replace(/"/g, '""') + '"';
        });
        out.push(vals.join(','));
      }
      return out.join('\r\n');
    }

    function downloadTextCsv(filename, csvText) {
      const blob = new Blob([csvText || ''], { type: 'text/csv;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 5000);
    }

    async function downloadCombinedReport(staffId, month) {
      const sid = String(staffId || '').trim();
      const mon = String(month || '').trim();
      if (!sid || !mon) return;
      const qs = new URLSearchParams({ staff_id: sid, month: mon });
      const res = await fetch('/api/spreadsheet/report?' + qs.toString(), { method: 'GET', credentials: 'same-origin' });
      if (res.status === 401) throw new Error('unauthorized');
      if (!res.ok) throw new Error(await res.text());
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'attendance_report_' + sid + '_' + mon + '.csv';
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 8000);
    }

    async function refreshAttendanceSpreadsheet() {
      function isoDate(d) { return d.toISOString().slice(0, 10); }
      function dateFromIso(iso) { return new Date(String(iso || '').slice(0, 10) + 'T00:00:00Z'); }
      function addDaysIso(iso, days) { const d = dateFromIso(iso); d.setUTCDate(d.getUTCDate() + days); return isoDate(d); }
      function clampRange(fromIso, toIso, maxDays) {
        let f = String(fromIso || '').slice(0, 10);
        let t = String(toIso || '').slice(0, 10);
        if (!f) f = todayKlIso();
        if (!t) t = f;
        if (f > t) { const tmp = f; f = t; t = tmp; }
        const out = [];
        for (let i = 0; i < maxDays; i++) {
          const cur = addDaysIso(f, i);
          out.push(cur);
          if (cur === t) break;
        }
        return out;
      }
      function startOfWeekIso(iso) {
        const d = dateFromIso(iso);
        const dow = (d.getUTCDay() + 6) % 7;
        d.setUTCDate(d.getUTCDate() - dow);
        return isoDate(d);
      }
      function clampWeeks(fromIso, toIso, maxWeeks) {
        let f = startOfWeekIso(fromIso);
        let t = startOfWeekIso(toIso);
        if (f > t) { const tmp = f; f = t; t = tmp; }
        const out = [];
        for (let i = 0; i < maxWeeks; i++) {
          const cur = addDaysIso(f, i * 7);
          out.push(cur);
          if (cur === t) break;
        }
        return out;
      }
      function monthKey(d) { return d.getUTCFullYear() * 12 + d.getUTCMonth(); }
      function monthFromYm(ym) {
        const s = String(ym || '').trim();
        const m = s.match(/^(\d{4})-(\d{2})$/);
        if (!m) return null;
        return new Date(m[1] + '-' + m[2] + '-01T00:00:00Z');
      }
      function clampMonths(fromYm, toYm, maxMonths) {
        const a = monthFromYm(fromYm) || new Date();
        const b = monthFromYm(toYm) || a;
        let f = a;
        let t = b;
        if (monthKey(f) > monthKey(t)) { const tmp = f; f = t; t = tmp; }
        const out = [];
        const startKey = monthKey(f);
        const endKey = monthKey(t);
        for (let k = 0; k < maxMonths; k++) {
          const curKey = startKey + k;
          const y = Math.floor(curKey / 12);
          const mo = (curKey % 12) + 1;
          out.push(String(y) + '-' + String(mo).padStart(2, '0'));
          if (curKey === endKey) break;
        }
        return out;
      }

      if (activeSheetSubTab === 'daily') {
        const fromEl = el('sheetDailyFrom');
        const toEl = el('sheetDailyTo');
        const todayIso = todayKlIso();
        if (fromEl && !String(fromEl.value || '').trim()) { try { fromEl.value = todayIso; } catch { } }
        if (toEl && !String(toEl.value || '').trim() && fromEl && fromEl.value) { try { toEl.value = fromEl.value; } catch { } }
        normalizeDmyInput('sheetDailyFrom');
        normalizeDmyInput('sheetDailyTo');
        const fromIso = parseUserDateToIso(fromEl && fromEl.value ? fromEl.value : '') || todayIso;
        const toIso = parseUserDateToIso(toEl && toEl.value ? toEl.value : '') || fromIso;
        const dates = clampRange(fromIso, toIso, 62);
        if (dates.length >= 62 && dates[dates.length - 1] !== String(toIso).slice(0, 10)) {
          renderSheetDailyRows([], 'Daily range too large. Please select a smaller range.');
          return;
        }
        logActivity('Attendance Spreadsheet / Daily', 'Refreshing daily report: ' + isoToDmy(dates[0]) + ' to ' + isoToDmy(dates[dates.length - 1]));

        let j = null;
        let allRows = [];
        for (const d of dates) {
          const q = new URLSearchParams();
          q.set('date', d);
          const rr = await getJson('/api/spreadsheet/daily?' + q.toString());
          if (!rr || rr.ok === false) { j = rr; break; }
          const rows = Array.isArray(rr.rows) ? rr.rows : [];
          allRows = allRows.concat(rows);
        }

        if (j && j.ok === false) {
          lastSheetDailyRows = [];
          renderSheetDailyRows([], (j && j.error) ? j.error : 'Failed to load daily spreadsheet');
          return;
        }
        const rows = sortRows(allRows, 'daily');
        lastSheetDailyRows = rows.slice();
        renderSheetDailyRows(rows, null);
        return;
      }

      if (activeSheetSubTab === 'weekly') {
        const fromEl = el('sheetWeeklyFrom');
        const toEl = el('sheetWeeklyTo');
        const todayIso = todayKlIso();
        if (fromEl && !String(fromEl.value || '').trim()) { try { fromEl.value = todayIso; } catch { } }
        if (toEl && !String(toEl.value || '').trim() && fromEl && fromEl.value) { try { toEl.value = fromEl.value; } catch { } }
        normalizeDmyInput('sheetWeeklyFrom');
        normalizeDmyInput('sheetWeeklyTo');
        const fromIso = parseUserDateToIso(fromEl && fromEl.value ? fromEl.value : '') || todayIso;
        const toIso = parseUserDateToIso(toEl && toEl.value ? toEl.value : '') || fromIso;
        const weeks = clampWeeks(fromIso, toIso, 26);
        if (weeks.length >= 26 && startOfWeekIso(toIso) !== weeks[weeks.length - 1]) {
          renderSheetWeeklyRows([], 'Weekly range too large. Please select a smaller range.');
          return;
        }
        logActivity('Attendance Spreadsheet / Weekly', 'Refreshing weekly report: ' + isoToDmy(weeks[0]) + ' to ' + isoToDmy(weeks[weeks.length - 1]));

        let j = null;
        let allRows = [];
        for (const w of weeks) {
          const q = new URLSearchParams();
          q.set('date', w);
          const rr = await getJson('/api/spreadsheet/weekly?' + q.toString());
          if (!rr || rr.ok === false) { j = rr; break; }
          const rows = Array.isArray(rr.rows) ? rr.rows : [];
          allRows = allRows.concat(rows);
        }

        if (j && j.ok === false) {
          lastSheetWeeklyRows = [];
          renderSheetWeeklyRows([], (j && j.error) ? j.error : 'Failed to load weekly spreadsheet');
          return;
        }
        const rows = sortRows(allRows, 'weekly');
        lastSheetWeeklyRows = rows.slice();
        renderSheetWeeklyRows(rows, null);
        return;
      }

      if (activeSheetSubTab === 'monthly') {
        const fromEl = el('sheetMonthlyFrom');
        const toEl = el('sheetMonthlyTo');
        if (fromEl && !fromEl.value) { try { fromEl.value = todayKlIso().slice(0, 7); } catch { } }
        if (toEl && !toEl.value && fromEl && fromEl.value) { try { toEl.value = fromEl.value; } catch { } }
        const from = fromEl && fromEl.value ? fromEl.value : todayKlIso().slice(0, 7);
        const to = toEl && toEl.value ? toEl.value : from;
        const months = clampMonths(from, to, 24);
        logActivity('Attendance Spreadsheet / Monthly', 'Refreshing monthly report: ' + months[0] + ' to ' + months[months.length - 1]);

        let j = null;
        let allRows = [];
        for (const m of months) {
          const q = new URLSearchParams();
          q.set('month', m);
          const rr = await getJson('/api/spreadsheet/monthly?' + q.toString());
          if (!rr || rr.ok === false) { j = rr; break; }
          const rows = Array.isArray(rr.rows) ? rr.rows : [];
          allRows = allRows.concat(rows);
        }

        if (j && j.ok === false) {
          lastSheetMonthlyRows = [];
          renderSheetMonthlyRows([], (j && j.error) ? j.error : 'Failed to load monthly spreadsheet');
          return;
        }
        const rows = sortRows(allRows, 'monthly');
        lastSheetMonthlyRows = rows.slice();
        renderSheetMonthlyRows(rows, null);
      }
    }

    const sheetDailyRefreshBtn = el('sheetDailyRefresh');
    if (sheetDailyRefreshBtn) sheetDailyRefreshBtn.addEventListener('click', async () => {
      await refreshAttendanceSpreadsheet();
      notify('Success', 'Table refreshed.', 'ok');
    });
    const sheetWeeklyRefreshBtn = el('sheetWeeklyRefresh');
    if (sheetWeeklyRefreshBtn) sheetWeeklyRefreshBtn.addEventListener('click', async () => {
      await refreshAttendanceSpreadsheet();
      notify('Success', 'Table refreshed.', 'ok');
    });
    const sheetMonthlyRefreshBtn = el('sheetMonthlyRefresh');
    if (sheetMonthlyRefreshBtn) sheetMonthlyRefreshBtn.addEventListener('click', async () => {
      await refreshAttendanceSpreadsheet();
      notify('Success', 'Table refreshed.', 'ok');
    });

    const sheetDailyFromEl = el('sheetDailyFrom');
    if (sheetDailyFromEl && !sheetDailyFromEl.value) { try { sheetDailyFromEl.value = todayKlIso(); } catch { } }
    const sheetDailyToEl = el('sheetDailyTo');
    if (sheetDailyToEl && !sheetDailyToEl.value && sheetDailyFromEl && sheetDailyFromEl.value) { try { sheetDailyToEl.value = sheetDailyFromEl.value; } catch { } }
    const sheetWeeklyFromEl = el('sheetWeeklyFrom');
    if (sheetWeeklyFromEl && !sheetWeeklyFromEl.value) { try { sheetWeeklyFromEl.value = todayKlIso(); } catch { } }
    const sheetWeeklyToEl = el('sheetWeeklyTo');
    if (sheetWeeklyToEl && !sheetWeeklyToEl.value && sheetWeeklyFromEl && sheetWeeklyFromEl.value) { try { sheetWeeklyToEl.value = sheetWeeklyFromEl.value; } catch { } }
    const sheetMonthlyFromEl = el('sheetMonthlyFrom');
    if (sheetMonthlyFromEl && !sheetMonthlyFromEl.value) { try { sheetMonthlyFromEl.value = todayKlIso().slice(0, 7); } catch { } }
    const sheetMonthlyToEl = el('sheetMonthlyTo');
    if (sheetMonthlyToEl && !sheetMonthlyToEl.value && sheetMonthlyFromEl && sheetMonthlyFromEl.value) { try { sheetMonthlyToEl.value = sheetMonthlyFromEl.value; } catch { } }

    if (sheetDailyFromEl) sheetDailyFromEl.addEventListener('blur', () => normalizeDmyInput('sheetDailyFrom'));
    if (sheetDailyToEl) sheetDailyToEl.addEventListener('blur', () => normalizeDmyInput('sheetDailyTo'));
    if (sheetWeeklyFromEl) sheetWeeklyFromEl.addEventListener('blur', () => normalizeDmyInput('sheetWeeklyFrom'));
    if (sheetWeeklyToEl) sheetWeeklyToEl.addEventListener('blur', () => normalizeDmyInput('sheetWeeklyTo'));
    for (const d of [sheetDailyFromEl, sheetDailyToEl, sheetWeeklyFromEl, sheetWeeklyToEl]) {
      if (!d) continue;
      if (String(d.type || '').toLowerCase() !== 'date') continue;
      d.addEventListener('focus', () => { try { if (d.showPicker) d.showPicker(); } catch { } });
    }

    const sheetDailyDownloadBtn = el('sheetDailyDownload');
    if (sheetDailyDownloadBtn) sheetDailyDownloadBtn.addEventListener('click', async () => {
      if (!isSuperadmin) return;
      if (!lastSheetDailyRows.length) await refreshAttendanceSpreadsheet();
      const fromRaw = el('sheetDailyFrom')?.value || '';
      const toRaw = el('sheetDailyTo')?.value || fromRaw;
      const from = parseUserDateToIso(fromRaw) || '';
      const to = parseUserDateToIso(toRaw) || from;
      const tag = (from && to && from !== to) ? (from + '_to_' + to) : (from || 'date');
      downloadTextCsv('attendance_daily_' + tag + '.csv', toSheetDailyCsv(lastSheetDailyRows || []));
    });

    const sheetWeeklyDownloadBtn = el('sheetWeeklyDownload');
    if (sheetWeeklyDownloadBtn) sheetWeeklyDownloadBtn.addEventListener('click', async () => {
      if (!isSuperadmin) return;
      if (!lastSheetWeeklyRows.length) await refreshAttendanceSpreadsheet();
      const fromRaw = el('sheetWeeklyFrom')?.value || '';
      const toRaw = el('sheetWeeklyTo')?.value || fromRaw;
      const from = parseUserDateToIso(fromRaw) || '';
      const to = parseUserDateToIso(toRaw) || from;
      const tag = (from && to && from !== to) ? (from + '_to_' + to) : (from || 'week');
      downloadTextCsv('attendance_weekly_' + tag + '.csv', toSheetWeeklyCsv(lastSheetWeeklyRows || []));
    });

    const sheetMonthlyDownloadBtn = el('sheetMonthlyDownload');
    if (sheetMonthlyDownloadBtn) sheetMonthlyDownloadBtn.addEventListener('click', async () => {
      if (!isSuperadmin) return;
      if (!lastSheetMonthlyRows.length) await refreshAttendanceSpreadsheet();
      const from = el('sheetMonthlyFrom')?.value || '';
      const to = el('sheetMonthlyTo')?.value || from;
      const tag = (from && to && from !== to) ? (from + '_to_' + to) : (from || 'month');
      downloadTextCsv('attendance_monthly_' + tag + '.csv', toSheetMonthlyCsv(lastSheetMonthlyRows || []));
    });

    const sheetMonthlyBodyEl = el('sheetMonthlyBody');
    if (sheetMonthlyBodyEl) sheetMonthlyBodyEl.addEventListener('click', async (e) => {
      const t = e && e.target ? e.target : null;
      if (!t) return;
      const btn = t.closest ? t.closest('button[data-sheet-download]') : null;
      if (btn && isSuperadmin) {
        const sid = String(btn.getAttribute('data-sheet-download') || '').trim();
        const m = String(btn.getAttribute('data-sheet-month') || '').trim() || (el('sheetMonthlyFrom')?.value || '');
        await downloadCombinedReport(sid, m);
        return;
      }
      const pick = t.closest ? t.closest('input.sheetMonthlyPick') : null;
      if (pick) return;
      const tr = t.closest ? t.closest('tr') : null;
      if (!tr || !tr.dataset) return;
      const sid2 = String(tr.dataset.staff || '').trim();
      const mo = String(tr.dataset.month || '').trim();
      if (!sid2 || !/^\d{4}-\d{2}$/.test(mo)) return;
      openStaffAttendanceModal(sid2, mo + '-01');
    });

    const sheetMonthlySelectAll = el('sheetMonthlySelectAll');
    if (sheetMonthlySelectAll) sheetMonthlySelectAll.addEventListener('change', async () => {
      if (!isSuperadmin) return;
      const on = !!sheetMonthlySelectAll.checked;
      for (const r of (lastSheetMonthlyRows || [])) {
        const sid = String(r.staff_id || '').trim();
        if (!sid) continue;
        if (on) monthlySelectedStaff.add(sid);
        else monthlySelectedStaff.delete(sid);
      }
      updateSheetMonthlyBulkUi();
      await refreshAttendanceSpreadsheet();
    });

    if (sheetMonthlyBodyEl) sheetMonthlyBodyEl.addEventListener('change', async (e) => {
      if (!isSuperadmin) return;
      const t = e && e.target ? e.target : null;
      if (!t) return;
      if (!t.classList || !t.classList.contains('sheetMonthlyPick')) return;
      const sid = String(t.getAttribute('data-staff') || '').trim();
      if (!sid) return;
      if (t.checked) monthlySelectedStaff.add(sid);
      else monthlySelectedStaff.delete(sid);
      updateSheetMonthlyBulkUi();
    });

    const sheetMonthlyBulkDownloadBtn = el('sheetMonthlyBulkDownload');
    if (sheetMonthlyBulkDownloadBtn) sheetMonthlyBulkDownloadBtn.addEventListener('click', async () => {
      if (!isSuperadmin) return;
      if (!monthlySelectedStaff || monthlySelectedStaff.size === 0) return;
      const from = el('sheetMonthlyFrom')?.value || todayKlIso().slice(0, 7);
      const to = el('sheetMonthlyTo')?.value || from;
      function monthKey(ym) { const m = String(ym || '').trim().match(/^(\d{4})-(\d{2})$/); if (!m) return null; return Number(m[1]) * 12 + (Number(m[2]) - 1); }
      function ymFromKey(k) { const y = Math.floor(k / 12); const mo = (k % 12) + 1; return String(y) + '-' + String(mo).padStart(2, '0'); }
      const fk = monthKey(from);
      const tk = monthKey(to);
      if (fk === null || tk === null) return;
      const a = Math.min(fk, tk);
      const b = Math.max(fk, tk);
      const months = [];
      for (let k = a; k <= b && months.length < 24; k++) months.push(ymFromKey(k));
      const ids = Array.from(monthlySelectedStaff.values());
      for (const sid of ids) {
        for (const m of months) {
          await downloadCombinedReport(sid, m);
          await new Promise(r => setTimeout(r, 180));
        }
      }
    });

    const sheetWeeklyBodyEl = el('sheetWeeklyBody');
    if (sheetWeeklyBodyEl) sheetWeeklyBodyEl.addEventListener('click', (e) => {
      const t = e && e.target ? e.target : null;
      if (!t) return;
      const tr = t.closest ? t.closest('tr') : null;
      if (!tr || !tr.dataset) return;
      const sid = String(tr.dataset.staff || '').trim();
      const ws = String(tr.dataset.weekStart || '').trim();
      if (!sid || !/^\d{4}-\d{2}-\d{2}$/.test(ws)) return;
      openStaffAttendanceModal(sid, ws);
    });

    const sheetDailyBodyEl = el('sheetDailyBody');
    if (sheetDailyBodyEl) sheetDailyBodyEl.addEventListener('click', (e) => {
      const tr = e && e.target ? e.target.closest('tr') : null;
      const key = tr && tr.dataset ? tr.dataset.key : '';
      if (!key) return;
      const row = sheetDailyByKey.get(key);
      if (!row) return;
      openStaffAttendanceModal(String(row.staff_id || ''), String(row.date || ''));
      return;
      const d = row.details || {};
      const allPunches = Array.isArray(d.punches) ? d.punches.join(', ') : String(d.punches || '');
      const usedPunches = Array.isArray(d.used_punches) ? d.used_punches.join(', ') : String(d.used_punches || '');
      const flaggedPunches = Array.isArray(d.flagged_punches) ? d.flagged_punches.join(', ') : String(d.flagged_punches || '');

      let html =
        '<table class="modalTable">' +
          '<tbody>' +
            '<tr><th>First In</th><td class="mono">' + escHtml(String(d.first_in || '-')) + '</td></tr>' +
            '<tr><th>First Out</th><td class="mono">' + escHtml(String(d.first_out || '-')) + '</td></tr>' +
            '<tr><th>Second In</th><td class="mono">' + escHtml(String(d.second_in || '-')) + '</td></tr>' +
            '<tr><th>Second Out</th><td class="mono">' + escHtml(String(d.second_out || '-')) + '</td></tr>' +
            '<tr><th>OT In</th><td class="mono">' + escHtml(String(d.ot_in || '-')) + '</td></tr>' +
            '<tr><th>OT Out</th><td class="mono">' + escHtml(String(d.ot_out || '-')) + '</td></tr>' +
            '<tr><th>Break (min)</th><td class="mono">' + escHtml(String(d.break_minutes ?? '')) + '</td></tr>' +
            '<tr><th>Late (min)</th><td class="mono">' + escHtml(String(d.late_minutes ?? '')) + '</td></tr>' +
            '<tr><th>Early Leave (min)</th><td class="mono">' + escHtml(String(d.early_leave_minutes ?? '')) + '</td></tr>' +
            '<tr><th>Scheduled Hours</th><td class="mono">' + escHtml(String(d.scheduled_hours ?? '')) + '</td></tr>' +
            '<tr><th>All Punches</th><td class="mono">' + escHtml(allPunches || '-') + '</td></tr>' +
            '<tr><th>Used For Pairing</th><td class="mono">' + escHtml(usedPunches || '-') + '</td></tr>' +
            '<tr><th>Flagged Punch</th><td class="mono">' + escHtml(flaggedPunches || '-') + '</td></tr>' +
          '</tbody>' +
        '</table>';

      if (isSuperadmin) {
        const entries = Array.isArray(d.punch_entries) ? d.punch_entries : [];
        html +=
          '<div style="height:10px"></div>' +
          '<div class="row" style="justify-content:space-between;gap:10px;flex-wrap:wrap">' +
            '<div class="muted" style="font-size:12px">Punch Entries</div>' +
            '<button class="btn primary" type="button" id="sheetPunchAddBtn">Add Punch</button>' +
          '</div>' +
          '<table class="modalPunchTable">' +
            '<thead><tr><th style="width:120px">Time</th><th style="width:120px">Flag</th><th>Device</th><th style="width:180px"></th></tr></thead>' +
            '<tbody>' +
              (entries.length ? entries.map(p =>
                '<tr data-utc="' + escHtml(String(p.occurred_at_utc || '')) + '" data-staff="' + escHtml(String(row.staff_id || '')) + '" data-date="' + escHtml(String(row.date || '')) + '" data-time="' + escHtml(String(p.time || '')) + '">' +
                  '<td class="mono">' + escHtml(String(p.time || '')) + '</td>' +
                  '<td>' + (p.flagged ? 'Duplicate' : '') + '</td>' +
                  '<td class="mono">' + escHtml(String(p.device_id || '')) + '</td>' +
                  '<td>' +
                    '<button class="btn" type="button" data-punch-edit="1">Edit</button> ' +
                    '<button class="btn" type="button" data-punch-delete="1">Delete</button>' +
                  '</td>' +
                '</tr>'
              ).join('') : '<tr><td colspan="4" class="muted">(none)</td></tr>') +
            '</tbody>' +
          '</table>';
      }

      openModalHtml((row.name || row.staff_id || 'Employee') + ' • ' + (row.date || ''), html, { hideSave: true, cancelText: 'Close' });

      if (isSuperadmin) {
        const addBtn = el('sheetPunchAddBtn');
        if (addBtn) addBtn.addEventListener('click', () => {
          const title = 'Add Punch • ' + (row.name || row.staff_id || '') + ' • ' + (row.date || '');
          openModal(title, [{ key: 'occurred_at_local', label: 'Date & Time', type: 'datetime-local' }], { occurred_at_local: String(row.date || '') + 'T09:00' }, async (v) => {
            const payload = { op: 'add', staff_id: String(row.staff_id || ''), occurred_at_local: String(v.occurred_at_local || '').trim(), device_id: 'manual', verify_mode: 255 };
            const r = await postJson('/api/spreadsheet/punch', payload);
            if (!r || r.ok !== true) throw new Error((r && r.error) ? r.error : 'save failed');
            await refreshAttendanceSpreadsheet();
            await refreshLogs();
          });
        });

        const modalBody = el('modalBody');
        if (modalBody) modalBody.onclick = async (ev) => {
          const t = ev && ev.target ? ev.target : null;
          if (!t) return;
          const btnEdit = t.closest ? t.closest('button[data-punch-edit="1"]') : null;
          const btnDel = t.closest ? t.closest('button[data-punch-delete="1"]') : null;
          if (!btnEdit && !btnDel) return;
          const tr2 = t.closest ? t.closest('tr[data-utc]') : null;
          if (!tr2) return;
          const staff = String(tr2.dataset.staff || '').trim();
          const utc = String(tr2.dataset.utc || '').trim();
          const date = String(tr2.dataset.date || '').trim();
          const time = String(tr2.dataset.time || '').trim();

          if (btnDel) {
            if (!confirm('Delete this punch (' + time + ')?')) return;
            const r = await postJson('/api/spreadsheet/punch', { op: 'delete', staff_id: staff, occurred_at_utc: utc });
            if (!r || r.ok !== true) throw new Error((r && r.error) ? r.error : 'delete failed');
            await refreshAttendanceSpreadsheet();
            await refreshLogs();
            closeModal();
            return;
          }

          const title = 'Edit Punch • ' + (row.name || row.staff_id || '') + ' • ' + date;
          const init = { new_occurred_at_local: (date ? (date + 'T' + (time || '09:00').slice(0, 5)) : '') };
          openModal(title, [{ key: 'new_occurred_at_local', label: 'New Date & Time', type: 'datetime-local' }], init, async (v) => {
            const r = await postJson('/api/spreadsheet/punch', { op: 'edit', staff_id: staff, occurred_at_utc: utc, new_occurred_at_local: String(v.new_occurred_at_local || '').trim() });
            if (!r || r.ok !== true) throw new Error((r && r.error) ? r.error : 'save failed');
            await refreshAttendanceSpreadsheet();
            await refreshLogs();
          });
        };
      }
    });

    document.addEventListener('click', (e) => {
      const th = e && e.target ? e.target.closest('th.sortable[data-sort]') : null;
      if (!th) return;
      const table = th.closest('table');
      if (!table) return;
      const scope =
        table.id === 'sheetDailyTable' ? 'daily'
        : table.id === 'sheetWeeklyTable' ? 'weekly'
        : table.id === 'sheetMonthlyTable' ? 'monthly'
        : '';
      if (!scope) return;

      const key = String(th.getAttribute('data-sort') || '').trim();
      if (!key) return;

      const cfg = sheetSort[scope] || { key: 'date', dir: 'asc' };
      if (cfg.key === key) cfg.dir = (cfg.dir === 'asc') ? 'desc' : 'asc';
      else { cfg.key = key; cfg.dir = 'asc'; }
      sheetSort[scope] = cfg;

      if (scope === 'daily') {
        const rows = sortRows(lastSheetDailyRows || [], 'daily');
        lastSheetDailyRows = rows.slice();
        renderSheetDailyRows(rows, null);
      }
      if (scope === 'weekly') {
        const rows = sortRows(lastSheetWeeklyRows || [], 'weekly');
        lastSheetWeeklyRows = rows.slice();
        renderSheetWeeklyRows(rows, null);
      }
      if (scope === 'monthly') {
        const rows = sortRows(lastSheetMonthlyRows || [], 'monthly');
        lastSheetMonthlyRows = rows.slice();
        renderSheetMonthlyRows(rows, null);
      }
    }, true);

    const anaRefresh = el('anaRefresh');
    if (anaRefresh) anaRefresh.addEventListener('click', async () => refreshAttendance());

    const anaCalMonth = el('anaCalMonth');
    if (anaCalMonth) {
      anaCalMonth.addEventListener('change', async () => {
        const v = String(anaCalMonth.value || '').trim();
        if (!/^\d{4}-\d{2}$/.test(v)) return;
        anaCalendarMonth = v;
        await refreshAttendance();
      });
    }

    const anaDate = el('anaDate');
    if (anaDate && !anaDate.value) {
      try { anaDate.value = todayKlIso(); } catch { }
    }

    const anaSnapGrid = el('anaSnapGrid');
    if (anaSnapGrid && !anaSnapGrid.__wired) {
      anaSnapGrid.__wired = true;
      anaSnapGrid.addEventListener('click', (ev) => {
        const btn = ev && ev.target && ev.target.closest ? ev.target.closest('button[data-go]') : null;
        if (!btn) return;
        const go = String(btn.getAttribute('data-go') || '').trim();
        if (go === 'sheetDaily') {
          setActiveTab('attendanceSpreadsheet');
          setActiveSubTab('sheet', 'daily');
        }
      });
    }

    if (!document.body.__snapGoWired) {
      document.body.__snapGoWired = true;
      document.body.addEventListener('click', (ev) => {
        const btn = ev && ev.target && ev.target.closest ? ev.target.closest('button.snapCard[data-go]') : null;
        if (!btn) return;
        const go = String(btn.getAttribute('data-go') || '').trim();
        if (go === 'sheetDaily') {
          setActiveTab('attendanceSpreadsheet');
          setActiveSubTab('sheet', 'daily');
        }
      });
    }

    const devRefreshBtn = el('devRefresh');
    if (devRefreshBtn) devRefreshBtn.addEventListener('click', async () => {
      const pick = getRawDevicePick();
      if (pick.toUpperCase().startsWith('W30-')) {
        notify('Info', 'W30 uses file import. Copy AttendanceLog*.dat to the Reference folder then use Sync Database.', '');
        return;
      }
      devRefreshBtn.disabled = true;
      try {
        logActivity('Raw Data / Device Records', 'Refreshing from device...');
        const res = await fetch('/api/sync?today=1&supabase=0', { method: 'POST', credentials: 'same-origin' });
        if (res.status === 401) {
          logActivity('Raw Data / Device Records', 'Unauthorized. Please reload and login again.', 'ERR');
          notify('Error', 'Unauthorized (please login again).', 'bad');
        } else if (!res.ok) {
          logActivity('Raw Data / Device Records', 'Refresh failed: ' + (await res.text()), 'ERR');
          notify('Error', 'Sync Device failed (see Activity Log).', 'bad');
        } else {
          logActivity('Raw Data / Device Records', 'Refresh complete.', 'OK');
          notify('Success', 'Device synced.', 'ok');
        }
      } finally {
        devRefreshBtn.disabled = false;
      }
      await refreshStatus();
      await refreshLogs();
      await refreshDeviceRecords();
      await refreshDbRecords();
      await refreshAnalytics();
    });

    const dbUpdateSupabaseBtn = el('dbUpdateSupabase');
    if (dbUpdateSupabaseBtn) dbUpdateSupabaseBtn.addEventListener('click', async () => {
      const box = el('dbUpdateBox');
      const ring = el('dbUpdateRing');
      const pctEl = el('dbUpdatePct');
      const txtEl = el('dbUpdateText');
      function setLoad(pct, text) {
        if (box) box.style.display = '';
        if (ring) ring.style.setProperty('--pct', Math.max(0, Math.min(100, pct)) + '%');
        if (pctEl) pctEl.textContent = Math.max(0, Math.min(100, pct)) + '%';
        if (txtEl) txtEl.innerHTML = '<strong>Updating</strong> ' + escHtml(text || '');
      }

      dbUpdateSupabaseBtn.disabled = true;
      let pct = 0;
      setLoad(0, 'Preparing...');
      const t = setInterval(() => {
        pct = Math.min(90, pct + (pct < 30 ? 4 : pct < 60 ? 2 : 1));
        setLoad(pct, 'Uploading to Supabase...');
      }, 350);
      try {
        const r = await postJson('/api/supabase/update', {});
        if (r && r.ok === false) {
          logActivity('Raw Data / Database Records', String(r.error || 'Update Supabase failed'), 'ERR');
          setLoad(Math.min(100, pct), 'Failed. ' + String(r.error || ''));
          notify('Error', 'Sync Database failed (see Activity Log).', 'bad');
        } else {
          const upserted = Number.isFinite(Number(r.upserted)) ? Number(r.upserted) : 0;
          const expected = Number.isFinite(Number(r.distinct)) ? Number(r.distinct) : upserted;
          const from = (r.rangeFrom || '').trim();
          const to = (r.rangeTo || '').trim();

          setLoad(Math.max(pct, 92), 'Refreshing table...');
          const dbRes = await refreshDbRecords();
          if (!dbRes || dbRes.ok !== true) {
            setLoad(99, 'Could not load Database Records.');
            logActivity('Raw Data / Database Records', 'Supabase update OK. Rows upserted: ' + String(upserted ?? 0) + '. Could not load Database Records (API offline or blocked).', 'ERR');
            return;
          }

          setLoad(96, 'Validating...');
          let validated = false;
          if (from && to) {
            const v = await getJson('/api/supabase/validate?from=' + encodeURIComponent(from) + '&to=' + encodeURIComponent(to) + '&expected=' + encodeURIComponent(String(expected)), true);
            if (v && v.ok === true) {
              validated = true;
              setLoad(100, 'Validated. Rows upserted: ' + String(upserted));
              logActivity('Raw Data / Database Records', 'Supabase update OK. Rows upserted: ' + String(upserted) + '. Validated.', 'OK');
              notify('Success', 'Database synced and validated.', 'ok');
            } else {
              const found = (v && Number.isFinite(Number(v.found))) ? Number(v.found) : 0;
              const disc = (v && Number.isFinite(Number(v.discrepancy))) ? Number(v.discrepancy) : null;
              const err = (v && v.error) ? String(v.error || '') : '';
              setLoad(99, 'Validation failed.');
              if (err) logActivity('Raw Data / Database Records', 'Validation failed: ' + err, 'ERR');
              else if (disc !== null) logActivity('Raw Data / Database Records', 'Validation failed: expected at least ' + String(expected) + ' rows in Supabase (range ' + from + ' to ' + to + '), but found ' + String(found) + '. Missing: ' + String(disc), 'ERR');
              else logActivity('Raw Data / Database Records', 'Validation failed.', 'ERR');
              notify('Warning', 'Database synced, but validation failed (see Activity Log).', 'bad');
            }
          }
          if (!validated && (!from || !to)) {
            setLoad(99, 'Done. (Could not validate)');
            logActivity('Raw Data / Database Records', 'Supabase update OK. Rows upserted: ' + String(upserted) + '. (Could not validate)', 'OK');
            notify('Success', 'Database synced (validation unavailable).', 'ok');
          }
        }
      } catch (e) {
        logActivity('Raw Data / Database Records', 'Update Supabase failed', 'ERR');
        setLoad(Math.min(100, pct), 'Failed.');
        notify('Error', 'Sync Database failed (see Activity Log).', 'bad');
      } finally {
        clearInterval(t);
        dbUpdateSupabaseBtn.disabled = false;
        setTimeout(() => { if (box) box.style.display = 'none'; }, 1100);
      }
      await refreshStatus();
      await refreshLogs();
      await refreshAnalytics();
    });

    const logsRefreshBtn = el('logsRefresh');
    if (logsRefreshBtn) logsRefreshBtn.addEventListener('click', async () => {
      try {
        const r = await postJson('/api/logs/sync', {});
        if (r && r.ok) notify('Success', 'System log synced (' + String(r.inserted ?? 0) + ' row(s)).', 'ok');
        else notify('Warning', 'System log sync failed: ' + String((r && r.error) ? r.error : 'error'), 'bad');
      } catch {
        notify('Warning', 'System log sync failed.', 'bad');
      }
      await refreshLogs();
    });

    const activityRefreshBtn = el('activityRefresh');
    if (activityRefreshBtn) activityRefreshBtn.addEventListener('click', async () => {
      try {
        const r = await postJson('/api/activity/sync', {});
        if (r && r.ok) notify('Success', 'Activity log synced (' + String(r.inserted ?? 0) + ' row(s)).', 'ok');
        else notify('Warning', 'Activity log sync failed: ' + String((r && r.error) ? r.error : 'error'), 'bad');
      } catch {
        notify('Warning', 'Activity log sync failed.', 'bad');
      }
    });

    const activityDownloadBtn = el('activityDownload');
    if (activityDownloadBtn) activityDownloadBtn.addEventListener('click', () => {
      const text = el('activityLogs')?.textContent || '';
      const ts = nowKlFileStamp();
      downloadTextFile('activity_log_' + ts + '.txt', text);
      notify('Success', 'Activity log downloaded.', 'ok');
    });

    const systemDownloadBtn = el('systemDownload');
    if (systemDownloadBtn) systemDownloadBtn.addEventListener('click', () => {
      const text = (lastSystemLogLines || []).join('\n');
      const ts = nowKlFileStamp();
      downloadTextFile('system_log_' + ts + '.txt', text);
      notify('Success', 'System log downloaded.', 'ok');
    });

    const activityClearBtn = el('activityClear');
    if (activityClearBtn) activityClearBtn.addEventListener('click', () => {
      const x = el('activityLogs');
      if (x) x.textContent = '';
      notify('Success', 'Activity log cleared.', 'ok');
    });

    const restartDashboardBtn = el('restartDashboard');
    if (restartDashboardBtn) restartDashboardBtn.addEventListener('click', async () => {
      const ok = confirm('Restart dashboard now?\n\nThis will briefly disconnect the page and reload when the dashboard is back online.');
      if (!ok) return;
      restartDashboardBtn.disabled = true;
      out('Restarting dashboard...');
      notify('Success', 'Restart requested. The page will reload.', 'ok');
      await postJson('/api/restart', {});

      let tries = 0;
      const t = setInterval(async () => {
        tries++;
        try {
          const res = await fetch('/login', { cache: 'no-store', credentials: 'same-origin' });
          if (res && res.ok) {
            clearInterval(t);
            window.location.href = '/login';
          }
        } catch { }
        if (tries >= 60) {
          clearInterval(t);
          out('Restart requested. If the page does not come back, please start WL10Middleware on this PC.');
        }
      }, 1000);
    });

    const schedTimeAddBtn = el('schedTimeAdd');
    if (schedTimeAddBtn) schedTimeAddBtn.addEventListener('click', () => {
      const raw = el('schedTimeNew')?.value || '';
      const t = normTimeHHmm(raw);
      if (!t) { notify('Error', 'Please select a valid time.', 'bad'); return; }
      setMandatoryTimes((mandatorySyncTimes || []).concat([t]));
      notify('Success', 'Mandatory sync time added.', 'ok');
    });

    const schedCards = el('schedTimeCards');
    if (schedCards) schedCards.addEventListener('click', (e) => {
      const btn = e && e.target ? e.target.closest('button[data-edit-time],button[data-del-time]') : null;
      if (!btn) return;
      const edit = btn.getAttribute('data-edit-time');
      const del = btn.getAttribute('data-del-time');
      if (del) {
        const t = String(del || '').trim();
        if (!t) return;
        if (!confirm('Delete mandatory sync time ' + t + '?')) return;
        setMandatoryTimes((mandatorySyncTimes || []).filter(x => String(x) !== t));
        notify('Success', 'Mandatory sync time deleted.', 'ok');
        return;
      }
      if (edit) {
        const oldT = String(edit || '').trim();
        openModalHtml('Edit Mandatory Sync Time', '' +
          '<div class="formGrid compactLabels">' +
            '<label>Time</label>' +
            '<input id="editSyncTime" type="time" step="60" class="timeInput" value="' + escHtml(oldT) + '" />' +
          '</div>'
        , {
          onSaveAsync: async () => {
            const v = normTimeHHmm(el('editSyncTime')?.value || '');
            if (!v) throw new Error('Please select a valid time.');
            const next = (mandatorySyncTimes || []).map(x => (String(x) === oldT ? v : x));
            setMandatoryTimes(next);
            notify('Success', 'Mandatory sync time updated.', 'ok');
          }
        });
      }
    });

    const supaPubTa = el('supaPubKey'); if (supaPubTa) supaPubTa.addEventListener('input', () => autoGrowTextarea(supaPubTa));
    const supaKeyTa = el('supaKey'); if (supaKeyTa) supaKeyTa.addEventListener('input', () => autoGrowTextarea(supaKeyTa));
    const supaJwtTa = el('supaJwt'); if (supaJwtTa) supaJwtTa.addEventListener('input', () => autoGrowTextarea(supaJwtTa));
    try { if (supaPubTa) autoGrowTextarea(supaPubTa); } catch { }
    try { if (supaKeyTa) autoGrowTextarea(supaKeyTa); } catch { }
    try { if (supaJwtTa) autoGrowTextarea(supaJwtTa); } catch { }

    const saveDeviceBtn = el('saveDevice');
    if (saveDeviceBtn) saveDeviceBtn.addEventListener('click', async () => {
      const ip = el('setIp').value.trim();
      const port = parseInt(el('setPort').value, 10);
      const readerMode = el('setReader').value;
      const r = await postJson('/api/settings', { deviceIp: ip, devicePort: port, readerMode });
      if (r && r.ok === false) out(r.error || 'Save failed');
      await refreshStatus();
    });

    const pingNowBtn = el('pingNow');
    if (pingNowBtn) pingNowBtn.addEventListener('click', async () => {
      pingNowBtn.disabled = true;
      const t = getDeviceTarget();
      try {
        const r = await postJson('/api/device/ping', {});
        const ip = (r && r.deviceIp) ? r.deviceIp : (t.ip || '-');
        if (r && r.ok) out('Ping OK to ' + ip + ' ' + (r.rttMs ? (r.rttMs + 'ms') : ''));
        else if (r && r.ok === false) {
          const hint = String(r.error || 'error') === 'timeout' ? ' (check same network / ICMP blocked / device offline)' : '';
          out('Ping failed to ' + ip + ': ' + String(r.error || 'error') + hint);
        }
        else out('Ping failed');
      } finally {
        pingNowBtn.disabled = false;
      }
      await refreshStatus();
    });

    const testNowBtn = el('testNow');
    if (testNowBtn) testNowBtn.addEventListener('click', async () => {
      testNowBtn.disabled = true;
      const t = getDeviceTarget();
      try {
        const r = await postJson('/api/device/test', {});
        const ip = (r && r.deviceIp) ? r.deviceIp : (t.ip || '-');
        const port = (r && (r.devicePort ?? null) !== null) ? r.devicePort : (t.port ?? '-');
        const target = ip + ':' + port;
        if (r && r.ok) out('TCP OK to ' + target + ' ' + (r.rttMs ? (r.rttMs + 'ms') : ''));
        else if (r && r.ok === false) {
          const hint = String(r.error || 'error') === 'timeout' ? ' (check firewall/port ' + (t.port ?? '-') + ' / device offline)' : '';
          out('TCP failed to ' + target + ': ' + String(r.error || 'error') + hint);
        }
        else out('TCP failed');
      } finally {
        testNowBtn.disabled = false;
      }
      await refreshStatus();
      await refreshPresets();
    });

    const connectNowBtn = el('connectNow');
    if (connectNowBtn) connectNowBtn.addEventListener('click', async () => {
      connectNowBtn.disabled = true;
      let ok = false;
      try {
        out('');
        const t = getDeviceTarget();
        const readerMode = (el('setReader')?.value || '').trim();
        const ip = (t.ip || '').trim();
        const port = (t.port ?? null);
        const target = (ip || '-') + ':' + (port ?? '-');

        logActivity('Settings / Device', 'Connect Now started: saving device settings...', 'TRACE');
        outAppend('Saving device settings...');
        const saveRes = await postJson('/api/settings', { deviceIp: ip, devicePort: port, readerMode });
        if (saveRes && saveRes.ok === false) {
          outAppend('Save device failed: ' + String(saveRes.error || 'error'));
          logActivity('Settings / Device', 'Save device failed: ' + String(saveRes.error || 'error'), 'ERR');
          notify('Error', 'Save Device failed (see Activity Log).', 'bad');
          return;
        }
        outAppend('Saved device.');

        logActivity('Settings / Device', 'Pinging device ' + target + '...', 'TRACE');
        outAppend('Pinging device...');
        const pingRes = await postJson('/api/device/ping', {});
        if (!pingRes || pingRes.ok !== true) {
          const e = pingRes && pingRes.error ? String(pingRes.error) : 'ping failed';
          outAppend('Ping failed: ' + e);
          logActivity('Settings / Device', 'Ping failed: ' + e, 'ERR');
          notify('Error', 'Ping failed (see Activity Log).', 'bad');
          return;
        }
        outAppend('Ping OK ' + (pingRes.rttMs ? (pingRes.rttMs + 'ms') : ''));

        logActivity('Settings / Device', 'Testing TCP to ' + target + '...', 'TRACE');
        outAppend('Testing TCP...');
        const testRes = await postJson('/api/device/test', {});
        if (!testRes || testRes.ok !== true) {
          const e = testRes && testRes.error ? String(testRes.error) : 'tcp test failed';
          outAppend('TCP failed: ' + e);
          logActivity('Settings / Device', 'TCP failed: ' + e, 'ERR');
          notify('Error', 'TCP test failed (see Activity Log).', 'bad');
          return;
        }
        outAppend('TCP OK ' + (testRes.rttMs ? (testRes.rttMs + 'ms') : ''));

        logActivity('Settings / Device', 'Connecting (TCP) to ' + target + '...', 'TRACE');
        outAppend('Connecting...');
        const r = await postJson('/api/device/connect', { deviceIp: ip, devicePort: port, readerMode });
        if (r && r.ok) {
          ok = true;
          let msg = 'Connected (TCP) to ' + target + ' ' + (r.rttMs ? (r.rttMs + 'ms') : '');
          if (r.verifyOk === true) msg += ' | Verified (read OK)';
          else if (r.verifyOk === false) msg += ' | Verify failed: ' + String(r.verifyError || 'error');
          outAppend(msg);
          logActivity('Settings / Device', msg, 'OK');
          notify('Success', 'Connected to device.', 'ok');
        } else {
          const msg = (r && r.ok === false) ? String(r.error || 'error') : 'Connect failed';
          outAppend('Connect failed: ' + msg);
          logActivity('Settings / Device', 'Connect failed: ' + msg, 'ERR');
          notify('Error', 'Connect failed (see Activity Log).', 'bad');
        }
      } finally {
        connectNowBtn.disabled = false;
      }
      await refreshStatus();
      await refreshPresets();
      if (ok) triggerAutoSync('device-connect', true);
    });

    const grantAdminBtn = el('grantAdminBtn');
    if (grantAdminBtn) grantAdminBtn.addEventListener('click', async () => {
      const uid = (el('grantAdminUserId')?.value || '').trim();
      if (!uid) { out('Please enter a User ID.'); return; }
      grantAdminBtn.disabled = true;
      try {
        logActivity('Settings / Device', 'Granting admin to User ID ' + uid + '...');
        const r = await postJson('/api/device/user/grant-admin', { user_id: uid });
        if (r && r.ok) {
          out('Admin granted for User ID ' + uid + '.');
          logActivity('Settings / Device', 'Admin granted for User ID ' + uid + '.', 'OK');
        } else {
          const msg = (r && r.error) ? String(r.error) : 'Grant admin failed';
          out('Grant admin failed: ' + msg);
          logActivity('Settings / Device', 'Grant admin failed: ' + msg, 'ERR');
        }
      } finally {
        grantAdminBtn.disabled = false;
      }
      await refreshStatus();
      await refreshLogs();
    });

    const disconnectNowBtn = el('disconnectNow');
    if (disconnectNowBtn) disconnectNowBtn.addEventListener('click', async () => {
      disconnectNowBtn.disabled = true;
      try {
        const r = await postJson('/api/device/disconnect', {});
        if (r && r.ok === false) out(r.error || 'Disconnect failed');
        else out('Disconnected.');
        if (r && r.ok === false) notify('Error', 'Disconnect failed (see Activity Log).', 'bad');
        else notify('Success', 'Disconnected from device.', 'ok');
      } finally {
        disconnectNowBtn.disabled = false;
      }
      await refreshStatus();
    });

    const devCsvBtn = el('devCsv');
    if (devCsvBtn) devCsvBtn.addEventListener('click', () => {
      const rows = (lastDeviceRows || [])
        .slice()
        .sort((a, b) => String(b.datetime ?? '').localeCompare(String(a.datetime ?? '')));
      downloadAttlogCsv('1_attlog.csv', rows);
    });
    const dbCsvBtn = el('dbCsv');
    if (dbCsvBtn) dbCsvBtn.addEventListener('click', () => {
      const rows = (lastDbRows || [])
        .slice()
        .sort((a, b) => String(b.datetime ?? '').localeCompare(String(a.datetime ?? '')));
      downloadCsv('db_records.csv', rows);
    });
    const staffExportBtn = el('staffExport');
    if (staffExportBtn) staffExportBtn.addEventListener('click', () => {
      downloadStaffCsv('staff_records.csv', currentStaffViewRows());
      notify('Success', 'Download started.', 'ok');
    });

    const staffImportBtn = el('staffImport');
    if (staffImportBtn) staffImportBtn.addEventListener('click', () => {
      if (!isSuperadmin) return;
      openModalHtml('Import Staff CSV', '' +
        '<div class="formGrid">' +
          '<label>CSV File</label>' +
          '<input id="staffImportFile" type="file" accept=".csv" />' +
          '<label>Mode</label>' +
          '<div class="pill ok">Merge/Upsert by User ID</div>' +
        '</div>' +
        '<div class="hint" style="margin-top:10px">Use "Download CSV" as the template format.</div>'
      , {
        onSaveAsync: async () => {
          const fileEl = el('staffImportFile');
          const file = fileEl && fileEl.files && fileEl.files.length ? fileEl.files[0] : null;
          if (!file) throw new Error('Please choose a CSV file.');

          const text = await file.text();
          const parsed = parseCsv(text);
          if (!parsed.length) throw new Error('CSV is empty.');
          const header = parsed[0].map(x => String(x ?? '').trim().toLowerCase());
          const idx = (name) => {
            const n = String(name).trim().toLowerCase();
            return header.findIndex(h => h.replace(/\s+/g, ' ') === n || h.replace(/\s+/g, '') === n.replace(/\s+/g, ''));
          };

          const iUser = idx('user id');
          const iFirst = idx('first name');
          const iFull = idx('full name');
          const iRole = idx('role');
          const iDept = idx('department');
          const iStatus = idx('status');
          const iJoined = idx('date joined');
          const iShift = idx('shift pattern');
          if (iUser === -1) throw new Error('Missing required column "User ID".');

          const imported = [];
          for (let r = 1; r < parsed.length; r++) {
            const row = parsed[r];
            const raw = {
              user_id: row[iUser] ?? '',
              full_name: (iFirst !== -1 ? row[iFirst] : '') || (iFull !== -1 ? row[iFull] : ''),
              role: iRole !== -1 ? row[iRole] : '',
              department: iDept !== -1 ? row[iDept] : '',
              status: iStatus !== -1 ? row[iStatus] : '',
              date_joined: iJoined !== -1 ? row[iJoined] : '',
              shift_pattern: iShift !== -1 ? row[iShift] : '',
            };
            const normalized = normalizeStaffRow(raw);
            if (normalized) imported.push(normalized);
          }
          if (!imported.length) throw new Error('No valid rows found.');

          const existingMap = new Map();
          for (const r of (lastStaffRows || [])) {
            const id = String(r.user_id ?? '').trim();
            if (!id) continue;
            existingMap.set(id, r);
          }

          const importMap = new Map();
          for (const r of imported) {
            const id = String(r.user_id ?? '').trim();
            if (!id) continue;
            importMap.set(id, r);
          }

          let added = 0;
          let updated = 0;
          for (const [id, r] of importMap.entries()) {
            if (existingMap.has(id)) updated++;
            else added++;
            existingMap.set(id, r);
          }

          lastStaffRows = Array.from(existingMap.values()).sort((a, b) => {
            const an = Number(String(a.user_id ?? '').trim());
            const bn = Number(String(b.user_id ?? '').trim());
            if (Number.isFinite(an) && Number.isFinite(bn) && an !== bn) return an - bn;
            return String(a.user_id ?? '').localeCompare(String(b.user_id ?? ''));
          });

          logActivity('Staff Records / Employee List', 'Importing (upsert) ' + importMap.size + ' row(s) to Supabase... added=' + added + ' updated=' + updated + ' total=' + lastStaffRows.length);
          await saveStaffRows(lastStaffRows);
          await renderStaffTable(currentStaffViewRows(), 'No staff records.');
          refreshStaffRecords();
          logActivity('Staff Records / Employee List', 'Import complete.', 'OK');
        }
      });
    });

    const shiftExportBtn = el('shiftExport');
    if (shiftExportBtn) shiftExportBtn.addEventListener('click', () => {
      downloadShiftCsv('shift_patterns.csv', (lastShiftRows && lastShiftRows.length ? lastShiftRows : fallbackShiftRows));
    });

    const shiftImportBtn = el('shiftImport');
    if (shiftImportBtn) shiftImportBtn.addEventListener('click', () => {
      if (!isSuperadmin) return;
      openModalHtml('Import Shift Pattern CSV', '' +
        '<div class="formGrid">' +
          '<label>CSV File</label>' +
          '<input id="shiftImportFile" type="file" accept=".csv" />' +
          '<label>Mode</label>' +
          '<div class="pill ok">Merge/Upsert by Pattern</div>' +
        '</div>' +
        '<div class="hint" style="margin-top:10px">Use "Download CSV" as the template format.</div>'
      , {
        onSaveAsync: async () => {
          const fileEl = el('shiftImportFile');
          const file = fileEl && fileEl.files && fileEl.files.length ? fileEl.files[0] : null;
          if (!file) throw new Error('Please choose a CSV file.');

          const text = await file.text();
          const parsed = parseCsv(text);
          if (!parsed.length) throw new Error('CSV is empty.');
          const header = parsed[0].map(x => String(x ?? '').trim().toLowerCase());
          const idx = (name) => {
            const n = String(name).trim().toLowerCase();
            return header.findIndex(h => h.replace(/\s+/g, ' ') === n || h.replace(/\s+/g, '') === n.replace(/\s+/g, ''));
          };

          const iPattern = idx('pattern');
          const iDays = idx('working days');
          const iHours = idx('working hours');
          const iBreak = idx('break');
          const iNotes = idx('notes');
          if (iPattern === -1) throw new Error('Missing required column "Pattern".');

          const imported = [];
          for (let r = 1; r < parsed.length; r++) {
            const row = parsed[r];
            const pattern = String(row[iPattern] ?? '').trim();
            if (!pattern) continue;
            imported.push({
              pattern,
              workingDays: iDays !== -1 ? String(row[iDays] ?? '').trim() : '',
              workingHours: iHours !== -1 ? String(row[iHours] ?? '').trim() : '',
              break: iBreak !== -1 ? String(row[iBreak] ?? '').trim() : '',
              notes: iNotes !== -1 ? String(row[iNotes] ?? '').trim() : '',
            });
          }
          if (!imported.length) throw new Error('No valid rows found.');

          const existingMap = new Map();
          for (const r of (lastShiftRows || [])) {
            const id = String(r.pattern ?? '').trim();
            if (!id) continue;
            existingMap.set(id, r);
          }

          const importMap = new Map();
          for (const r of imported) {
            const id = String(r.pattern ?? '').trim();
            if (!id) continue;
            importMap.set(id, r);
          }

          let added = 0;
          let updated = 0;
          for (const [id, r] of importMap.entries()) {
            if (existingMap.has(id)) updated++;
            else added++;
            existingMap.set(id, r);
          }

          lastShiftRows = Array.from(existingMap.values()).sort((a, b) => String(a.pattern ?? '').localeCompare(String(b.pattern ?? '')));
          logActivity('Staff Records / Shift Pattern', 'Importing (upsert) ' + importMap.size + ' shift pattern(s)... added=' + added + ' updated=' + updated + ' total=' + lastShiftRows.length);
          await saveShiftRows(lastShiftRows);
          await renderShiftTable(applyShiftFilters(lastShiftRows, ''));
          refreshShiftPatterns();
          logActivity('Staff Records / Shift Pattern', 'Import complete.', 'OK');
        }
      });
    });

    const staffSelectAll = el('staffSelectAll');
    if (staffSelectAll) staffSelectAll.addEventListener('change', () => {
      const body = el('staffBody');
      if (!body) return;
      const checked = !!staffSelectAll.checked;
      for (const cb of body.querySelectorAll('input.staffChk[type="checkbox"]')) {
        const id = String(cb.getAttribute('data-id') || '').trim();
        if (checked) staffSelectedIds.add(id);
        else staffSelectedIds.delete(id);
        cb.checked = checked;
      }
      updateStaffDeleteEnabled();
    });

    const staffHead = document.querySelector('#subtab-staff-staff thead');
    if (staffHead) staffHead.addEventListener('click', (e) => {
      const t = e && e.target;
      const th = (t && t.closest) ? t.closest('th[data-sort]') : null;
      if (!th || !isSuperadmin) return;
      const key = String(th.getAttribute('data-sort') || '').trim();
      if (!key) return;
      if (staffSortKey === key) staffSortDir = (staffSortDir === 'asc' ? 'desc' : 'asc');
      else { staffSortKey = key; staffSortDir = 'asc'; }
      updateStaffSortUi();
      renderStaffTable(currentStaffViewRows(), 'No staff records.');
    });

    const staffProvisionSelectedBtn = el('staffProvisionSelected');
    if (staffProvisionSelectedBtn) staffProvisionSelectedBtn.addEventListener('click', async () => {
      if (!isSuperadmin) return;
      const ids = Array.from(staffSelectedIds);
      if (!ids.length) return;
      staffProvisionSelectedBtn.disabled = true;
      try {
        out('');
        for (const id of ids) {
          const row = lastStaffRows.find(r => String(r.user_id ?? '').trim() === id);
          if (!row) continue;
          await provisionStaffToDevice(row, null);
        }
        notify('Success', 'Device updated for selected staff.', 'ok');
      } finally {
        staffProvisionSelectedBtn.disabled = false;
        updateStaffDeleteEnabled();
      }
    });

    const modalCloseBtn = el('modalClose');
    const modalCancelBtn = el('modalCancel');
    const modalSaveBtn = el('modalSave');
    const modalBackEl = el('modalBack');
    if (modalCloseBtn) modalCloseBtn.addEventListener('click', closeModal);
    if (modalCancelBtn) modalCancelBtn.addEventListener('click', closeModal);
    if (modalSaveBtn) modalSaveBtn.addEventListener('click', async () => { if (modalOnSave) await modalOnSave(); });
    if (modalBackEl) modalBackEl.addEventListener('click', (e) => { if (e && e.target === modalBackEl) closeModal(); });
    document.addEventListener('keydown', (e) => { if (e && e.key === 'Escape') closeModal(); });

    const staffAddBtn = el('staffAdd');
    if (staffAddBtn) staffAddBtn.addEventListener('click', async () => {
      await refreshShiftPatterns();
      openModal('Add Staff', staffFields('add'), {}, async (vals) => {
        const userId = String(vals.user_id || '').trim();
        if (!userId) throw new Error('User ID is required');
        if (lastStaffRows.some(r => String(r.user_id ?? '').trim() === userId)) throw new Error('User ID already exists');
        const normalized = normalizeStaffRow({
          user_id: userId,
          full_name: String(vals.full_name || '').trim(),
          role: String(vals.role || '').trim(),
          department: String(vals.department || '').trim(),
          status: String(vals.status || '').trim(),
          date_joined: String(vals.date_joined || '').trim(),
          shift_pattern: String(vals.shift_pattern || '').trim(),
        });
        if (!normalized) throw new Error('Invalid staff row');
        lastStaffRows = lastStaffRows.concat([normalized]).sort((a, b) => String(a.user_id ?? '').localeCompare(String(b.user_id ?? '')));
        await saveStaffRows(lastStaffRows);
        await renderStaffTable(currentStaffViewRows(), 'No staff records.');
        refreshStaffRecords();
        if (vals.provision_to_device) await provisionStaffToDevice(normalized, null);
      });
    });

    const staffBodyEl = el('staffBody');
    if (staffBodyEl) {
      staffBodyEl.addEventListener('change', (e) => {
        const t = e && e.target;
        if (!t || !t.classList || !t.classList.contains('staffChk')) return;
        const id = String(t.getAttribute('data-id') || '').trim();
        if (t.checked) staffSelectedIds.add(id);
        else staffSelectedIds.delete(id);
        updateStaffDeleteEnabled();
      });
      staffBodyEl.addEventListener('click', (e) => {
        const t = e && e.target;
        const target = (t && t.closest) ? t.closest('button.staffProvisionRow,button.staffEditRow') : null;
        if (!target || !target.classList) return;
        if (target.classList.contains('staffProvisionRow')) {
          const id = String(target.getAttribute('data-id') || '').trim();
          if (!id) return;
          const existing = lastStaffRows.find(r => String(r.user_id ?? '').trim() === id);
          if (!existing) return;
          provisionStaffToDevice(existing, target);
          return;
        }
        if (!target.classList.contains('staffEditRow')) return;
        const id = String(target.getAttribute('data-id') || '').trim();
        if (!id) return;
        const existing = lastStaffRows.find(r => String(r.user_id ?? '').trim() === id);
        if (!existing) return;
        refreshShiftPatterns().then(() => openModal('Edit Staff', staffFields('edit'), existing, async (vals) => {
          lastStaffRows = lastStaffRows.map(r => {
            if (String(r.user_id ?? '').trim() !== id) return r;
            return normalizeStaffRow({
              user_id: id,
              full_name: String(vals.full_name || '').trim(),
              role: String(vals.role || '').trim(),
              department: String(vals.department || '').trim(),
              status: String(vals.status || '').trim(),
              date_joined: String(vals.date_joined || '').trim(),
              shift_pattern: String(vals.shift_pattern || '').trim(),
            }) || r;
          });
          await saveStaffRows(lastStaffRows);
          await renderStaffTable(currentStaffViewRows(), 'No staff records.');
          refreshStaffRecords();
        }));
      });
    }

    const staffDeleteBtn = el('staffDelete');
    if (staffDeleteBtn) staffDeleteBtn.addEventListener('click', async () => {
      const ids = Array.from(staffSelectedIds);
      if (!ids.length) return;
      if (!confirm('Delete ' + ids.length + ' staff record(s)?')) return;
      for (const id of ids) staffSelectedIds.delete(id);
      lastStaffRows = (lastStaffRows || []).filter(r => !ids.includes(String(r.user_id ?? '').trim()));
      await saveStaffRows(lastStaffRows);
      await refreshStaffRecords();
      notify('Success', 'Staff records deleted.', 'ok');
    });

    const shiftAddBtn = el('shiftAdd');
    function parseTimeRangeText(raw) {
      const s = String(raw || '').trim();
      if (!s) return { start: '', end: '' };
      const sep = s.includes('–') ? '–' : (s.includes('-') ? '-' : null);
      if (!sep) return { start: '', end: '' };
      const p = s.split(sep).map(x => String(x || '').trim()).filter(Boolean);
      if (p.length !== 2) return { start: '', end: '' };
      return { start: p[0].slice(0, 5), end: p[1].slice(0, 5) };
    }

    function normalizeHm(raw) {
      const s = String(raw || '').trim();
      const m = s.match(/^(\d{1,2}):(\d{2})$/);
      if (!m) return '';
      const hh = Number(m[1]);
      const mm = Number(m[2]);
      if (hh < 0 || hh > 23 || mm < 0 || mm > 59) return '';
      return String(hh).padStart(2, '0') + ':' + String(mm).padStart(2, '0');
    }

    function openShiftPatternModal(mode, existing) {
      const isEdit = mode === 'edit';
      const row = existing || {};
      const pattern = String(row.pattern || '').trim();
      const wdRaw = String(row.workingDays || '').trim() || 'Mon–Fri';
      const wh = parseTimeRangeText(row.workingHours || '');
      const br = parseTimeRangeText(row.break || '');
      const notes = String(row.notes || '').trim();

      function parseWdSet(raw) {
        const s0 = String(raw || '').trim();
        const set = new Set();
        const order = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
        function normDayToken(x) {
          const k = String(x || '').trim().slice(0, 3).toLowerCase();
          if (k === 'mon') return 'Mon';
          if (k === 'tue') return 'Tue';
          if (k === 'wed') return 'Wed';
          if (k === 'thu') return 'Thu';
          if (k === 'fri') return 'Fri';
          if (k === 'sat') return 'Sat';
          if (k === 'sun') return 'Sun';
          return '';
        }
        function addRange(aRaw, bRaw) {
          const a = normDayToken(aRaw);
          const b = normDayToken(bRaw);
          const ai = order.indexOf(a);
          const bi = order.indexOf(b);
          if (ai < 0 || bi < 0) return false;
          let cur = ai;
          while (true) {
            set.add(order[cur]);
            if (cur === bi) break;
            cur = (cur + 1) % 7;
          }
          return true;
        }
        if (!s0) { for (const d of ['Mon','Tue','Wed','Thu','Fri']) set.add(d); return set; }
        const s = s0.replace(/to/ig, '–').replace(/-/g, '–');
        const parts = s.split(',').map(x => String(x || '').trim()).filter(Boolean);
        for (const p of parts) {
          if (p.includes('–')) {
            const rr = p.split('–').map(x => String(x || '').trim()).filter(Boolean);
            if (rr.length === 2 && addRange(rr[0], rr[1])) continue;
          }
          const d = normDayToken(p);
          if (d) set.add(d);
        }
        if (!set.size && s.includes('–')) {
          const rr = s.split('–').map(x => String(x || '').trim()).filter(Boolean);
          if (rr.length === 2) addRange(rr[0], rr[1]);
        }
        if (!set.size) { for (const d of ['Mon','Tue','Wed','Thu','Fri']) set.add(d); }
        return set;
      }

      const wdSet = parseWdSet(wdRaw);
      const wdDays = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
      const wdHtml = '<div class="wdBox">' + wdDays.map(d =>
        '<label class="wdItem"><input class="wdPick" type="checkbox" data-day="' + escHtml(d) + '"' + (wdSet.has(d) ? ' checked' : '') + '/><span>' + escHtml(d) + '</span></label>'
      ).join('') + '</div>';

      const html =
        '<div class="modalGrid" style="grid-template-columns:160px minmax(0,1fr)">' +
          '<label>Pattern</label>' +
          '<input id="spPattern" ' + (isEdit ? 'disabled' : '') + ' value="' + escHtml(pattern) + '" placeholder="e.g. Normal" />' +
          '<label>Working Days</label>' +
          '<div>' + wdHtml + '</div>' +
          '<label>Working Hours</label>' +
          '<div class="row" style="gap:10px;flex-wrap:wrap">' +
            '<input id="spWorkStart" class="timeInput" type="time" step="60" value="' + escHtml(wh.start || '09:00') + '" />' +
            '<span class="muted">to</span>' +
            '<input id="spWorkEnd" class="timeInput" type="time" step="60" value="' + escHtml(wh.end || '18:00') + '" />' +
          '</div>' +
          '<label>Break</label>' +
          '<div class="row" style="gap:10px;flex-wrap:wrap">' +
            '<input id="spBreakStart" class="timeInput" type="time" step="60" value="' + escHtml(br.start || '13:00') + '" />' +
            '<span class="muted">to</span>' +
            '<input id="spBreakEnd" class="timeInput" type="time" step="60" value="' + escHtml(br.end || '14:00') + '" />' +
          '</div>' +
          '<label>Notes</label>' +
          '<input id="spNotes" value="' + escHtml(notes) + '" placeholder="Optional" />' +
        '</div>';

      openModalHtml(isEdit ? 'Edit Shift Pattern' : 'Add Shift Pattern', html, {
        cancelText: 'Cancel',
        onSaveAsync: async () => {
          const p = String(el('spPattern')?.value || '').trim();
          if (!p) throw new Error('Pattern is required');
          if (!isEdit && lastShiftRows.some(r => String(r.pattern ?? '').trim() === p)) throw new Error('Pattern already exists');

          const picked = new Set();
          for (const cb of Array.from(el('modalBody')?.querySelectorAll('input.wdPick[type="checkbox"]') || [])) {
            const day = String(cb.getAttribute('data-day') || '').trim();
            if (cb.checked && day) picked.add(day);
          }
          const monFri = ['Mon','Tue','Wed','Thu','Fri'];
          const monSun = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun'];
          let wd = '';
          if (picked.size === monFri.length && monFri.every(x => picked.has(x))) wd = 'Mon–Fri';
          else if (picked.size === monSun.length) wd = 'Mon–Sun';
          else wd = wdDays.filter(x => picked.has(x)).join(', ');
          if (!wd) wd = 'Mon–Fri';

          const ws = normalizeHm(el('spWorkStart')?.value || '');
          const we = normalizeHm(el('spWorkEnd')?.value || '');
          const bs = normalizeHm(el('spBreakStart')?.value || '');
          const be = normalizeHm(el('spBreakEnd')?.value || '');
          const workHours = (ws && we) ? (ws + '–' + we) : '';
          const breakTime = (bs && be) ? (bs + '–' + be) : '';
          const nt = String(el('spNotes')?.value || '').trim();

          const nextRow = { pattern: p, workingDays: wd, workingHours: workHours, break: breakTime, notes: nt };
          if (isEdit) {
            lastShiftRows = lastShiftRows.map(r => (String(r.pattern ?? '').trim() === pattern ? nextRow : r));
          } else {
            lastShiftRows = lastShiftRows.concat([nextRow]);
          }
          lastShiftRows = (lastShiftRows || []).slice().sort((a, b) => String(a.pattern ?? '').localeCompare(String(b.pattern ?? '')));
          await saveShiftRows(lastShiftRows);
          await refreshShiftPatterns();
          closeModal();
          notify('Success', isEdit ? 'Shift pattern updated.' : 'Shift pattern added.', 'ok');
        },
      });

      for (const t of ['spWorkStart','spWorkEnd','spBreakStart','spBreakEnd']) {
        const inp = el(t);
        if (inp && String(inp.type || '').toLowerCase() === 'time') {
          inp.addEventListener('focus', () => { try { if (inp.showPicker) inp.showPicker(); } catch { } });
        }
      }
    }

    if (shiftAddBtn) shiftAddBtn.addEventListener('click', () => openShiftPatternModal('add', { workingDays: 'Mon–Fri' }));

    const shiftBodyEl = el('shiftBody');
    if (shiftBodyEl) {
      shiftBodyEl.addEventListener('change', async (e) => {
        const t = e && e.target;
        if (!t) return;
        if (t.classList && t.classList.contains('shiftChk')) {
          const id = String(t.getAttribute('data-id') || '').trim();
          if (t.checked) shiftSelectedKeys.add(id);
          else shiftSelectedKeys.delete(id);
          updateShiftDeleteEnabled();
          return;
        }
      });
      shiftBodyEl.addEventListener('click', (e) => {
        const t = e && e.target;
        const target = (t && t.closest) ? t.closest('button.shiftEditRow') : null;
        if (!target || !target.classList || !target.classList.contains('shiftEditRow')) return;
        const id = String(target.getAttribute('data-id') || '').trim();
        if (!id) return;
        const existing = lastShiftRows.find(r => String(r.pattern ?? '').trim() === id);
        if (!existing) return;
        openShiftPatternModal('edit', existing);
      });
    }

    const shiftDeleteBtn = el('shiftDelete');
    if (shiftDeleteBtn) shiftDeleteBtn.addEventListener('click', async () => {
      const ids = Array.from(shiftSelectedKeys);
      if (!ids.length) return;
      if (!confirm('Delete ' + ids.length + ' shift pattern(s)?')) return;
      for (const id of ids) shiftSelectedKeys.delete(id);
      lastShiftRows = (lastShiftRows || []).filter(r => !ids.includes(String(r.pattern ?? '').trim()));
      await saveShiftRows(lastShiftRows);
      await refreshShiftPatterns();
    });

    const quickFixCopyBtn = el('quickFixCopy');
    if (quickFixCopyBtn) quickFixCopyBtn.addEventListener('click', async () => {
      const ok = await copyText(getQuickFixCommand());
      out(ok ? 'Copied.' : 'Copy failed. Select the text manually.');
    });

    const quickFixOpenLocalBtn = el('quickFixOpenLocal');
    if (quickFixOpenLocalBtn) quickFixOpenLocalBtn.addEventListener('click', () => {
      window.open(window.location.origin + '/', '_blank');
    });

    const quickFixOpenLanBtn = el('quickFixOpenLan');
    if (quickFixOpenLanBtn) quickFixOpenLanBtn.addEventListener('click', () => {
      let ip = '';
      try { ip = (localStorage.getItem('wl10dash.bestIpv4') || '').trim(); } catch { }
      if (!ip) { out('LAN IP not known yet. Use: http://<this-pc-ip>:' + (window.location.port || '5099') + '/'); return; }
      window.open('http://' + ip + ':' + (window.location.port || '5099') + '/', '_blank');
    });

    const saveSyncBtn = el('saveSync');
    if (saveSyncBtn) saveSyncBtn.addEventListener('click', async () => {
      const autoSyncEnabled = el('setAuto').value === 'true';
      const raw = (el('schedTimes')?.value || '').trim();
      const scheduleLocalTimes = raw ? raw.split(',').map(s => s.trim()).filter(s => !!s) : [];
      const everyRaw = (el('pollEvery')?.value || '').trim();
      const unit = (el('pollUnit')?.value || 'min').trim();
      const every = Math.max(1, parseInt(everyRaw || '1', 10));
      const pollIntervalSeconds = unit === 'hr' ? (every * 3600) : (every * 60);

      const dashboardRefreshMinutes = Math.max(1, Math.round(pollIntervalSeconds / 60));
      const setDashRefresh = el('setDashRefresh');
      if (setDashRefresh) setDashRefresh.value = String(dashboardRefreshMinutes);
      const body = { autoSyncEnabled, scheduleLocalTimes, pollIntervalSeconds, dashboardRefreshMinutes };
      const r = await postJson('/api/settings', body);
      if (r && r.ok === false) {
        out(r.error || 'Save failed');
        notify('Error', 'Save Sync failed.', 'bad');
        return;
      }
      notify('Success', 'Sync settings saved.', 'ok');
      await refreshStatus();
    });

    const setAutoSel = el('setAuto');
    if (setAutoSel) setAutoSel.addEventListener('change', () => {
      const on = setAutoSel.value === 'true';
      const pollEvery = el('pollEvery');
      const pollUnit = el('pollUnit');
      if (pollEvery) pollEvery.disabled = !on;
      if (pollUnit) pollUnit.disabled = !on;
      const refreshPeriodLabel = el('refreshPeriodLabel');
      const refreshPeriodRow = el('refreshPeriodRow');
      if (refreshPeriodLabel) refreshPeriodLabel.style.display = on ? '' : 'none';
      if (refreshPeriodRow) refreshPeriodRow.style.display = on ? '' : 'none';
    });

    const saveSupabaseBtn = el('saveSupabase');
    if (saveSupabaseBtn) saveSupabaseBtn.addEventListener('click', async () => {
      const supabaseUrl = el('supaUrl').value.trim();
      const supabaseProjectId = el('supaProjectId').value.trim();
      const supabaseAttendanceTable = el('supaTable').value.trim();
      const supabaseSyncEnabled = el('supaSyncMode').value === 'enabled';
      const supabaseAnonKey = el('supaPubKey').value.trim();
      const supabaseServiceRoleKey = el('supaKey').value.trim();
      const supabaseJwt = el('supaJwt').value.trim();
      const body = { supabaseUrl, supabaseAttendanceTable, supabaseSyncEnabled, supabaseServiceRoleKey, supabaseAnonKey, supabaseProjectId, supabaseJwt };
      const r = await postJson('/api/settings', body);
      if (r && r.ok === false) {
        el('supaTestResult').textContent = 'Save failed: ' + String(r.error || 'error');
        notify('Error', 'Save failed (see Activity Log).', 'bad');
        return;
      }

      let envOk = false;
      try {
        const envRes = await postJson('/api/env/write', { supabaseUrl, supabaseAnonKey, supabaseProjectId, supabaseServiceRoleKey, supabaseJwtSecret: supabaseJwt });
        envOk = !!(envRes && envRes.ok);
      } catch { envOk = false; }

      el('supaTestResult').textContent = envOk ? 'Saved (settings + .env.local).' : 'Saved settings. (.env.local write failed)';
      notify(envOk ? 'Success' : 'Warning', envOk ? 'Settings saved.' : 'Settings saved, but .env.local write failed.', envOk ? 'ok' : 'bad');
      await refreshStatus();
      if (supabaseSyncEnabled) triggerAutoSync('supabase-save', true);
    });

    const testSupabaseBtn = el('testSupabase');
    if (testSupabaseBtn) testSupabaseBtn.addEventListener('click', async () => {
      el('supaTestResult').textContent = 'Testing...';
      try {
        const r = await postJson('/api/supabase/test', {});
        if (r.ok) {
          el('supaTestResult').textContent = 'Connection OK (' + (r.rttMs ?? '-') + 'ms)';
          refreshStatus().catch(() => {});
          triggerAutoSync('supabase-test', true);
        }
        else el('supaTestResult').textContent = 'Failed: ' + (r.reason || 'error');
      } catch (e) {
        el('supaTestResult').textContent = 'Failed: ' + String(e);
      }
    });

    async function refreshConvertedList() {
      const body = el('convertedBody');
      if (!body) return;
      const j = await getJson('/api/files/converted');
      body.innerHTML = '';
      if (!j || !Array.isArray(j.items) || j.items.length === 0) {
        const tr = document.createElement('tr');
        tr.innerHTML = '<td colspan="4" class="muted">(none)</td>';
        body.appendChild(tr);
        return;
      }
      for (const it of j.items) {
        const tr = document.createElement('tr');
        tr.innerHTML =
          '<td>' + (it.baseName || '') + '</td>' +
          '<td>' + (it.txtMtime || '') + '</td>' +
          '<td>' + (it.datMtime || '') + '</td>' +
          '<td>' + (it.downloadUrl ? ('<a class=\"btn\" href=\"' + it.downloadUrl + '\">Download</a>') : '') + '</td>';
        body.appendChild(tr);
      }
    }

    async function refreshPresets() {
      const j = await getJson('/api/presets');
      if (!j) return;

      const devBody = el('presetDeviceBody');
      if (!devBody) return;
      devBody.innerHTML = '';
      const devList = j.device || [];
      if (!devList.length) devBody.innerHTML = '<tr><td colspan="4" class="muted">No successful device connections yet. Use Test TCP or Connect Now.</td></tr>';
      for (const p of devList) {
        const tr = document.createElement('tr');
        const okAt = p.lastOkAtUtc ? new Date(p.lastOkAtUtc).toLocaleString() : '';
        tr.innerHTML =
          '<td>' + (p.deviceIp + ':' + p.devicePort) + '</td>' +
          '<td>' + (p.readerMode || '') + '</td>' +
          '<td>' + okAt + '</td>' +
          '<td><button class="btn" type="button">Use</button></td>';
        tr.querySelector('button').addEventListener('click', async () => {
          const r = await postJson('/api/presets/apply', { kind: 'device', idx: p.idx });
          if (r && r.ok === false) out(r.error || 'Apply failed');
          await refreshStatus();
          await refreshPresets();
        });
        devBody.appendChild(tr);
      }

      const pollBody = el('presetPollingBody');
      if (!pollBody) return;
      pollBody.innerHTML = '';
      const pollList = j.polling || [];
      if (!pollList.length) pollBody.innerHTML = '<tr><td colspan="4" class="muted">No saved polling settings yet. Use Save Sync.</td></tr>';
      for (const p of pollList) {
        const tr = document.createElement('tr');
        const mins = Math.max(1, Math.round((p.pollIntervalSeconds || 600) / 60));
        tr.innerHTML =
          '<td>' + mins + '</td>' +
          '<td>' + (p.autoSyncEnabled ? 'on' : 'off') + '</td>' +
          '<td>' + (p.savedAtUtc ? new Date(p.savedAtUtc).toLocaleString() : '') + '</td>' +
          '<td><button class="btn" type="button">Use</button></td>';
        tr.querySelector('button').addEventListener('click', async () => {
          const r = await postJson('/api/presets/apply', { kind: 'polling', idx: p.idx });
          if (r && r.ok === false) out(r.error || 'Apply failed');
          await refreshStatus();
          await refreshPresets();
        });
        pollBody.appendChild(tr);
      }
    }

    async function tick() {
      const ok = await refreshStatus();
      if (!ok)
      {
        nextPollMs = Math.min(30000, nextPollMs * 2);
        return;
      }
      nextPollMs = uiPollMs;
      if (activeTab === 'summary') {
        await refreshAnalytics();
        if (activeSummarySubTab === 'attendance') await refreshAttendance();
      }
      if (activeTab === 'rawData') {
        if (activeRawSubTab === 'db') await refreshDbRecords();
        else await refreshDeviceRecords();
      }
      if (activeTab === 'staffRecords') {
        await refreshStaffRecords();
        if (activeStaffSubTab === 'shift') await refreshShiftPatterns();
      }
      if (activeTab === 'attendanceSpreadsheet') {
        await refreshAttendanceSpreadsheet();
      }
      if (activeTab === 'settings') {
        if (activeSettingsSubTab === 'logs') await refreshLogs();
      }
    }

    async function loop() {
      try {
        await tick();
      } catch {
        nextPollMs = Math.min(30000, nextPollMs * 2);
      }
      setTimeout(loop, nextPollMs);
    }

    loop();
  </script>
</body>
</html>
""";
  }
}
