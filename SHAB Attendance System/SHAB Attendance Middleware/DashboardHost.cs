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

  private static async Task<(bool ok, List<(string Id, string Name, string Dept, string Status, string ShiftPattern)> rows, string? error)> TryLoadStaffFromSupabase(AppConfig cfg, CancellationToken ct)
  {
    if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
    {
      return (false, new List<(string, string, string, string, string)>(), "Supabase not configured");
    }

    static async Task<(bool ok, List<(string Id, string Name, string Dept, string Status, string ShiftPattern)> rows, string? error)> FetchAsync(AppConfig cfg, bool includeShiftPattern, CancellationToken ct)
    {
      var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
      var cols = includeShiftPattern
        ? "id,full_name,department,status,shift_pattern"
        : "id,full_name,department,status";
      var url = $"{baseUrl}/rest/v1/staff?select={Uri.EscapeDataString(cols)}&order=id.asc&limit=5000";
      using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
      using var req = new HttpRequestMessage(HttpMethod.Get, url);
      req.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
      using var resp = await http.SendAsync(req, ct);
      var body = await resp.Content.ReadAsStringAsync(ct);
      if (!resp.IsSuccessStatusCode)
      {
        return (false, new List<(string, string, string, string, string)>(), body.Length > 350 ? body[..350] : body);
      }

      using var doc = JsonDocument.Parse(body);
      if (doc.RootElement.ValueKind != JsonValueKind.Array)
      {
        return (false, new List<(string, string, string, string, string)>(), "Unexpected response shape");
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
      return (true, list, null);
    }

    var r1 = await FetchAsync(cfg, includeShiftPattern: true, ct);
    if (!r1.ok && r1.error is not null && r1.error.Contains("shift_pattern", StringComparison.OrdinalIgnoreCase))
    {
      var r2 = await FetchAsync(cfg, includeShiftPattern: false, ct);
      if (!r2.ok) return (false, r2.rows, r2.error);
      return (true, r2.rows.Select(x => (x.Id, x.Name, x.Dept, x.Status, "")).ToList(), null);
    }
    return r1;
  }

  private static async Task<(bool ok, ShiftPatternRow[] rows, string? error)> TryLoadShiftPatternsFromSupabase(AppConfig cfg, CancellationToken ct)
  {
    if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
    {
      return (false, Array.Empty<ShiftPatternRow>(), "Supabase not configured");
    }
    var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
    var url = $"{baseUrl}/rest/v1/shift_patterns?select=pattern,working_days,working_hours,break_time,notes&order=pattern.asc&limit=500";
    using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
    req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
    using var resp = await http.SendAsync(req, ct);
    var body = await resp.Content.ReadAsStringAsync(ct);
    if (!resp.IsSuccessStatusCode)
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
    var ringOut = new RingBufferTextWriter(originalOut, maxLines: 500);
    var ringErr = new RingBufferTextWriter(originalErr, maxLines: 500);
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
        if (!string.IsNullOrWhiteSpace(s.SupabaseUrl)) startingConfig = startingConfig with { SupabaseUrl = s.SupabaseUrl };
        if (!string.IsNullOrWhiteSpace(s.SupabaseAttendanceTable)) startingConfig = startingConfig with { SupabaseAttendanceTable = s.SupabaseAttendanceTable };
        startingConfig = startingConfig with { SupabaseSyncEnabled = s.SupabaseSyncEnabled };
        if (!string.IsNullOrWhiteSpace(key)) startingConfig = startingConfig with { SupabaseServiceRoleKey = key };
        dashboardSupabaseAnonKey = (s.SupabaseAnonKey ?? string.Empty).Trim();
        dashboardSupabaseProjectId = (s.SupabaseProjectId ?? string.Empty).Trim();
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
        startingAutoSyncEnabled = poll.AutoSyncEnabled;
      }

      syncScheduleLocalTimes = (state.SyncScheduleLocalTimes ?? Array.Empty<string>())
        .Select(s => (s ?? string.Empty).Trim())
        .Where(s => s.Length > 0)
        .Distinct(StringComparer.Ordinal)
        .ToArray();
    }
    catch { }

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
    if (envSupaUrl.Length > 0) startingConfig = startingConfig with { SupabaseUrl = envSupaUrl };
    else if (envViteSupaUrl.Length > 0) startingConfig = startingConfig with { SupabaseUrl = envViteSupaUrl };
    if (envSupaService.Length > 0) startingConfig = startingConfig with { SupabaseServiceRoleKey = envSupaService };
    if (envSupaAnon.Length > 0) dashboardSupabaseAnonKey = envSupaAnon;
    else if (envViteSupaAnon.Length > 0) dashboardSupabaseAnonKey = envViteSupaAnon;
    if (envSupaProjectId.Length > 0) dashboardSupabaseProjectId = envSupaProjectId;
    else if (envViteSupaProjectId.Length > 0) dashboardSupabaseProjectId = envViteSupaProjectId;
    if (envSupaJwt.Length > 0) dashboardSupabaseJwtSecret = envSupaJwt;
    if (string.IsNullOrWhiteSpace(dashboardSupabaseProjectId)) dashboardSupabaseProjectId = InferSupabaseProjectId(startingConfig.SupabaseUrl);
    if (string.IsNullOrWhiteSpace(startingConfig.SupabaseUrl)) startingConfig = startingConfig with { SupabaseUrl = DefaultSupabaseUrl };
    if (string.IsNullOrWhiteSpace(dashboardSupabaseProjectId)) dashboardSupabaseProjectId = DefaultSupabaseProjectId;
    if (string.IsNullOrWhiteSpace(dashboardSupabaseAnonKey)) dashboardSupabaseAnonKey = DefaultSupabasePublishableKey;

    lock (stateGate)
    {
      currentConfig = startingConfig;
      pollIntervalSeconds = Math.Clamp((startingPollIntervalSeconds <= 0 ? (startingConfig.PollIntervalSeconds <= 0 ? 900 : startingConfig.PollIntervalSeconds) : startingPollIntervalSeconds), 60, 3600);
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
      ctx.Response.Redirect("/?tab=settings&subtab=settings:connection");
    }).AllowAnonymous();

    app.MapPost("/logout", async (HttpContext ctx) =>
    {
      await ctx.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
      ctx.Response.Redirect("/login");
    });

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
          serviceRoleKey = isSuperadmin ? cfg.SupabaseServiceRoleKey : string.Empty,
          anonKey = anon,
          jwtSecret = isSuperadmin ? jwt : string.Empty,
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
          var url = (supaUrlEl.GetString() ?? "").Trim();
          currentConfig = currentConfig with { SupabaseUrl = url };
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
          currentConfig = currentConfig with { SupabaseServiceRoleKey = k };
        }
        else if (root.TryGetProperty("supabaseApiKey", out var supaKeyEl))
        {
          var k = (supaKeyEl.GetString() ?? "").Trim();
          currentConfig = currentConfig with { SupabaseServiceRoleKey = k };
        }

        if (root.TryGetProperty("supabaseAnonKey", out var supaAnonEl))
        {
          dashboardSupabaseAnonKey = (supaAnonEl.GetString() ?? string.Empty).Trim();
        }

        if (root.TryGetProperty("supabaseProjectId", out var supaPidEl))
        {
          dashboardSupabaseProjectId = (supaPidEl.GetString() ?? string.Empty).Trim();
        }

        if (root.TryGetProperty("supabaseJwt", out var supaJwtEl))
        {
          dashboardSupabaseJwtSecret = (supaJwtEl.GetString() ?? string.Empty).Trim();
        }

        if (saveSupabaseSettings)
        {
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
      var (ok, rows, error) = await TryFetchLatestAttendanceEventsVerbose(cfg, anon, limit: 50, null, null, ctx.RequestAborted);
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

      var (ok, rows, error) = await TryFetchLatestAttendanceEventsVerbose(cfg, anon, limit: limit, from, to, ctx.RequestAborted);
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

        await UpsertAttlogRowsToSupabase(cfg, distinctRows, ctx.RequestAborted);
        runtime.LastSupabaseSyncResult = "ok";
        runtime.LastSupabaseUpsertedCount = keys.Count;
        return Results.Json(new { ok = true, upserted = rows.Count, distinct = keys.Count, rangeFrom = minDt, rangeTo = maxDt }, JsonOptions);
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
      var root = Path.Combine(Directory.GetCurrentDirectory(), "Reference");
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
      var root = Path.Combine(Directory.GetCurrentDirectory(), "Reference");
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
      var outDir = Path.Combine(Directory.GetCurrentDirectory(), "SHAB Dashboard");
      var outPath = Path.Combine(outDir, ".env.local");
      Directory.CreateDirectory(outDir);
      var text = $"VITE_SUPABASE_URL={url}\nVITE_SUPABASE_ANON_KEY={anon}\nVITE_SUPABASE_PROJECT_ID={pid}\n";
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

      var dateLocal = DateOnly.FromDateTime(DateTime.Now);
      if (!string.IsNullOrWhiteSpace(dateRaw))
      {
        _ = DateOnly.TryParseExact(dateRaw, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out dateLocal);
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
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        var supa = await TryLoadStaffFromSupabase(cfg, ctx.RequestAborted);
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
          new ShiftPatternRow("Shift 1", "Mon–Sat", "08:00–16:00", "12:00–13:00", "Default"),
          new ShiftPatternRow("Shift 2", "Mon–Sat", "16:00–00:00", "20:00–20:30", "Default"),
          new ShiftPatternRow("Shift 3", "Mon–Sat", "00:00–08:00", "04:00–04:30", "Default"),
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
        var select = Uri.EscapeDataString("staff_id,datetime");
        var startDt = Uri.EscapeDataString(rangeStart.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + " 00:00:00");
        var endDt = Uri.EscapeDataString(dateLocal.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + " 23:59:59");
        var url =
          $"{baseUrl}/rest/v1/{cfg.SupabaseAttendanceTable}?select={select}" +
          $"&datetime=gte.{startDt}" +
          $"&datetime=lte.{endDt}" +
          $"&order=datetime.desc&limit=50000";

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
          var dtRaw = el.TryGetProperty("datetime", out var dtEl) && dtEl.ValueKind == JsonValueKind.String ? dtEl.GetString() : null;
          if (string.IsNullOrWhiteSpace(staffId) || string.IsNullOrWhiteSpace(dtRaw)) continue;
          if (!DateTime.TryParseExact(dtRaw.Trim(), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt)) continue;
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
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        var supa = await TryLoadStaffFromSupabase(cfg, ctx.RequestAborted);
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
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        var supaShifts = await TryLoadShiftPatternsFromSupabase(cfg, ctx.RequestAborted);
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
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        var supa = await TryLoadStaffFromSupabase(cfg, ctx.RequestAborted);
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
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        var supaShifts = await TryLoadShiftPatternsFromSupabase(cfg, ctx.RequestAborted);
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

      var staffName = staffId;
      var staffPattern = "Normal";
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        var supa = await TryLoadStaffFromSupabase(cfg, ctx.RequestAborted);
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
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        var supaShifts = await TryLoadShiftPatternsFromSupabase(cfg, ctx.RequestAborted);
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
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        var supa = await TryLoadStaffFromSupabase(cfg, ctx.RequestAborted);
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
      if (!string.IsNullOrWhiteSpace(cfg.SupabaseUrl) && !string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        var supaShifts = await TryLoadShiftPatternsFromSupabase(cfg, ctx.RequestAborted);
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
          pollIntervalSeconds = Math.Clamp(p.PollIntervalSeconds <= 0 ? 600 : p.PollIntervalSeconds, 60, 3600);
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
      var path = TryResolveAttlogExportPath();
      if (string.IsNullOrWhiteSpace(path)) return Results.Json(new { rows = Array.Empty<object>(), error = "Could not resolve 1_attlog.dat path" }, JsonOptions);
      if (!File.Exists(path)) return Results.Json(new { rows = Array.Empty<object>(), error = $"File not found: {path}" }, JsonOptions);

      var rows = new List<object>(capacity: 200);
      foreach (var raw in File.ReadLines(path))
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
          staff_id = staffId,
          datetime = dtRaw,
          verified = parts[3].Trim(),
          status = parts[4].Trim(),
          workcode = parts[5].Trim(),
          reserved = parts[6].Trim(),
        });
      }

      return Results.Json(new { rows = rows.ToArray() }, JsonOptions);
    }).RequireAuthorization();

    app.MapPost("/api/records/upload", async (HttpContext ctx) =>
    {
      if (!ctx.Request.HasFormContentType) return Results.Json(new { ok = false, reason = "bad content type" }, JsonOptions);
      var form = await ctx.Request.ReadFormAsync(ctx.RequestAborted);
      var file = form.Files.GetFile("file");
      if (file is null || file.Length == 0) return Results.Json(new { ok = false, reason = "no file" }, JsonOptions);

      var rows = new List<object>(capacity: 256);
      using (var stream = file.OpenReadStream())
      using (var reader = new StreamReader(stream))
      {
        while (true)
        {
          var line = await reader.ReadLineAsync();
          if (line is null) break;
          var s = line.Trim();
          if (s.Length == 0) continue;
          var parts = s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
          if (parts.Length < 7) continue;
          var staffId = parts[0].Trim();
          if (staffId.Length == 0) continue;
          var dtRaw = parts[1].Trim() + " " + parts[2].Trim();
          _ = DateTime.TryParseExact(dtRaw, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out _);
          rows.Add(new
          {
            staff_id = staffId,
            datetime = dtRaw,
            verified = parts[3].Trim(),
            status = parts[4].Trim(),
            workcode = parts[5].Trim(),
            reserved = parts[6].Trim(),
          });
        }
      }

      return Results.Json(new { ok = true, rows = rows.ToArray() }, JsonOptions);
    }).RequireAuthorization();

    app.MapGet("/api/staff/file", (HttpContext ctx) =>
    {
      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;
      if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
      {
        return Results.Json(new { rows = Array.Empty<object>(), error = "Supabase not configured. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in Database Sync settings." }, JsonOptions);
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

      static async Task<(bool ok, object[] rows, string? error, bool shiftPatternSupported)> TryFetchAsync(AppConfig cfg, bool includeShiftPattern, CancellationToken ct)
      {
        var baseUrl = cfg.SupabaseUrl.TrimEnd('/');
        var cols = includeShiftPattern
          ? "id,full_name,role,department,status,date_joined,shift_pattern"
          : "id,full_name,role,department,status,date_joined";
        var url = $"{baseUrl}/rest/v1/staff?select={Uri.EscapeDataString(cols)}&order=id.asc&limit=5000";
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
        req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
        using var resp = await http.SendAsync(req, ct);
        var body = await resp.Content.ReadAsStringAsync(ct);
        if (!resp.IsSuccessStatusCode)
        {
          return (false, Array.Empty<object>(), body.Length > 350 ? body[..350] : body, includeShiftPattern);
        }

        using var doc = JsonDocument.Parse(body);
        if (doc.RootElement.ValueKind != JsonValueKind.Array) return (false, Array.Empty<object>(), "Unexpected response shape", includeShiftPattern);
        var list = new List<object>(capacity: 256);
        foreach (var el in doc.RootElement.EnumerateArray())
        {
          var id = el.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.String ? (idEl.GetString() ?? "") : "";
          if (id.Length == 0) continue;
          var fullName = el.TryGetProperty("full_name", out var nEl) && nEl.ValueKind == JsonValueKind.String ? (nEl.GetString() ?? "") : "";
          var role = el.TryGetProperty("role", out var rEl) && rEl.ValueKind == JsonValueKind.String ? (rEl.GetString() ?? "") : "";
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
        return (true, list.ToArray(), null, includeShiftPattern);
      }

      var t = TryFetchAsync(cfg, includeShiftPattern: true, ctx.RequestAborted).GetAwaiter().GetResult();
      if (!t.ok && t.error is not null && t.error.Contains("shift_pattern", StringComparison.OrdinalIgnoreCase))
      {
        t = TryFetchAsync(cfg, includeShiftPattern: false, ctx.RequestAborted).GetAwaiter().GetResult();
      }
      if (!t.ok) return Results.Json(new { rows = Array.Empty<object>(), error = t.error ?? "Failed to load staff" }, JsonOptions);
      return Results.Json(new { rows = t.rows }, JsonOptions);
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

      static string GetProp(JsonElement el, string name)
      {
        if (el.ValueKind != JsonValueKind.Object) return string.Empty;
        if (!el.TryGetProperty(name, out var v)) return string.Empty;
        return (v.GetString() ?? string.Empty).Trim();
      }

      using var doc = await JsonDocument.ParseAsync(ctx.Request.Body, cancellationToken: ctx.RequestAborted);
      var userId = GetProp(doc.RootElement, "user_id");
      var firstName = GetProp(doc.RootElement, "first_name");
      if (userId.Length == 0) return Results.Json(new { ok = false, error = "user_id is required" }, JsonOptions);
      if (!userId.All(char.IsDigit)) return Results.Json(new { ok = false, error = "user_id must be numeric for WL10 enrollment number" }, JsonOptions);
      if (firstName.Length == 0) firstName = userId;

      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;

      try
      {
        var type = Type.GetTypeFromProgID("zkemkeeper.CZKEM", throwOnError: false);
        if (type is null)
        {
          try { type = Type.GetTypeFromCLSID(new Guid("00853A19-BD51-419B-9269-2DABE57EB61F"), throwOnError: false); } catch { type = null; }
        }
        if (type is null) return Results.Json(new { ok = false, error = "ZKTeco SDK (zkemkeeper) is not installed/registered on this PC." }, JsonOptions);

        dynamic? zk = null;
        try { zk = Activator.CreateInstance(type); } catch { zk = null; }
        if (zk is null) return Results.Json(new { ok = false, error = "Failed to initialize zkemkeeper COM object." }, JsonOptions);

        try
        {
          try { _ = zk.SetCommPassword(cfg.CommPassword); } catch { }
          var connected = false;
          try { connected = (bool)zk.Connect_Net(cfg.DeviceIp, cfg.DevicePort); } catch { connected = false; }
          if (!connected) return Results.Json(new { ok = false, error = $"Failed to connect to WL10 at {cfg.DeviceIp}:{cfg.DevicePort}.", device_ip = cfg.DeviceIp, device_port = cfg.DevicePort, machine_number = cfg.MachineNumber }, JsonOptions);

          try { _ = (bool)zk.ReadAllUserID(cfg.MachineNumber); } catch { }

          while (true)
          {
            string enrollNumber;
            string name;
            string password;
            int privilege;
            bool enabled;

            bool ok;
            try { ok = (bool)zk.SSR_GetAllUserInfo(cfg.MachineNumber, out enrollNumber, out name, out password, out privilege, out enabled); }
            catch { break; }
            if (!ok) break;
            if (string.Equals((enrollNumber ?? string.Empty).Trim(), userId, StringComparison.Ordinal))
            {
              return Results.Json(new { ok = true, already_exists = true }, JsonOptions);
            }
          }

          try { _ = zk.EnableDevice(cfg.MachineNumber, false); } catch { }

          var created = false;
          try { created = (bool)zk.SSR_SetUserInfo(cfg.MachineNumber, userId, firstName, "", 0, true); } catch { created = false; }
          if (!created)
          {
            try { created = (bool)zk.SetUserInfo(cfg.MachineNumber, int.Parse(userId, CultureInfo.InvariantCulture), firstName, "", 0, true); } catch { created = false; }
          }
          if (!created)
          {
            var lastErr = 0;
            try { _ = (bool)zk.GetLastError(out lastErr); } catch { lastErr = 0; }
            try { _ = zk.EnableDevice(cfg.MachineNumber, true); } catch { }
            return Results.Json(new { ok = false, error = "Device rejected user create request.", last_error = lastErr, device_ip = cfg.DeviceIp, device_port = cfg.DevicePort, machine_number = cfg.MachineNumber }, JsonOptions);
          }

          try { _ = (bool)zk.RefreshData(cfg.MachineNumber); } catch { }
          try { _ = zk.EnableDevice(cfg.MachineNumber, true); } catch { }

          var verified = false;
          try
          {
            string vName;
            string vPwd;
            int vPriv;
            bool vEnabled;
            verified = (bool)zk.SSR_GetUserInfo(cfg.MachineNumber, userId, out vName, out vPwd, out vPriv, out vEnabled);
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
              string enrollNumber;
              string name;
              string password;
              int privilege;
              bool enabled;

              bool ok;
              try { ok = (bool)zk.SSR_GetAllUserInfo(cfg.MachineNumber, out enrollNumber, out name, out password, out privilege, out enabled); }
              catch { break; }
              if (!ok) break;
              if (string.Equals((enrollNumber ?? string.Empty).Trim(), userId, StringComparison.Ordinal))
              {
                verified = true;
                break;
              }
            }
          }

          return Results.Json(new { ok = true, created = true, verified, device_ip = cfg.DeviceIp, device_port = cfg.DevicePort, machine_number = cfg.MachineNumber }, JsonOptions);
        }
        finally
        {
          try { zk.Disconnect(); } catch { }
        }
      }
      catch (Exception ex)
      {
        return Results.Json(new { ok = false, error = ex.Message }, JsonOptions);
      }
    }).RequireAuthorization();

    app.MapGet("/api/shifts", (HttpContext ctx) =>
    {
      AppConfig cfg;
      lock (stateGate) cfg = currentConfig;

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

      if (string.IsNullOrWhiteSpace(cfg.SupabaseUrl) || string.IsNullOrWhiteSpace(cfg.SupabaseServiceRoleKey))
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
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(15) };
        using var req = new HttpRequestMessage(HttpMethod.Get, url);
        req.Headers.TryAddWithoutValidation("apikey", cfg.SupabaseServiceRoleKey);
        req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {cfg.SupabaseServiceRoleKey}");
        using var resp = http.Send(req);
        var body = resp.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        if (!resp.IsSuccessStatusCode) throw new InvalidOperationException(body);
        using var doc = JsonDocument.Parse(body);
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
          _ = ExecuteSync(verify: false, today: false, supabaseOverride: null, app.Lifetime.ApplicationStopping);
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
    Console.WriteLine("Login: superadmin / abcd1234 (override with WL10_DASHBOARD_USER and WL10_DASHBOARD_PASSWORD)");

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
    var ips = new List<System.Net.IPAddress>();
    var labels = new List<string>();
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
          ips.Add(ip);
          labels.Add($"{ni.Name}:{ip}");
        }
      }
    }
    catch { }

    bool? same24 = null;
    try
    {
      if (System.Net.IPAddress.TryParse(deviceIp, out var dev) && dev.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork && ips.Count > 0)
      {
        var d = dev.GetAddressBytes();
        same24 = ips.Any(p =>
        {
          var b = p.GetAddressBytes();
          return b.Length == 4 && d.Length == 4 && b[0] == d[0] && b[1] == d[1] && b[2] == d[2];
        });
      }
    }
    catch { }

    return new
    {
      ipv4 = labels.Distinct().Take(6).ToArray(),
      sameSubnet24 = same24,
      bestIpv4 = ips.Count > 0 ? ips[0].ToString() : string.Empty,
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
      var url = $"{baseUrl}/rest/v1/{config.SupabaseAttendanceTable}?select=datetime&order=datetime.desc&limit=1";

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
      if (!first.TryGetProperty("datetime", out var dtEl)) return null;
      var s = dtEl.GetString();
      if (string.IsNullOrWhiteSpace(s)) return null;
      if (!DateTime.TryParseExact(s.Trim(), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt)) return null;
      var dto = new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
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
      var select = Uri.EscapeDataString("staff_id,datetime,verified,status,workcode,reserved");
      var url = $"{baseUrl}/rest/v1/{config.SupabaseAttendanceTable}?select={select}&order=datetime.desc&limit={Math.Clamp(limit, 1, 200)}";

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
      var arr = new object[doc.RootElement.GetArrayLength()];
      for (var i = 0; i < doc.RootElement.GetArrayLength(); i++)
      {
        arr[i] = doc.RootElement[i].Clone();
      }
      return arr;
    }
    catch
    {
      return Array.Empty<object>();
    }
  }

  private static async Task<(bool Ok, object[] Rows, string? Error)> TryFetchLatestAttendanceEventsVerbose(AppConfig config, string? anonKey, int limit, string? from, string? to, CancellationToken ct)
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
      var select = Uri.EscapeDataString("staff_id,datetime,verified,status,workcode,reserved");
      var url = $"{baseUrl}/rest/v1/{tableRaw}?select={select}&order=datetime.desc&limit={Math.Clamp(limit, 1, 200)}";
      if (!string.IsNullOrWhiteSpace(from)) url += "&datetime=gte." + Uri.EscapeDataString(from.Trim());
      if (!string.IsNullOrWhiteSpace(to)) url += "&datetime=lte." + Uri.EscapeDataString(to.Trim());

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
        return (false, Array.Empty<object>(), msg);
      }

      var text = await res.Content.ReadAsStringAsync(ct);
      using var doc = JsonDocument.Parse(text);
      if (doc.RootElement.ValueKind != JsonValueKind.Array) return (false, Array.Empty<object>(), "Supabase returned non-array JSON");
      var arr = new object[doc.RootElement.GetArrayLength()];
      for (var i = 0; i < doc.RootElement.GetArrayLength(); i++)
      {
        arr[i] = doc.RootElement[i].Clone();
      }
      return (true, arr, null);
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

      using var http = new HttpClient();
      using var req = new HttpRequestMessage(HttpMethod.Get, url);
      req.Headers.TryAddWithoutValidation("apikey", apiKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
      req.Headers.TryAddWithoutValidation("Accept", "application/json");
      req.Headers.TryAddWithoutValidation("Prefer", "count=exact");
      req.Headers.TryAddWithoutValidation("Range-Unit", "items");
      req.Headers.TryAddWithoutValidation("Range", "0-0");

      using var res = await http.SendAsync(req, ct);
      if (!res.IsSuccessStatusCode)
      {
        var body = await res.Content.ReadAsStringAsync(ct);
        return (false, 0, $"Supabase count failed: {(int)res.StatusCode} {res.ReasonPhrase}. Body={body}");
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

      var text = await res.Content.ReadAsStringAsync(ct);
      using var doc = JsonDocument.Parse(text);
      if (doc.RootElement.ValueKind != JsonValueKind.Array) return (false, 0, "Supabase returned non-array JSON");
      return (true, doc.RootElement.GetArrayLength(), null);
    }
    catch (Exception ex)
    {
      return (false, 0, ex.Message);
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
      var url = $"{baseUrl}/rest/v1/{tableRaw}?select=staff_id&datetime=gte.{Uri.EscapeDataString(from)}&datetime=lte.{Uri.EscapeDataString(to)}";

      using var http = new HttpClient();
      using var req = new HttpRequestMessage(HttpMethod.Get, url);
      req.Headers.TryAddWithoutValidation("apikey", apiKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {apiKey}");
      req.Headers.TryAddWithoutValidation("Accept", "application/json");
      req.Headers.TryAddWithoutValidation("Prefer", "count=exact");
      req.Headers.TryAddWithoutValidation("Range-Unit", "items");
      req.Headers.TryAddWithoutValidation("Range", "0-0");

      using var res = await http.SendAsync(req, ct);
      if (!res.IsSuccessStatusCode)
      {
        var body = await res.Content.ReadAsStringAsync(ct);
        return (false, 0, $"Supabase count failed: {(int)res.StatusCode} {res.ReasonPhrase}. Body={body}");
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

      var text = await res.Content.ReadAsStringAsync(ct);
      using var doc = JsonDocument.Parse(text);
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
      var select = Uri.EscapeDataString("staff_id,datetime");
      var startDt = Uri.EscapeDataString(rangeStart.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + " 00:00:00");
      var endDt = Uri.EscapeDataString(todayLocal + " 23:59:59");
      var url =
        $"{baseUrl}/rest/v1/{config.SupabaseAttendanceTable}?select={select}" +
        $"&datetime=gte.{startDt}" +
        $"&datetime=lte.{endDt}" +
        $"&order=datetime.desc&limit=50000";

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
        var dtRaw = el.TryGetProperty("datetime", out var dtEl) && dtEl.ValueKind == JsonValueKind.String ? dtEl.GetString() : null;
        if (string.IsNullOrWhiteSpace(dtRaw)) continue;
        if (!DateTime.TryParseExact(dtRaw.Trim(), "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt)) continue;
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
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Nunito:wght@400;600;700;800&display=swap" rel="stylesheet">
  <style>
    *{box-sizing:border-box}
    :root{--bg:#fff7ed;--panel:#ffffff;--panel2:#f8fafc;--text:#0f172a;--muted:#64748b;--border:#e5e7eb;--border2:#cbd5e1;--btn:#f1f5f9;--btnH:#e2e8f0;--accent:#2563eb;--accent2:#0ea5e9;--ok:#16a34a;--bad:#dc2626;--fontHead:"Nunito","Segoe UI Rounded","Arial Rounded MT Bold","Trebuchet MS",system-ui,sans-serif}
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;background:var(--bg);color:var(--text);overflow-x:hidden}
    header{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-bottom:1px solid var(--border);background:var(--panel);position:sticky;top:0;z-index:2000}
    h1{font-family:var(--fontHead);font-size:16px;margin:0;font-weight:900}
    .title,.summaryTitle,.summaryWrap .title,.kTitle,.miniKpiTitle,.donutTitle,.sparkTitle,.subTitle,.modalTitle{font-family:var(--fontHead)}
    .btn{padding:8px 10px;border-radius:10px;border:1px solid var(--border2);background:var(--btn);color:var(--text);cursor:pointer;transition:background .12s ease,border-color .12s ease,transform .05s ease,box-shadow .12s ease}
    .btn:hover{background:var(--btnH);border-color:var(--border2);box-shadow:0 0 0 3px rgba(37,99,235,.10)}
    .btn:active{transform:translateY(1px);box-shadow:0 0 0 3px rgba(37,99,235,.14)}
    .btn:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
    .btn:disabled{opacity:.55;cursor:not-allowed;box-shadow:none}
    .btn.primary{background:var(--accent);border-color:var(--accent);color:#fff}
    main{max-width:1200px;margin:0 auto;padding:16px;overflow-x:hidden}
    .tabs{display:flex;gap:8px;flex-wrap:wrap;margin:0 0 14px}
    .tabBtn{padding:9px 12px;border-radius:12px;border:1px solid var(--border);background:var(--panel);color:var(--text);cursor:pointer;font-size:12px;transition:background .12s ease,border-color .12s ease,transform .05s ease,box-shadow .12s ease}
    .tabBtn:hover{background:var(--btn);border-color:var(--border2);box-shadow:0 0 0 3px rgba(37,99,235,.08)}
    .tabBtn:active{transform:translateY(1px)}
    .tabBtn:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
    .tabBtn.active{background:var(--btn);border-color:var(--border2)}
    .tabPanel{display:none}
    .tabPanel.active{display:block}
    .subTabs{display:flex;gap:8px;flex-wrap:wrap;margin:0 0 14px}
    .subTabBtn{padding:9px 12px;border-radius:12px;border:1px solid #e5e7eb;background:#ffffff;color:#0f172a;cursor:pointer;font-size:12px;transition:background .12s ease,border-color .12s ease,transform .05s ease,box-shadow .12s ease}
    .subTabBtn:hover{background:#f1f5f9;border-color:#cbd5e1;box-shadow:0 0 0 3px rgba(37,99,235,.08)}
    .subTabBtn:active{transform:translateY(1px)}
    .subTabBtn:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
    .subTabBtn.active{background:#f1f5f9;border-color:#cbd5e1}
    .subTabPanel{display:none}
    .subTabPanel.active{display:block}
    .grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px;overflow-x:hidden}
    .card{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:14px;min-width:0}
    .title{font-size:12px;color:var(--muted);margin:0 0 10px;min-width:0}
    .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;min-width:0}
    .kv{font-size:12px;color:var(--muted);min-width:0}
    .val{color:var(--text)}
    .pill{display:inline-flex;align-items:center;gap:6px;padding:4px 8px;border-radius:999px;border:1px solid var(--border2);background:var(--panel2);font-size:12px}
    .ok{border-color:rgba(22,163,74,.45);background:rgba(22,163,74,.10)}
    .bad{border-color:rgba(220,38,38,.45);background:rgba(220,38,38,.10)}
    input,select{padding:8px 10px;border-radius:10px;border:1px solid var(--border2);background:var(--panel);color:var(--text)}
    pre{margin:0;max-height:60vh;overflow-y:auto;overflow-x:hidden;background:var(--panel2);border:1px solid var(--border);border-radius:10px;padding:10px;font-size:12px;line-height:1.35;white-space:pre-wrap;overflow-wrap:anywhere;word-break:break-word}
    table{width:100%;border-collapse:collapse;table-layout:fixed}
    th,td{border-bottom:1px solid var(--border);padding:8px 6px;font-size:12px;text-align:left;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    th{color:var(--muted);font-weight:600}
    td .btn{display:inline-block;margin:4px 0}
    .muted{color:var(--muted)}
    .formGrid{display:grid;grid-template-columns:160px minmax(0,1fr);gap:10px 12px;align-items:center}
    label{font-size:12px;color:var(--muted)}
    .hint{font-size:12px;color:var(--muted)}
    .kpis{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
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
    .summaryWrap .kpi::after{content:'';position:absolute;inset:-2px -2px auto auto;width:120px;height:120px;background:radial-gradient(circle at 40% 40%,rgba(37,99,235,.16),transparent 60%);pointer-events:none}
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
    .barsGrid .bar2.device{background:linear-gradient(180deg,#2563eb,#1d4ed8)}
    .barsGrid .bar2.db{background:linear-gradient(180deg,#0ea5e9,#0284c7)}
    .xAxis{position:relative;display:grid;grid-template-columns:repeat(24,minmax(0,1fr));gap:4px;margin-top:6px;font-size:9px;color:#64748b}
    .xAxis span{text-align:center;opacity:.85}
    .legend{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:10px}
    .legItem{display:inline-flex;align-items:center;gap:6px;font-size:12px;color:#64748b}
    .dot{width:10px;height:10px;border-radius:999px}
    .dot.device{background:#2563eb}
    .dot.db{background:#0ea5e9}
    .groupGrid{position:relative;display:grid;grid-template-columns:repeat(7,minmax(0,1fr));gap:10px;align-items:end;height:170px}
    .gCol{position:relative;display:flex;flex-direction:column;align-items:stretch;gap:6px;min-width:0}
    .gBars{display:flex;gap:4px;align-items:flex-end;justify-content:center;height:140px}
    .gBar{width:12px;border-radius:6px 6px 0 0;min-height:2px}
    .gBar.device{background:linear-gradient(180deg,#2563eb,#1d4ed8)}
    .gBar.db{background:linear-gradient(180deg,#0ea5e9,#0284c7)}
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
    .ring{position:relative;width:38px;height:38px;border-radius:999px;display:grid;place-items:center;background:conic-gradient(#2563eb var(--pct), #e2e8f0 0)}
    .ring::before{content:'';width:30px;height:30px;border-radius:999px;background:#fff}
    .ringText{position:absolute;font-size:11px;font-weight:800;color:#0f172a}
    .loadText{font-size:12px;color:#64748b}
    .loadText strong{color:#0f172a}
    .modalBack{position:fixed;inset:0;background:rgba(15,23,42,.35);display:none;align-items:flex-start;justify-content:center;padding:16px;z-index:5000;overflow:auto}
    .modalCard{width:min(720px,100%);max-height:calc(100vh - 32px);background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:14px;display:flex;flex-direction:column;overflow:hidden}
    .modalHead{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px}
    .modalTitle{font-size:13px;color:var(--text);font-weight:700}
    .modalBody{flex:1;min-height:0;overflow:auto}
    .modalGrid{display:grid;grid-template-columns:160px minmax(0,1fr);gap:10px 12px;align-items:center}
    .modalGrid label{color:var(--muted)}
    .modalTable{width:100%;border-collapse:collapse;font-size:12px}
    .modalTable th,.modalTable td{border:1px solid var(--border);padding:8px 10px;vertical-align:top}
    .modalTable th{background:var(--panel2);text-align:left;color:var(--muted);font-weight:800;width:220px}
    .modalPunchTable{width:100%;border-collapse:collapse;font-size:12px;margin-top:10px}
    .modalPunchTable th,.modalPunchTable td{border:1px solid var(--border);padding:8px 10px;vertical-align:top}
    .modalPunchTable th{background:var(--panel2);text-align:left;color:var(--muted);font-weight:800}
    .modalFoot{display:flex;gap:10px;justify-content:flex-end;margin-top:12px}
    .iconBtn{padding:6px 8px;border-radius:10px;border:1px solid var(--border2);background:var(--btn);color:var(--text);cursor:pointer}
    .iconBtn:hover{background:var(--btnH);border-color:var(--border2)}
    .dlIconBtn{display:inline-flex;align-items:center;justify-content:center;width:30px;height:30px;padding:0;border-radius:10px;border:1px solid var(--border2);background:var(--btn);color:var(--text);cursor:pointer}
    .dlIconBtn:hover{background:var(--btnH);border-color:var(--border2)}
    .dlIconBtn svg{width:16px;height:16px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round}
    .analysisGrid{display:grid;grid-template-columns:repeat(12,minmax(0,1fr));gap:14px}
    .analysisCard{grid-column:1/-1}
    .analysisFilters{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:10px}
    .anaTopSplit{grid-column:1/-1;display:grid;grid-template-columns:2fr 1fr;gap:14px;align-items:stretch}
    @media (max-width: 900px){.anaTopSplit{grid-template-columns:1fr}}
    .anaLeftStack{display:grid;grid-template-rows:1fr 1fr;gap:14px;min-width:0;height:100%}
    .anaLeftStack>.card{height:100%}
    .anaSysCard{height:100%;display:flex;flex-direction:column}
    .anaSysCard .miniKpisV{flex:0 0 auto}
    .anaSysCard .anaAlertsWrap{margin-top:auto}
    .miniKpis{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}
    @media (max-width: 900px){.miniKpis{grid-template-columns:repeat(2,minmax(0,1fr))}}
    @media (max-width: 520px){.miniKpis{grid-template-columns:1fr}}
    .miniKpisV{display:flex;flex-direction:column;gap:12px}
    #subtab-summary-attendance .card{position:relative;border-radius:18px;border-color:rgba(148,163,184,.55);box-shadow:0 14px 40px rgba(2,6,23,.08);background:linear-gradient(180deg,#ffffff,#f8fafc)}
    #subtab-summary-attendance .card::before{content:'';position:absolute;left:0;right:0;top:0;height:4px;background:linear-gradient(90deg,rgba(37,99,235,.92),rgba(14,165,233,.86));opacity:.9}
    #subtab-summary-attendance .title{color:#0f172a}
    #subtab-summary-attendance .title::after{content:'';display:block;height:2px;width:52px;margin-top:8px;border-radius:999px;background:linear-gradient(90deg,rgba(37,99,235,.92),rgba(14,165,233,.86))}

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
    .gauge{position:relative;width:180px;height:108px;overflow:hidden}
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
  </style>
</head>
<body>
  <header>
    <h1>SHAB Attendance Dashboard</h1>
    <div class="row">
      <form method="post" action="/logout"><button class="btn" type="submit">Logout</button></form>
    </div>
  </header>
  <main>
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
                <input id="anaDate" type="date" />
                <label for="anaDept">Department</label>
                <select id="anaDept"><option value="All">All</option></select>
                <button class="btn" id="anaRefresh" type="button">Refresh</button>
              </div>
              <div class="muted" id="anaNote" style="display:none"></div>
              <div class="miniKpis">
                <div class="miniKpi">
                  <div class="miniKpiTitle">Employees</div>
                  <div class="miniKpiVal" id="anaTotal">-</div>
                  <div class="miniKpiMeta" id="anaScope">-</div>
                </div>
                <div class="miniKpi">
                  <div class="miniKpiTitle">Present</div>
                  <div class="miniKpiVal" id="anaPresent">-</div>
                  <div class="miniKpiMeta" id="anaAttendancePct">-</div>
                </div>
                <div class="miniKpi">
                  <div class="miniKpiTitle">Absent</div>
                  <div class="miniKpiVal" id="anaAbsent">-</div>
                  <div class="miniKpiMeta" id="anaAbsenteePct">-</div>
                </div>
                <div class="miniKpi">
                  <div class="miniKpiTitle">Attendance %</div>
                  <div class="miniKpiVal" id="anaAttRateVal">-</div>
                  <div class="miniKpiMeta">Rate</div>
                </div>
                <div class="miniKpi">
                  <div class="miniKpiTitle">Absenteeism %</div>
                  <div class="miniKpiVal" id="anaAbsRateVal">-</div>
                  <div class="miniKpiMeta">Rate</div>
                </div>
                <div class="miniKpi">
                  <div class="miniKpiTitle">Late Comers</div>
                  <div class="miniKpiVal" id="anaLate">-</div>
                  <div class="miniKpiMeta">After 09:15</div>
                </div>
                <div class="miniKpi">
                  <div class="miniKpiTitle">Avg Work Hours</div>
                  <div class="miniKpiVal" id="anaAvgWork">-</div>
                  <div class="miniKpiMeta">Per present employee</div>
                </div>
                <div class="miniKpi">
                  <div class="miniKpiTitle">Break vs OT</div>
                  <div class="miniKpiVal" id="anaBreakVsOt">-</div>
                  <div class="miniKpiMeta"><span id="anaAvgBreak">-</span> break • <span id="anaAvgOt">-</span> OT</div>
                </div>
              </div>
            </div>

            <div class="anaTopSplit">
              <div class="anaLeftStack">
                <div class="card">
                  <div class="title">Top Employees</div>
                  <div class="hint" style="margin-bottom:8px">Top 5 employees by attendance (last 30 days)</div>
                  <div id="anaTopEmployees" class="hBars"></div>
                </div>

                <div class="card">
                  <div class="title">Last 7 Days (Present)</div>
                  <div class="chartHost" id="anaAttendDays"></div>
                </div>
              </div>

              <div class="card anaSysCard">
                <div class="title">System &amp; Alerts</div>
                <div class="miniKpisV">
                  <div class="miniKpi">
                    <div class="miniKpiTitle">Database Punches (Today)</div>
                    <div class="miniKpiVal" id="anaIntDbToday">-</div>
                    <div class="miniKpiMeta">From Supabase</div>
                  </div>
                  <div class="miniKpi">
                    <div class="miniKpiTitle">Missing Out</div>
                    <div class="miniKpiVal" id="anaIntMissingOut">-</div>
                    <div class="miniKpiMeta">Single punch day</div>
                  </div>
                  <div class="miniKpi">
                    <div class="miniKpiTitle">Duplicate Punches</div>
                    <div class="miniKpiVal" id="anaIntDuplicates">-</div>
                    <div class="miniKpiMeta">Within 3 min</div>
                  </div>
                  <div class="miniKpi">
                    <div class="miniKpiTitle">Last Sync</div>
                    <div class="miniKpiVal" id="anaIntLastSync">-</div>
                    <div class="miniKpiMeta">Middleware</div>
                  </div>
                </div>
                <div class="anaAlertsWrap">
                  <div class="hint" style="margin:8px 0 6px">Alerts</div>
                  <div id="anaAlerts" class="muted"></div>
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
            <div class="title">Daily</div>
            <div class="row" style="margin-bottom:8px">
              <label for="sheetDailyDate">Date</label><input id="sheetDailyDate" type="date" />
              <button class="btn primary" id="sheetDailyRefresh" type="button">Refresh</button>
              <button class="btn" id="sheetDailyDownload" type="button" style="display:none">Download CSV</button>
            </div>
            <table>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Date</th>
                  <th>Shift</th>
                  <th>First In</th>
                  <th>Last Out</th>
                  <th>Total Hours</th>
                  <th>OT Hours</th>
                  <th>Status</th>
                  <th>Flagged Punch</th>
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
            <div class="title">Weekly</div>
            <div class="row" style="margin-bottom:8px">
              <label for="sheetWeeklyDate">Week of</label><input id="sheetWeeklyDate" type="date" />
              <button class="btn primary" id="sheetWeeklyRefresh" type="button">Refresh</button>
              <button class="btn" id="sheetWeeklyDownload" type="button" style="display:none">Download CSV</button>
            </div>
            <table>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Week</th>
                  <th>Flagged Punches</th>
                  <th>Total Hours</th>
                  <th>OT Hours</th>
                  <th>Days Present</th>
                  <th>Days Absent</th>
                  <th>Attendance %</th>
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
            <div class="title">Monthly</div>
            <div class="row" style="margin-bottom:8px">
              <label for="sheetMonthlyMonth">Month</label><input id="sheetMonthlyMonth" type="month" />
              <button class="btn primary" id="sheetMonthlyRefresh" type="button">Refresh</button>
              <button class="btn" id="sheetMonthlyDownload" type="button" style="display:none">Download CSV</button>
              <button class="btn" id="sheetMonthlyBulkDownload" type="button" style="display:none">Download Selected Reports</button>
            </div>
            <table>
              <thead>
                <tr>
                  <th style="width:44px"><input id="sheetMonthlySelectAll" type="checkbox" style="display:none" /></th>
                  <th>Name</th>
                  <th>Month</th>
                  <th>Flagged Punches</th>
                  <th>Total Hours</th>
                  <th>OT Hours</th>
                  <th>Days Present</th>
                  <th>Days Absent</th>
                  <th>Attendance %</th>
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
      <div class="subTabs" role="tablist" aria-label="Raw data sub-tabs">
        <button class="subTabBtn active" type="button" data-subtab-group="raw" data-subtab="device">Device Records</button>
        <button class="subTabBtn" type="button" data-subtab-group="raw" data-subtab="db">Database Records</button>
      </div>

      <div class="subTabPanel active" data-subtab-group="raw" id="subtab-raw-device">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="title">Device Records</div>
            <div class="row" style="margin-bottom:8px">
              <input id="devFilterStaff" placeholder="Filter staff id/name" style="min-width:200px" />
              <label for="devFilterFrom">From</label><input id="devFilterFrom" type="date" />
              <label for="devFilterTo">To</label><input id="devFilterTo" type="date" />
              <input id="devFile" type="file" accept=".dat,.txt" />
              <button class="btn" id="devLoadFile" type="button">Load From File</button>
              <button class="btn" id="devCsv" type="button">Download CSV</button>
              <button class="btn primary" id="devRefresh" type="button">Refresh From Device</button>
            </div>
            <div id="deviceLoadBox" class="loadRow" style="display:none">
              <div class="ring" id="deviceLoadRing" style="--pct:0%"><div class="ringText" id="deviceLoadPct">0%</div></div>
              <div class="loadText" id="deviceLoadText"><strong>Loading</strong></div>
            </div>
            <table>
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
              <tbody id="deviceBody"></tbody>
            </table>
            <div class="hint">Shows records parsed from Reference\1_attlog.dat (same columns and order as the file). Use Refresh From Device to pull the latest logs and rewrite 1_attlog.dat.</div>
          </div>
        </div>
      </div>

      <div class="subTabPanel" data-subtab-group="raw" id="subtab-raw-db">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="title">Database Records (Supabase)</div>
            <div class="row" style="margin-bottom:8px">
              <input id="dbFilterStaff" placeholder="Filter staff id/name" style="min-width:200px" />
              <label for="dbFilterFrom">From</label><input id="dbFilterFrom" type="date" />
              <label for="dbFilterTo">To</label><input id="dbFilterTo" type="date" />
              <button class="btn" id="dbCsv" type="button">Download CSV</button>
              <button class="btn primary" id="dbUpdateSupabase" type="button">Update Supabase</button>
            </div>
            <div id="dbUpdateBox" class="loadRow" style="display:none">
              <div class="ring" id="dbUpdateRing" style="--pct:0%"><div class="ringText" id="dbUpdatePct">0%</div></div>
              <div class="loadText" id="dbUpdateText"><strong>Updating</strong> -</div>
            </div>
            <table>
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
            <div class="title">Employee List</div>
            <div class="row" style="margin-bottom:8px">
              <input id="staffFilter" placeholder="Filter by ID / name / department / role / shift" style="min-width:260px" />
              <input id="staffFile" type="file" accept=".csv" disabled />
              <button class="btn" id="staffImport" type="button" disabled>Import CSV</button>
              <button class="btn" id="staffExport" type="button">Download CSV</button>
              <button class="btn" id="staffAdd" type="button">Add</button>
              <button class="btn" id="staffDelete" type="button" disabled>Delete</button>
            </div>
            <table>
              <thead>
                <tr>
                  <th style="width:44px"></th>
                  <th style="width:90px">User ID</th>
                  <th style="width:180px">First Name</th>
                  <th style="width:140px">Role</th>
                  <th style="width:160px">Department</th>
                  <th style="width:110px">Status</th>
                  <th style="width:170px">Date Joined</th>
                  <th style="width:130px">Shift Pattern</th>
                  <th style="width:70px"></th>
                </tr>
              </thead>
              <tbody id="staffBody"></tbody>
            </table>
            <div class="hint"></div>
          </div>
        </div>
      </div>

      <div class="subTabPanel" data-subtab-group="staff" id="subtab-staff-shift">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="title">Shift Pattern</div>
            <div class="row" style="margin-bottom:8px">
              <input id="shiftFilter" placeholder="Filter shift pattern" style="min-width:260px" />
              <input id="shiftFile" type="file" accept=".csv" disabled />
              <button class="btn" id="shiftImport" type="button" disabled>Import CSV</button>
              <button class="btn" id="shiftExport" type="button" disabled>Download CSV</button>
              <button class="btn" id="shiftAdd" type="button">Add</button>
              <button class="btn" id="shiftDelete" type="button" disabled>Delete</button>
            </div>
            <table>
              <thead>
                <tr>
                  <th style="width:44px"></th>
                  <th style="width:140px">Pattern</th>
                  <th style="width:160px">Working Days</th>
                  <th style="width:160px">Working Hours</th>
                  <th style="width:140px">Break</th>
                  <th>Notes</th>
                  <th style="width:70px"></th>
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
        <button class="subTabBtn active" type="button" data-subtab-group="settings" data-subtab="connection">Connection Summary</button>
        <button class="subTabBtn" type="button" data-subtab-group="settings" data-subtab="deviceSync">Device Sync</button>
        <button class="subTabBtn" type="button" data-subtab-group="settings" data-subtab="databaseSync">Database Sync</button>
        <button class="subTabBtn" type="button" data-subtab-group="settings" data-subtab="logs">Logs</button>
      </div>

      <div class="subTabPanel active" data-subtab-group="settings" id="subtab-settings-connection">
        <div class="grid" style="margin:0">
        <div class="card" style="grid-column:1/-1">
          <div class="kpis">
            <div class="kpi">
              <div class="summaryRow">
                <div class="kTitle">Connection</div>
                <div id="reach" class="pill">Checking...</div>
              </div>
              <div class="kMeta">IP <strong id="ip"></strong></div>
              <div class="kMeta">Port <strong id="port"></strong> • Reader <strong id="readerMode"></strong></div>
            </div>
            <div class="kpi">
              <div class="kTitle">Today Device</div>
              <div class="kVal" id="totalTodayDevice">-</div>
              <div class="kMeta">Unique staff <strong id="uniqueTodayDevice">-</strong></div>
            </div>
            <div class="kpi">
              <div class="kTitle">Today Database</div>
              <div class="kVal" id="totalTodayDb">-</div>
              <div class="kMeta">Unique staff <strong id="uniqueTodayDb">-</strong></div>
            </div>
            <div class="kpi">
              <div class="kTitle">Last Sync</div>
              <div class="kVal" id="lastSyncKpi">-</div>
              <div class="kMeta" id="lastSyncDateKpi">Update on -</div>
              <div class="kMeta">Read <strong id="lastResKpi">-</strong></div>
            </div>
          </div>
        </div>

        <div class="card">
          <div class="title">Device Sync Summary</div>
          <div class="subCards">
            <div class="subCard">
              <div class="subTitle">Status</div>
              <div class="subVal" id="deviceSyncStatusWrap"><span id="deviceSyncStatus" class="pill">Unknown</span></div>
            </div>
            <div class="subCard">
              <div class="subTitle">Table</div>
              <div class="subVal mono" id="deviceSyncTable"></div>
            </div>
            <div class="subCard">
              <div class="subTitle">Auto Sync</div>
              <div class="subVal" id="auto"></div>
            </div>
            <div class="subCard">
              <div class="subTitle">Next Sync</div>
              <div class="subVal" id="intervalWithUnit"></div>
            </div>
            <div class="subCard">
              <div class="subTitle">Last Read</div>
              <div class="subVal" id="lastRes"></div>
            </div>
            <div class="subCard">
              <div class="subTitle">Last Sync</div>
              <div class="subVal mono" id="lastSyncAt"></div>
            </div>
            <div class="subCard">
              <div class="subTitle">Total Records</div>
              <div class="subVal" id="runCount"></div>
            </div>
            <div class="subCard">
              <div class="subTitle">Local Database Date</div>
              <div class="subVal mono" id="localWm"></div>
            </div>
          </div>
          <div style="height:10px"></div>
          <div class="hint">
            Last Sync is when the middleware finished its last sync run. Local Database Date is the timestamp of the newest punch saved locally (used for incremental reads), so it is usually the same as or slightly earlier than Last Sync.
          </div>
          <div style="height:8px"></div>
          <div class="muted" id="lastErrBrief"></div>
          <details id="lastErrBox" style="display:none"><summary>Error details</summary><pre id="lastErr"></pre></details>
        </div>

        <div class="card">
          <div class="title">Supabase Sync Summary</div>
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
              <div class="subVal" id="supaLastRes"></div>
            </div>
            <div class="subCard">
              <div class="subTitle">Last Sync</div>
              <div class="subVal mono" id="supaLastSyncAt"></div>
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
        </div>
        </div>
      </div>

      <div class="subTabPanel" data-subtab-group="settings" id="subtab-settings-deviceSync">
        <div class="grid">
          <div class="card">
            <div class="title">Device Connection</div>
            <div class="formGrid">
              <label for="setIp">Device IP</label><input id="setIp" placeholder="Device IP" />
              <label for="setPort">Device Port</label><input id="setPort" placeholder="Port" inputmode="numeric" />
              <label for="setReader">Reader Mode</label>
              <select id="setReader">
                <option value="native">native</option>
                <option value="auto">auto</option>
                <option value="com">com</option>
              </select>
            </div>
            <div style="height:10px"></div>
            <div class="row">
              <button class="btn" id="saveDevice" type="button">Save Device</button>
              <button class="btn" id="pingNow" type="button">Ping</button>
              <button class="btn" id="testNow" type="button">Test TCP</button>
              <button class="btn" id="connectNow" type="button">Connect Now</button>
              <button class="btn" id="disconnectNow" type="button">Disconnect</button>
            </div>
            <div style="height:8px"></div>
            <div class="hint" id="actionOut"></div>
            <div style="height:10px"></div>
            <div class="row"><span class="kv">Connection: <span id="settingsReach" class="pill">Unknown</span></span></div>
            <div style="height:6px"></div>
            <div class="hint" id="savedDeviceHint">Saved Device: -</div>
            <div class="hint" id="pcNetHint"></div>
          </div>

          <div class="card">
            <div class="title">Sync &amp; Dashboard</div>
            <div class="formGrid">
              <label for="schedTimes">Sync time(s)</label>
              <input id="schedTimes" placeholder="HH:mm,HH:mm (e.g. 07:00,12:30,18:00)" />
              <label id="setDashRefreshLabel" for="setDashRefresh">Dashboard refresh (min)</label>
              <input id="setDashRefresh" placeholder="Dashboard refresh (min)" inputmode="numeric" />
              <label for="setAuto">Auto sync</label>
              <select id="setAuto">
                <option value="true">Enable</option>
                <option value="false">Disabled</option>
              </select>
            </div>
            <div style="height:10px"></div>
            <div class="row">
              <button class="btn" id="saveSync" type="button">Save Sync</button>
              <button class="btn" id="restartDashboard" type="button">Restart Dashboard</button>
            </div>
          </div>
        </div>
      </div>

      <div class="subTabPanel" data-subtab-group="settings" id="subtab-settings-databaseSync">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="title">Database Settings</div>
            <div class="formGrid">
              <label for="supaUrl">SUPABASE_URL</label><input id="supaUrl" placeholder="https://your-project.supabase.co" />
              <label for="supaProjectId">SUPABASE_PROJECT_ID</label><input id="supaProjectId" placeholder="project id" />
              <label for="supaTable">Attendance table</label><input id="supaTable" placeholder="attendance_events" />
              <label for="supaSyncMode">Sync to Supabase</label>
              <select id="supaSyncMode">
              <option value="enabled">enabled</option>
              <option value="disabled">disabled</option>
              </select>
              <label for="supaPubKey">SUPABASE_PUBLISHABLE_KEY</label><input id="supaPubKey" type="text" placeholder="anon (publishable) key" autocomplete="off" />
              <label for="supaKey">SUPABASE_SERVICE_ROLE_KEY</label><input id="supaKey" type="text" placeholder="service role key" autocomplete="off" />
              <label for="supaJwt">SUPABASE_JWT_SECRET</label><input id="supaJwt" type="text" placeholder="JWT secret" autocomplete="off" />
            </div>
            <div class="hint">ADMIN ONLY: These secrets grant privileged access. Do not share screenshots, logs, or exports containing these values.</div>
            <div style="height:10px"></div>
            <div class="row">
              <button class="btn" id="saveSupabase" type="button">Save Settings</button>
              <button class="btn primary" id="testSupabase" type="button">Test Connection</button>
              <button class="btn" id="saveEnv" type="button">Save .env.local</button>
              <span class="muted" id="supaTestResult"></span>
            </div>
            <div style="height:10px"></div>
            <div class="hint">Keys are stored locally for this dashboard. .env.local will contain publishable values for the web app.</div>
          </div>
        </div>
      </div>

      <div class="subTabPanel" data-subtab-group="settings" id="subtab-settings-logs">
        <div class="grid">
          <div class="card" style="grid-column:1/-1">
            <div class="title">Logs</div>
            <div class="row" style="margin-bottom:10px">
              <button class="btn" id="logsRefresh" type="button">Refresh Logs</button>
            </div>
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
      }
    }

    function out(msg) {
      const x = (activeTab === 'settings' && activeSettingsSubTab !== 'logs' ? el('actionOut') : null) || el('logs');
      if (x) x.textContent = String(msg || '');
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
        setActiveSubTab('staff', 'shift');
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
        refreshStaffRecords().catch(() => {});
        if (activeStaffSubTab === 'shift') refreshShiftPatterns().catch(() => {});
      }
      if (name === 'settings') {
        if (activeSettingsSubTab === 'logs') refreshLogs().catch(() => {});
      }
      if (name === 'attendanceSpreadsheet') {
        refreshAttendanceSpreadsheet().catch(() => {});
      }
    }

    for (const btn of document.querySelectorAll('.tabBtn')) {
      btn.addEventListener('click', () => setActiveTab(btn.dataset.tab));
    }

    for (const btn of document.querySelectorAll('.subTabBtn')) {
      btn.addEventListener('click', () => setActiveSubTab(btn.dataset.subtabGroup, btn.dataset.subtab));
    }

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
      if (savedSettings) setActiveSubTab('settings', savedSettings);
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

    function setValIfNotFocused(id, value) {
      const x = el(id);
      if (document.activeElement === x) return;
      x.value = value;
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

    async function refreshStatus() {
      const j = await getJson('/api/status');
      if (!j) { setApiOfflineUi(); return false; }
      hideQuickFix();

      isSuperadmin = !!j.isSuperadmin;
      const sheetDailyDownloadBtn = el('sheetDailyDownload');
      if (sheetDailyDownloadBtn) sheetDailyDownloadBtn.style.display = isSuperadmin ? '' : 'none';
      const sheetWeeklyDownloadBtn = el('sheetWeeklyDownload');
      if (sheetWeeklyDownloadBtn) sheetWeeklyDownloadBtn.style.display = isSuperadmin ? '' : 'none';
      const sheetMonthlyDownloadBtn = el('sheetMonthlyDownload');
      if (sheetMonthlyDownloadBtn) sheetMonthlyDownloadBtn.style.display = isSuperadmin ? '' : 'none';
      const sheetMonthlyBulkDownloadBtn = el('sheetMonthlyBulkDownload');
      if (sheetMonthlyBulkDownloadBtn) sheetMonthlyBulkDownloadBtn.style.display = isSuperadmin ? '' : 'none';
      const sheetMonthlySelectAll = el('sheetMonthlySelectAll');
      if (sheetMonthlySelectAll) sheetMonthlySelectAll.style.display = isSuperadmin ? '' : 'none';

      const ipEl = el('ip'); if (ipEl) ipEl.textContent = j.device.ip;
      const portEl = el('port'); if (portEl) portEl.textContent = j.device.port;
      const deviceIdEl = el('deviceId'); if (deviceIdEl) deviceIdEl.textContent = j.device.deviceId;
      const readerModeEl = el('readerMode'); if (readerModeEl) {
        const raw = String(j.device.readerMode || '').trim();
        readerModeEl.textContent = raw ? (raw.charAt(0).toUpperCase() + raw.slice(1)) : '-';
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
      setValIfNotFocused('schedTimes', (j.sync && Array.isArray(j.sync.scheduleLocalTimes)) ? j.sync.scheduleLocalTimes.join(',') : '');

      const dashLabel = el('setDashRefreshLabel');
      const dashInput = el('setDashRefresh');
      if (dashLabel && dashInput) {
        dashLabel.style.display = autoOn ? 'none' : '';
        dashInput.style.display = autoOn ? 'none' : '';
      }
      setValIfNotFocused('supaUrl', (j.supabase && j.supabase.url) ? j.supabase.url : '');
      setValIfNotFocused('supaProjectId', (j.supabase && j.supabase.projectId) ? j.supabase.projectId : '');
      setValIfNotFocused('supaTable', (j.supabase && j.supabase.attendanceTable) ? j.supabase.attendanceTable : '');
      setValIfNotFocused('supaSyncMode', (j.supabase && j.supabase.syncEnabled) ? 'enabled' : 'disabled');
      setValIfNotFocused('supaPubKey', (j.supabase && j.supabase.anonKey) ? j.supabase.anonKey : '');
      setValIfNotFocused('supaKey', (j.supabase && j.supabase.serviceRoleKey) ? j.supabase.serviceRoleKey : '');
      setValIfNotFocused('supaJwt', (j.supabase && j.supabase.jwtSecret) ? j.supabase.jwtSecret : '');
      const savedDeviceHintEl = el('savedDeviceHint');
      if (savedDeviceHintEl) savedDeviceHintEl.textContent = 'Saved Device: ' + (j.device.ip || '-') + ':' + (j.device.port || '-') + ' (reader=' + (j.device.readerMode || '-') + ')';
      const pcNetHint = el('pcNetHint');
      if (j.pc && Array.isArray(j.pc.ipv4) && j.pc.ipv4.length) {
        const ips = j.pc.ipv4.map(x => String(x)).join(', ');
        let msg = 'This PC IPv4: ' + ips;
        if (j.dashboard && j.dashboard.port) {
          const best = (j.pc && j.pc.bestIpv4) ? String(j.pc.bestIpv4) : '';
          const dashHost = best || '127.0.0.1';
          msg += ' | Dashboard: http://' + dashHost + ':' + String(j.dashboard.port) + '/';
          if (j.dashboard.bind && String(j.dashboard.bind) !== '0.0.0.0' && String(j.dashboard.bind) !== dashHost) {
            msg += ' (listening on ' + String(j.dashboard.bind) + ')';
          }
          try { if (best) localStorage.setItem('wl10dash.bestIpv4', best); } catch { }
        }
        if (!j.device.reachable && j.pc.sameSubnet24 === false) {
          msg += ' | Device ' + (j.device.ip || '-') + ' is on a different subnet. Connect this PC to the device LAN (e.g. 192.168.1.x) or change the device IP to match.';
        }
        pcNetHint.textContent = msg;
      } else {
        pcNetHint.textContent = '';
      }

      const supaPill = el('supaPill');
      if (!j.supabase.configured) {
        supaPill.className = 'pill bad';
        supaPill.textContent = 'Not Configured';
      } else if (j.supabase.syncEnabled) {
        supaPill.className = 'pill ok';
        supaPill.textContent = 'Enabled';
      } else {
        supaPill.className = 'pill bad';
        supaPill.textContent = 'Disabled';
      }
      el('supaTableState').textContent = normText(j.supabase.attendanceTable);
      const supaAutoEl = el('supaAuto'); if (supaAutoEl) supaAutoEl.textContent = normText(j.supabase.syncEnabled ? 'on' : 'off');
      const supaIntervalEl = el('supaInterval'); if (supaIntervalEl) supaIntervalEl.textContent = (j.sync && j.sync.nextSyncAtUtc) ? fmt(j.sync.nextSyncAtUtc) : '-';
      const supaLastResEl = el('supaLastRes'); if (supaLastResEl) supaLastResEl.textContent = String(j.sync.lastSupabaseUpsertedCount ?? 0);
      const supaLastSyncAt = j.sync.lastSupabaseSyncFinishedAtUtc || j.sync.lastSupabaseSyncStartedAtUtc || '';
      const supaLastSyncAtEl = el('supaLastSyncAt'); if (supaLastSyncAtEl) supaLastSyncAtEl.textContent = fmt(supaLastSyncAt);
      const supaTotalRecordsEl = el('supaTotalRecords'); if (supaTotalRecordsEl) supaTotalRecordsEl.textContent = String(j.sync.dbRecordsTotal ?? 0);
      const supaUrlState = el('supaUrlState');
      if (supaUrlState) supaUrlState.textContent = '';

      setValIfNotFocused('supaUrl', j.supabase.url || '');
      setValIfNotFocused('supaProjectId', j.supabase.projectId || '');
      setValIfNotFocused('supaTable', j.supabase.attendanceTable || '');
      setValIfNotFocused('supaSyncMode', j.supabase.syncEnabled ? 'enabled' : 'disabled');
      setValIfNotFocused('supaPubKey', j.supabase.anonKey || '');
      setValIfNotFocused('supaKey', j.supabase.serviceRoleKey || '');
      setValIfNotFocused('supaJwt', j.supabase.jwtSecret || '');

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
      titleEl.textContent = title || '';
      err.textContent = '';
      body.className = 'modalBody' + ((options && options.bodyClass) ? (' ' + String(options.bodyClass)) : '');
      body.innerHTML = html || '';

      const saveBtn = el('modalSave');
      const cancelBtn = el('modalCancel');
      if (saveBtn) saveBtn.style.display = (options && options.hideSave) ? 'none' : '';
      if (cancelBtn) cancelBtn.textContent = (options && options.cancelText) ? String(options.cancelText) : 'Cancel';
      modalOnSave = (options && options.onSaveAsync) ? options.onSaveAsync : null;

      back.style.display = 'flex';
    }

    async function refreshLogs() {
      const j = await getJson('/api/logs');
      if (!j) return;
      const logsEl = el('logs');
      if (!logsEl) return;
      logsEl.innerHTML = (j.lines || []).slice(-50).map(escHtml).join('<br><br>');
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
        tr.innerHTML = '<td colspan="6" class="muted">' + escHtml(emptyText) + '</td>';
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

    function toStaffCsv(rows) {
      const header = ['User ID', 'Full Name', 'Role', 'Department', 'Status', 'Date Joined', 'Shift Pattern'];
      const out = [header.join(',')];
      for (const r of (rows || [])) {
        const vals = [
          r.user_id ?? '',
          r.full_name ?? '',
          r.role ?? '',
          r.department ?? '',
          r.status ?? '',
          r.date_joined ?? '',
          r.shift_pattern ?? '',
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

    let lastDeviceRows = [];
    let lastDbRows = [];
    let lastStaffRows = [];
    let lastShiftRows = [];
    const fallbackShiftRows = [
      { pattern: 'Normal', workingDays: 'Mon–Fri', workingHours: '09:00–18:00', break: '13:00–14:00', notes: 'Default' },
      { pattern: 'Shift 1', workingDays: 'Mon–Sat', workingHours: '08:00–16:00', break: '12:00–13:00', notes: 'Default' },
      { pattern: 'Shift 2', workingDays: 'Mon–Sat', workingHours: '16:00–00:00', break: '20:00–20:30', notes: 'Default' },
      { pattern: 'Shift 3', workingDays: 'Mon–Sat', workingHours: '00:00–08:00', break: '04:00–04:30', notes: 'Default' },
    ];

    async function refreshDeviceRecords() {
      const j = await getJson('/api/records/file');
      lastDeviceRows = j ? (j.rows || []) : [];
      const err = (j && j.error) ? String(j.error || '') : '';
      const filtered = applyAttlogFilters(lastDeviceRows, el('devFilterStaff').value, el('devFilterFrom').value, el('devFilterTo').value)
        .slice()
        .sort((a, b) => String(b.datetime ?? '').localeCompare(String(a.datetime ?? '')));
      const msg = err ? ('No device records. ' + err) : 'No device records. Import a file or refresh from device.';
      await renderAttlogTable(filtered, msg);
    }

    async function refreshDbRecords() {
      let j = null;
      for (let attempt = 0; attempt < 3; attempt++) {
        j = await getJson('/api/db/records', true);
        if (j && j.ok !== false) break;
        await new Promise(r => setTimeout(r, 250));
      }
      lastDbRows = (j && j.ok !== false) ? (j.rows || []) : [];
      const err = (j && j.ok === false && j.error) ? String(j.error || '') : (j && j.error) ? String(j.error || '') : '';
      const filtered = applyFilters(lastDbRows, el('dbFilterStaff').value, el('dbFilterFrom').value, el('dbFilterTo').value);
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

    function updateStaffDeleteEnabled() {
      const btn = el('staffDelete');
      if (!btn) return;
      const body = el('staffBody');
      const has = !!body && !!body.querySelector('input.staffChk[type="checkbox"]:checked');
      btn.disabled = !has;
    }

    async function renderStaffTable(rows, emptyText) {
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
        const tr = document.createElement('tr');
        tr.innerHTML =
          '<td><input class="staffChk" type="checkbox" data-id="' + escHtml(staffId) + '"/></td>' +
          '<td>' + escHtml(staffId) + '</td>' +
          '<td>' + escHtml(r.full_name ?? '') + '</td>' +
          '<td>' + escHtml(r.role ?? '') + '</td>' +
          '<td>' + escHtml(r.department ?? '') + '</td>' +
          '<td>' + escHtml(r.status ?? '') + '</td>' +
          '<td>' + escHtml(r.date_joined ?? '') + '</td>' +
          '<td>' + escHtml(r.shift_pattern ?? '') + '</td>' +
          '<td>' +
            '<button class="iconBtn staffProvisionRow" type="button" data-id="' + escHtml(staffId) + '" title="Create on device">↥</button>' +
            '<button class="iconBtn staffEditRow" type="button" data-id="' + escHtml(staffId) + '" title="Edit">✎</button>' +
          '</td>';
        body.appendChild(tr);
      }
      updateStaffDeleteEnabled();
    }

    async function refreshStaffRecords() {
      const j = await getJson('/api/staff/file');
      lastStaffRows = j ? (j.rows || []) : [];
      const err = (j && j.error) ? String(j.error || '') : '';
      const filtered = applyStaffFilters(lastStaffRows, el('staffFilter')?.value || '');
      const msg = err ? ('No staff records. ' + err) : 'No staff records.';
      await renderStaffTable(filtered, msg);
    }

    async function provisionStaffToDevice(row) {
      const id = String(row && row.user_id ? row.user_id : '').trim();
      if (!id) { out('Missing user id.'); return; }
      const firstName = String(row && row.full_name ? row.full_name : '').trim();
      const r = await postJson('/api/device/user/create', { user_id: id, first_name: firstName });
      if (r && r.ok) {
        if (r.already_exists) out('Device already has user ' + id + '.');
        else if (r.created && r.verified === false) out('Created user ' + id + ' on device, but could not verify. Check device screen.');
        else out('Created user ' + id + ' on device.');
      } else {
        const base = String((r && r.error) ? r.error : 'unknown error');
        const le = (r && (r.last_error ?? null) !== null) ? (' last_error=' + String(r.last_error)) : '';
        const dev = (r && r.device_ip) ? (' device=' + String(r.device_ip) + ':' + String(r.device_port || '')) : '';
        const mn = (r && (r.machine_number ?? null) !== null) ? (' machine=' + String(r.machine_number)) : '';
        out('Device provision failed: ' + base + le + dev + mn);
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
      const body = el('shiftBody');
      const has = !!body && !!body.querySelector('input.shiftChk[type="checkbox"]:checked');
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
      for (let i = 0; i < list.length; i++) {
        const r = list[i];
        const key = String(r.pattern ?? '');
        const tr = document.createElement('tr');
        tr.innerHTML =
          '<td><input class="shiftChk" type="checkbox" data-id="' + escHtml(key) + '"/></td>' +
          '<td>' + escHtml(key) + '</td>' +
          '<td>' + escHtml(r.workingDays ?? '') + '</td>' +
          '<td>' + escHtml(r.workingHours ?? '') + '</td>' +
          '<td>' + escHtml(r.break ?? '') + '</td>' +
          '<td>' + escHtml(r.notes ?? '') + '</td>' +
          '<td><button class="iconBtn shiftEditRow" type="button" data-id="' + escHtml(key) + '" title="Edit">✎</button></td>';
        body.appendChild(tr);
      }
      updateShiftDeleteEnabled();
    }

    async function refreshShiftPatterns() {
      const j = await getJson('/api/shifts');
      lastShiftRows = j ? (j.rows || []) : [];
      if (!lastShiftRows.length) lastShiftRows = fallbackShiftRows.slice();
      const filtered = applyShiftFilters(lastShiftRows, el('shiftFilter')?.value || '');
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
        const color = (x.color || '').trim() || 'rgba(37,99,235,.85)';
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
      if (!host.querySelector('.gaugeArc')) {
        host.innerHTML = '';
        const arc = document.createElement('div');
        arc.className = 'gaugeArc';
        const needle = document.createElement('div');
        needle.className = 'gaugeNeedle';
        const hub = document.createElement('div');
        hub.className = 'gaugeHub';
        const min = document.createElement('div');
        min.className = 'gaugeMin';
        const max = document.createElement('div');
        max.className = 'gaugeMax';
        host.appendChild(arc);
        host.appendChild(needle);
        host.appendChild(hub);
        host.appendChild(min);
        host.appendChild(max);
      }

      const palettes = {
        traffic: [
          { deg: 0, color: '#ef4444' },
          { deg: 60, color: '#f97316' },
          { deg: 120, color: '#f59e0b' },
          { deg: 180, color: '#22c55e' },
        ],
        ocean: [
          { deg: 0, color: '#0ea5e9' },
          { deg: 90, color: '#14b8a6' },
          { deg: 180, color: '#2563eb' },
        ],
        violet: [
          { deg: 0, color: '#a855f7' },
          { deg: 90, color: '#3b82f6' },
          { deg: 180, color: '#14b8a6' },
        ],
      };

      function hexToRgb(hex) {
        const h = String(hex || '').trim().replace('#', '');
        if (h.length !== 6) return { r: 0, g: 0, b: 0 };
        const n = parseInt(h, 16);
        return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255 };
      }

      function lerp(a, b, t) { return a + (b - a) * t; }

      function lerpColorHex(aHex, bHex, t) {
        const a = hexToRgb(aHex);
        const b = hexToRgb(bHex);
        const r = Math.round(lerp(a.r, b.r, t));
        const g = Math.round(lerp(a.g, b.g, t));
        const b2 = Math.round(lerp(a.b, b.b, t));
        return 'rgb(' + r + ',' + g + ',' + b2 + ')';
      }

      const v = Number(value);
      const mx = Math.max(1e-9, Number(maxValue) || 0);
      const pct = Number.isFinite(v) ? Math.max(0, Math.min(1, v / mx)) : 0;
      const deg = pct * 180;
      host.style.setProperty('--gNeedle', (-90 + deg).toFixed(2) + 'deg');
      const arc = host.querySelector('.gaugeArc');
      const pal = palettes[String(paletteName || '').trim()] || palettes.ocean;
      const d = Math.max(0, Math.min(180, deg));
      let endColor = '#0ea5e9';

      if (arc) {
        const pts = pal.slice().sort((a, b) => a.deg - b.deg);
        const steps = pts.filter(p => p.deg <= d);
        const last = steps.length ? steps[steps.length - 1] : pts[0];
        const next = pts.find(p => p.deg >= d) || pts[pts.length - 1];
        endColor = String(next.color || '').trim() || '#0ea5e9';
        if (d <= pts[0].deg) endColor = String(pts[0].color || '').trim() || endColor;
        else if (d >= pts[pts.length - 1].deg) endColor = String(pts[pts.length - 1].color || '').trim() || endColor;
        else {
          for (let i = 0; i < pts.length - 1; i++) {
            const a = pts[i];
            const b = pts[i + 1];
            if (d >= a.deg && d <= b.deg) {
              const t = (d - a.deg) / Math.max(1e-9, (b.deg - a.deg));
              endColor = lerpColorHex(a.color, b.color, t);
              break;
            }
          }
        }

        const fillStops = [];
        const baseStart = String(pts[0].color || '').trim() || '#0ea5e9';
        fillStops.push(baseStart + ' 0deg');
        for (const p of pts) {
          if (p.deg > 0 && p.deg < d) fillStops.push(String(p.color || '').trim() + ' ' + p.deg.toFixed(2) + 'deg');
        }
        fillStops.push(endColor + ' ' + d.toFixed(2) + 'deg');
        arc.style.background = 'conic-gradient(from 180deg,' + fillStops.join(',') + ',#e5e7eb ' + d.toFixed(2) + 'deg 180deg,transparent 180deg 360deg)';
      }

      const minEl = host.querySelector('.gaugeMin');
      const maxEl = host.querySelector('.gaugeMax');
      if (minEl) minEl.textContent = String(minLabel ?? '0');
      if (maxEl) maxEl.textContent = String(maxLabel ?? (Number.isFinite(mx) ? Math.round(mx) : ''));
      host.dataset.endColor = String(endColor || '').trim() || '#0ea5e9';
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
        area.setAttribute('fill', 'rgba(37,99,235,.18)');
        area.setAttribute('stroke', 'none');
        svg.appendChild(area);
      }

      const line = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      line.setAttribute('d', pathD);
      line.setAttribute('fill', 'none');
      line.setAttribute('stroke', 'rgba(37,99,235,.92)');
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
        c.setAttribute('stroke', 'rgba(37,99,235,.92)');
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
        const pctA = (max > 0 ? Math.max(2, Math.round((aVal / max) * 100)) : 2);
        const pctB = (max > 0 ? Math.max(2, Math.round((bVal / max) * 100)) : 2);

        const b1 = document.createElement('div');
        b1.className = 'gBar ' + (kindA || 'device');
        b1.style.height = pctA + '%';
        b1.title = (kindA || 'A') + ': ' + aVal;
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

    async function refreshAttendance() {
      const dateEl = el('anaDate');
      const deptEl = el('anaDept');
      const date = (dateEl && dateEl.value) ? dateEl.value : '';
      const dept = (deptEl && deptEl.value) ? deptEl.value : 'All';

      const q = new URLSearchParams();
      if (date) q.set('date', date);
      if (dept) q.set('department', dept);

      const j = await getJson('/api/attendance/insights?' + q.toString());
      if (!j) return;
      if (j.ok === false) {
        out(j.error || 'Attendance insights failed.');
        return;
      }

      const r = j.roster || {};
      setText('anaTotal', Number.isFinite(Number(r.totalEmployees)) ? Number(r.totalEmployees) : '-');
      setText('anaScope', (j.department || 'All') + ' • DB • ' + (j.date || ''));
      setText('anaPresent', Number.isFinite(Number(r.present)) ? Number(r.present) : '-');
      setText('anaAbsent', Number.isFinite(Number(r.absent)) ? Number(r.absent) : '-');
      setText('anaLate', Number.isFinite(Number(r.lateComers)) ? Number(r.lateComers) : '-');
      setText('anaAttendancePct', 'Attendance: ' + fmtPct(r.attendanceRatePct));
      setText('anaAbsenteePct', 'Absenteeism: ' + fmtPct(r.absenteeRatePct));
      setText('anaAttRateVal', fmtPct(r.attendanceRatePct));
      setText('anaAbsRateVal', fmtPct(r.absenteeRatePct));
      const avgWork = Number(r.avgWorkingHours);
      const avgBreak = Number(r.avgBreakHours);
      const avgOt = Number(r.avgOtHours);
      setText('anaAvgWork', Number.isFinite(avgWork) ? avgWork.toFixed(2) : '-');
      setText('anaAvgBreak', Number.isFinite(avgBreak) ? avgBreak.toFixed(2) : '-');
      setText('anaAvgOt', Number.isFinite(avgOt) ? avgOt.toFixed(2) : '-');
      const bH = Number(r.avgBreakHours) || 0;
      const oH = Number(r.avgOtHours) || 0;
      setText('anaBreakVsOt', (bH + oH) > 0 ? (Math.round((oH / Math.max(1e-9, bH + oH)) * 1000) / 10).toFixed(1) + '% OT' : '-');

      const note = el('anaNote');
      if (note) {
        const raw = String(j.note || '').trim();
        if (raw) { note.style.display = ''; note.textContent = raw; }
        else { note.style.display = 'none'; note.textContent = ''; }
      }

      const attRate = Number(r.attendanceRatePct);
      const attC = renderGauge('anaGaugeAttendance', Number.isFinite(attRate) ? attRate : 0, 100, 'traffic', '0', '100');
      setText('anaGaugeAttendanceVal', fmtPct(attRate));
      const attVal = el('anaGaugeAttendanceVal');
      if (attVal && attC) attVal.style.color = attC;

      const maxAvg = Math.max(6, Math.ceil((Number.isFinite(avgWork) ? avgWork : 0) + 2));
      renderGauge('anaGaugeAvgHours', Number.isFinite(avgWork) ? avgWork : 0, maxAvg, 'ocean', '0', String(maxAvg));
      setText('anaGaugeAvgHoursVal', Number.isFinite(avgWork) ? (avgWork.toFixed(2) + ' hrs') : '-');

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
        color: ['rgba(37,99,235,.86)','rgba(14,165,233,.80)','rgba(100,116,139,.70)','rgba(220,38,38,.66)','rgba(22,163,74,.60)','rgba(245,158,11,.64)'][i % 6],
      }));
      const avgAbsM = months.length ? (months.reduce((m, x) => m + (Number(x.absenteePct) || 0), 0) / months.length) : 0;
      renderDonut('anaDonutAbsentM', 'anaDonutAbsentMCenter', 'anaDonutAbsentMLegend', absentMParts, fmtPct(avgAbsM));

      renderLineSvg('anaLineAttendance', months, 'attendancePct', 'label', 'line');
      renderLineSvg('anaAreaAbsentee', months, 'absenteePct', 'label', 'area');

      const top = Array.isArray(j.topEmployees) ? j.topEmployees.slice() : [];
      renderHBars('anaTopEmployees', top, x => (x.full_name ? (x.full_name + (x.department ? (' • ' + x.department) : '')) : (x.staff_id || '')), x => Math.round((Number(x.attendancePct) || 0) * 10) / 10, '%');

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
        setText('anaIntDbToday', db.ok ? String(db.totalPunches ?? 0) : '-');
      }
      setText('anaIntMissingOut', Number.isFinite(Number(r.missingOutCount)) ? Number(r.missingOutCount) : '-');
      setText('anaIntDuplicates', Number.isFinite(Number(r.duplicatePunches)) ? Number(r.duplicatePunches) : '-');

      const alerts = [];
      const missingOut = Number(r.missingOutCount) || 0;
      const duplicates = Number(r.duplicatePunches) || 0;
      const att = Number(r.attendanceRatePct) || 0;
      const abs = Number(r.absenteeRatePct) || 0;
      if (att > 0 && att < 85) alerts.push('Attendance rate below 85%.');
      if (abs > 10) alerts.push('Absenteeism above 10%.');
      if (missingOut > 0) alerts.push(missingOut + ' employee(s) with missing OUT punch.');
      if (duplicates > 0) alerts.push(duplicates + ' potential duplicate punch(es) detected.');
      const alertsEl = el('anaAlerts');
      if (alertsEl) alertsEl.innerHTML = alerts.length ? ('<div class="flagWrap">' + alerts.map(x => escHtml(x)).join('<br/>') + '</div>') : '<span class="muted">No alerts.</span>';
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
      if (activeSheetSubTab === 'daily') {
        const dateEl = el('sheetDailyDate');
        if (dateEl && !dateEl.value) {
          try { dateEl.value = new Date().toISOString().slice(0, 10); } catch { }
        }
        const q = new URLSearchParams();
        if (dateEl && dateEl.value) q.set('date', dateEl.value);
        const j = await getJson('/api/spreadsheet/daily?' + q.toString());
        const body = el('sheetDailyBody');
        if (!body) return;
        body.innerHTML = '';
        sheetDailyByKey = new Map();
        if (!j || j.ok === false) {
          const tr = document.createElement('tr');
          tr.innerHTML = '<td colspan="9" class="muted">' + escHtml((j && j.error) ? j.error : 'Failed to load daily spreadsheet') + '</td>';
          body.appendChild(tr);
          return;
        }
        const rows = Array.isArray(j.rows) ? j.rows : [];
        lastSheetDailyRows = rows.slice();
        if (!rows.length) {
          const tr = document.createElement('tr');
          tr.innerHTML = '<td colspan="9" class="muted">(none)</td>';
          body.appendChild(tr);
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
          const flaggedText = String(flagged || '').trim() || '-';
          let flaggedHtml = '-';
          if (flagsText || flaggedText !== '-') {
            const shortFlags = compactFlags(flagsText);
            const top = shortFlags ? escHtml(shortFlags) : '<span class="muted">-</span>';
            flaggedHtml = '<div class="flagWrap">' + top + '</div><div class="mono">' + escHtml(flaggedText) + '</div>';
          }
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
        return;
      }

      if (activeSheetSubTab === 'weekly') {
        const dateEl = el('sheetWeeklyDate');
        if (dateEl && !dateEl.value) {
          try { dateEl.value = mondayIso(new Date()); } catch { }
        }
        const q = new URLSearchParams();
        if (dateEl && dateEl.value) q.set('date', dateEl.value);
        const j = await getJson('/api/spreadsheet/weekly?' + q.toString());
        const body = el('sheetWeeklyBody');
        if (!body) return;
        body.innerHTML = '';
        if (!j || j.ok === false) {
          const tr = document.createElement('tr');
          tr.innerHTML = '<td colspan="8" class="muted">' + escHtml((j && j.error) ? j.error : 'Failed to load weekly spreadsheet') + '</td>';
          body.appendChild(tr);
          return;
        }
        const rows = Array.isArray(j.rows) ? j.rows : [];
        for (const r of rows) r.week_display = fmtWeekShortRange(r.week_start, r.week_end);
        lastSheetWeeklyRows = rows.slice();
        if (!rows.length) {
          const tr = document.createElement('tr');
          tr.innerHTML = '<td colspan="8" class="muted">(none)</td>';
          body.appendChild(tr);
          return;
        }
        for (const r of rows) {
          const tr = document.createElement('tr');
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
        return;
      }

      if (activeSheetSubTab === 'monthly') {
        const monthEl = el('sheetMonthlyMonth');
        if (monthEl && !monthEl.value) {
          try { monthEl.value = new Date().toISOString().slice(0, 7); } catch { }
        }
        const q = new URLSearchParams();
        if (monthEl && monthEl.value) q.set('month', monthEl.value);
        const j = await getJson('/api/spreadsheet/monthly?' + q.toString());
        const body = el('sheetMonthlyBody');
        if (!body) return;
        body.innerHTML = '';
        if (!j || j.ok === false) {
          const tr = document.createElement('tr');
          tr.innerHTML = '<td colspan="10" class="muted">' + escHtml((j && j.error) ? j.error : 'Failed to load monthly spreadsheet') + '</td>';
          body.appendChild(tr);
          return;
        }
        const rows = Array.isArray(j.rows) ? j.rows : [];
        for (const r of rows) r.month_display = fmtMonthShort(r.month);
        lastSheetMonthlyRows = rows.slice();
        if (!rows.length) {
          const tr = document.createElement('tr');
          tr.innerHTML = '<td colspan="10" class="muted">(none)</td>';
          body.appendChild(tr);
          return;
        }
        for (const r of rows) {
          const tr = document.createElement('tr');
          const sid = String(r.staff_id || '').trim();
          const checked = monthlySelectedStaff.has(sid);
          const cbHtml = isSuperadmin
            ? ('<input type="checkbox" class="sheetMonthlyPick" data-staff="' + escHtml(sid) + '"' + (checked ? ' checked' : '') + ' />')
            : '';
          const dlHtml = isSuperadmin
            ? ('<button class="dlIconBtn" type="button" data-sheet-download="' + escHtml(sid) + '" title="Download report" aria-label="Download report"><svg viewBox="0 0 24 24"><path d="M12 3v10"/><path d="M8 11l4 4 4-4"/><path d="M4 17v4h16v-4"/></svg></button>')
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
      }
    }

    const sheetDailyRefreshBtn = el('sheetDailyRefresh');
    if (sheetDailyRefreshBtn) sheetDailyRefreshBtn.addEventListener('click', async () => refreshAttendanceSpreadsheet());
    const sheetWeeklyRefreshBtn = el('sheetWeeklyRefresh');
    if (sheetWeeklyRefreshBtn) sheetWeeklyRefreshBtn.addEventListener('click', async () => refreshAttendanceSpreadsheet());
    const sheetMonthlyRefreshBtn = el('sheetMonthlyRefresh');
    if (sheetMonthlyRefreshBtn) sheetMonthlyRefreshBtn.addEventListener('click', async () => refreshAttendanceSpreadsheet());

    const sheetDailyDateEl = el('sheetDailyDate');
    if (sheetDailyDateEl && !sheetDailyDateEl.value) { try { sheetDailyDateEl.value = new Date().toISOString().slice(0, 10); } catch { } }
    const sheetWeeklyDateEl = el('sheetWeeklyDate');
    if (sheetWeeklyDateEl && !sheetWeeklyDateEl.value) { try { sheetWeeklyDateEl.value = mondayIso(new Date()); } catch { } }
    const sheetMonthlyMonthEl = el('sheetMonthlyMonth');
    if (sheetMonthlyMonthEl && !sheetMonthlyMonthEl.value) { try { sheetMonthlyMonthEl.value = new Date().toISOString().slice(0, 7); } catch { } }

    const sheetDailyDownloadBtn = el('sheetDailyDownload');
    if (sheetDailyDownloadBtn) sheetDailyDownloadBtn.addEventListener('click', async () => {
      if (!isSuperadmin) return;
      if (!lastSheetDailyRows.length) await refreshAttendanceSpreadsheet();
      const dateEl = el('sheetDailyDate');
      const d = dateEl && dateEl.value ? dateEl.value : '';
      downloadTextCsv('attendance_daily_' + (d || 'date') + '.csv', toSheetDailyCsv(lastSheetDailyRows || []));
    });

    const sheetWeeklyDownloadBtn = el('sheetWeeklyDownload');
    if (sheetWeeklyDownloadBtn) sheetWeeklyDownloadBtn.addEventListener('click', async () => {
      if (!isSuperadmin) return;
      if (!lastSheetWeeklyRows.length) await refreshAttendanceSpreadsheet();
      const dateEl = el('sheetWeeklyDate');
      const d = dateEl && dateEl.value ? dateEl.value : '';
      downloadTextCsv('attendance_weekly_' + (d || 'week') + '.csv', toSheetWeeklyCsv(lastSheetWeeklyRows || []));
    });

    const sheetMonthlyDownloadBtn = el('sheetMonthlyDownload');
    if (sheetMonthlyDownloadBtn) sheetMonthlyDownloadBtn.addEventListener('click', async () => {
      if (!isSuperadmin) return;
      if (!lastSheetMonthlyRows.length) await refreshAttendanceSpreadsheet();
      const monthEl = el('sheetMonthlyMonth');
      const m = monthEl && monthEl.value ? monthEl.value : '';
      downloadTextCsv('attendance_monthly_' + (m || 'month') + '.csv', toSheetMonthlyCsv(lastSheetMonthlyRows || []));
    });

    const sheetMonthlyBodyEl = el('sheetMonthlyBody');
    if (sheetMonthlyBodyEl) sheetMonthlyBodyEl.addEventListener('click', async (e) => {
      const t = e && e.target ? e.target : null;
      if (!t) return;
      const btn = t.closest ? t.closest('button[data-sheet-download]') : null;
      if (!btn || !isSuperadmin) return;
      const sid = String(btn.getAttribute('data-sheet-download') || '').trim();
      const monthEl = el('sheetMonthlyMonth');
      const m = monthEl && monthEl.value ? monthEl.value : '';
      await downloadCombinedReport(sid, m);
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
    });

    const sheetMonthlyBulkDownloadBtn = el('sheetMonthlyBulkDownload');
    if (sheetMonthlyBulkDownloadBtn) sheetMonthlyBulkDownloadBtn.addEventListener('click', async () => {
      if (!isSuperadmin) return;
      const monthEl = el('sheetMonthlyMonth');
      const m = monthEl && monthEl.value ? monthEl.value : '';
      const ids = Array.from(monthlySelectedStaff.values());
      for (const sid of ids) {
        await downloadCombinedReport(sid, m);
        await new Promise(r => setTimeout(r, 180));
      }
    });

    const sheetDailyBodyEl = el('sheetDailyBody');
    if (sheetDailyBodyEl) sheetDailyBodyEl.addEventListener('click', (e) => {
      const tr = e && e.target ? e.target.closest('tr') : null;
      const key = tr && tr.dataset ? tr.dataset.key : '';
      if (!key) return;
      const row = sheetDailyByKey.get(key);
      if (!row) return;
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

    const anaRefresh = el('anaRefresh');
    if (anaRefresh) anaRefresh.addEventListener('click', async () => refreshAttendance());

    const anaDate = el('anaDate');
    if (anaDate && !anaDate.value) {
      try { anaDate.value = new Date().toISOString().slice(0, 10); } catch { }
    }

    const devRefreshBtn = el('devRefresh');
    if (devRefreshBtn) devRefreshBtn.addEventListener('click', async () => {
      devRefreshBtn.disabled = true;
      try {
        await fetch('/api/sync?today=1', { method: 'POST', credentials: 'same-origin' });
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
          out(r.error || 'Update Supabase failed');
          setLoad(Math.min(100, pct), 'Failed. ' + String(r.error || ''));
        } else {
          const upserted = Number.isFinite(Number(r.upserted)) ? Number(r.upserted) : 0;
          const expected = Number.isFinite(Number(r.distinct)) ? Number(r.distinct) : upserted;
          const from = (r.rangeFrom || '').trim();
          const to = (r.rangeTo || '').trim();

          setLoad(Math.max(pct, 92), 'Refreshing table...');
          const staffEl = el('dbFilterStaff'); if (staffEl) staffEl.value = '';
          const fromEl = el('dbFilterFrom'); if (fromEl) fromEl.value = '';
          const toEl = el('dbFilterTo'); if (toEl) toEl.value = '';

          const dbRes = await refreshDbRecords();
          if (!dbRes || dbRes.ok !== true) {
            setLoad(99, 'Could not load Database Records.');
            out('Supabase update OK. Rows upserted: ' + String(upserted ?? 0) + '. Could not load Database Records (API offline or blocked).');
            return;
          }

          setLoad(96, 'Validating...');
          let validated = false;
          if (from && to) {
            const v = await getJson('/api/supabase/validate?from=' + encodeURIComponent(from) + '&to=' + encodeURIComponent(to) + '&expected=' + encodeURIComponent(String(expected)), true);
            if (v && v.ok === true) {
              validated = true;
              setLoad(100, 'Validated. Rows upserted: ' + String(upserted));
              out('Supabase update OK. Rows upserted: ' + String(upserted) + '. Validated.');
            } else {
              const found = (v && Number.isFinite(Number(v.found))) ? Number(v.found) : 0;
              const disc = (v && Number.isFinite(Number(v.discrepancy))) ? Number(v.discrepancy) : null;
              const err = (v && v.error) ? String(v.error || '') : '';
              setLoad(99, 'Validation failed.');
              if (err) out('Validation failed: ' + err);
              else if (disc !== null) out('Validation failed: expected at least ' + String(expected) + ' rows in Supabase (range ' + from + ' to ' + to + '), but found ' + String(found) + '. Missing: ' + String(disc));
              else out('Validation failed.');
            }
          }
          if (!validated && (!from || !to)) {
            setLoad(99, 'Done. (Could not validate)');
            out('Supabase update OK. Rows upserted: ' + String(upserted) + '. (Could not validate)');
          }
        }
      } catch (e) {
        out('Update Supabase failed');
        setLoad(Math.min(100, pct), 'Failed.');
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
    if (logsRefreshBtn) logsRefreshBtn.addEventListener('click', async () => refreshLogs());

    const restartDashboardBtn = el('restartDashboard');
    if (restartDashboardBtn) restartDashboardBtn.addEventListener('click', async () => {
      const ok = confirm('Restart dashboard now?\n\nThis will briefly disconnect the page and reload when the dashboard is back online.');
      if (!ok) return;
      restartDashboardBtn.disabled = true;
      out('Restarting dashboard...');
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
      const t = getDeviceTarget();
      try {
        const readerMode = (el('setReader')?.value || '').trim();
        const body = { deviceIp: (t.ip || '').trim(), devicePort: (t.port ?? null), readerMode };
        const r = await postJson('/api/device/connect', body);
        const ip = (r && r.deviceIp) ? r.deviceIp : (t.ip || '-');
        const port = (r && (r.devicePort ?? null) !== null) ? r.devicePort : (t.port ?? '-');
        const target = ip + ':' + port;
        if (r && r.ok) {
          let msg = 'Connected (TCP) to ' + target + ' ' + (r.rttMs ? (r.rttMs + 'ms') : '');
          if (r.verifyOk === true) msg += ' | Verified (read OK)';
          else if (r.verifyOk === false) msg += ' | Verify failed: ' + String(r.verifyError || 'error');
          out(msg);
        }
        else if (r && r.ok === false) {
          const hint = String(r.error || 'error') === 'timeout' ? ' (device not reachable from this PC)' : '';
          out('Connect failed to ' + target + ': ' + String(r.error || 'error') + hint);
        }
        else out('Connect failed');
      } finally {
        connectNowBtn.disabled = false;
      }
      await refreshStatus();
      await refreshPresets();
    });

    const disconnectNowBtn = el('disconnectNow');
    if (disconnectNowBtn) disconnectNowBtn.addEventListener('click', async () => {
      disconnectNowBtn.disabled = true;
      try {
        const r = await postJson('/api/device/disconnect', {});
        if (r && r.ok === false) out(r.error || 'Disconnect failed');
        else out('Disconnected.');
      } finally {
        disconnectNowBtn.disabled = false;
      }
      await refreshStatus();
    });

    async function uploadDeviceFile() {
      const f = el('devFile').files && el('devFile').files[0];
      if (!f) return;
      const fd = new FormData();
      fd.append('file', f, f.name);
      try {
        const res = await fetch('/api/records/upload', { method: 'POST', body: fd, credentials: 'same-origin' });
        if (res.status === 401) { out('Unauthorized. Please reload and login again.'); return; }
        if (!res.ok) { out('Load file failed: ' + (await res.text())); return; }
        const j = await res.json();
        lastDeviceRows = j ? (j.rows || []) : [];
        const filtered = applyAttlogFilters(lastDeviceRows, el('devFilterStaff').value, el('devFilterFrom').value, el('devFilterTo').value);
        await renderAttlogTable(filtered, 'No device records. Import a file or refresh from device.');
        await refreshStatus();
      } catch (e) {
        out('Load file failed: ' + String(e));
      }
    }

    const devLoadFileBtn = el('devLoadFile');
    if (devLoadFileBtn) devLoadFileBtn.addEventListener('click', uploadDeviceFile);
    for (const id of ['devFilterStaff','devFilterFrom','devFilterTo']) {
      const x = el(id);
      if (!x) continue;
      x.addEventListener('input', () => renderAttlogTable(applyAttlogFilters(lastDeviceRows, el('devFilterStaff').value, el('devFilterFrom').value, el('devFilterTo').value), 'No device records. Import a file or run Sync Now.'));
      x.addEventListener('change', () => renderAttlogTable(applyAttlogFilters(lastDeviceRows, el('devFilterStaff').value, el('devFilterFrom').value, el('devFilterTo').value), 'No device records. Import a file or run Sync Now.'));
    }
    for (const id of ['dbFilterStaff','dbFilterFrom','dbFilterTo']) {
      const x = el(id);
      if (!x) continue;
      x.addEventListener('input', () => renderTable('dbBody', applyFilters(lastDbRows, el('dbFilterStaff').value, el('dbFilterFrom').value, el('dbFilterTo').value), 'No database records or Supabase not configured.'));
      x.addEventListener('change', () => renderTable('dbBody', applyFilters(lastDbRows, el('dbFilterStaff').value, el('dbFilterFrom').value, el('dbFilterTo').value), 'No database records or Supabase not configured.'));
    }
    const staffFilterEl = el('staffFilter');
    if (staffFilterEl) {
      staffFilterEl.addEventListener('input', () => renderStaffTable(applyStaffFilters(lastStaffRows, staffFilterEl.value), 'No staff records.'));
      staffFilterEl.addEventListener('change', () => renderStaffTable(applyStaffFilters(lastStaffRows, staffFilterEl.value), 'No staff records.'));
    }
    const shiftFilterEl = el('shiftFilter');
    if (shiftFilterEl) {
      shiftFilterEl.addEventListener('input', () => refreshShiftPatterns());
      shiftFilterEl.addEventListener('change', () => refreshShiftPatterns());
    }
    const devCsvBtn = el('devCsv');
    if (devCsvBtn) devCsvBtn.addEventListener('click', () => {
      const filtered = applyAttlogFilters(lastDeviceRows, el('devFilterStaff').value, el('devFilterFrom').value, el('devFilterTo').value);
      downloadAttlogCsv('1_attlog.csv', filtered);
    });
    const dbCsvBtn = el('dbCsv');
    if (dbCsvBtn) dbCsvBtn.addEventListener('click', () => {
      const filtered = applyFilters(lastDbRows, el('dbFilterStaff').value, el('dbFilterFrom').value, el('dbFilterTo').value);
      downloadCsv('db_records.csv', filtered);
    });
    const staffExportBtn = el('staffExport');
    if (staffExportBtn) staffExportBtn.addEventListener('click', () => {
      const filtered = applyStaffFilters(lastStaffRows, el('staffFilter')?.value || '');
      downloadStaffCsv('staff_records.csv', filtered);
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
        const row = {
          user_id: userId,
          full_name: String(vals.full_name || '').trim(),
          role: String(vals.role || '').trim(),
          department: String(vals.department || '').trim(),
          status: String(vals.status || '').trim(),
          date_joined: String(vals.date_joined || '').trim(),
          shift_pattern: String(vals.shift_pattern || '').trim(),
        };
        lastStaffRows = lastStaffRows.concat([row]).sort((a, b) => String(a.user_id ?? '').localeCompare(String(b.user_id ?? '')));
        await saveStaffRows(lastStaffRows);
        await refreshStaffRecords();
        if (vals.provision_to_device) await provisionStaffToDevice(row);
      });
    });

    const staffBodyEl = el('staffBody');
    if (staffBodyEl) {
      staffBodyEl.addEventListener('change', (e) => {
        const t = e && e.target;
        if (t && t.classList && t.classList.contains('staffChk')) updateStaffDeleteEnabled();
      });
      staffBodyEl.addEventListener('click', (e) => {
        const t = e && e.target;
        if (t && t.classList && t.classList.contains('staffProvisionRow')) {
          const id = String(t.getAttribute('data-id') || '').trim();
          if (!id) return;
          const existing = lastStaffRows.find(r => String(r.user_id ?? '').trim() === id);
          if (!existing) return;
          provisionStaffToDevice(existing);
          return;
        }
        if (!t || !t.classList || !t.classList.contains('staffEditRow')) return;
        const id = String(t.getAttribute('data-id') || '').trim();
        if (!id) return;
        const existing = lastStaffRows.find(r => String(r.user_id ?? '').trim() === id);
        if (!existing) return;
        refreshShiftPatterns().then(() => openModal('Edit Staff', staffFields('edit'), existing, async (vals) => {
          lastStaffRows = lastStaffRows.map(r => {
            if (String(r.user_id ?? '').trim() !== id) return r;
            return {
              user_id: id,
              full_name: String(vals.full_name || '').trim(),
              role: String(vals.role || '').trim(),
              department: String(vals.department || '').trim(),
              status: String(vals.status || '').trim(),
              date_joined: String(vals.date_joined || '').trim(),
              shift_pattern: String(vals.shift_pattern || '').trim(),
            };
          });
          await saveStaffRows(lastStaffRows);
          await refreshStaffRecords();
        }));
      });
    }

    const staffDeleteBtn = el('staffDelete');
    if (staffDeleteBtn) staffDeleteBtn.addEventListener('click', async () => {
      const ids = getCheckedDataIds('staffBody', 'staffChk');
      if (!ids.length) return;
      if (!confirm('Delete ' + ids.length + ' staff record(s)?')) return;
      lastStaffRows = (lastStaffRows || []).filter(r => !ids.includes(String(r.user_id ?? '').trim()));
      await saveStaffRows(lastStaffRows);
      await refreshStaffRecords();
    });

    const shiftAddBtn = el('shiftAdd');
    if (shiftAddBtn) shiftAddBtn.addEventListener('click', () => {
      openModal('Add Shift Pattern', shiftFields('add'), {}, async (vals) => {
        const pattern = String(vals.pattern || '').trim();
        if (!pattern) throw new Error('Pattern is required');
        if (lastShiftRows.some(r => String(r.pattern ?? '').trim() === pattern)) throw new Error('Pattern already exists');
        const row = {
          pattern,
          workingDays: String(vals.workingDays || '').trim(),
          workingHours: String(vals.workingHours || '').trim(),
          break: String(vals.break || '').trim(),
          notes: String(vals.notes || '').trim(),
        };
        lastShiftRows = lastShiftRows.concat([row]).sort((a, b) => String(a.pattern ?? '').localeCompare(String(b.pattern ?? '')));
        await saveShiftRows(lastShiftRows);
        await refreshShiftPatterns();
      });
    });

    const shiftBodyEl = el('shiftBody');
    if (shiftBodyEl) {
      shiftBodyEl.addEventListener('change', (e) => {
        const t = e && e.target;
        if (t && t.classList && t.classList.contains('shiftChk')) updateShiftDeleteEnabled();
      });
      shiftBodyEl.addEventListener('click', (e) => {
        const t = e && e.target;
        if (!t || !t.classList || !t.classList.contains('shiftEditRow')) return;
        const id = String(t.getAttribute('data-id') || '').trim();
        if (!id) return;
        const existing = lastShiftRows.find(r => String(r.pattern ?? '').trim() === id);
        if (!existing) return;
        openModal('Edit Shift Pattern', shiftFields('edit'), existing, async (vals) => {
          lastShiftRows = lastShiftRows.map(r => {
            if (String(r.pattern ?? '').trim() !== id) return r;
            return {
              pattern: id,
              workingDays: String(vals.workingDays || '').trim(),
              workingHours: String(vals.workingHours || '').trim(),
              break: String(vals.break || '').trim(),
              notes: String(vals.notes || '').trim(),
            };
          });
          await saveShiftRows(lastShiftRows);
          await refreshShiftPatterns();
        });
      });
    }

    const shiftDeleteBtn = el('shiftDelete');
    if (shiftDeleteBtn) shiftDeleteBtn.addEventListener('click', async () => {
      const ids = getCheckedDataIds('shiftBody', 'shiftChk');
      if (!ids.length) return;
      if (!confirm('Delete ' + ids.length + ' shift pattern(s)?')) return;
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
      let body = { autoSyncEnabled, scheduleLocalTimes };
      if (!autoSyncEnabled) {
        const dashboardRefreshMinutes = parseInt(el('setDashRefresh')?.value || '10', 10);
        body = { autoSyncEnabled, dashboardRefreshMinutes, scheduleLocalTimes };
      }
      const r = await postJson('/api/settings', body);
      if (r && r.ok === false) out(r.error || 'Save failed');
      await refreshStatus();
    });

    const setAutoSel = el('setAuto');
    if (setAutoSel) setAutoSel.addEventListener('change', () => {
      const on = setAutoSel.value === 'true';
      const dashLabel = el('setDashRefreshLabel');
      const dashInput = el('setDashRefresh');
      if (dashLabel) dashLabel.style.display = on ? 'none' : '';
      if (dashInput) dashInput.style.display = on ? 'none' : '';
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
      el('supaTestResult').textContent = (r && r.ok === false) ? ('Save failed: ' + String(r.error || 'error')) : 'Saved.';
      await refreshStatus();
    });

    const testSupabaseBtn = el('testSupabase');
    if (testSupabaseBtn) testSupabaseBtn.addEventListener('click', async () => {
      el('supaTestResult').textContent = 'Testing...';
      try {
        const r = await postJson('/api/supabase/test', {});
        if (r.ok) el('supaTestResult').textContent = 'Connection OK (' + (r.rttMs ?? '-') + 'ms)';
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

    const saveEnvBtn = el('saveEnv');
    if (saveEnvBtn) saveEnvBtn.addEventListener('click', async () => {
      const supabaseUrl = el('supaUrl').value.trim();
      const supabaseAnonKey = el('supaPubKey').value.trim();
      const supabaseProjectId = el('supaProjectId').value.trim();
      const r = await postJson('/api/env/write', { supabaseUrl, supabaseAnonKey, supabaseProjectId });
      el('supaTestResult').textContent = (r && r.ok) ? 'Saved .env.local' : ('Failed: ' + (r && r.error ? r.error : 'error'));
    });

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
