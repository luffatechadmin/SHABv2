using System.Globalization;
using System.Diagnostics;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

sealed record AppConfig(
  string DeviceIp,
  int DevicePort,
  int MachineNumber,
  int CommPassword,
  string DeviceId,
  int PollIntervalSeconds,
  int BackfillMinutes,
  string StaffIdPrefix,
  int StaffIdPadWidth,
  bool AutoCreateStaff,
  bool SyncStaffNames,
  bool FilterToDeviceUsers,
  DateOnly? MinEventDate,
  int MaxStaffNumber,
  string WatermarkSource,
  string TimeCorrectionMode,
  int MaxTimeOffsetHours,
  bool SetDeviceTime,
  string BadDateTimeMode,
  int MinValidYear,
  int MaxValidYear,
  string ReaderMode,
  bool Debug,
  bool DryRun,
  string SupabaseUrl,
  string SupabaseServiceRoleKey,
  string SupabaseAttendanceTable,
  bool SupabaseSyncEnabled
);

sealed record Punch(string StaffId, DateTimeOffset OccurredAtUtc, string EventDate, string DeviceId, int VerifyMode = 255);

sealed record DashboardSettings(
  string SupabaseUrl,
  string SupabaseKeyProtectedBase64,
  bool SupabaseKeyIsProtected,
  string SupabaseAttendanceTable,
  bool SupabaseSyncEnabled,
  string SupabaseAnonKey = "",
  string SupabaseProjectId = "",
  string SupabaseJwtSecret = ""
);

sealed record DevicePreset(string DeviceIp, int DevicePort, string ReaderMode, DateTimeOffset SavedAtUtc, DateTimeOffset? LastOkAtUtc);
sealed record PollingPreset(int PollIntervalSeconds, bool AutoSyncEnabled, DateTimeOffset SavedAtUtc);
sealed record DbPreset(DashboardSettings Settings, DateTimeOffset SavedAtUtc, DateTimeOffset? LastOkAtUtc);
sealed record ShiftPatternRow(string Pattern, string WorkingDays, string WorkingHours, string Break, string Notes);

sealed record PollState(
  DateTimeOffset? LastSeenOccurredAtUtc,
  DashboardSettings? DashboardSettings,
  Punch[]? DevicePunches,
  ShiftPatternRow[]? ShiftPatterns = null,
  DevicePreset[]? DevicePresets = null,
  PollingPreset[]? PollingPresets = null,
  DbPreset[]? DbPresets = null,
  string[]? SyncScheduleLocalTimes = null,
  int DashboardRefreshSeconds = 600,
  ProcessedFileEntry[]? ProcessedFiles = null,
  ConfiguredDeviceEntry[]? ConfiguredDevices = null
);

sealed record ProcessedFileEntry(
  string DeviceId,
  string FileName,
  long SizeBytes,
  long LastWriteUtcTicks,
  DateTimeOffset ProcessedAtUtc
);

sealed record ConfiguredDeviceEntry(
  string DeviceId,
  string DeviceType,
  string DeviceIp,
  int DevicePort,
  string ReaderMode,
  string LogDir = "",
  string FilePattern = "",
  DateTimeOffset? LastOkAtUtc = null,
  DateTimeOffset SavedAtUtc = default
);

static partial class Program
{
  private sealed record AttlogRow(string StaffId, string DateTime, string Verified, string Status, string Workcode, string Reserved);

  private static readonly JsonSerializerOptions JsonOptions = new()
  {
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    WriteIndented = true,
  };

  private static void LogInfo(string message) => Console.WriteLine("[INFO] " + (message ?? string.Empty));
  private static void LogWarn(string message) => Console.WriteLine("[WARN] " + (message ?? string.Empty));
  private static void LogError(string message) => Console.Error.WriteLine("[ERROR] " + (message ?? string.Empty));
  private static void LogDebug(AppConfig config, string message)
  {
    if (config.Debug) Console.WriteLine("[DEBUG] " + (message ?? string.Empty));
  }

  private static async Task TryWriteSupabaseLogSummary(AppConfig config, string level, string message, object? meta)
  {
    try
    {
      var table = (Environment.GetEnvironmentVariable("WL10_SUPABASE_LOGS_TABLE") ?? "middleware_logs").Trim();
      if (table.Length == 0) table = "middleware_logs";
      if (string.IsNullOrWhiteSpace(config.SupabaseUrl) || string.IsNullOrWhiteSpace(config.SupabaseServiceRoleKey)) return;

      using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };
      var url = $"{config.SupabaseUrl.TrimEnd('/')}/rest/v1/{table}";
      var row = new
      {
        device_id = config.DeviceId,
        level = (level ?? "INFO").Trim(),
        message = (message ?? string.Empty).Trim(),
        meta
      };

      using var req = new HttpRequestMessage(HttpMethod.Post, url);
      req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");
      req.Headers.TryAddWithoutValidation("Prefer", "return=minimal");
      req.Content = new StringContent(JsonSerializer.Serialize(new[] { row }, JsonOptions), Encoding.UTF8, "application/json");
      using var resp = await http.SendAsync(req);
      if (!resp.IsSuccessStatusCode) return;
      await TryEnforceSupabaseLogRetention(http, config, table);
    }
    catch { }
  }

  private static async Task TryEnforceSupabaseLogRetention(HttpClient http, AppConfig config, string table)
  {
    try
    {
      var baseUrl = config.SupabaseUrl.TrimEnd('/');
      var nthUrl = $"{baseUrl}/rest/v1/{table}?select=id&order=id.desc&limit=1&offset=1999";
      using var nthReq = new HttpRequestMessage(HttpMethod.Get, nthUrl);
      nthReq.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
      nthReq.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");
      using var nthResp = await http.SendAsync(nthReq);
      if (!nthResp.IsSuccessStatusCode) return;
      var nthBody = await nthResp.Content.ReadAsStringAsync();
      using var nthDoc = JsonDocument.Parse(nthBody);
      if (nthDoc.RootElement.ValueKind != JsonValueKind.Array) return;
      var first = nthDoc.RootElement.GetArrayLength() > 0 ? nthDoc.RootElement[0] : default;
      if (first.ValueKind != JsonValueKind.Object) return;
      var idEl = first.TryGetProperty("id", out var idProp) ? idProp : default;
      if (idEl.ValueKind != JsonValueKind.Number) return;
      if (!idEl.TryGetInt64(out var cutoffId) || cutoffId <= 0) return;

      var delUrl = $"{baseUrl}/rest/v1/{table}?id=lt.{cutoffId}";
      using var delReq = new HttpRequestMessage(HttpMethod.Delete, delUrl);
      delReq.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
      delReq.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");
      delReq.Headers.TryAddWithoutValidation("Prefer", "return=minimal");
      _ = await http.SendAsync(delReq);
    }
    catch { }
  }

  internal static int LastRunPunchCount;
  internal static int LastRunUpsertedCount;
  internal static int LastRunSkippedUpsertCount;
  internal static Punch[] LastRunPunches = Array.Empty<Punch>();
  internal static Punch[] DevicePunches = Array.Empty<Punch>();
  private static int ZkComNotRegisteredWarned;

  private static string? TryResolveAttlogExportPath()
  {
    var deviceId = (Environment.GetEnvironmentVariable("WL10_DEVICE_ID") ?? string.Empty).Trim();
    return TryResolveAttlogExportPath(deviceId);
  }

  private static string? TryResolveAttlogExportPath(string? deviceId)
  {
    var exportPath = (Environment.GetEnvironmentVariable("WL10_ATTLOG_EXPORT_PATH") ?? "").Trim();
    var filePath = (Environment.GetEnvironmentVariable("WL10_ATTLOG_FILE_PATH") ?? "").Trim();
    var p = exportPath.Length > 0 ? exportPath : filePath;
    if (p.Length > 0)
    {
      try
      {
        var full = Path.GetFullPath(p);
        var dir = Path.GetDirectoryName(full);
        if (!string.IsNullOrWhiteSpace(dir) && (Directory.Exists(dir) || File.Exists(full))) return full;
      }
      catch { }
    }

    try
    {
      static string? FindReferenceDir(string startDir)
      {
        try
        {
          var d = new DirectoryInfo(startDir);
          string? firstFound = null;
          for (var i = 0; i < 25 && d is not null; i++)
          {
            if (d.Name.Equals("SHAB Attendance System", StringComparison.OrdinalIgnoreCase))
            {
              var direct = Path.Combine(d.FullName, "Reference");
              if (Directory.Exists(direct)) return direct;
            }

            var shabRef = Path.Combine(d.FullName, "SHAB Attendance System", "Reference");
            if (Directory.Exists(shabRef)) return shabRef;

            var candidate = d.Name.Equals("SHAB Attendance System", StringComparison.OrdinalIgnoreCase)
              ? Path.Combine(d.FullName, "Reference")
              : Path.Combine(d.FullName, "Reference");
            if (Directory.Exists(candidate))
            {
              firstFound ??= candidate;
              if (File.Exists(Path.Combine(candidate, "1_attlog.dat"))) return candidate;
              var shabRoot = Path.GetDirectoryName(candidate);
              if (!string.IsNullOrWhiteSpace(shabRoot) && shabRoot.Contains("SHAB Attendance System", StringComparison.OrdinalIgnoreCase))
              {
                firstFound = candidate;
              }
            }
            d = d.Parent;
          }
          return firstFound;
        }
        catch { }
        return null;
      }

      static string SafeFileName(string raw)
      {
        var s = (raw ?? string.Empty).Trim();
        if (s.Length == 0) return "device";
        var bad = Path.GetInvalidFileNameChars();
        var sb = new StringBuilder(capacity: s.Length);
        foreach (var ch in s)
        {
          sb.Append(bad.Contains(ch) ? '_' : ch);
        }
        var norm = sb.ToString().Trim('_', ' ');
        if (norm.Length == 0) norm = "device";
        if (norm.Length > 80) norm = norm[..80];
        return norm;
      }

      var refDir = FindReferenceDir(Directory.GetCurrentDirectory());
      refDir ??= FindReferenceDir(AppContext.BaseDirectory);
      if (refDir is not null)
      {
        var id = (deviceId ?? string.Empty).Trim();
        var isWl10 = id.Length == 0 || id.StartsWith("WL10", StringComparison.OrdinalIgnoreCase);
        if (isWl10) return Path.Combine(refDir, "1_attlog.dat");
        return Path.Combine(refDir, $"attlog_{SafeFileName(id)}.dat");
      }
    }
    catch { }

    return null;
  }

  private static void TryExportAttlogFile(string? path, IEnumerable<Punch> punches)
  {
    if (string.IsNullOrWhiteSpace(path)) return;
    try
    {
      var full = Path.GetFullPath(path);
      var dir = Path.GetDirectoryName(full);
      if (string.IsNullOrWhiteSpace(dir)) return;
      Directory.CreateDirectory(dir);

      static bool TryParseDateOnly(string raw, out DateOnly date)
      {
        return DateOnly.TryParseExact(raw.Trim(), "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out date);
      }

      var startDateRaw = (Environment.GetEnvironmentVariable("WL10_ATTLOG_EXPORT_START_DATE") ?? "").Trim();
      var endDateRaw = (Environment.GetEnvironmentVariable("WL10_ATTLOG_EXPORT_END_DATE") ?? "").Trim();

      var startDate = new DateOnly(2025, 1, 1);
      var endDate = DateOnly.FromDateTime(DateTime.Now);

      var hasStart = true;
      if (startDateRaw.Length > 0)
      {
        hasStart = TryParseDateOnly(startDateRaw, out startDate);
        if (!hasStart) { startDate = new DateOnly(2025, 1, 1); hasStart = true; }
      }

      var hasEnd = true;
      if (endDateRaw.Length > 0)
      {
        hasEnd = TryParseDateOnly(endDateRaw, out endDate);
        if (!hasEnd) { endDate = DateOnly.FromDateTime(DateTime.Now); hasEnd = true; }
      }

      if (hasStart && hasEnd && endDate < startDate)
      {
        var tmp = startDate;
        startDate = endDate;
        endDate = tmp;
      }

      using var sw = new StreamWriter(full, append: false, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
      foreach (var p in punches.OrderBy(x => x.OccurredAtUtc))
      {
        var local = p.OccurredAtUtc.ToLocalTime().DateTime;
        if (hasStart || hasEnd)
        {
          var d = DateOnly.FromDateTime(local);
          if (hasStart && d < startDate) continue;
          if (hasEnd && d > endDate) continue;
        }
        var verify = p.VerifyMode;
        if (verify < 0 || verify > 255) verify = 255;
        sw.WriteLine($"{p.StaffId}\t{local:yyyy-MM-dd HH:mm:ss}\t1\t{verify}\t1\t0");
      }
    }
    catch (Exception ex)
    {
      Console.Error.WriteLine($"Attlog export failed: {ex.Message}");
    }
  }

  private static string ResolveStatePath()
  {
    var env = (Environment.GetEnvironmentVariable("WL10_STATE_PATH") ?? string.Empty).Trim();
    if (!string.IsNullOrWhiteSpace(env)) return env;

    var legacy = Path.Combine(AppContext.BaseDirectory, "state.json");
    static bool TryEnsureAtomicWritable(string path)
    {
      try
      {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir);
        using var fs = File.Open(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);
        var tmp = path + ".tmpcheck";
        try
        {
          using var tfs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
        }
        finally
        {
          try { if (File.Exists(tmp)) File.Delete(tmp); } catch { }
        }
        return true;
      }
      catch
      {
        return false;
      }
    }
    try
    {
      var local = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
      var legacyDir = Path.Combine(local, "WL10Middleware");
      var curDir = Path.Combine(local, "SHABMiddleware");
      var dir = Directory.Exists(legacyDir) ? legacyDir : curDir;
      Directory.CreateDirectory(dir);
      var stable = Path.Combine(dir, "state.json");
      try
      {
        if (!File.Exists(stable) && File.Exists(legacy))
        {
          File.Copy(legacy, stable);
        }
      }
      catch { }
      var stableOk = TryEnsureAtomicWritable(stable);
      var legacyOk = TryEnsureAtomicWritable(legacy);

      if (stableOk) return stable;
      if (legacyOk)
      {
        try
        {
          if (File.Exists(stable) && !File.Exists(legacy))
          {
            File.Copy(stable, legacy);
          }
        }
        catch { }
        return legacy;
      }

      return stable;
    }
    catch
    {
      return legacy;
    }
  }

  public static async Task<int> Main(string[] args)
  {
    try
    {
      var dryRun = args.Any(a => string.Equals(a, "--dry-run", StringComparison.OrdinalIgnoreCase));
      var config = LoadConfig(dryRun);
      var once = args.Any(a => string.Equals(a, "--once", StringComparison.OrdinalIgnoreCase));
      var clearAttendance = args.Any(a => string.Equals(a, "--clear-attendance", StringComparison.OrdinalIgnoreCase));
      var importAttlogPath = GetArgValue(args, "--import-attlog");
      var verify = args.Any(a => string.Equals(a, "--verify", StringComparison.OrdinalIgnoreCase));
      var today = args.Any(a => string.Equals(a, "--today", StringComparison.OrdinalIgnoreCase));
      var dashboard = args.Any(a => string.Equals(a, "--dashboard", StringComparison.OrdinalIgnoreCase));
      var statePath = ResolveStatePath();

      if (dashboard)
      {
        await RunDashboard(config, statePath, args);
        return 0;
      }
      Console.WriteLine("Dashboard is not running in this mode. To start the web UI, run with: --dashboard (default port 5099).");

      if (clearAttendance)
      {
        if (!config.DryRun)
        {
          await ClearSupabaseAttendanceForDevice(config);
        }
        ResetLocalWatermarkState(statePath);
      }

      if (!string.IsNullOrWhiteSpace(importAttlogPath))
      {
        await ImportAttendanceFromFile(config, importAttlogPath, verify);
        return 0;
      }

      if (once)
      {
        await RunOnce(config, statePath, verify, today);
        return 0;
      }

      while (true)
      {
        try
        {
          await RunOnce(config, statePath, verify, today);
        }
        catch (Exception ex)
        {
          LogError("Poll error: " + ex.Message);
          LogDebug(config, ex.ToString());
          await TryWriteSupabaseLogSummary(config, "ERROR", "Poll error", new
          {
            device_ip = config.DeviceIp,
            device_port = config.DevicePort,
            error = ex.Message,
            error_type = ex.GetType().FullName,
          });
        }

        await Task.Delay(TimeSpan.FromSeconds(config.PollIntervalSeconds));
      }
    }
    catch (Exception ex)
    {
      Console.Error.WriteLine(ex);
      return 1;
    }
  }

  private static void ResetLocalWatermarkState(string statePath)
  {
    try
    {
      if (!File.Exists(statePath)) return;
      var state = LoadState(statePath);
      SaveState(statePath, state with { LastSeenOccurredAtUtc = null });
    }
    catch { }
  }

  private static string? GetArgValue(string[] args, string name)
  {
    if (args.Length == 0) return null;
    for (var i = 0; i < args.Length; i++)
    {
      var a = args[i] ?? string.Empty;
      if (a.Equals(name, StringComparison.OrdinalIgnoreCase))
      {
        if (i + 1 < args.Length) return args[i + 1];
        return null;
      }

      if (a.StartsWith(name + "=", StringComparison.OrdinalIgnoreCase))
      {
        return a.Substring(name.Length + 1);
      }
    }
    return null;
  }

  private static async Task ImportAttendanceFromFile(AppConfig config, string filePath, bool verify)
  {
    var fullPath = Path.GetFullPath(filePath);
    if (!File.Exists(fullPath)) throw new FileNotFoundException("Attendance log file not found.", fullPath);

    var importTimeKind = (Environment.GetEnvironmentVariable("WL10_IMPORT_ATTLOG_TIME_KIND") ?? "utc").Trim();
    var assumeUtc = importTimeKind.Equals("utc", StringComparison.OrdinalIgnoreCase);

    var results = new List<Punch>(capacity: 1024);
    var raw = 0;
    var skipped = 0;

    var hostNowLocal = DateTime.Now;
    var deviceNowLocal = hostNowLocal;

    foreach (var line in File.ReadLines(fullPath))
    {
      raw++;
      if (!TryParseAttlogTextLine(config, line, hostNowLocal, deviceNowLocal, assumeUtc, out var punch))
      {
        skipped++;
        continue;
      }
      results.Add(punch);
    }

    var distinct = results
      .GroupBy(p => BuildEventId(p.DeviceId, p.StaffId, p.OccurredAtUtc), StringComparer.Ordinal)
      .Select(g => g.First())
      .OrderBy(p => p.OccurredAtUtc)
      .ToList();

    Console.WriteLine($"Imported punches from file: raw_lines={raw} kept={distinct.Count} skipped={skipped} path='{fullPath}'");

    if (config.Debug)
    {
      foreach (var p in distinct.Take(50))
      {
        Console.WriteLine($"Punch: staff_id={p.StaffId} occurred_at_utc={p.OccurredAtUtc:O} event_date={p.EventDate} device_id={p.DeviceId}");
      }
    }

    if (distinct.Count == 0)
    {
      Console.WriteLine("No punches to upsert.");
      return;
    }

    if (config.DryRun)
    {
      Console.WriteLine($"Dry run: would upsert {distinct.Count} punches to Supabase.");
      return;
    }

    if (!config.SupabaseSyncEnabled)
    {
      Console.WriteLine($"Supabase sync disabled: skipping upsert of {distinct.Count} punches.");
      return;
    }

    await UpsertToSupabase(config, distinct, new Dictionary<string, string>(StringComparer.Ordinal));

    if (verify)
    {
      await VerifySupabaseAttendanceEventsForDevice(config, limit: 50);
    }
  }

  private static List<Punch>? TryReadAttlogPunchesForSync(AppConfig config, string? filePath, DateTimeOffset? afterUtc, bool onlyToday, HashSet<string>? restrictToStaffIds)
  {
    if (string.IsNullOrWhiteSpace(filePath)) return null;
    string fullPath;
    try { fullPath = Path.GetFullPath(filePath); } catch { return null; }
    if (!File.Exists(fullPath)) return null;

    var results = new List<Punch>(capacity: 1024);
    var hostNowLocal = DateTime.Now;
    var deviceNowLocal = hostNowLocal;
    var assumeUtc = false;

    foreach (var line in File.ReadLines(fullPath))
    {
      if (!TryParseAttlogTextLine(config, line, hostNowLocal, deviceNowLocal, assumeUtc, out var punch)) continue;
      if (restrictToStaffIds is not null && !restrictToStaffIds.Contains(punch.StaffId)) continue;
      if (afterUtc is not null && punch.OccurredAtUtc <= afterUtc.Value) continue;
      if (onlyToday)
      {
        var localDate = DateOnly.FromDateTime(punch.OccurredAtUtc.ToLocalTime().DateTime).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        if (!string.Equals(localDate, punch.EventDate, StringComparison.Ordinal)) continue;
      }
      results.Add(punch);
    }

    return results
      .GroupBy(p => BuildEventId(p.DeviceId, p.StaffId, p.OccurredAtUtc), StringComparer.Ordinal)
      .Select(g => g.First())
      .OrderBy(p => p.OccurredAtUtc)
      .ToList();
  }

  private static bool TryParseAttlogTextLine(AppConfig config, string? line, DateTime hostNowLocal, DateTime deviceNowLocal, bool assumeUtc, out Punch punch)
  {
    punch = default!;
    var s = (line ?? string.Empty).Trim();
    if (s.Length == 0) return false;

    var parts = s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
    if (parts.Length < 3) return false;

    var staffRaw = parts[0];
    if (string.IsNullOrWhiteSpace(staffRaw)) return false;

    var staffId = MapStaffId(config, staffRaw);
    if (string.IsNullOrWhiteSpace(staffId)) return false;

    string dtText;
    if (parts.Length >= 3 && parts[1].Length == 10 && parts[2].Length == 8)
    {
      dtText = parts[1] + " " + parts[2];
    }
    else
    {
      dtText = parts[1];
    }

    if (!DateTime.TryParseExact(
          dtText,
          "yyyy-MM-dd HH:mm:ss",
          CultureInfo.InvariantCulture,
          DateTimeStyles.AllowWhiteSpaces,
          out var dt
        ))
    {
      if (!DateTime.TryParse(dtText, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out dt))
      {
        return false;
      }
    }

    var dtUnspecified = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Unspecified);
    var sanitizedLocalDt = SanitizePunchLocalDateTime(config, dtUnspecified, deviceNowLocal, hostNowLocal);
    if (sanitizedLocalDt is not DateTime corrected) return false;

    DateTimeOffset occurredAtUtc;
    DateOnly eventLocalDate;
    if (assumeUtc)
    {
      occurredAtUtc = new DateTimeOffset(DateTime.SpecifyKind(corrected, DateTimeKind.Utc));
      eventLocalDate = DateOnly.FromDateTime(corrected);
    }
    else
    {
      var localDt = DateTime.SpecifyKind(corrected, DateTimeKind.Local);
      var localOffset = new DateTimeOffset(localDt);
      occurredAtUtc = localOffset.ToUniversalTime();
      eventLocalDate = DateOnly.FromDateTime(localOffset.LocalDateTime);
    }

    if (config.MinEventDate is not null && eventLocalDate < config.MinEventDate.Value) return false;

    var verify = 255;
    if (parts.Length >= 5)
    {
      _ = int.TryParse(parts[4], NumberStyles.Integer, CultureInfo.InvariantCulture, out verify);
    }
    if (verify < 0 || verify > 255) verify = 255;

    var eventDate = eventLocalDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    punch = new Punch(staffId, occurredAtUtc, eventDate, config.DeviceId, verify);
    return true;
  }

  private static async Task ClearSupabaseAttendanceForDevice(AppConfig config)
  {
    if (!config.SupabaseSyncEnabled) throw new InvalidOperationException("Supabase sync disabled (WL10_SUPABASE_SYNC_ENABLED=0).");
    if (string.IsNullOrWhiteSpace(config.SupabaseUrl)) throw new InvalidOperationException("Missing Supabase URL.");
    if (string.IsNullOrWhiteSpace(config.SupabaseServiceRoleKey)) throw new InvalidOperationException("Missing Supabase API key.");

    using var http = new HttpClient();
    http.Timeout = TimeSpan.FromSeconds(30);

    var deviceId = Uri.EscapeDataString(config.DeviceId);
    var url = $"{config.SupabaseUrl.TrimEnd('/')}/rest/v1/{config.SupabaseAttendanceTable}?device_id=eq.{deviceId}";

    using var req = new HttpRequestMessage(HttpMethod.Delete, url);
    req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
    req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");
    req.Headers.TryAddWithoutValidation("Prefer", "count=exact,return=minimal");

    using var resp = await http.SendAsync(req);
    if (!resp.IsSuccessStatusCode)
    {
      var body = await resp.Content.ReadAsStringAsync();
      throw new InvalidOperationException($"Supabase delete attendance_events failed: {(int)resp.StatusCode} {resp.ReasonPhrase}. Body={body}");
    }

    var contentRange = resp.Headers.TryGetValues("Content-Range", out var ranges) ? ranges.FirstOrDefault() : null;
    Console.WriteLine($"Cleared Supabase attendance_events for device_id='{config.DeviceId}'. Deleted={contentRange ?? "(unknown)"}");
  }

  private static async Task VerifySupabaseAttendanceEventsForDevice(AppConfig config, int limit)
  {
    if (!config.SupabaseSyncEnabled) return;
    if (string.IsNullOrWhiteSpace(config.SupabaseUrl) || string.IsNullOrWhiteSpace(config.SupabaseServiceRoleKey)) return;

    using var http = new HttpClient();
    http.Timeout = TimeSpan.FromSeconds(15);

    var url = $"{config.SupabaseUrl.TrimEnd('/')}/rest/v1/{config.SupabaseAttendanceTable}?select=staff_id,device_id,datetime,occurred_at,event_date,verified,status,workcode,reserved,created_at&order=occurred_at.desc&limit={limit}";

    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
    req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");

    using var resp = await http.SendAsync(req);
    var body = await resp.Content.ReadAsStringAsync();
    if (!resp.IsSuccessStatusCode)
    {
      throw new InvalidOperationException($"Supabase verify failed: {(int)resp.StatusCode} {resp.ReasonPhrase}. Body={body}");
    }

    using var doc = JsonDocument.Parse(body);
    if (doc.RootElement.ValueKind != JsonValueKind.Array)
    {
      Console.WriteLine("Verify: unexpected response shape (expected JSON array).");
      return;
    }

    Console.WriteLine($"Verify: latest {limit} attendance_events");

    var rows = doc.RootElement.EnumerateArray().ToArray();
    if (rows.Length == 0)
    {
      Console.WriteLine("Verify: (none)");
      return;
    }

    var staffIds = rows
      .Select(el => el.TryGetProperty("staff_id", out var staffEl) && staffEl.ValueKind == JsonValueKind.String ? staffEl.GetString() : null)
      .OfType<string>()
      .Where(id => !string.IsNullOrWhiteSpace(id))
      .Distinct(StringComparer.Ordinal)
      .ToArray();

    var eventDates = rows
      .Select(el =>
        el.TryGetProperty("event_date", out var dEl) && dEl.ValueKind == JsonValueKind.String ? dEl.GetString()
        : (el.TryGetProperty("occurred_at", out var dtEl) && dtEl.ValueKind == JsonValueKind.String ? dtEl.GetString() : null)
      )
      .OfType<string>()
      .Where(d => !string.IsNullOrWhiteSpace(d) && d.Length >= 10)
      .Select(d => d.Trim()[..10])
      .Distinct(StringComparer.Ordinal)
      .ToArray();

    var staffNames = await FetchStaffNamesById(http, config, staffIds);
    var attendanceRecords = await FetchAttendanceRecordsByStaffAndDate(http, config, staffIds, eventDates);

    for (var i = 0; i < rows.Length; i++)
    {
      var el = rows[i];
      var staffId = el.TryGetProperty("staff_id", out var staffEl) && staffEl.ValueKind == JsonValueKind.String ? staffEl.GetString() : null;
      var occurredAt = el.TryGetProperty("occurred_at", out var oaEl) && oaEl.ValueKind == JsonValueKind.String ? oaEl.GetString() : null;
      var deviceId = el.TryGetProperty("device_id", out var didEl) && didEl.ValueKind == JsonValueKind.String ? didEl.GetString() : null;
      var datetime = el.TryGetProperty("datetime", out var dtEl) && dtEl.ValueKind == JsonValueKind.String ? dtEl.GetString() : null;
      var eventDate = el.TryGetProperty("event_date", out var edEl) && edEl.ValueKind == JsonValueKind.String ? edEl.GetString() : null;
      var verified = el.TryGetProperty("verified", out var vEl) && vEl.ValueKind == JsonValueKind.String ? vEl.GetString() : null;
      var status = el.TryGetProperty("status", out var stEl) && stEl.ValueKind == JsonValueKind.String ? stEl.GetString() : null;
      var workcode = el.TryGetProperty("workcode", out var wcEl) && wcEl.ValueKind == JsonValueKind.String ? wcEl.GetString() : null;
      var reserved = el.TryGetProperty("reserved", out var rsEl) && rsEl.ValueKind == JsonValueKind.String ? rsEl.GetString() : null;
      var createdAt = el.TryGetProperty("created_at", out var createdEl) && createdEl.ValueKind == JsonValueKind.String ? createdEl.GetString() : null;

      var name = staffId is not null && staffNames.TryGetValue(staffId, out var n) ? n : null;
      var recordKey = (staffId is null || eventDate is null) ? null : $"{staffId}|{eventDate}";
      var dayClockIn = recordKey is not null && attendanceRecords.TryGetValue(recordKey, out var rec) ? rec.ClockIn : null;
      var dayClockOut = recordKey is not null && attendanceRecords.TryGetValue(recordKey, out var rec2) ? rec2.ClockOut : null;

      Console.WriteLine($"{i + 1,2}. staff_id={staffId ?? "(null)"} staff_name={name ?? "(unknown)"} datetime={datetime ?? "(null)"} occurred_at={occurredAt ?? "(null)"} event_date={eventDate ?? "(null)"} device_id={deviceId ?? "(null)"} verified={verified ?? "(null)"} status={status ?? "(null)"} workcode={workcode ?? "(null)"} reserved={reserved ?? "(null)"} day_clock_in={dayClockIn ?? "(n/a)"} day_clock_out={dayClockOut ?? "(n/a)"} created_at={createdAt ?? "(null)"}");
    }
  }

  private static async Task<Dictionary<string, string>> FetchStaffNamesById(HttpClient http, AppConfig config, string[] staffIds)
  {
    var ids = staffIds.Where(id => !string.IsNullOrWhiteSpace(id)).Distinct(StringComparer.Ordinal).ToArray();
    if (ids.Length == 0) return new Dictionary<string, string>(StringComparer.Ordinal);

    var quoted = string.Join(",", ids.Select(id => $"\"{id.Replace("\"", "\"\"", StringComparison.Ordinal)}\""));
    var url = $"{config.SupabaseUrl.TrimEnd('/')}/rest/v1/staff?select=id,full_name&id=in.({quoted})";

    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
    req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");

    using var resp = await http.SendAsync(req);
    var body = await resp.Content.ReadAsStringAsync();
    if (!resp.IsSuccessStatusCode) return new Dictionary<string, string>(StringComparer.Ordinal);

    using var doc = JsonDocument.Parse(body);
    if (doc.RootElement.ValueKind != JsonValueKind.Array) return new Dictionary<string, string>(StringComparer.Ordinal);

    var map = new Dictionary<string, string>(StringComparer.Ordinal);
    foreach (var el in doc.RootElement.EnumerateArray())
    {
      if (!el.TryGetProperty("id", out var idEl) || idEl.ValueKind != JsonValueKind.String) continue;
      var id = idEl.GetString();
      if (string.IsNullOrWhiteSpace(id)) continue;

      var name = el.TryGetProperty("full_name", out var nameEl) && nameEl.ValueKind == JsonValueKind.String ? nameEl.GetString() : null;
      if (string.IsNullOrWhiteSpace(name)) continue;

      map[id] = name;
    }

    return map;
  }

  private sealed record AttendanceRecordRow(string StaffId, string Date, string? ClockIn, string? ClockOut);

  private static async Task<Dictionary<string, AttendanceRecordRow>> FetchAttendanceRecordsByStaffAndDate(HttpClient http, AppConfig config, string[] staffIds, string[] eventDates)
  {
    var ids = staffIds.Where(id => !string.IsNullOrWhiteSpace(id)).Distinct(StringComparer.Ordinal).ToArray();
    var dates = eventDates.Where(d => !string.IsNullOrWhiteSpace(d)).Distinct(StringComparer.Ordinal).ToArray();
    if (ids.Length == 0 || dates.Length == 0) return new Dictionary<string, AttendanceRecordRow>(StringComparer.Ordinal);

    var quotedIds = string.Join(",", ids.Select(id => $"\"{id.Replace("\"", "\"\"", StringComparison.Ordinal)}\""));
    var quotedDates = string.Join(",", dates.Select(d => $"\"{d.Replace("\"", "\"\"", StringComparison.Ordinal)}\""));

    var url = $"{config.SupabaseUrl.TrimEnd('/')}/rest/v1/attendance_records?select=staff_id,date,clock_in,clock_out&staff_id=in.({quotedIds})&date=in.({quotedDates})";

    using var req = new HttpRequestMessage(HttpMethod.Get, url);
    req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
    req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");

    using var resp = await http.SendAsync(req);
    var body = await resp.Content.ReadAsStringAsync();
    if (!resp.IsSuccessStatusCode) return new Dictionary<string, AttendanceRecordRow>(StringComparer.Ordinal);

    using var doc = JsonDocument.Parse(body);
    if (doc.RootElement.ValueKind != JsonValueKind.Array) return new Dictionary<string, AttendanceRecordRow>(StringComparer.Ordinal);

    var map = new Dictionary<string, AttendanceRecordRow>(StringComparer.Ordinal);
    foreach (var el in doc.RootElement.EnumerateArray())
    {
      var staffId = el.TryGetProperty("staff_id", out var staffEl) && staffEl.ValueKind == JsonValueKind.String ? staffEl.GetString() : null;
      var date = el.TryGetProperty("date", out var dateEl) && dateEl.ValueKind == JsonValueKind.String ? dateEl.GetString() : null;
      if (string.IsNullOrWhiteSpace(staffId) || string.IsNullOrWhiteSpace(date)) continue;

      var clockIn = el.TryGetProperty("clock_in", out var inEl) && inEl.ValueKind == JsonValueKind.String ? inEl.GetString() : null;
      var clockOut = el.TryGetProperty("clock_out", out var outEl) && outEl.ValueKind == JsonValueKind.String ? outEl.GetString() : null;

      var key = $"{staffId}|{date}";
      map[key] = new AttendanceRecordRow(staffId, date, clockIn, clockOut);
    }

    return map;
  }

  private static AppConfig LoadConfig(bool dryRun)
  {
    var dotenv = LoadDotEnv();

    string Env(string key, string fallback = "")
    {
      var v = Environment.GetEnvironmentVariable(key);
      if (!string.IsNullOrWhiteSpace(v)) return v.Trim();
      if (dotenv.TryGetValue(key, out var fromFile) && !string.IsNullOrWhiteSpace(fromFile)) return fromFile.Trim();
      return fallback;
    }

    bool EnvBool(string key, bool fallback)
    {
      var raw = Env(key);
      if (string.IsNullOrWhiteSpace(raw)) return fallback;
      return raw.Equals("1", StringComparison.OrdinalIgnoreCase)
        || raw.Equals("true", StringComparison.OrdinalIgnoreCase)
        || raw.Equals("yes", StringComparison.OrdinalIgnoreCase)
        || raw.Equals("y", StringComparison.OrdinalIgnoreCase)
        || raw.Equals("on", StringComparison.OrdinalIgnoreCase);
    }

    int EnvInt(string key, int fallback)
    {
      var raw = Env(key);
      return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : fallback;
    }

    DateOnly? EnvDateOnly(string key)
    {
      var raw = Env(key);
      if (string.IsNullOrWhiteSpace(raw)) return null;
      return DateOnly.TryParseExact(raw, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var d) ? d : null;
    }

    Dictionary<string, string> LoadDotEnv()
    {
      var result = new Dictionary<string, string>(StringComparer.Ordinal);

      foreach (var path in EnumerateDotEnvCandidatePaths())
      {
        if (!File.Exists(path)) continue;

        foreach (var rawLine in File.ReadAllLines(path))
        {
          var line = rawLine.Trim();
          if (line.Length == 0) continue;
          if (line.StartsWith('#')) continue;
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

      return result;
    }

    IEnumerable<string> EnumerateDotEnvCandidatePaths()
    {
      var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
      var starts = new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory };

      foreach (var start in starts)
      {
        var dir = start;
        for (var i = 0; i < 7; i++)
        {
          if (!string.IsNullOrWhiteSpace(dir))
          {
            var a = Path.GetFullPath(Path.Combine(dir, ".env.local"));
            if (seen.Add(a)) yield return a;

            var b = Path.GetFullPath(Path.Combine(dir, "SHAB Dashboard", ".env.local"));
            if (seen.Add(b)) yield return b;
          }

          var parent = Directory.GetParent(dir);
          if (parent is null) break;
          dir = parent.FullName;
        }
      }
    }

    var deviceIp = Env("WL10_IP", "192.168.1.170");
    var devicePort = EnvInt("WL10_PORT", 4370);
    var machineNumber = EnvInt("WL10_MACHINE_NUMBER", 1);
    var commPassword = EnvInt("WL10_COMM_PASSWORD", 0);
    var deviceId = Env("WL10_DEVICE_ID", $"WL10-{deviceIp}");
    var pollIntervalSeconds = EnvInt("WL10_POLL_INTERVAL_SECONDS", 3600);
    var backfillMinutes = EnvInt("WL10_BACKFILL_MINUTES", 5);
    var staffIdPrefix = Env("WL10_STAFF_ID_PREFIX", "");
    var staffIdPadWidth = EnvInt("WL10_STAFF_ID_PAD_WIDTH", 0);
    var autoCreateStaff = EnvBool("WL10_AUTO_CREATE_STAFF", false);
    var syncStaffNames = EnvBool("WL10_SYNC_STAFF_NAMES", true);
    var filterToDeviceUsers = EnvBool("WL10_FILTER_TO_DEVICE_USERS", true);
    var minEventDate = EnvDateOnly("WL10_MIN_EVENT_DATE");
    var maxStaffNumber = EnvInt("WL10_MAX_STAFF_NUMBER", 999999);
    var watermarkSource = Env("WL10_WATERMARK_SOURCE", "db");
    var timeCorrectionMode = Env("WL10_TIME_CORRECTION_MODE", "none");
    var maxTimeOffsetHours = EnvInt("WL10_MAX_TIME_OFFSET_HOURS", 48);
    var setDeviceTime = EnvBool("WL10_SET_DEVICE_TIME", false);
    var badDateTimeMode = Env("WL10_BAD_DATETIME_MODE", "device_date");
    var minValidYear = EnvInt("WL10_MIN_VALID_YEAR", 2015);
    var maxValidYear = EnvInt("WL10_MAX_VALID_YEAR", 0);
    var readerMode = Env("WL10_READER_MODE", "auto");
    var debug = EnvBool("WL10_DEBUG", false);
    if (dryRun) debug = true;

    var supabaseUrl = Env("SUPABASE_URL");
    if (string.IsNullOrWhiteSpace(supabaseUrl)) supabaseUrl = Env("VITE_SUPABASE_URL");

    var supabaseServiceRoleKey = Env("SUPABASE_SERVICE_ROLE_KEY");
    if (string.IsNullOrWhiteSpace(supabaseServiceRoleKey)) supabaseServiceRoleKey = Env("WL10_SUPABASE_API_KEY");

    var supabaseAttendanceTable = Env("WL10_SUPABASE_ATTENDANCE_TABLE", "attendance_events");
    var supabaseSyncEnabled = EnvBool("WL10_SUPABASE_SYNC_ENABLED", true);

    if (dryRun)
    {
      supabaseUrl = string.Empty;
      supabaseServiceRoleKey = string.Empty;
      supabaseSyncEnabled = false;
    }
    else if (supabaseSyncEnabled)
    {
      if (string.IsNullOrWhiteSpace(supabaseUrl) || string.IsNullOrWhiteSpace(supabaseServiceRoleKey))
      {
        Console.WriteLine("Supabase sync enabled but missing SUPABASE_URL and/or SUPABASE_SERVICE_ROLE_KEY. Disabling Supabase sync.");
        supabaseSyncEnabled = false;
      }
    }

    return new AppConfig(
      deviceIp,
      devicePort,
      machineNumber,
      commPassword,
      deviceId,
      pollIntervalSeconds,
      backfillMinutes,
      staffIdPrefix,
      staffIdPadWidth,
      autoCreateStaff,
      syncStaffNames,
      filterToDeviceUsers,
      minEventDate,
      maxStaffNumber,
      watermarkSource,
      timeCorrectionMode,
      maxTimeOffsetHours,
      setDeviceTime,
      badDateTimeMode,
      minValidYear,
      maxValidYear,
      readerMode,
      debug,
      dryRun,
      supabaseUrl,
      supabaseServiceRoleKey,
      supabaseAttendanceTable,
      supabaseSyncEnabled
    );
  }

  private static async Task RunOnce(AppConfig config, string statePath, bool verify, bool today)
  {
    LastRunPunchCount = 0;
    LastRunUpsertedCount = 0;
    LastRunSkippedUpsertCount = 0;
    LastRunPunches = Array.Empty<Punch>();
    var cycleSw = Stopwatch.StartNew();

    var priorState = LoadState(statePath);
    DevicePunches = priorState.DevicePunches ?? Array.Empty<Punch>();
    var watermarkLocal = priorState.LastSeenOccurredAtUtc;
    var nowUtc = DateTimeOffset.UtcNow;
    if (watermarkLocal is not null && watermarkLocal.Value > nowUtc.AddMinutes(5))
    {
      watermarkLocal = null;
    }
    var watermarkSource = (config.WatermarkSource ?? string.Empty).Trim();
    if (!config.SupabaseSyncEnabled)
    {
      if (watermarkSource.Length == 0 || watermarkSource.Equals("db", StringComparison.OrdinalIgnoreCase)) watermarkSource = "local";
    }

    var watermarkDb = (!config.DryRun && config.SupabaseSyncEnabled && NeedsDbWatermark(watermarkSource)) ? await LoadSupabaseWatermark(config) : null;
    var watermark = ResolveWatermark(watermarkSource, watermarkDb, watermarkLocal);
    if (watermark is not null) watermark = watermark.Value.AddMinutes(-Math.Abs(config.BackfillMinutes));
    if (today) watermark = null;

    LogInfo($"Polling device {config.DeviceId} ({config.DeviceIp}:{config.DevicePort})");
    if (config.Debug)
    {
      LogDebug(config, $"Watermark source: {config.WatermarkSource}");
      LogDebug(config, $"Watermark local (UTC): {(watermarkLocal is null ? "(none)" : watermarkLocal.Value.ToString("O", CultureInfo.InvariantCulture))}");
      LogDebug(config, $"Watermark db (UTC): {(watermarkDb is null ? "(none)" : watermarkDb.Value.ToString("O", CultureInfo.InvariantCulture))}");
      LogDebug(config, $"Min event date: {(config.MinEventDate is null ? "(none)" : config.MinEventDate.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture))}");
      LogDebug(config, $"FilterToDeviceUsers: {config.FilterToDeviceUsers}");
      LogDebug(config, $"Bad datetime mode: {config.BadDateTimeMode} (valid years {config.MinValidYear}..{(config.MaxValidYear > 0 ? config.MaxValidYear : DateTime.Now.Year + 1)})");
      LogDebug(config, $"Time correction: {config.TimeCorrectionMode} (set device time={config.SetDeviceTime})");
    }
    if (watermark is not null) LogInfo($"Watermark (UTC): {watermark:O}");

    var deviceUsers = (config.FilterToDeviceUsers || config.AutoCreateStaff || config.SyncStaffNames)
      ? ReadDeviceUsers(config)
      : new Dictionary<string, string>(StringComparer.Ordinal);
    if (config.Debug) Console.WriteLine($"WL10 users read: {deviceUsers.Count}");

    var validStaffIds = (config.FilterToDeviceUsers && deviceUsers.Count > 0)
      ? new HashSet<string>(deviceUsers.Keys, StringComparer.Ordinal)
      : null;
    var pollSw = Stopwatch.StartNew();
    var punches = ReadDevicePunches(config, today ? null : watermark, today, validStaffIds);
    if (config.FilterToDeviceUsers && deviceUsers.Count > 0)
    {
      var valid = new HashSet<string>(deviceUsers.Keys, StringComparer.Ordinal);
      var before = punches.Count;
      punches = punches.Where(p => valid.Contains(p.StaffId)).ToList();
      var after = punches.Count;
      if (after != before) LogInfo($"Filtered punches by WL10 user list: kept {after}/{before}");
    }
    else if (config.Debug)
    {
      LogDebug(config, $"Filtered punches by WL10 user list: (skipped) FilterToDeviceUsers={config.FilterToDeviceUsers} users={deviceUsers.Count}");
    }
    pollSw.Stop();
    LogInfo($"Retrieved punches: {punches.Count} records (device poll {pollSw.ElapsedMilliseconds}ms)");

    if (config.Debug)
    {
      Console.WriteLine($"Punches ready: {punches.Count}");
      foreach (var p in punches.Take(50))
      {
        Console.WriteLine($"Punch: staff_id={p.StaffId} occurred_at_utc={p.OccurredAtUtc:O} event_date={p.EventDate} device_id={p.DeviceId}");
      }
      if (punches.Count > 50) Console.WriteLine($"(Truncated) Printed first 50 punches.");
    }

    DevicePunches = MergePunches(DevicePunches, punches, max: 5000);
    var exportPath = TryResolveAttlogExportPath(config.DeviceId);

    var exportMode = (Environment.GetEnvironmentVariable("WL10_ATTLOG_EXPORT_MODE") ?? "full").Trim();
    if (exportMode.Equals("full", StringComparison.OrdinalIgnoreCase))
    {
      try
      {
        var exportConfig = config;
        var mode = (exportConfig.BadDateTimeMode ?? string.Empty).Trim();
        if (!mode.Equals("skip", StringComparison.OrdinalIgnoreCase))
        {
          exportConfig = exportConfig with { BadDateTimeMode = "skip" };
        }

        var fullPunches = ReadDevicePunches(exportConfig, afterUtc: null, onlyDeviceToday: false, restrictToStaffIds: validStaffIds);
        if (config.FilterToDeviceUsers && deviceUsers.Count > 0)
        {
          var valid = new HashSet<string>(deviceUsers.Keys, StringComparer.Ordinal);
          fullPunches = fullPunches.Where(p => valid.Contains(p.StaffId)).ToList();
        }
        DevicePunches = MergePunches(Array.Empty<Punch>(), fullPunches, max: 50000);
      }
      catch (Exception ex)
      {
        Console.Error.WriteLine($"Attlog full export refresh failed: {ex.Message}");
      }
    }

    TryExportAttlogFile(exportPath, DevicePunches);
    var punchesFromFile = TryReadAttlogPunchesForSync(config, exportPath, today ? null : watermark, today, validStaffIds);
    if (punchesFromFile is not null)
    {
      punches = punchesFromFile;
    }

    LastRunPunchCount = punches.Count;
    LastRunPunches = punches
      .OrderByDescending(p => p.OccurredAtUtc)
      .Take(200)
      .ToArray();

    var distinctUpsert = 0;
    if (punches.Count > 0)
    {
      var keys = new HashSet<(string staffId, DateTimeOffset occurredAtUtc)>();
      foreach (var p in punches)
      {
        keys.Add((p.StaffId ?? string.Empty, p.OccurredAtUtc));
      }
      distinctUpsert = keys.Count;
    }

    if (punches.Count == 0)
    {
      LastRunUpsertedCount = 0;
      LastRunSkippedUpsertCount = 0;
      if (!config.DryRun && config.SupabaseSyncEnabled && (config.AutoCreateStaff || config.SyncStaffNames) && deviceUsers.Count > 0)
      {
        await SyncStaffToSupabase(config, deviceUsers);
      }
      DateTimeOffset? localMax = null;
      if (DevicePunches.Length > 0)
      {
        localMax = DevicePunches.Max(p => p.OccurredAtUtc);
      }
      localMax ??= watermarkLocal ?? priorState.LastSeenOccurredAtUtc;
      if (localMax is not null)
      {
        SaveState(statePath, priorState with { LastSeenOccurredAtUtc = localMax, DevicePunches = DevicePunches });
      }
      LogInfo("No new punches.");
      await TryWriteSupabaseLogSummary(config, "INFO", "No new punches", new
      {
        device_ip = config.DeviceIp,
        device_port = config.DevicePort,
        punches = 0,
        duration_ms = cycleSw.ElapsedMilliseconds
      });
      if (verify && !config.DryRun && config.SupabaseSyncEnabled) await VerifySupabaseAttendanceEventsForDevice(config, 20);
      return;
    }

    var maxSeen = punches.Max(p => p.OccurredAtUtc);
    var staffIds = punches.Select(p => p.StaffId).Distinct(StringComparer.Ordinal).ToArray();
    var staffNames = (config.AutoCreateStaff || config.SyncStaffNames) && deviceUsers.Count > 0
      ? deviceUsers.Where(kv => staffIds.Contains(kv.Key, StringComparer.Ordinal) && !string.IsNullOrWhiteSpace(kv.Value))
        .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal)
      : new Dictionary<string, string>(StringComparer.Ordinal);

    if (config.DryRun)
    {
      LogInfo($"Dry run: would upsert {distinctUpsert} punches to Supabase.");
      LastRunUpsertedCount = 0;
      LastRunSkippedUpsertCount = distinctUpsert;
      await TryWriteSupabaseLogSummary(config, "INFO", "Dry run completed", new
      {
        device_ip = config.DeviceIp,
        device_port = config.DevicePort,
        punches = punches.Count,
        distinct_upsert = distinctUpsert,
        duration_ms = cycleSw.ElapsedMilliseconds
      });
      return;
    }

    if (!config.SupabaseSyncEnabled)
    {
      LogWarn($"Supabase sync disabled: skipping upsert of {punches.Count} punches.");
      SaveState(statePath, priorState with { LastSeenOccurredAtUtc = maxSeen, DevicePunches = DevicePunches });
      LogInfo($"Saved watermark (UTC): {maxSeen:O}");
      LastRunUpsertedCount = 0;
      LastRunSkippedUpsertCount = distinctUpsert;
      LastRunPunches = punches
        .OrderByDescending(p => p.OccurredAtUtc)
        .Take(200)
        .ToArray();
      return;
    }

    var upsertSw = Stopwatch.StartNew();
    await UpsertToSupabase(config, punches, staffNames);
    upsertSw.Stop();
    LastRunUpsertedCount = distinctUpsert;
    LastRunSkippedUpsertCount = 0;
    LastRunPunches = punches
      .OrderByDescending(p => p.OccurredAtUtc)
      .Take(200)
      .ToArray();

    SaveState(statePath, priorState with { LastSeenOccurredAtUtc = maxSeen, DevicePunches = DevicePunches });
    LogInfo($"Sync to database: upserted={distinctUpsert} duration_ms={upsertSw.ElapsedMilliseconds}");
    LogInfo($"Saved watermark (UTC): {maxSeen:O}");
    await TryWriteSupabaseLogSummary(config, "INFO", "Sync completed", new
    {
      device_ip = config.DeviceIp,
      device_port = config.DevicePort,
      punches = punches.Count,
      distinct_upsert = distinctUpsert,
      upserted = distinctUpsert,
      duration_ms = cycleSw.ElapsedMilliseconds,
      device_poll_ms = pollSw.ElapsedMilliseconds,
      supabase_ms = upsertSw.ElapsedMilliseconds,
      watermark_utc = maxSeen.ToString("O", CultureInfo.InvariantCulture)
    });
    if (verify) await VerifySupabaseAttendanceEventsForDevice(config, 20);
  }

  private static Punch[] MergePunches(Punch[] existing, List<Punch> incoming, int max)
  {
    if ((existing.Length == 0) && (incoming.Count == 0)) return Array.Empty<Punch>();
    if (incoming.Count == 0) return existing;

    var map = new Dictionary<(string staffId, DateTimeOffset occurredAtUtc), Punch>(existing.Length + incoming.Count);
    foreach (var p in existing)
    {
      map[(p.StaffId ?? string.Empty, p.OccurredAtUtc)] = p;
    }
    foreach (var p in incoming)
    {
      map[(p.StaffId ?? string.Empty, p.OccurredAtUtc)] = p;
    }

    return map.Values
      .OrderByDescending(p => p.OccurredAtUtc)
      .Take(Math.Clamp(max, 100, 50000))
      .ToArray();
  }

  private static async Task SyncStaffToSupabase(AppConfig config, Dictionary<string, string> deviceUsers)
  {
    var ids = deviceUsers.Keys.Where(k => !string.IsNullOrWhiteSpace(k)).Distinct(StringComparer.Ordinal).ToArray();
    if (ids.Length == 0) return;

    using var http = new HttpClient();
    http.Timeout = TimeSpan.FromSeconds(15);

    var names = deviceUsers
      .Where(kv => !string.IsNullOrWhiteSpace(kv.Key) && !string.IsNullOrWhiteSpace(kv.Value))
      .ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.Ordinal);

    await EnsureStaffRowsExist(http, config, ids, names);
  }

  private static PollState LoadState(string statePath)
  {
    try
    {
      static string AltPath(string p)
      {
        var dir = Path.GetDirectoryName(p) ?? string.Empty;
        var name = Path.GetFileNameWithoutExtension(p);
        var ext = Path.GetExtension(p);
        return Path.Combine(dir, name + ".alt" + ext);
      }

      var alt = AltPath(statePath);
      var candidates = new List<string>(capacity: 2);
      if (File.Exists(statePath)) candidates.Add(statePath);
      if (File.Exists(alt)) candidates.Add(alt);
      if (candidates.Count == 0) return new PollState(null, null, null);

      var ordered = candidates
        .Select(p => (p, t: SafeGetLastWriteUtc(p)))
        .OrderByDescending(x => x.t)
        .Select(x => x.p)
        .ToArray();

      foreach (var p in ordered)
      {
        try
        {
          var raw = File.ReadAllText(p);
          var parsed = JsonSerializer.Deserialize<PollState>(raw, JsonOptions);
          if (parsed is not null) return parsed;
        }
        catch { }
      }
      return new PollState(null, null, null);
    }
    catch
    {
      return new PollState(null, null, null);
    }
  }

  private static void SaveState(string statePath, PollState state)
  {
    try
    {
      static string AltPath(string p)
      {
        var dir = Path.GetDirectoryName(p) ?? string.Empty;
        var name = Path.GetFileNameWithoutExtension(p);
        var ext = Path.GetExtension(p);
        return Path.Combine(dir, name + ".alt" + ext);
      }

      static void EnsureWritableFile(string path)
      {
        if (!File.Exists(path)) return;
        try
        {
          var a = File.GetAttributes(path);
          if ((a & FileAttributes.ReadOnly) != 0) File.SetAttributes(path, a & ~FileAttributes.ReadOnly);
        }
        catch { }
      }

      static void WriteAtomic(string path, string content)
      {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir);
        EnsureWritableFile(path);
        var tmp = path + ".tmp";
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
        using (var sw = new StreamWriter(fs, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false)))
        {
          sw.Write(content);
        }
        try
        {
          if (File.Exists(path)) File.Replace(tmp, path, null);
          else File.Move(tmp, path);
        }
        finally
        {
          try { if (File.Exists(tmp)) File.Delete(tmp); } catch { }
        }
      }

      static void WriteInPlace(string path, string content)
      {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir);
        EnsureWritableFile(path);

        if (!File.Exists(path))
        {
          File.WriteAllText(path, content, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
          return;
        }

        using var fs = new FileStream(path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite);
        fs.SetLength(0);
        using var sw = new StreamWriter(fs, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
        sw.Write(content);
      }

      var payload = JsonSerializer.Serialize(state, JsonOptions);
      try
      {
        WriteAtomic(statePath, payload);
      }
      catch (Exception ex1)
      {
        try
        {
          WriteInPlace(statePath, payload);
          try { Console.Error.WriteLine($"SaveState fallback: wrote state file in-place '{statePath}' because atomic write failed: {ex1.Message}"); } catch { }
          return;
        }
        catch { }

        var alt = AltPath(statePath);
        try
        {
          WriteAtomic(alt, payload);
          try { Console.Error.WriteLine($"SaveState fallback: wrote alt state file '{alt}' because '{statePath}' failed: {ex1.Message}"); } catch { }
        }
        catch (Exception ex2)
        {
          try
          {
            WriteInPlace(alt, payload);
            try { Console.Error.WriteLine($"SaveState fallback: wrote alt state file in-place '{alt}' because atomic writes failed: {ex2.Message}"); } catch { }
            return;
          }
          catch { }

          try { Console.Error.WriteLine($"SaveState failed ({statePath}): {ex1.Message}"); } catch { }
          try { Console.Error.WriteLine($"SaveState alt failed ({alt}): {ex2.Message}"); } catch { }
        }
      }
    }
    catch (Exception ex)
    {
      try { Console.Error.WriteLine($"SaveState failed ({statePath}): {ex.Message}"); } catch { }
    }
  }

  private static DateTime SafeGetLastWriteUtc(string path)
  {
    try { return File.GetLastWriteTimeUtc(path); } catch { return DateTime.MinValue; }
  }

  private static bool NeedsDbWatermark(string watermarkSource)
  {
    var mode = (watermarkSource ?? string.Empty).Trim();
    if (mode.Length == 0) return true;
    return mode.Equals("db", StringComparison.OrdinalIgnoreCase)
      || mode.Equals("max", StringComparison.OrdinalIgnoreCase);
  }

  private static DateTimeOffset? ResolveWatermark(string watermarkSource, DateTimeOffset? db, DateTimeOffset? local)
  {
    var mode = (watermarkSource ?? string.Empty).Trim();
    if (mode.Length == 0) mode = "db";

    if (mode.Equals("none", StringComparison.OrdinalIgnoreCase)) return null;
    if (mode.Equals("local", StringComparison.OrdinalIgnoreCase)) return local;
    if (mode.Equals("db", StringComparison.OrdinalIgnoreCase)) return db;

    if (db is null) return local;
    if (local is null) return db;
    return db.Value >= local.Value ? db : local;
  }

  private static async Task<DateTimeOffset?> LoadSupabaseWatermark(AppConfig config)
  {
    if (!config.SupabaseSyncEnabled) return null;
    if (string.IsNullOrWhiteSpace(config.SupabaseUrl) || string.IsNullOrWhiteSpace(config.SupabaseServiceRoleKey)) return null;

    try
    {
      using var http = new HttpClient();
      http.Timeout = TimeSpan.FromSeconds(15);

      var url = $"{config.SupabaseUrl.TrimEnd('/')}/rest/v1/{config.SupabaseAttendanceTable}?select=occurred_at&order=occurred_at.desc&limit=1";

      using var req = new HttpRequestMessage(HttpMethod.Get, url);
      req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");

      using var resp = await http.SendAsync(req);
      if (!resp.IsSuccessStatusCode) return null;

      var json = await resp.Content.ReadAsStringAsync();
      using var doc = JsonDocument.Parse(json);
      if (doc.RootElement.ValueKind != JsonValueKind.Array) return null;
      using var enumerator = doc.RootElement.EnumerateArray();
      if (!enumerator.MoveNext()) return null;

      var el = enumerator.Current;
      if (!el.TryGetProperty("occurred_at", out var dtEl) || dtEl.ValueKind != JsonValueKind.String) return null;
      var raw = dtEl.GetString();
      if (string.IsNullOrWhiteSpace(raw)) return null;

      if (!DateTimeOffset.TryParse(raw.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dto)) return null;
      return dto.ToUniversalTime();
    }
    catch
    {
      return null;
    }
  }

  private static TimeSpan? GetTimeOffsetForCorrection(AppConfig config, dynamic zk)
  {
    var mode = (config.TimeCorrectionMode ?? string.Empty).Trim();
    if (mode.Length == 0 || mode.Equals("none", StringComparison.OrdinalIgnoreCase)) return null;

    if (config.SetDeviceTime)
    {
      try { zk.SetDeviceTime(config.MachineNumber); } catch { }
    }

    if (!mode.Equals("offset", StringComparison.OrdinalIgnoreCase)) return null;

    try
    {
      int y, mo, d, hh, mm, ss;
      var ok = (bool)zk.GetDeviceTime(config.MachineNumber, out y, out mo, out d, out hh, out mm, out ss);
      if (!ok) return null;

      var deviceNowLocal = new DateTime(y, mo, d, hh, mm, ss, DateTimeKind.Local);
      var hostNowLocal = DateTime.Now;
      var delta = hostNowLocal - deviceNowLocal;

      var limitHours = config.MaxTimeOffsetHours <= 0 ? 0 : config.MaxTimeOffsetHours;
      if (limitHours > 0 && Math.Abs(delta.TotalHours) > limitHours)
      {
        Console.WriteLine($"Skipping time offset (too large): {delta}");
        return null;
      }

      if (Math.Abs(delta.TotalSeconds) >= 30)
      {
        Console.WriteLine($"Device time (local): {deviceNowLocal:yyyy-MM-dd HH:mm:ss}");
        Console.WriteLine($"Host time (local):   {hostNowLocal:yyyy-MM-dd HH:mm:ss}");
        Console.WriteLine($"Applying time offset: {delta}");
      }

      return delta;
    }
    catch
    {
      return null;
    }
  }

  private static DateTime? FixInvalidDateFieldsToBaseDate(AppConfig config, int hour, int minute, int second, DateTime deviceNowLocal, DateTime hostNowLocal)
  {
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return null;

    var mode = (config.BadDateTimeMode ?? string.Empty).Trim();
    if (mode.Length == 0 || mode.Equals("none", StringComparison.OrdinalIgnoreCase) || mode.Equals("skip", StringComparison.OrdinalIgnoreCase)) return null;

    var baseDate = mode.Equals("host_date", StringComparison.OrdinalIgnoreCase) ? hostNowLocal.Date : deviceNowLocal.Date;
    try
    {
      return baseDate.AddHours(hour).AddMinutes(minute).AddSeconds(second);
    }
    catch
    {
      return null;
    }
  }

  private static DateTime GetDeviceNowLocal(AppConfig config, dynamic zk)
  {
    try
    {
      int y, mo, d, hh, mm, ss;
      var ok = (bool)zk.GetDeviceTime(config.MachineNumber, out y, out mo, out d, out hh, out mm, out ss);
      if (!ok) return DateTime.Now;
      return new DateTime(y, mo, d, hh, mm, ss, DateTimeKind.Local);
    }
    catch
    {
      return DateTime.Now;
    }
  }

  private static DateTime? SanitizePunchLocalDateTime(AppConfig config, DateTime localDt, DateTime deviceNowLocal, DateTime hostNowLocal)
  {
    var minYear = config.MinValidYear <= 0 ? 2015 : config.MinValidYear;
    var maxYear = config.MaxValidYear > 0 ? config.MaxValidYear : hostNowLocal.Year + 1;
    if (localDt.Year >= minYear && localDt.Year <= maxYear) return localDt;

    var mode = (config.BadDateTimeMode ?? string.Empty).Trim();
    if (mode.Length == 0 || mode.Equals("none", StringComparison.OrdinalIgnoreCase)) return localDt;
    if (mode.Equals("skip", StringComparison.OrdinalIgnoreCase)) return null;

    var baseDate = mode.Equals("host_date", StringComparison.OrdinalIgnoreCase) ? hostNowLocal.Date : deviceNowLocal.Date;
    try
    {
      return baseDate.Add(localDt.TimeOfDay);
    }
    catch
    {
      return null;
    }
  }

  private static int? ExtractStaffNumber(AppConfig config, string raw)
  {
    var s = raw ?? string.Empty;
    if (s.Length == 0) return null;

    var candidates = new List<int>(capacity: 8);
    var current = new StringBuilder(capacity: 8);

    void Flush()
    {
      if (current.Length == 0) return;
      if (int.TryParse(current.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) && n > 0)
      {
        candidates.Add(n);
      }
      current.Clear();
    }

    foreach (var ch in s)
    {
      if (char.IsDigit(ch))
      {
        if (current.Length < 6) current.Append(ch);
        continue;
      }
      Flush();
    }
    Flush();

    if (candidates.Count == 0)
    {
      if (s.Length == 1)
      {
        if (!char.IsDigit(s[0]))
        {
          var code = (int)s[0];
          if (code > 0) candidates.Add(code);
        }
      }
    }

    if (candidates.Count == 0) return null;

    var max = config.MaxStaffNumber > 0 ? config.MaxStaffNumber : 999;
    for (var i = candidates.Count - 1; i >= 0; i--)
    {
      var n = candidates[i];
      if (n >= 1 && n <= max) return n;
    }
    return null;
  }

  private static string DebugCharCodes(string value)
  {
    var s = value ?? string.Empty;
    if (s.Length == 0) return string.Empty;
    return string.Join(" ", s.Select(c => ((int)c).ToString("X2", CultureInfo.InvariantCulture)));
  }

  private static string ResolveDeviceId(AppConfig config)
  {
    var id = (config.DeviceId ?? string.Empty).Trim();
    if (id.Length > 0) return id;
    var ip = (config.DeviceIp ?? string.Empty).Trim();
    if (ip.Length > 0) return $"WL10-{ip}";
    return "WL10";
  }

  private static string MapStaffId(AppConfig config, string normalizedFromDevice)
  {
    var v = (normalizedFromDevice ?? string.Empty).Trim();
    if (v.Length == 0) return string.Empty;

    if (config.StaffIdPrefix.Length > 0 && v.All(char.IsDigit))
    {
      var width = Math.Max(0, config.StaffIdPadWidth);
      return config.StaffIdPrefix + (width > 0 ? v.PadLeft(width, '0') : v);
    }

    return v;
  }

  private static List<Punch> ReadDevicePunches(AppConfig config, DateTimeOffset? afterUtc, bool onlyDeviceToday, HashSet<string>? restrictToStaffIds)
  {
    var readerMode = (config.ReaderMode ?? string.Empty).Trim();
    if (readerMode.Length == 0) readerMode = "auto";

    List<Punch> ReadNativeOrThrow()
    {
      try
      {
        return ReadDevicePunchesNativeTcp(config, afterUtc, onlyDeviceToday, restrictToStaffIds);
      }
      catch (Exception ex)
      {
        throw new InvalidOperationException($"Failed to read punches from WL10 {config.DeviceIp}:{config.DevicePort} (native reader).", ex);
      }
    }

    if (readerMode.Equals("native", StringComparison.OrdinalIgnoreCase))
    {
      return ReadNativeOrThrow();
    }

    dynamic? zk = null;
    try
    {
      Type? type = null;
      foreach (var progId in new[] { "zkemkeeper.CZKEM", "zkemkeeper.ZKEM", "zkemkeeper.ZKEM.1" })
      {
        try
        {
          var candidate = Type.GetTypeFromProgID(progId, throwOnError: false);
          if (candidate is not null && candidate.GUID != Guid.Empty)
          {
            type = candidate;
            break;
          }
        }
        catch { }
      }
      if (type is null)
      {
        try { type = Type.GetTypeFromCLSID(new Guid("00853A19-BD51-419B-9269-2DABE57EB61F"), throwOnError: false); } catch { type = null; }
      }

      if (type is null)
      {
        return ReadNativeOrThrow();
      }

      try { zk = Activator.CreateInstance(type); }
      catch { return ReadNativeOrThrow(); }
      if (zk is null) return ReadNativeOrThrow();

      var commPasswordOk = zk.SetCommPassword(config.CommPassword);
      _ = commPasswordOk;

      var connected = (bool)zk.Connect_Net(config.DeviceIp, config.DevicePort);
      if (!connected)
      {
        int errorCode = 0; try { zk.GetLastError(out errorCode); } catch { }
        return ReadNativeOrThrow();
      }

      zk.EnableDevice(config.MachineNumber, false);
      try { zk.RefreshData(config.MachineNumber); } catch { }
      try { zk.RegEvent(config.MachineNumber, 65535); } catch { }

      TimeSpan? timeOffset = GetTimeOffsetForCorrection(config, zk);
      var hostNowLocal = DateTime.Now;
      var deviceNowLocal = GetDeviceNowLocal(config, zk);

      var minYear = config.MinValidYear <= 0 ? 2015 : config.MinValidYear;
      var maxYear = config.MaxValidYear > 0 ? config.MaxValidYear : hostNowLocal.Year + 1;
      if (deviceNowLocal.Year < minYear || deviceNowLocal.Year > maxYear)
      {
        if (config.Debug)
        {
          Console.WriteLine($"Device clock out of range (using host date for today/base): deviceNowLocal={deviceNowLocal:yyyy-MM-dd HH:mm:ss} hostNowLocal={hostNowLocal:yyyy-MM-dd HH:mm:ss}");
        }
        deviceNowLocal = hostNowLocal;
      }

      var deviceNowForToday = deviceNowLocal;
      if (timeOffset.HasValue)
      {
        var offs = timeOffset.GetValueOrDefault();
        deviceNowForToday = deviceNowForToday.Add(offs);
      }
      var deviceToday = DateOnly.FromDateTime(deviceNowForToday);
      var correctedLogged = 0;
      var rawCount = 0;
      var keptCount = 0;
      var skippedInvalidStaff = 0;
      var skippedBadDate = 0;
      var skippedAfter = 0;
      var skippedMinDate = 0;
      var skippedEmptyStaffId = 0;
      var skippedInvalidDateFields = 0;
      var skippedNotToday = 0;
      var invalidDateLogged = 0;
      var invalidStaffLogged = 0;
      var normalizedLogged = 0;
      var normalizedStaffCount = 0;
      var correctedTimestampCount = 0;

      void ProcessRecord(
        List<Punch> results,
        string staffIdRaw,
        int workCode,
        int verifyMode,
        int inOutMode,
        int year,
        int month,
        int day,
        int hour,
        int minute,
        int second
      )
      {
        rawCount++;

        var staffNumber = ExtractStaffNumber(config, staffIdRaw);
        var maxN = config.MaxStaffNumber > 0 ? config.MaxStaffNumber : 999;
        if (staffNumber is null && workCode > 0)
        {
          if (workCode >= 1 && workCode <= maxN) staffNumber = workCode;
        }
        if (staffNumber is null)
        {
          skippedInvalidStaff++;
          if (config.Debug && invalidStaffLogged < 10)
          {
            invalidStaffLogged++;
            Console.WriteLine(
              $"Skipped punch with invalid staff id: {year:D4}-{month:D2}-{day:D2} {hour:D2}:{minute:D2}:{second:D2} verifyMode={verifyMode} inOutMode={inOutMode} workCode={workCode} staffRawCodes=[{DebugCharCodes(staffIdRaw)}]"
            );
          }
          return;
        }

        var staffId = MapStaffId(config, staffNumber.Value.ToString(CultureInfo.InvariantCulture));
        if (string.IsNullOrWhiteSpace(staffId))
        {
          skippedEmptyStaffId++;
          return;
        }

        if (restrictToStaffIds is not null && restrictToStaffIds.Count > 0 && !restrictToStaffIds.Contains(staffId))
        {
          skippedInvalidStaff++;
          return;
        }

        DateTime localDt;
        var fieldsInvalid = year < 1
          || month < 1
          || month > 12
          || day < 1
          || day > 31
          || hour < 0
          || hour > 23
          || minute < 0
          || minute > 59
          || second < 0
          || second > 59;

        if (fieldsInvalid)
        {
          skippedInvalidDateFields++;
          if (config.Debug && invalidDateLogged < 10)
          {
            invalidDateLogged++;
            Console.WriteLine(
              $"Skipped punch with invalid datetime fields: {year:D4}-{month:D2}-{day:D2} {hour:D2}:{minute:D2}:{second:D2} staffRawCodes=[{DebugCharCodes(staffIdRaw)}]"
            );
          }

          var mode = (config.BadDateTimeMode ?? string.Empty).Trim();
          var baseDate = mode.Equals("host_date", StringComparison.OrdinalIgnoreCase) ? hostNowLocal.Date : deviceNowLocal.Date;
          if (onlyDeviceToday && month >= 1 && month <= 12 && day >= 1 && day <= 31 && (month != baseDate.Month || day != baseDate.Day))
          {
            skippedBadDate++;
            return;
          }

          var fixedDt = FixInvalidDateFieldsToBaseDate(config, hour, minute, second, deviceNowLocal, hostNowLocal);
          if (fixedDt is not DateTime fixedLocalDt) return;
          localDt = fixedLocalDt;
          if (timeOffset.HasValue)
          {
            var offs = timeOffset.GetValueOrDefault();
            localDt = localDt.Add(offs);
          }
          correctedTimestampCount++;
          if (config.Debug && correctedLogged < 10)
          {
            correctedLogged++;
            Console.WriteLine($"Corrected punch datetime (local): {year:D4}-{month:D2}-{day:D2} {hour:D2}:{minute:D2}:{second:D2} -> {localDt:yyyy-MM-dd HH:mm:ss}");
          }
        }
        else
        {
          try
          {
            localDt = new DateTime(year, month, day, hour, minute, second, DateTimeKind.Local);
            if (timeOffset.HasValue)
            {
              var offs = timeOffset.GetValueOrDefault();
              localDt = localDt.Add(offs);
            }
          }
          catch
          {
            skippedInvalidDateFields++;
            if (config.Debug && invalidDateLogged < 10)
            {
              invalidDateLogged++;
              Console.WriteLine(
                $"Skipped punch with invalid datetime fields: {year:D4}-{month:D2}-{day:D2} {hour:D2}:{minute:D2}:{second:D2} staffRawCodes=[{DebugCharCodes(staffIdRaw)}]"
              );
            }

            var fixedDt = FixInvalidDateFieldsToBaseDate(config, hour, minute, second, deviceNowLocal, hostNowLocal);
            if (fixedDt is not DateTime fixedLocalDt) return;
            localDt = fixedLocalDt;
            if (timeOffset.HasValue)
            {
              var offs = timeOffset.GetValueOrDefault();
              localDt = localDt.Add(offs);
            }
            correctedTimestampCount++;
            if (config.Debug && correctedLogged < 10)
            {
              correctedLogged++;
              Console.WriteLine($"Corrected punch datetime (local): {year:D4}-{month:D2}-{day:D2} {hour:D2}:{minute:D2}:{second:D2} -> {localDt:yyyy-MM-dd HH:mm:ss}");
            }
          }
        }

        var sanitizedLocalDt = SanitizePunchLocalDateTime(config, localDt, deviceNowLocal, hostNowLocal);
        if (sanitizedLocalDt is not DateTime corrected)
        {
          skippedBadDate++;
          return;
        }

        if (corrected != localDt)
        {
          correctedTimestampCount++;
          if (config.Debug && correctedLogged < 10)
          {
            correctedLogged++;
            Console.WriteLine($"Corrected punch datetime (local): {localDt:yyyy-MM-dd HH:mm:ss} -> {corrected:yyyy-MM-dd HH:mm:ss}");
          }
        }
        localDt = corrected;

        var localOffset = new DateTimeOffset(localDt);
        var occurredAtUtc = localOffset.ToUniversalTime();
        if (afterUtc is not null && occurredAtUtc <= afterUtc.Value)
        {
          skippedAfter++;
          return;
        }

        var localDate = DateOnly.FromDateTime(localOffset.LocalDateTime);
        if (onlyDeviceToday && localDate != deviceToday)
        {
          skippedNotToday++;
          return;
        }
        if (config.MinEventDate is not null && localDate < config.MinEventDate.Value)
        {
          skippedMinDate++;
          return;
        }

        if (!string.Equals(staffIdRaw, staffId, StringComparison.Ordinal))
        {
          normalizedStaffCount++;
          if (config.Debug && normalizedLogged < 10)
          {
            normalizedLogged++;
            Console.WriteLine($"Normalized staff_id: rawCodes=[{DebugCharCodes(staffIdRaw)}] -> '{staffId}'");
          }
        }

        var eventDate = localDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        results.Add(new Punch(staffId, occurredAtUtc, eventDate, ResolveDeviceId(config), inOutMode));
        keptCount++;
      }

      var bestResults = new List<Punch>(capacity: 1024);
      var bestScore = int.MinValue;
      var bestReadMode = string.Empty;
      if (config.Debug)
      {
        try
        {
          var logCount = 0;
          zk.GetDeviceStatus(config.MachineNumber, 6, ref logCount);
          Console.WriteLine($"Device log count: {logCount}");
        }
        catch { }
      }

      var readModesToTry = new[] { "ReadGeneralLogData", "GetGeneralLogData", "GetGeneralExtLogData", "ReadAllGLogData", "ReadNewGLogData" };
      foreach (var readMode in readModesToTry)
      {
        try
        {
          var readOk = false;
          try
          {
            if (string.Equals(readMode, "ReadGeneralLogData", StringComparison.Ordinal))
            {
              readOk = (bool)zk.ReadGeneralLogData(config.MachineNumber);
              if (!readOk) readOk = (bool)zk.ReadAllGLogData(config.MachineNumber);
            }
            else if (string.Equals(readMode, "GetGeneralLogData", StringComparison.Ordinal))
            {
              readOk = (bool)zk.ReadGeneralLogData(config.MachineNumber);
              if (!readOk) readOk = (bool)zk.ReadAllGLogData(config.MachineNumber);
            }
            else if (string.Equals(readMode, "GetGeneralExtLogData", StringComparison.Ordinal))
            {
              readOk = (bool)zk.ReadGeneralLogData(config.MachineNumber);
              if (!readOk) readOk = (bool)zk.ReadAllGLogData(config.MachineNumber);
            }
            else if (string.Equals(readMode, "ReadAllGLogData", StringComparison.Ordinal))
            {
              readOk = (bool)zk.ReadAllGLogData(config.MachineNumber);
            }
            else if (string.Equals(readMode, "ReadNewGLogData", StringComparison.Ordinal))
            {
              readOk = (bool)zk.ReadNewGLogData(config.MachineNumber);
            }
          }
          catch
          {
            readOk = false;
          }

          if (!readOk) continue;
          if (config.Debug) Console.WriteLine($"Log read mode: {readMode}");

          rawCount = 0;
          keptCount = 0;
          skippedInvalidStaff = 0;
          skippedBadDate = 0;
          skippedAfter = 0;
          skippedMinDate = 0;
          skippedEmptyStaffId = 0;
          skippedInvalidDateFields = 0;
          skippedNotToday = 0;
          invalidDateLogged = 0;
          invalidStaffLogged = 0;
          correctedLogged = 0;

          var results = new List<Punch>(capacity: 1024);

          var allSig = 0;
          var genSig = 0;
          var extSig = 0;
          var consecutiveReadErrors = 0;

          bool TryReadGeneralExtLogDataBatch()
          {
            var count = -1;
            try
            {
              zk.GetGeneralExtLogDataCount(config.MachineNumber, out count);
            }
            catch
            {
              count = -1;
            }
            if (count <= 0)
            {
              if (config.Debug)
              {
                try
                {
                  int errorCode = 0;
                  zk.GetLastError(out errorCode);
                  Console.WriteLine($"GetGeneralExtLogDataCount returned {count}. ErrorCode={errorCode}");
                }
                catch
                {
                  Console.WriteLine($"GetGeneralExtLogDataCount returned {count}.");
                }
              }
              return false;
            }

            object? enrollObj = null;
            object? verifyObj = null;
            object? inOutObj = null;
            object? yearObj = null;
            object? monthObj = null;
            object? dayObj = null;
            object? hourObj = null;
            object? minuteObj = null;
            object? secondObj = null;
            object? workCodeObj = null;

            var ok = false;
            try
            {
              ok = (bool)zk.GetGeneralExtLogData(
                config.MachineNumber,
                out enrollObj,
                out verifyObj,
                out inOutObj,
                out yearObj,
                out monthObj,
                out dayObj,
                out hourObj,
                out minuteObj,
                out secondObj,
                out workCodeObj
              );
            }
            catch
            {
              try
              {
                ok = (bool)zk.GetGeneralExtLogData(
                  config.MachineNumber,
                  out enrollObj,
                  out verifyObj,
                  out inOutObj,
                  out yearObj,
                  out monthObj,
                  out dayObj,
                  out hourObj,
                  out minuteObj,
                  out secondObj
                );
              }
              catch
              {
                ok = false;
              }
            }

            if (!ok) return false;

            if (enrollObj is not Array enrollArr) return false;
            if (verifyObj is not Array verifyArr) return false;
            if (inOutObj is not Array inOutArr) return false;
            if (yearObj is not Array yearArr) return false;
            if (monthObj is not Array monthArr) return false;
            if (dayObj is not Array dayArr) return false;
            if (hourObj is not Array hourArr) return false;
            if (minuteObj is not Array minuteArr) return false;
            if (secondObj is not Array secondArr) return false;

            static int GetInt(Array arr, int i)
            {
              var v = arr.GetValue(i);
              return v is null ? 0 : Convert.ToInt32(v, CultureInfo.InvariantCulture);
            }

            var n = enrollArr.Length;
            n = Math.Min(n, verifyArr.Length);
            n = Math.Min(n, inOutArr.Length);
            n = Math.Min(n, yearArr.Length);
            n = Math.Min(n, monthArr.Length);
            n = Math.Min(n, dayArr.Length);
            n = Math.Min(n, hourArr.Length);
            n = Math.Min(n, minuteArr.Length);
            n = Math.Min(n, secondArr.Length);
            n = Math.Min(n, count);
            if (n <= 0) return false;

            for (var i = 0; i < n; i++)
            {
              var enroll = enrollArr.GetValue(i)?.ToString() ?? string.Empty;
              var wc = 0;
              if (workCodeObj is Array workCodeArr && i < workCodeArr.Length)
              {
                var wcv = workCodeArr.GetValue(i);
                wc = wcv is null ? 0 : Convert.ToInt32(wcv, CultureInfo.InvariantCulture);
              }
              ProcessRecord(
                results,
                enroll,
                wc,
                GetInt(verifyArr, i),
                GetInt(inOutArr, i),
                GetInt(yearArr, i),
                GetInt(monthArr, i),
                GetInt(dayArr, i),
                GetInt(hourArr, i),
                GetInt(minuteArr, i),
                GetInt(secondArr, i)
              );
            }

            _ = workCodeObj;
            return true;
          }

          var batchHandled = false;
          if (string.Equals(readMode, "GetGeneralExtLogData", StringComparison.Ordinal))
          {
            try
            {
              batchHandled = TryReadGeneralExtLogDataBatch();
            }
            catch
            {
              batchHandled = false;
            }
          }

          if (batchHandled)
          {
            if (config.Debug)
            {
              Console.WriteLine(
                $"Punches read summary ({readMode}): raw={rawCount} kept={keptCount} skipped_invalid_staff={skippedInvalidStaff} skipped_empty_staffid={skippedEmptyStaffId} skipped_invalid_datetime_fields={skippedInvalidDateFields} skipped_bad_datetime={skippedBadDate} skipped_watermark={skippedAfter} skipped_not_today={skippedNotToday} skipped_min_date={skippedMinDate}"
              );
            }

            var batchScore = (keptCount * 1000) - (skippedInvalidDateFields * 10) - skippedInvalidStaff - skippedBadDate;
            if (batchScore > bestScore)
            {
              bestScore = batchScore;
              bestReadMode = readMode;
              bestResults = results;
            }
            continue;
          }
          if (string.Equals(readMode, "GetGeneralExtLogData", StringComparison.Ordinal))
          {
            continue;
          }

          while (true)
          {
            var ok = false;
            var staffIdRaw = string.Empty;
            var workCode = 0;
            var verifyMode = 0;
            var inOutMode = 0;
            var year = 0;
            var month = 0;
            var day = 0;
            var hour = 0;
            var minute = 0;
            var second = 0;
            var useSsr = true;

            try
            {
              if (string.Equals(readMode, "GetGeneralExtLogData", StringComparison.Ordinal))
              {
                string enrollNumber = string.Empty;

                if (extSig == 0)
                {
                  try
                  {
                    ok = (bool)zk.GetGeneralExtLogData(
                      config.MachineNumber,
                      out enrollNumber,
                      out verifyMode,
                      out inOutMode,
                      out year,
                      out month,
                      out day,
                      out hour,
                      out minute,
                      out second,
                      out workCode
                    );
                    extSig = 1;
                  }
                  catch
                  {
                    ok = (bool)zk.GetGeneralExtLogData(
                      config.MachineNumber,
                      out enrollNumber,
                      out verifyMode,
                      out inOutMode,
                      out year,
                      out month,
                      out day,
                      out hour,
                      out minute,
                      out second
                    );
                    extSig = 2;
                  }
                }
                else if (extSig == 1)
                {
                  ok = (bool)zk.GetGeneralExtLogData(
                    config.MachineNumber,
                    out enrollNumber,
                    out verifyMode,
                    out inOutMode,
                    out year,
                    out month,
                    out day,
                    out hour,
                    out minute,
                    out second,
                    out workCode
                  );
                }
                else if (extSig == 2)
                {
                  ok = (bool)zk.GetGeneralExtLogData(
                    config.MachineNumber,
                    out enrollNumber,
                    out verifyMode,
                    out inOutMode,
                    out year,
                    out month,
                    out day,
                    out hour,
                    out minute,
                    out second
                  );
                }
                else
                {
                  ok = false;
                }

                staffIdRaw = enrollNumber ?? string.Empty;
                useSsr = false;
              }
              else if (string.Equals(readMode, "GetGeneralLogData", StringComparison.Ordinal))
              {
                int enrollNumberInt = 0;
                int tMachine = 0;
                int eMachine = 0;

                if (genSig == 0)
                {
                  try
                  {
                    ok = (bool)zk.GetGeneralLogData(
                      config.MachineNumber,
                      out tMachine,
                      out enrollNumberInt,
                      out eMachine,
                      out verifyMode,
                      out inOutMode,
                      out year,
                      out month,
                      out day,
                      out hour,
                      out minute
                    );
                    second = 0;
                    genSig = 1;
                  }
                  catch
                  {
                    try
                    {
                      ok = (bool)zk.GetGeneralLogData(
                        config.MachineNumber,
                        out enrollNumberInt,
                        out verifyMode,
                        out inOutMode,
                        out year,
                        out month,
                        out day,
                        out hour,
                        out minute,
                        out second,
                        out workCode
                      );
                      genSig = 2;
                    }
                    catch
                    {
                      ok = (bool)zk.GetGeneralLogData(
                        config.MachineNumber,
                        out enrollNumberInt,
                        out verifyMode,
                        out inOutMode,
                        out year,
                        out month,
                        out day,
                        out hour,
                        out minute,
                        out second
                      );
                      genSig = 3;
                    }
                  }
                }
                else if (genSig == 1)
                {
                  ok = (bool)zk.GetGeneralLogData(
                    config.MachineNumber,
                    out tMachine,
                    out enrollNumberInt,
                    out eMachine,
                    out verifyMode,
                    out inOutMode,
                    out year,
                    out month,
                    out day,
                    out hour,
                    out minute
                  );
                  second = 0;
                }
                else if (genSig == 2)
                {
                  ok = (bool)zk.GetGeneralLogData(
                    config.MachineNumber,
                    out enrollNumberInt,
                    out verifyMode,
                    out inOutMode,
                    out year,
                    out month,
                    out day,
                    out hour,
                    out minute,
                    out second,
                    out workCode
                  );
                }
                else if (genSig == 3)
                {
                  ok = (bool)zk.GetGeneralLogData(
                    config.MachineNumber,
                    out enrollNumberInt,
                    out verifyMode,
                    out inOutMode,
                    out year,
                    out month,
                    out day,
                    out hour,
                    out minute,
                    out second
                  );
                }
                else
                {
                  ok = false;
                }

                staffIdRaw = enrollNumberInt.ToString(CultureInfo.InvariantCulture);
                _ = tMachine;
                _ = eMachine;
                useSsr = false;
              }
              else if (string.Equals(readMode, "ReadAllGLogData", StringComparison.Ordinal))
              {
                  int tMachine = 0;
                  int enrollNumberInt = 0;
                  int eMachine = 0;

                  if (allSig == 0)
                  {
                    try
                    {
                      ok = (bool)zk.GetAllGLogData(
                        config.MachineNumber,
                        out tMachine,
                        out enrollNumberInt,
                        out eMachine,
                        out verifyMode,
                        out inOutMode,
                        out year,
                        out month,
                        out day,
                        out hour,
                        out minute,
                        out second
                      );
                      allSig = 1;
                    }
                    catch
                    {
                      try
                      {
                        ok = (bool)zk.GetAllGLogData(
                          config.MachineNumber,
                          out enrollNumberInt,
                          out verifyMode,
                          out inOutMode,
                          out year,
                          out month,
                          out day,
                          out hour,
                          out minute,
                          out second
                        );
                        allSig = 2;
                      }
                      catch
                      {
                        allSig = -1;
                      }
                    }
                  }
                  else if (allSig == 1)
                  {
                    ok = (bool)zk.GetAllGLogData(
                      config.MachineNumber,
                      out tMachine,
                      out enrollNumberInt,
                      out eMachine,
                      out verifyMode,
                      out inOutMode,
                      out year,
                      out month,
                      out day,
                      out hour,
                      out minute,
                      out second
                    );
                  }
                  else if (allSig == 2)
                  {
                    ok = (bool)zk.GetAllGLogData(
                      config.MachineNumber,
                      out enrollNumberInt,
                      out verifyMode,
                      out inOutMode,
                      out year,
                      out month,
                      out day,
                      out hour,
                      out minute,
                      out second
                    );
                  }
                  else
                  {
                    ok = false;
                  }

                  staffIdRaw = enrollNumberInt.ToString(CultureInfo.InvariantCulture);
                  if (allSig > 0) useSsr = false;
                }
              if (useSsr)
              {
                string enrollNumber;
                try
                {
                  ok = (bool)zk.SSR_GetGeneralLogData(
                    config.MachineNumber,
                    out enrollNumber,
                    out verifyMode,
                    out inOutMode,
                    out year,
                    out month,
                    out day,
                    out hour,
                    out minute,
                    out second,
                    out workCode
                  );
                }
                catch
                {
                  workCode = 0;
                  ok = (bool)zk.SSR_GetGeneralLogData(
                    config.MachineNumber,
                    out enrollNumber,
                    out verifyMode,
                    out inOutMode,
                    out year,
                    out month,
                    out day,
                    out hour,
                    out minute,
                    out second,
                    ref workCode
                  );
                }
                staffIdRaw = enrollNumber ?? string.Empty;
              }

              consecutiveReadErrors = 0;
            }
            catch
            {
              consecutiveReadErrors++;
              if (config.Debug)
              {
                try
                {
                  int errorCode = 0;
                  zk.GetLastError(out errorCode);
                  Console.WriteLine($"Log read error (attempt {consecutiveReadErrors}): ErrorCode={errorCode}");
                }
                catch { }
              }
              if (consecutiveReadErrors < 3) continue;
              break;
            }

            if (!ok)
            {
              if (config.Debug)
              {
                try
                {
                  int errorCode = 0;
                  zk.GetLastError(out errorCode);
                  Console.WriteLine($"End of log read (ok=false). ErrorCode={errorCode}");
                }
                catch { }
              }
              break;
            }

            ProcessRecord(results, staffIdRaw, workCode, verifyMode, inOutMode, year, month, day, hour, minute, second);
          }

          if (config.Debug)
          {
            Console.WriteLine(
              $"Punches read summary ({readMode}): raw={rawCount} kept={keptCount} skipped_invalid_staff={skippedInvalidStaff} skipped_empty_staffid={skippedEmptyStaffId} skipped_invalid_datetime_fields={skippedInvalidDateFields} skipped_bad_datetime={skippedBadDate} skipped_watermark={skippedAfter} skipped_not_today={skippedNotToday} skipped_min_date={skippedMinDate}"
            );
          }

          var score = (keptCount * 1000) - (skippedInvalidDateFields * 10) - skippedInvalidStaff - skippedBadDate;
          if (score > bestScore)
          {
            bestScore = score;
            bestReadMode = readMode;
            bestResults = results;
          }
        }
        catch
        {
          continue;
        }
      }

      if (bestReadMode.Length == 0)
      {
        int errorCode = 0;
        try { zk.GetLastError(out errorCode); } catch { }
        throw new InvalidOperationException($"Failed to read logs from device. ErrorCode={errorCode}");
      }

      if (config.Debug && bestReadMode.Length > 0) Console.WriteLine($"Selected log read mode: {bestReadMode}");

      var comResults = bestResults
        .GroupBy(p => $"{p.StaffId}|{p.OccurredAtUtc:O}|{p.DeviceId}", StringComparer.Ordinal)
        .Select(g => g.First())
        .OrderBy(p => p.OccurredAtUtc)
        .ToList();

      if (!readerMode.Equals("auto", StringComparison.OrdinalIgnoreCase))
      {
        return comResults;
      }

      try { zk.EnableDevice(config.MachineNumber, true); } catch { }
      try { zk.Disconnect(); } catch { }
      zk = null;

      List<Punch> nativeResults;
      try { nativeResults = ReadDevicePunchesNativeTcp(config, afterUtc, onlyDeviceToday, restrictToStaffIds); }
      catch (Exception ex) { if (config.Debug) Console.WriteLine($"Native punch read failed (continuing with COM): {ex.Message}"); return comResults; }

      var merged = comResults
        .Concat(nativeResults)
        .GroupBy(p => $"{p.StaffId}|{p.OccurredAtUtc:O}|{p.DeviceId}", StringComparer.Ordinal)
        .Select(g => g.First())
        .OrderBy(p => p.OccurredAtUtc)
        .ToList();

      if (config.Debug && merged.Count != comResults.Count)
      {
        Console.WriteLine($"Native reader added {merged.Count - comResults.Count} punches.");
      }

      return merged;
    }
    catch (Exception ex)
    {
      if (config.Debug) Console.WriteLine($"COM punch read failed (falling back to native): {ex.Message}");
      return ReadNativeOrThrow();
    }
    finally
    {
      try
      {
        if (zk is not null)
        {
          try { zk.EnableDevice(config.MachineNumber, true); } catch { }
          try { zk.Disconnect(); } catch { }
        }
      }
      catch { }
    }
  }

  private static List<Punch> ReadDevicePunchesNativeTcp(AppConfig config, DateTimeOffset? afterUtc, bool onlyDeviceToday, HashSet<string>? restrictToStaffIds)
  {
    var hostNowLocal = DateTime.Now;
    using var zk = new ZkTcpSession(config.DeviceIp, config.DevicePort, config.CommPassword);
    zk.Connect();

    if (config.SetDeviceTime)
    {
      try
      {
        zk.SetTime(hostNowLocal);
      }
      catch { }
    }

    TimeSpan? timeOffset = null;
    var timeMode = (config.TimeCorrectionMode ?? string.Empty).Trim();
    if (timeMode.Equals("offset", StringComparison.OrdinalIgnoreCase))
    {
      try
      {
        var deviceNowLocal0 = zk.GetTime();
        var delta = hostNowLocal - deviceNowLocal0;
        var limitHours = config.MaxTimeOffsetHours <= 0 ? 0 : config.MaxTimeOffsetHours;
        if (limitHours <= 0 || Math.Abs(delta.TotalHours) <= limitHours)
        {
          timeOffset = delta;
        }
        else
        {
          Console.WriteLine($"Skipping time offset (too large): {delta}");
        }
      }
      catch { }
    }

    var deviceNowLocal = hostNowLocal;
    try
    {
      deviceNowLocal = zk.GetTime();
    }
    catch { }

    var minYear = config.MinValidYear <= 0 ? 2015 : config.MinValidYear;
    var maxYear = config.MaxValidYear > 0 ? config.MaxValidYear : hostNowLocal.Year + 1;
    if (deviceNowLocal.Year < minYear || deviceNowLocal.Year > maxYear)
    {
      if (config.Debug)
      {
        Console.WriteLine($"Device clock out of range (using host date for today/base): deviceNowLocal={deviceNowLocal:yyyy-MM-dd HH:mm:ss} hostNowLocal={hostNowLocal:yyyy-MM-dd HH:mm:ss}");
      }
      deviceNowLocal = hostNowLocal;
    }

    var deviceNowForToday = deviceNowLocal;
    if (timeOffset.HasValue)
    {
      var offs = timeOffset.GetValueOrDefault();
      deviceNowForToday = deviceNowForToday.Add(offs);
    }
    var deviceToday = DateOnly.FromDateTime(deviceNowForToday);

    var results = new List<Punch>(capacity: 1024);

    var correctedLogged = 0;
    var rawCount = 0;
    var keptCount = 0;
    var skippedInvalidStaff = 0;
    var skippedBadDate = 0;
    var skippedAfter = 0;
    var skippedMinDate = 0;
    var skippedEmptyStaffId = 0;
    var skippedInvalidDateFields = 0;
    var skippedNotToday = 0;
    var invalidDateLogged = 0;
    var invalidStaffLogged = 0;

    void ProcessRecord(
      string staffIdRaw,
      int workCode,
      int verifyMode,
      int inOutMode,
      int year,
      int month,
      int day,
      int hour,
      int minute,
      int second
    )
    {
      rawCount++;

      var staffNumber = ExtractStaffNumber(config, staffIdRaw);
      var maxN = config.MaxStaffNumber > 0 ? config.MaxStaffNumber : 999;
      if (staffNumber is null && workCode > 0)
      {
        if (workCode >= 1 && workCode <= maxN) staffNumber = workCode;
      }
      if (staffNumber is null)
      {
        skippedInvalidStaff++;
        if (config.Debug && invalidStaffLogged < 10)
        {
          invalidStaffLogged++;
          Console.WriteLine(
            $"Skipped punch with invalid staff id: {year:D4}-{month:D2}-{day:D2} {hour:D2}:{minute:D2}:{second:D2} verifyMode={verifyMode} inOutMode={inOutMode} workCode={workCode} staffRawCodes=[{DebugCharCodes(staffIdRaw)}]"
          );
        }
        return;
      }

      var staffId = MapStaffId(config, staffNumber.Value.ToString(CultureInfo.InvariantCulture));
      if (string.IsNullOrWhiteSpace(staffId))
      {
        skippedEmptyStaffId++;
        return;
      }

      if (restrictToStaffIds is not null && restrictToStaffIds.Count > 0 && !restrictToStaffIds.Contains(staffId))
      {
        skippedInvalidStaff++;
        return;
      }

      DateTime localDt;
      var fieldsInvalid = year < 1
        || month < 1
        || month > 12
        || day < 1
        || day > 31
        || hour < 0
        || hour > 23
        || minute < 0
        || minute > 59
        || second < 0
        || second > 59;

      if (fieldsInvalid)
      {
        skippedInvalidDateFields++;
        if (config.Debug && invalidDateLogged < 10)
        {
          invalidDateLogged++;
          Console.WriteLine(
            $"Skipped punch with invalid datetime fields: {year:D4}-{month:D2}-{day:D2} {hour:D2}:{minute:D2}:{second:D2} staffRawCodes=[{DebugCharCodes(staffIdRaw)}]"
          );
        }

        var fixedDt = FixInvalidDateFieldsToBaseDate(config, hour, minute, second, deviceNowLocal, hostNowLocal);
        if (fixedDt is not DateTime fixedLocalDt) return;
        localDt = fixedLocalDt;
        if (timeOffset.HasValue)
        {
          var offs = timeOffset.GetValueOrDefault();
          localDt = localDt.Add(offs);
        }
        if (correctedLogged < 10)
        {
          correctedLogged++;
          Console.WriteLine($"Corrected punch datetime (local): {year:D4}-{month:D2}-{day:D2} {hour:D2}:{minute:D2}:{second:D2} -> {localDt:yyyy-MM-dd HH:mm:ss}");
        }
      }
      else
      {
        try
        {
          localDt = new DateTime(year, month, day, hour, minute, second, DateTimeKind.Local);
          if (timeOffset.HasValue)
          {
            var offs = timeOffset.GetValueOrDefault();
            localDt = localDt.Add(offs);
          }
        }
        catch
        {
          skippedBadDate++;
          return;
        }
      }

      var sanitizedLocalDt = SanitizePunchLocalDateTime(config, localDt, deviceNowLocal, hostNowLocal);
      if (sanitizedLocalDt is not DateTime corrected)
      {
        skippedBadDate++;
        return;
      }

      if (corrected != localDt && correctedLogged < 10)
      {
        correctedLogged++;
        Console.WriteLine($"Corrected punch datetime (local): {localDt:yyyy-MM-dd HH:mm:ss} -> {corrected:yyyy-MM-dd HH:mm:ss}");
      }
      localDt = corrected;

      var localOffset = new DateTimeOffset(localDt);
      var occurredAtUtc = localOffset.ToUniversalTime();
      if (afterUtc is not null && occurredAtUtc <= afterUtc.Value)
      {
        skippedAfter++;
        return;
      }

      var localDate = DateOnly.FromDateTime(localOffset.LocalDateTime);
      if (onlyDeviceToday && localDate != deviceToday)
      {
        skippedNotToday++;
        return;
      }
      if (config.MinEventDate is not null && localDate < config.MinEventDate.Value)
      {
        skippedMinDate++;
        return;
      }

      if (!string.Equals(staffIdRaw, staffId, StringComparison.Ordinal))
      {
        Console.WriteLine($"Normalized staff_id: rawCodes=[{DebugCharCodes(staffIdRaw)}] -> '{staffId}'");
      }

      var eventDate = localDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
      results.Add(new Punch(staffId, occurredAtUtc, eventDate, ResolveDeviceId(config), inOutMode));
      keptCount++;
    }

    var attendanceBlob = zk.ReadWithBuffer(13);
    if (attendanceBlob.Length >= 4)
    {
      var totalSize = BitConverter.ToInt32(attendanceBlob, 0);
      if (totalSize > 0 && totalSize <= attendanceBlob.Length - 4)
      {
        var payload = new ReadOnlySpan<byte>(attendanceBlob, 4, totalSize);
        var maxStaff = config.MaxStaffNumber > 0 ? config.MaxStaffNumber : 9999;
        var candidateRecordSizes = new List<int>(capacity: 12);
        for (var size = 8; size <= 64; size++)
        {
          if (totalSize % size == 0) candidateRecordSizes.Add(size);
        }
        if (candidateRecordSizes.Count == 0) candidateRecordSizes.Add(16);

        static int ScoreLayout(ReadOnlySpan<byte> payload, int recordSize, int uidOffset, int uidSize, int tsOffset, int maxStaff, int minYear, int maxYear)
        {
          var good = 0;
          var bad = 0;
          var checkedCount = 0;
          var distinctTs = new HashSet<uint>();
          var distinctUid = new HashSet<int>();
          var haveMinMax = false;
          var minTicks = 0L;
          var maxTicks = 0L;
          for (var offset = 0; offset + recordSize <= payload.Length && checkedCount < 80; offset += recordSize)
          {
            checkedCount++;
            try
            {
              var rec = payload.Slice(offset, recordSize);
              int uid = uidSize == 2
                ? BitConverter.ToUInt16(rec.Slice(uidOffset, 2))
                : BitConverter.ToInt32(rec.Slice(uidOffset, 4));
              if (uid < 1 || uid > maxStaff) { bad++; continue; }
              var ts = BitConverter.ToUInt32(rec.Slice(tsOffset, 4));
              var local = ZkTcpSession.DecodeZkTime(ts);
              if (local.Year < minYear || local.Year > maxYear) { bad++; continue; }
              good++;
              distinctTs.Add(ts);
              distinctUid.Add(uid);
              if (!haveMinMax)
              {
                haveMinMax = true;
                minTicks = local.Ticks;
                maxTicks = local.Ticks;
              }
              else
              {
                if (local.Ticks < minTicks) minTicks = local.Ticks;
                if (local.Ticks > maxTicks) maxTicks = local.Ticks;
              }
            }
            catch
            {
              bad++;
            }
          }
          var score = (good * 1000) - (bad * 1000);
          score += distinctTs.Count * 25;
          score += distinctUid.Count * 10;
          if (good > 0 && distinctTs.Count <= 1) score -= 80000;
          if (good > 0 && distinctUid.Count <= 1) score -= 30000;
          if (good >= 12 && distinctTs.Count < Math.Max(2, good / 3)) score -= 30000;
          if (haveMinMax)
          {
            var spanMinutes = (maxTicks - minTicks) / TimeSpan.TicksPerMinute;
            if (good >= 12 && spanMinutes <= 1) score -= 30000;
          }
          return score;
        }

        var bestRecordSize = candidateRecordSizes[0];
        var bestUidOffset = 0;
        var bestUidSize = 4;
        var bestTsOffset = 0;
        var bestScore = int.MinValue;

        foreach (var recordSize in candidateRecordSizes)
        {
          for (var uidSize = 2; uidSize <= 4; uidSize += 2)
          {
            for (var uidOffset = 0; uidOffset + uidSize <= recordSize; uidOffset++)
            {
              for (var tsOffset = 0; tsOffset + 4 <= recordSize; tsOffset++)
              {
                if (tsOffset >= uidOffset && tsOffset < uidOffset + uidSize) continue;
                if (uidOffset >= tsOffset && uidOffset < tsOffset + 4) continue;
                var score = ScoreLayout(payload, recordSize, uidOffset, uidSize, tsOffset, maxStaff, minYear, maxYear);
                if (score > bestScore)
                {
                  bestScore = score;
                  bestRecordSize = recordSize;
                  bestUidOffset = uidOffset;
                  bestUidSize = uidSize;
                  bestTsOffset = tsOffset;
                }
              }
            }
          }
        }

        if (config.Debug)
        {
          Console.WriteLine($"Native attlog layout: recordSize={bestRecordSize} uidOffset={bestUidOffset} uidSize={bestUidSize} tsOffset={bestTsOffset} score={bestScore}");
        }

        for (var offset = 0; offset + bestRecordSize <= payload.Length; offset += bestRecordSize)
        {
          var rec = payload.Slice(offset, bestRecordSize);
          var verify = 0;
          var state = 0;
          var workCode = 0;
          var staffRaw = string.Empty;

          DateTime local;
          var uid = 0;
          try
          {
            uid = bestUidSize == 2
              ? BitConverter.ToUInt16(rec.Slice(bestUidOffset, 2))
              : BitConverter.ToInt32(rec.Slice(bestUidOffset, 4));
            staffRaw = uid.ToString(CultureInfo.InvariantCulture);
            var ts = BitConverter.ToUInt32(rec.Slice(bestTsOffset, 4));
            local = ZkTcpSession.DecodeZkTime(ts);
          }
          catch
          {
            skippedBadDate++;
            continue;
          }

          if (bestRecordSize == 22 && bestUidOffset == 0 && bestUidSize == 2 && bestTsOffset == 13)
          {
            verify = rec[12];
            state = rec[17];
          }

          ProcessRecord(
            staffRaw,
            workCode,
            verify,
            state,
            local.Year,
            local.Month,
            local.Day,
            local.Hour,
            local.Minute,
            local.Second
          );
        }
      }
    }

    if (config.Debug)
    {
      Console.WriteLine(
        $"Punches read summary (native): raw={rawCount} kept={keptCount} skipped_invalid_staff={skippedInvalidStaff} skipped_empty_staffid={skippedEmptyStaffId} skipped_invalid_datetime_fields={skippedInvalidDateFields} skipped_bad_datetime={skippedBadDate} skipped_watermark={skippedAfter} skipped_not_today={skippedNotToday} skipped_min_date={skippedMinDate}"
      );
    }

    return results
      .GroupBy(p => $"{p.StaffId}|{p.OccurredAtUtc:O}|{p.DeviceId}", StringComparer.Ordinal)
      .Select(g => g.First())
      .OrderBy(p => p.OccurredAtUtc)
      .ToList();
  }

  private static string DecodeStaffIdRaw(ReadOnlySpan<byte> b4)
  {
    if (b4.Length < 4) return string.Empty;
    var c1 = (char)(b4[0] | (b4[1] << 8));
    var c2 = (char)(b4[2] | (b4[3] << 8));
    var sb = new StringBuilder(capacity: 2);
    if (c1 != '\0') sb.Append(c1);
    if (c2 != '\0') sb.Append(c2);
    return sb.ToString();
  }

  private sealed class ZkTcpSession : IDisposable
  {
    private const ushort MachinePrepareData1 = 0x5050;
    private const ushort MachinePrepareData2 = 32130;

    private const ushort CmdConnect = 1000;
    private const ushort CmdExit = 1001;
    private const ushort CmdAuth = 1102;
    private const ushort CmdGetTime = 201;
    private const ushort CmdSetTime = 202;

    private const ushort CmdPrepareData = 1500;
    private const ushort CmdData = 1501;
    private const ushort CmdFreeData = 1502;

    private const ushort CmdBufferedRead = 1503;
    private const ushort CmdReadChunk = 1504;

    private const ushort CmdAckOk = 2000;
    private const ushort CmdAckUnauth = 2005;

    private readonly string _ip;
    private readonly int _port;
    private readonly int _commPassword;

    private TcpClient? _client;
    private NetworkStream? _stream;
    private ushort _sessionId;
    private ushort _replyId;

    public ZkTcpSession(string ip, int port, int commPassword)
    {
      _ip = ip;
      _port = port;
      _commPassword = commPassword;
    }

    public void Connect()
    {
      _client = new TcpClient();
      _client.ReceiveTimeout = 10000;
      _client.SendTimeout = 10000;
      _client.Connect(_ip, _port);
      _stream = _client.GetStream();
      _sessionId = 0;
      _replyId = 65534;

      var (code, _) = SendCommand(CmdConnect, ReadOnlySpan<byte>.Empty);
      if (code == CmdAckUnauth)
      {
        var key = MakeCommKey(_commPassword, _sessionId);
        (code, _) = SendCommand(CmdAuth, key);
      }

      if (code != CmdAckOk && code != CmdPrepareData && code != CmdData)
      {
        throw new InvalidOperationException($"ZK connect failed: code={code}");
      }
    }

    public DateTime GetTime()
    {
      var (code, data) = SendCommand(CmdGetTime, ReadOnlySpan<byte>.Empty);
      if (code != CmdAckOk && code != CmdPrepareData && code != CmdData)
      {
        throw new InvalidOperationException($"ZK get_time failed: code={code}");
      }

      if (data.Length < 4) throw new InvalidOperationException("ZK get_time returned too little data.");
      var ts = BitConverter.ToUInt32(data, 0);
      return DecodeZkTime(ts);
    }

    public void SetTime(DateTime localTime)
    {
      var ts = EncodeZkTime(localTime);
      Span<byte> payload = stackalloc byte[4];
      BitConverter.TryWriteBytes(payload, ts);
      var (code, _) = SendCommand(CmdSetTime, payload);
      if (code != CmdAckOk && code != CmdPrepareData && code != CmdData)
      {
        throw new InvalidOperationException($"ZK set_time failed: code={code}");
      }
    }

    public byte[] ReadWithBuffer(ushort command, int fct = 0, int ext = 0)
    {
      Span<byte> cs = stackalloc byte[11];
      cs[0] = 1;
      BitConverter.TryWriteBytes(cs.Slice(1, 2), command);
      BitConverter.TryWriteBytes(cs.Slice(3, 4), fct);
      BitConverter.TryWriteBytes(cs.Slice(7, 4), ext);

      var (code, data) = SendCommand(CmdBufferedRead, cs);
      if (code == CmdData) return data;

      if (data.Length < 5) throw new InvalidOperationException($"ZK buffered read returned too little data: len={data.Length} code={code}");
      var size = BitConverter.ToInt32(data, 1);
      if (size <= 0) return Array.Empty<byte>();

      const int maxChunk = 0xFFc0;
      var remain = size % maxChunk;
      var packets = (size - remain) / maxChunk;

      var buf = new byte[size];
      var pos = 0;
      var start = 0;
      for (var i = 0; i < packets; i++)
      {
        var chunk = ReadChunk(start, maxChunk);
        chunk.CopyTo(buf.AsSpan(pos));
        pos += chunk.Length;
        start += maxChunk;
      }
      if (remain > 0)
      {
        var chunk = ReadChunk(start, remain);
        chunk.CopyTo(buf.AsSpan(pos));
        pos += chunk.Length;
      }

      _ = SendCommand(CmdFreeData, ReadOnlySpan<byte>.Empty);
      if (pos == buf.Length) return buf;
      return buf.AsSpan(0, pos).ToArray();
    }

    private byte[] ReadChunk(int start, int size)
    {
      Span<byte> cs = stackalloc byte[8];
      BitConverter.TryWriteBytes(cs.Slice(0, 4), start);
      BitConverter.TryWriteBytes(cs.Slice(4, 4), size);

      var (code, data) = SendCommand(CmdReadChunk, cs);
      if (code == CmdData) return data;
      if (code == CmdPrepareData)
      {
        if (data.Length < 4) return Array.Empty<byte>();
        var expected = BitConverter.ToInt32(data, 0);
        if (expected <= 0) return Array.Empty<byte>();

        using var ms = new MemoryStream(capacity: expected);
        while (ms.Length < expected)
        {
          var (c, d) = ReadPacket();
          if (c == CmdData && d.Length > 0)
          {
            ms.Write(d, 0, d.Length);
            continue;
          }
          if (c == CmdAckOk) break;
        }

        return ms.ToArray();
      }

      return data;
    }

    private (ushort code, byte[] data) SendCommand(ushort command, ReadOnlySpan<byte> commandString)
    {
      if (_stream is null) throw new InvalidOperationException("Not connected.");
      var packet = CreateHeader(command, commandString);
      var top = CreateTcpTop(packet);
      _stream.Write(top, 0, top.Length);
      return ReadPacket();
    }

    private (ushort code, byte[] data) ReadPacket()
    {
      if (_stream is null) throw new InvalidOperationException("Not connected.");
      Span<byte> top = stackalloc byte[8];
      ReadExactly(_stream, top);

      var magic1 = BitConverter.ToUInt16(top.Slice(0, 2));
      var magic2 = BitConverter.ToUInt16(top.Slice(2, 2));
      if (magic1 != MachinePrepareData1 || magic2 != MachinePrepareData2)
      {
        throw new InvalidOperationException($"Invalid ZK TCP header: {magic1:X4} {magic2:X4}");
      }

      var length = BitConverter.ToUInt32(top.Slice(4, 4));
      if (length < 8 || length > 4_000_000) throw new InvalidOperationException($"Invalid ZK TCP packet length: {length}");

      var body = new byte[length];
      ReadExactly(_stream, body);

      var code = BitConverter.ToUInt16(body.AsSpan(0, 2));
      _sessionId = BitConverter.ToUInt16(body.AsSpan(4, 2));
      _replyId = BitConverter.ToUInt16(body.AsSpan(6, 2));
      var data = body.AsSpan(8).ToArray();
      return (code, data);
    }

    private byte[] CreateHeader(ushort command, ReadOnlySpan<byte> commandString)
    {
      var buf = new byte[8 + commandString.Length];
      BitConverter.TryWriteBytes(buf.AsSpan(0, 2), command);
      BitConverter.TryWriteBytes(buf.AsSpan(2, 2), (ushort)0);
      BitConverter.TryWriteBytes(buf.AsSpan(4, 2), _sessionId);
      BitConverter.TryWriteBytes(buf.AsSpan(6, 2), _replyId);
      commandString.CopyTo(buf.AsSpan(8));

      var checksum = ComputeChecksum(buf);

      var next = _replyId + 1;
      if (next >= 65535) next -= 65535;
      _replyId = (ushort)next;

      var header = new byte[8 + commandString.Length];
      BitConverter.TryWriteBytes(header.AsSpan(0, 2), command);
      BitConverter.TryWriteBytes(header.AsSpan(2, 2), checksum);
      BitConverter.TryWriteBytes(header.AsSpan(4, 2), _sessionId);
      BitConverter.TryWriteBytes(header.AsSpan(6, 2), _replyId);
      commandString.CopyTo(header.AsSpan(8));
      return header;
    }

    private static byte[] CreateTcpTop(byte[] packet)
    {
      var top = new byte[8 + packet.Length];
      BitConverter.TryWriteBytes(top.AsSpan(0, 2), MachinePrepareData1);
      BitConverter.TryWriteBytes(top.AsSpan(2, 2), MachinePrepareData2);
      BitConverter.TryWriteBytes(top.AsSpan(4, 4), (uint)packet.Length);
      Buffer.BlockCopy(packet, 0, top, 8, packet.Length);
      return top;
    }

    private static ushort ComputeChecksum(ReadOnlySpan<byte> p)
    {
      var checksum = 0;
      var i = 0;
      for (; i + 1 < p.Length; i += 2)
      {
        checksum += p[i] | (p[i + 1] << 8);
        if (checksum > 65535) checksum -= 65535;
      }
      if (i < p.Length)
      {
        checksum += p[i];
      }

      while (checksum > 65535) checksum -= 65535;
      checksum = ~checksum;
      while (checksum < 0) checksum += 65535;
      return (ushort)checksum;
    }

    private static void ReadExactly(Stream stream, Span<byte> buffer)
    {
      var offset = 0;
      while (offset < buffer.Length)
      {
        var n = stream.Read(buffer.Slice(offset));
        if (n <= 0) throw new EndOfStreamException();
        offset += n;
      }
    }

    private static byte[] MakeCommKey(int key, ushort sessionId, byte ticks = 50)
    {
      var k = 0u;
      var uk = (uint)key;
      for (var i = 0; i < 32; i++)
      {
        k <<= 1;
        if ((uk & (1u << i)) != 0) k |= 1u;
      }
      k += sessionId;

      Span<byte> kb = stackalloc byte[4];
      BitConverter.TryWriteBytes(kb, k);
      kb[0] = (byte)(kb[0] ^ (byte)'Z');
      kb[1] = (byte)(kb[1] ^ (byte)'K');
      kb[2] = (byte)(kb[2] ^ (byte)'S');
      kb[3] = (byte)(kb[3] ^ (byte)'O');

      var a = BitConverter.ToUInt16(kb.Slice(0, 2));
      var b = BitConverter.ToUInt16(kb.Slice(2, 2));
      Span<byte> swapped = stackalloc byte[4];
      BitConverter.TryWriteBytes(swapped.Slice(0, 2), b);
      BitConverter.TryWriteBytes(swapped.Slice(2, 2), a);

      var B = ticks;
      swapped[0] = (byte)(swapped[0] ^ B);
      swapped[1] = (byte)(swapped[1] ^ B);
      swapped[2] = B;
      swapped[3] = (byte)(swapped[3] ^ B);

      return swapped.ToArray();
    }

    public static DateTime DecodeZkTime(uint t)
    {
      var second = (int)(t % 60);
      t /= 60;
      var minute = (int)(t % 60);
      t /= 60;
      var hour = (int)(t % 24);
      t /= 24;
      var day = (int)(t % 31) + 1;
      t /= 31;
      var month = (int)(t % 12) + 1;
      t /= 12;
      var year = (int)t + 2000;
      return new DateTime(year, month, day, hour, minute, second, DateTimeKind.Local);
    }

    private static uint EncodeZkTime(DateTime t)
    {
      var d = (t.Year % 100 * 12 * 31 + (t.Month - 1) * 31 + t.Day - 1) * (24 * 60 * 60)
        + (t.Hour * 60 + t.Minute) * 60
        + t.Second;
      return (uint)d;
    }

    public void Dispose()
    {
      try
      {
        if (_stream is not null)
        {
          _ = SendCommand(CmdExit, ReadOnlySpan<byte>.Empty);
        }
      }
      catch { }

      try { _stream?.Dispose(); } catch { }
      try { _client?.Dispose(); } catch { }
    }
  }

  private static async Task UpsertToSupabase(AppConfig config, List<Punch> punches, Dictionary<string, string> staffNames)
  {
    using var http = new HttpClient();
    http.Timeout = TimeSpan.FromSeconds(15);

    Console.WriteLine($"Upserting {punches.Count} punches to Supabase...");

    if (config.AutoCreateStaff || config.SyncStaffNames)
    {
      await EnsureStaffRowsExist(http, config, punches.Select(p => p.StaffId).Distinct(StringComparer.Ordinal).ToArray(), staffNames);
    }

    var tz = TimeZoneInfo.Local;
    try { tz = TimeZoneInfo.FindSystemTimeZoneById("Singapore Standard Time"); } catch { }
    var rows = punches.Select(p =>
    {
      var local = TimeZoneInfo.ConvertTime(p.OccurredAtUtc, tz).DateTime;
      var dt = local.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
      return new AttlogRow(p.StaffId, dt, "1", p.VerifyMode.ToString(CultureInfo.InvariantCulture), "1", "0");
    }).ToList();
    await UpsertAttlogRowsToSupabase(config, rows, CancellationToken.None);
    Console.WriteLine("Supabase upsert OK.");
  }

  private static List<AttlogRow> ReadAttlogRows(string path)
  {
    var rows = new List<AttlogRow>(capacity: 1024);
    foreach (var raw in File.ReadLines(path))
    {
      var line = (raw ?? string.Empty).Trim();
      if (line.Length == 0) continue;
      var parts = line.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
      if (parts.Length < 7) continue;
      var staffId = parts[0].Trim();
      if (staffId.Length == 0) continue;
      var dtRaw = parts[1].Trim() + " " + parts[2].Trim();
      rows.Add(new AttlogRow(
        staffId,
        dtRaw,
        parts[3].Trim(),
        parts[4].Trim(),
        parts[5].Trim(),
        parts[6].Trim()
      ));
    }
    return rows;
  }

  private static async Task UpsertAttlogRowsToSupabase(AppConfig config, List<AttlogRow> rows, CancellationToken ct)
  {
    if (rows.Count == 0) return;
    using var http = new HttpClient();
    http.Timeout = TimeSpan.FromSeconds(30);

    var url = $"{config.SupabaseUrl.TrimEnd('/')}/rest/v1/{config.SupabaseAttendanceTable}?on_conflict=id";
    var chunkSize = 1000;
    for (var i = 0; i < rows.Count; i += chunkSize)
    {
      var batch = new List<object>(capacity: chunkSize);
      foreach (var r in rows.Skip(i).Take(chunkSize))
      {
        var staffId = (r.StaffId ?? string.Empty).Trim();
        if (staffId.Length == 0) continue;

        var dtRaw = (r.DateTime ?? string.Empty).Trim();
        if (dtRaw.Length == 0) continue;
        if (!DateTime.TryParseExact(dtRaw, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var localUnspec)) continue;

        var localOffset = new DateTimeOffset(DateTime.SpecifyKind(localUnspec, DateTimeKind.Unspecified), TimeSpan.FromHours(8));
        var occurredAtUtc = localOffset.ToUniversalTime();
        var eventDate = localOffset.ToOffset(TimeSpan.FromHours(8)).Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var id = BuildEventId(config.DeviceId, staffId, occurredAtUtc);

        batch.Add(new
        {
          id,
          device_id = config.DeviceId,
          staff_id = staffId,
          datetime = dtRaw,
          occurred_at = occurredAtUtc.ToString("O", CultureInfo.InvariantCulture),
          event_date = eventDate,
          verified = (r.Verified ?? string.Empty).Trim(),
          status = (r.Status ?? string.Empty).Trim(),
          workcode = (r.Workcode ?? string.Empty).Trim(),
          reserved = (r.Reserved ?? string.Empty).Trim(),
        });
      }

      using var req = new HttpRequestMessage(HttpMethod.Post, url);
      req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");
      req.Headers.TryAddWithoutValidation("Prefer", "resolution=merge-duplicates,return=minimal");
      req.Content = new StringContent(JsonSerializer.Serialize(batch, JsonOptions), Encoding.UTF8, "application/json");

      using var resp = await http.SendAsync(req, ct);
      if (!resp.IsSuccessStatusCode)
      {
        var body = await resp.Content.ReadAsStringAsync(ct);
        throw new InvalidOperationException($"Supabase upsert failed: {(int)resp.StatusCode} {resp.ReasonPhrase}. Body={body}");
      }
    }
  }

  private static async Task EnsureStaffRowsExist(HttpClient http, AppConfig config, string[] staffIds, Dictionary<string, string> staffNames)
  {
    var ids = staffIds.Where(id => !string.IsNullOrWhiteSpace(id)).Distinct(StringComparer.Ordinal).ToArray();
    if (ids.Length == 0) return;

    var quoted = string.Join(",", ids.Select(id => $"\"{id.Replace("\"", "\"\"", StringComparison.Ordinal)}\""));
    var checkUrl = $"{config.SupabaseUrl.TrimEnd('/')}/rest/v1/staff?select=id,full_name&id=in.({quoted})";

    using var check = new HttpRequestMessage(HttpMethod.Get, checkUrl);
    check.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
    check.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");

    using var checkResp = await http.SendAsync(check);
    if (!checkResp.IsSuccessStatusCode)
    {
      var body = await checkResp.Content.ReadAsStringAsync();
      throw new InvalidOperationException($"Supabase staff check failed: {(int)checkResp.StatusCode} {checkResp.ReasonPhrase}. Body={body}");
    }

    var existingJson = await checkResp.Content.ReadAsStringAsync();
    var existing = new Dictionary<string, string?>(StringComparer.Ordinal);
    try
    {
      using var doc = JsonDocument.Parse(existingJson);
      if (doc.RootElement.ValueKind == JsonValueKind.Array)
      {
        foreach (var el in doc.RootElement.EnumerateArray())
        {
          if (el.TryGetProperty("id", out var idEl) && idEl.ValueKind == JsonValueKind.String)
          {
            var v = idEl.GetString();
            if (string.IsNullOrWhiteSpace(v)) continue;
            string? fullName = null;
            if (el.TryGetProperty("full_name", out var fnEl) && fnEl.ValueKind == JsonValueKind.String)
            {
              fullName = fnEl.GetString();
            }
            existing[v] = string.IsNullOrWhiteSpace(fullName) ? null : fullName;
          }
        }
      }
    }
    catch
    {
      existing.Clear();
    }

    var missing = ids.Where(id => !existing.ContainsKey(id)).ToArray();

    var toUpdateName = ids
      .Where(id =>
        existing.TryGetValue(id, out var currentName)
        && (string.IsNullOrWhiteSpace(currentName) || string.Equals(currentName, id, StringComparison.Ordinal))
        && staffNames.TryGetValue(id, out var deviceName)
        && !string.IsNullOrWhiteSpace(deviceName)
      )
      .ToArray();

    if (config.AutoCreateStaff && missing.Length > 0)
    {
      Console.WriteLine($"Auto-creating {missing.Length} staff rows (WL10_AUTO_CREATE_STAFF=1)...");

      var upsertUrl = $"{config.SupabaseUrl.TrimEnd('/')}/rest/v1/staff?on_conflict=id";
      using var req = new HttpRequestMessage(HttpMethod.Post, upsertUrl);
      req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");
      req.Headers.TryAddWithoutValidation("Prefer", "resolution=merge-duplicates,return=minimal");

      var payload = missing.Select(id =>
      {
        staffNames.TryGetValue(id, out var deviceName);
        var fullName = string.IsNullOrWhiteSpace(deviceName) ? id : deviceName;
        return new { id, full_name = fullName, status = "active" };
      }).ToArray();
      req.Content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json");

      using var resp = await http.SendAsync(req);
      if (!resp.IsSuccessStatusCode)
      {
        var body = await resp.Content.ReadAsStringAsync();
        throw new InvalidOperationException($"Supabase staff upsert failed: {(int)resp.StatusCode} {resp.ReasonPhrase}. Body={body}");
      }
    }

    if (toUpdateName.Length > 0)
    {
      Console.WriteLine($"Updating {toUpdateName.Length} staff names from WL10...");

      var upsertUrl = $"{config.SupabaseUrl.TrimEnd('/')}/rest/v1/staff?on_conflict=id";
      using var req = new HttpRequestMessage(HttpMethod.Post, upsertUrl);
      req.Headers.TryAddWithoutValidation("apikey", config.SupabaseServiceRoleKey);
      req.Headers.TryAddWithoutValidation("Authorization", $"Bearer {config.SupabaseServiceRoleKey}");
      req.Headers.TryAddWithoutValidation("Prefer", "resolution=merge-duplicates,return=minimal");

      var payload = toUpdateName.Select(id => new { id, full_name = staffNames[id] }).ToArray();
      req.Content = new StringContent(JsonSerializer.Serialize(payload, JsonOptions), Encoding.UTF8, "application/json");

      using var resp = await http.SendAsync(req);
      if (!resp.IsSuccessStatusCode)
      {
        var body = await resp.Content.ReadAsStringAsync();
        throw new InvalidOperationException($"Supabase staff name update failed: {(int)resp.StatusCode} {resp.ReasonPhrase}. Body={body}");
      }
    }
  }

  private static Dictionary<string, string> ReadDeviceUsers(AppConfig config)
  {
    dynamic? zk = null;
    try
    {
      var type = Type.GetTypeFromProgID("zkemkeeper.CZKEM", throwOnError: false);
      if (type is null)
      {
        try
        {
          type = Type.GetTypeFromCLSID(new Guid("00853A19-BD51-419B-9269-2DABE57EB61F"), throwOnError: false);
        }
        catch
        {
          type = null;
        }
      }

      if (type is null) return new Dictionary<string, string>(StringComparer.Ordinal);

      try
      {
        zk = Activator.CreateInstance(type);
      }
      catch (System.Runtime.InteropServices.COMException ex) when (ex.HResult == unchecked((int)0x80040154))
      {
        if (Interlocked.Exchange(ref ZkComNotRegisteredWarned, 1) == 0)
        {
          Console.WriteLine("ZK COM SDK (zkemkeeper) is not installed/registered on this PC. Skipping user-name read.");
        }
        return new Dictionary<string, string>(StringComparer.Ordinal);
      }
      if (zk is null) return new Dictionary<string, string>(StringComparer.Ordinal);

      var commPasswordOk = zk.SetCommPassword(config.CommPassword);
      _ = commPasswordOk;

      var connected = (bool)zk.Connect_Net(config.DeviceIp, config.DevicePort);
      if (!connected) return new Dictionary<string, string>(StringComparer.Ordinal);

      var readUsersOk = false;
      try { readUsersOk = (bool)zk.ReadAllUserID(config.MachineNumber); } catch { }
      _ = readUsersOk;

      var map = new Dictionary<string, string>(StringComparer.Ordinal);
      while (true)
      {
        string enrollNumber;
        string name;
        string password;
        int privilege;
        bool enabled;

        var ok = false;
        try
        {
          ok = (bool)zk.SSR_GetAllUserInfo(config.MachineNumber, out enrollNumber, out name, out password, out privilege, out enabled);
        }
        catch
        {
          break;
        }

        if (!ok) break;

        var staffNumber = ExtractStaffNumber(config, enrollNumber ?? string.Empty);
        if (staffNumber is null) continue;
        var staffId = MapStaffId(config, staffNumber.Value.ToString(CultureInfo.InvariantCulture));
        if (string.IsNullOrWhiteSpace(staffId)) continue;

        var cleanedName = new string((name ?? string.Empty).Where(c => !char.IsControl(c)).ToArray()).Trim();
        map[staffId] = cleanedName;
      }

      return map;
    }
    catch (Exception ex)
    {
      for (var cur = ex; cur is not null; cur = cur.InnerException)
      {
        if (cur is System.Runtime.InteropServices.COMException ce && ce.HResult == unchecked((int)0x80040154))
        {
          if (Interlocked.Exchange(ref ZkComNotRegisteredWarned, 1) == 0)
          {
            Console.WriteLine("ZK COM SDK (zkemkeeper) is not installed/registered on this PC. Skipping user-name read.");
          }
          return new Dictionary<string, string>(StringComparer.Ordinal);
        }
      }

      Console.Error.WriteLine($"ReadDeviceUsers failed: {ex.Message}");
      return new Dictionary<string, string>(StringComparer.Ordinal);
    }
    finally
    {
      try
      {
        if (zk is not null)
        {
          try { zk.Disconnect(); } catch { }
        }
      }
      catch { }
    }
  }

  private static string BuildEventId(string deviceId, string staffId, DateTimeOffset occurredAtUtc)
  {
    var raw = $"{deviceId}|{staffId}|{occurredAtUtc.ToString("O", CultureInfo.InvariantCulture)}";
    var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(raw));
    var sb = new StringBuilder(bytes.Length * 2);
    foreach (var b in bytes) sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));
    return sb.ToString();
  }
}
