const UI_PREF_KEYS = {
  jobsPageSize: "ui_jobs_page_size",
  jobsAutoRefresh: "ui_jobs_auto_refresh",
  auditPageSize: "ui_audit_page_size",
  auditAutoRefresh: "ui_audit_auto_refresh",
  lastTab: "ui_last_tab",
  sidebarCollapsed: "ui_sidebar_collapsed",
  theme: "ui_theme",
};

const ALLOWED_JOBS_PAGE_SIZE = new Set([10, 20, 50, 100]);
const ALLOWED_AUDIT_PAGE_SIZE = new Set([10]);
const ALLOWED_THEMES = new Set(["light", "dark"]);
const USER_ROLE_OPTIONS = new Set(["SUPER_ADMIN", "ADMIN", "USER"]);
const TAB_IDS = new Set(["query-tab", "import-tab", "jobs-tab", "users-tab"]);
const QUERY_PAGE_SIZE = 10;
const IMPORT_POLL_INTERVAL_MS = 1000;
const AUTO_REFRESH_INTERVAL_MS = 10000;
const TOAST_MAX_ITEMS = 3;
const SESSION_EXPIRED_MESSAGE = "登录状态已失效，请重新登录";
const QUERY_IDLE_MESSAGE = "尚未查询。姓名单字按首字匹配，多字按完整姓名匹配；身份证完整18位按整证匹配，否则按前4位匹配；年份支持前缀筛选。";
const AUDIT_ACTION_LABELS = Object.freeze({
  LOGIN: "登录",
  LOGOUT: "退出登录",
  TOKEN_REFRESH: "刷新令牌",
  DATA_QUERY: "数据查询",
  DATA_DELETE: "删除记录",
  DATA_IMPORT: "数据导入",
  DATA_IMPORT_LIST: "导入任务列表",
  DATA_IMPORT_CANCEL: "取消导入任务",
  USER_CREATE: "创建用户",
  USER_UPDATE: "更新用户",
  USER_DELETE: "删除用户",
  QUOTA_UPDATE: "更新配额",
  QUOTA_VIEW: "查看配额",
});
const STATUS_LABELS = Object.freeze({
  PENDING: "排队中",
  RUNNING: "进行中",
  SUCCESS: "成功",
  FAILED: "失败",
  CANCELLED: "已取消",
  ACTIVE: "启用",
  INACTIVE: "停用",
});
const ROLE_LABELS = Object.freeze({
  SUPER_ADMIN: "超级管理员",
  ADMIN: "管理员",
  USER: "普通用户",
});
const TAB_LABELS = Object.freeze({
  "query-tab": "查询",
  "import-tab": "导入",
  "jobs-tab": "任务",
  "users-tab": "用户",
});
const TAB_SUBTITLES = Object.freeze({
  "query-tab": "姓名单字按首字、多字按全名；身份证完整18位精确，否则按前4位",
  "import-tab": "批量导入加密数据并进入任务追踪",
  "jobs-tab": "查看导入任务进度与处理结果",
  "users-tab": "管理用户权限、状态与配额",
});
const ERROR_REASON_LABELS = Object.freeze({
  api_error_unspecified: "请求失败，请稍后重试",
  validation_error: "请求参数有误，请检查后重试",
  internal_unhandled_exception: "系统异常，请稍后重试",
  auth_required: "请先登录",
  token_invalid: "登录状态已失效，请重新登录",
  token_revoked: "登录状态已失效，请重新登录",
  insufficient_permissions: "没有权限执行此操作",
  invalid_credentials: "用户名或密码错误",
  user_inactive: "账号已被停用，请联系管理员",
  user_not_found: "用户不存在",
  invalid_role: "角色无效，请重新选择",
  cannot_delete_self: "不能删除当前登录账号",
  super_admin_cannot_be_disabled: "超级管理员账号不可禁用",
  quota_target_role_invalid: "仅普通用户可设置配额",
  filename_required: "请选择要上传的文件",
  import_source_required: "请选择上传文件或填写服务器本地文件路径",
  import_source_conflict: "请二选一：上传文件或填写服务器本地文件路径",
  import_source_path_not_found: "服务器本地文件路径不存在",
  import_source_path_not_file: "服务器本地路径不是文件",
  invalid_file_extension: "仅支持上传 .xlsx/.csv 文件",
  import_empty_file: "上传文件为空，请检查后重试",
  import_super_admin_password_invalid: "超级管理员密码错误，请重新输入",
  import_job_missing_after_create: "导入任务创建后未找到，请重试",
  import_job_not_found: "导入任务不存在",
  import_job_not_cancellable: "当前任务状态不可取消",
  import_job_failed: "导入任务执行失败",
  import_job_cancelled: "导入任务已取消",
  import_worker_exception: "导入处理异常，请稍后重试",
  record_not_found: "记录不存在或已被删除",
  key_file_not_found: "密钥文件不存在",
  key_file_permission_invalid: "密钥文件权限不正确",
  active_key_version_missing: "未找到可用密钥版本",
  quota_exceeded_daily: "今日查询次数已达上限",
  quota_exceeded_total: "总查询次数已达上限",
  username_exists: "用户名已存在，请更换后重试",
  create_user_failed: "创建用户失败，请稍后重试",
});
const ERROR_MESSAGE_LABELS = Object.freeze({
  "Request failed": "请求失败，请稍后重试",
  "Request validation failed": "请求参数有误，请检查后重试",
  "Internal server error": "系统异常，请稍后重试",
  "Authentication required": "请先登录",
  "Authentication failed": "登录状态已失效，请重新登录",
  "Insufficient permissions": "没有权限执行此操作",
  "User is inactive": "账号已被停用，请联系管理员",
  "User not found": "用户不存在",
  "Invalid role": "角色无效，请重新选择",
  "Cannot delete current user": "不能删除当前登录账号",
  "Super admin cannot be disabled": "超级管理员账号不可禁用",
  "Quota can only be set for USER role": "仅普通用户可设置配额",
  "Filename is required": "请选择要上传的文件",
  "Either uploaded file or source path is required": "请选择上传文件或填写服务器本地文件路径",
  "Only one import source is allowed": "请二选一：上传文件或填写服务器本地文件路径",
  "Source path not found": "服务器本地文件路径不存在",
  "Source path must be a file": "服务器本地路径不是文件",
  "Only .xlsx/.csv is supported": "仅支持上传 .xlsx/.csv 文件",
  "Uploaded file is empty": "上传文件为空，请检查后重试",
  "Super admin password is invalid": "超级管理员密码错误，请重新输入",
  "Import job not found after creation": "导入任务创建后未找到，请重试",
  "Import job not found": "导入任务不存在",
  "Import job is not cancellable": "当前任务状态不可取消",
  "Import job failed": "导入任务执行失败",
  "Import job cancelled": "导入任务已取消",
  "Import worker failed": "导入处理异常，请稍后重试",
  "Record not found": "记录不存在或已被删除",
  "Key file not found": "密钥文件不存在",
  "Key file permission must be 400": "密钥文件权限必须为 400",
  "Active key version missing": "未找到可用密钥版本",
  "Daily query limit exceeded": "今日查询次数已达上限",
  "Total query limit exceeded": "总查询次数已达上限",
  "Username already exists": "用户名已存在，请更换后重试",
  "Failed to create user": "创建用户失败，请稍后重试",
});

function localizeErrorMessage(rawMessage) {
  const text = String(rawMessage || "").trim();
  if (!text) {
    return "操作失败，请稍后重试";
  }
  if (ERROR_MESSAGE_LABELS[text]) {
    return ERROR_MESSAGE_LABELS[text];
  }
  if (/Failed to fetch|NetworkError|Network request failed|Load failed/i.test(text)) {
    return "网络连接失败，请检查服务是否正常";
  }
  if (/Request failed/i.test(text)) {
    return "请求失败，请稍后重试";
  }
  if (/Upload failed/i.test(text)) {
    return "上传失败，请稍后重试";
  }
  return text;
}

function getSystemTheme() {
  return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function readThemePref() {
  const raw = localStorage.getItem(UI_PREF_KEYS.theme) || "";
  if (ALLOWED_THEMES.has(raw)) {
    return raw;
  }
  return getSystemTheme();
}

const state = {
  token: localStorage.getItem("access_token") || "",
  user: null,
  sessionExpiryTimer: null,
  queryAbortController: null,
  statsAbortController: null,
  jobsAbortController: null,
  auditAbortController: null,
  usersAbortController: null,
  queryRows: [],
  queryPage: 1,
  queryPageSize: QUERY_PAGE_SIZE,
  queryRowElements: [],
  processedRowIds: new Set(),
  selectedQueryRowIds: new Set(),
  activeQueryRowIndex: -1,
  activeQueryRowId: null,
  importFile: null,
  currentTab: TAB_IDS.has(localStorage.getItem(UI_PREF_KEYS.lastTab) || "")
    ? String(localStorage.getItem(UI_PREF_KEYS.lastTab))
    : "query-tab",
  sidebarCollapsed: localStorage.getItem(UI_PREF_KEYS.sidebarCollapsed) === "1",
  jobPollTimer: null,
  jobsPage: 1,
  jobsTotal: 0,
  jobsPageSize: 20,
  jobsAutoRefresh: false,
  jobsAutoRefreshTimer: null,
  jobsLastUpdated: "",
  auditPage: 1,
  auditTotal: 0,
  auditHasMore: false,
  auditPageSize: 10,
  auditAutoRefresh: false,
  auditAutoRefreshTimer: null,
  auditLastUpdated: "",
  theme: readThemePref(),
};

const els = {
  authScreen: document.getElementById("auth-screen"),
  appScreen: document.getElementById("app-screen"),
  userLabel: document.getElementById("user-label"),
  loginForm: document.getElementById("login-form"),
  username: document.getElementById("username"),
  password: document.getElementById("password"),
  loginPasswordToggleBtn: document.getElementById("login-password-toggle-btn"),
  loginCharacterStage: document.getElementById("login-character-stage"),
  loginCharacters: Array.from(document.querySelectorAll(".login-character")),
  themeToggleBtn: document.getElementById("theme-toggle-btn"),
  logoutBtn: document.getElementById("logout-btn"),
  tabs: Array.from(document.querySelectorAll(".tab")),
  panels: Array.from(document.querySelectorAll(".tab-panel")),
  shortcutToggleBtn: document.getElementById("shortcut-toggle-btn"),
  shortcutPanel: document.getElementById("shortcut-panel"),
  queryForm: document.getElementById("query-form"),
  clearQueryBtn: document.getElementById("clear-query-btn"),
  queryBulkActions: document.getElementById("query-bulk-actions"),
  querySelectedCount: document.getElementById("query-selected-count"),
  bulkCopyBtn: document.getElementById("bulk-copy-btn"),
  bulkDeleteBtn: document.getElementById("bulk-delete-btn"),
  bulkClearBtn: document.getElementById("bulk-clear-btn"),
  querySelectAll: document.getElementById("query-select-all"),
  queryMeta: document.getElementById("query-meta"),
  queryPageMeta: document.getElementById("query-page-meta"),
  queryPrevBtn: document.getElementById("query-prev-btn"),
  queryNextBtn: document.getElementById("query-next-btn"),
  queryBody: document.getElementById("query-body"),
  qName: document.getElementById("q-name"),
  qIdno: document.getElementById("q-idno"),
  qYear: document.getElementById("q-year"),
  statsRefreshBtn: document.getElementById("stats-refresh-btn"),
  statsTotalRecords: document.getElementById("stats-total-records"),
  statsTotalImportJobs: document.getElementById("stats-total-import-jobs"),
  statsRunningImportJobs: document.getElementById("stats-running-import-jobs"),
  statsMeta: document.getElementById("stats-meta"),
  importForm: document.getElementById("import-form"),
  importFileInput: document.getElementById("import-file"),
  importSourcePath: document.getElementById("import-source-path"),
  dropzone: document.getElementById("dropzone"),
  uploadProgress: document.getElementById("upload-progress"),
  uploadProgressText: document.getElementById("upload-progress-text"),
  importStatus: document.getElementById("import-status"),
  jobsFilterForm: document.getElementById("jobs-filter-form"),
  jobsStatus: document.getElementById("jobs-status"),
  jobsFilename: document.getElementById("jobs-filename"),
  jobsMeta: document.getElementById("jobs-meta"),
  jobsPageSize: document.getElementById("jobs-page-size"),
  jobsPageInput: document.getElementById("jobs-page-input"),
  jobsRefreshBtn: document.getElementById("jobs-refresh-btn"),
  jobsJumpBtn: document.getElementById("jobs-jump-btn"),
  jobsPrevBtn: document.getElementById("jobs-prev-btn"),
  jobsNextBtn: document.getElementById("jobs-next-btn"),
  jobsAutoRefresh: document.getElementById("jobs-auto-refresh"),
  jobsBody: document.getElementById("jobs-body"),
  createUserForm: document.getElementById("create-user-form"),
  newUsername: document.getElementById("new-username"),
  newPassword: document.getElementById("new-password"),
  toggleNewPasswordBtn: document.getElementById("toggle-new-password-btn"),
  newRole: document.getElementById("new-role"),
  reloadUsersBtn: document.getElementById("reload-users-btn"),
  quotaEditor: document.getElementById("quota-editor"),
  quotaEditorTitle: document.getElementById("quota-editor-title"),
  quotaEditorDaily: document.getElementById("quota-editor-daily"),
  quotaEditorTotal: document.getElementById("quota-editor-total"),
  quotaEditorSaveBtn: document.getElementById("quota-editor-save-btn"),
  quotaEditorCancelBtn: document.getElementById("quota-editor-cancel-btn"),
  usersMeta: document.getElementById("users-meta"),
  usersBody: document.getElementById("users-body"),
  auditFilterForm: document.getElementById("audit-filter-form"),
  auditFrom: document.getElementById("audit-from"),
  auditTo: document.getElementById("audit-to"),
  auditUserId: document.getElementById("audit-user-id"),
  auditActionType: document.getElementById("audit-action-type"),
  auditActionResult: document.getElementById("audit-action-result"),
  auditResetBtn: document.getElementById("audit-reset-btn"),
  auditMeta: document.getElementById("audit-meta"),
  auditPageSize: document.getElementById("audit-page-size"),
  auditPageInput: document.getElementById("audit-page-input"),
  auditRefreshBtn: document.getElementById("audit-refresh-btn"),
  auditJumpBtn: document.getElementById("audit-jump-btn"),
  auditBody: document.getElementById("audit-body"),
  auditPrevBtn: document.getElementById("audit-prev-btn"),
  auditNextBtn: document.getElementById("audit-next-btn"),
  auditAutoRefresh: document.getElementById("audit-auto-refresh"),
  topbarSubtitle: document.getElementById("topbar-subtitle"),
  breadcrumbCurrent: document.getElementById("breadcrumb-current"),
  sidebarToggleBtn: document.getElementById("sidebar-toggle-btn"),
  srAnnouncer: document.getElementById("sr-announcer"),
  toastStack: document.getElementById("toast-stack"),
};

function toast(message, kind = "success", timeoutMs = 3200) {
  const node = document.createElement("div");
  node.className = `toast ${kind}`;
  node.setAttribute("role", kind === "error" ? "alert" : "status");
  node.setAttribute("aria-live", kind === "error" ? "assertive" : "polite");
  const normalized = kind === "error" ? localizeErrorMessage(message) : String(message || "");
  node.textContent = normalized;
  while (els.toastStack.childElementCount >= TOAST_MAX_ITEMS) {
    els.toastStack.firstElementChild?.remove();
  }
  els.toastStack.appendChild(node);
  window.setTimeout(() => {
    node.remove();
  }, timeoutMs);
}

function parseErrorPayload(payload, fallback) {
  if (!payload || typeof payload !== "object") {
    return localizeErrorMessage(fallback);
  }
  const details = payload.details || {};
  const reasonValue = typeof details.reason === "string" ? details.reason.trim() : details.reason;
  const reasonKey = safeLower(reasonValue);
  if (reasonKey && ERROR_REASON_LABELS[reasonKey]) {
    return ERROR_REASON_LABELS[reasonKey];
  }
  if (payload.message) {
    return localizeErrorMessage(payload.message);
  }
  return localizeErrorMessage(fallback);
}

function isAbortError(err) {
  return !!(err && typeof err === "object" && err.name === "AbortError");
}

function cancelRequest(controller) {
  if (!controller) {
    return;
  }
  try {
    controller.abort();
  } catch {
    // no-op
  }
}

function clearSessionExpiryTimer() {
  if (!state.sessionExpiryTimer) {
    return;
  }
  window.clearTimeout(state.sessionExpiryTimer);
  state.sessionExpiryTimer = null;
}

function decodeJwtPayload(token) {
  const parts = String(token || "").split(".");
  if (parts.length < 2) {
    return null;
  }
  const encoded = parts[1].replace(/-/g, "+").replace(/_/g, "/");
  const padding = "=".repeat((4 - (encoded.length % 4)) % 4);
  try {
    return JSON.parse(window.atob(encoded + padding));
  } catch {
    return null;
  }
}

function getTokenExpiryMs(token) {
  const payload = decodeJwtPayload(token);
  const exp = Number(payload && payload.exp);
  if (!Number.isFinite(exp) || exp <= 0) {
    return 0;
  }
  return exp * 1000;
}

function clearSessionAndShowLogin(message = SESSION_EXPIRED_MESSAGE, options = {}) {
  const normalizedMessage = String(message || "").trim() || SESSION_EXPIRED_MESSAGE;
  const hadSession = !!state.token || !!state.user;

  clearSessionExpiryTimer();
  if (state.jobPollTimer) {
    window.clearTimeout(state.jobPollTimer);
    state.jobPollTimer = null;
  }

  state.token = "";
  state.user = null;
  localStorage.removeItem("access_token");
  els.userLabel.textContent = "未登录";
  setControlValue(els.password, "");
  setLoginPasswordVisibility(false);
  if (els.usersMeta) {
    els.usersMeta.textContent = "尚未登录";
  }
  if (els.importStatus) {
    els.importStatus.textContent = "尚未开始导入";
  }
  setUploadProgress(0);
  closeQuotaEditor();
  resetStatsSummary("尚未登录");
  setScreen("auth");
  setTab("query-tab", { persist: false });
  applyRoleUiState();

  if (options.toast === true && hadSession) {
    toast(normalizedMessage, "error");
  }
}

function scheduleSessionExpiry(token) {
  clearSessionExpiryTimer();
  const expiresAt = getTokenExpiryMs(token);
  if (!expiresAt) {
    return;
  }
  const delay = expiresAt - Date.now();
  if (delay <= 0) {
    clearSessionAndShowLogin(SESSION_EXPIRED_MESSAGE, { toast: true });
    return;
  }
  state.sessionExpiryTimer = window.setTimeout(() => {
    state.sessionExpiryTimer = null;
    if (state.token === token) {
      clearSessionAndShowLogin(SESSION_EXPIRED_MESSAGE, { toast: true });
    }
  }, delay);
}

async function api(path, options = {}) {
  const cfg = {
    method: options.method || "GET",
    headers: { ...(options.headers || {}) },
  };
  if (options.signal) {
    cfg.signal = options.signal;
  }
  if (options.auth !== false) {
    if (!state.token) {
      throw new Error("请先登录");
    }
    cfg.headers.Authorization = `Bearer ${state.token}`;
  }

  if (options.body instanceof FormData) {
    cfg.body = options.body;
  } else if (options.body !== undefined) {
    cfg.headers["Content-Type"] = "application/json";
    cfg.body = JSON.stringify(options.body);
  }

  let resp;
  try {
    resp = await fetch(path, cfg);
  } catch (err) {
    if (isAbortError(err)) {
      throw err;
    }
    throw new Error("网络连接失败，请检查服务是否正常");
  }
  const text = await resp.text();
  let payload = {};
  try {
    payload = text ? JSON.parse(text) : {};
  } catch {
    payload = {};
  }
  if (!resp.ok) {
    const errorMessage = parseErrorPayload(payload, `请求失败(${resp.status})`);
    if (resp.status === 401 && options.auth !== false) {
      clearSessionAndShowLogin(errorMessage, { toast: false });
    }
    throw new Error(errorMessage);
  }
  return payload;
}

function copyText(value) {
  return navigator.clipboard.writeText(value);
}

function escapeHtml(raw) {
  return String(raw ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function safeLower(value) {
  return typeof value === "string" ? value.toLowerCase() : "";
}

function statusLabel(status) {
  const code = String(status || "").trim().toUpperCase();
  return STATUS_LABELS[code] || String(status || "-");
}

function roleLabel(role) {
  const code = String(role || "").trim().toUpperCase();
  return ROLE_LABELS[code] || String(role || "-");
}

function currentUserRoleCode() {
  return String(state.user && state.user.role ? state.user.role : "").trim().toUpperCase();
}

function buildRoleOptions(selectedRole) {
  const current = String(selectedRole || "").trim().toUpperCase();
  const allRoles = ["SUPER_ADMIN", "ADMIN", "USER"];
  return allRoles
    .map((role) => `<option value="${role}"${current === role ? " selected" : ""}>${roleLabel(role)}</option>`)
    .join("");
}

function roleCanDelete() {
  const roleCode = currentUserRoleCode();
  return roleCode === "SUPER_ADMIN" || roleCode === "ADMIN";
}

function roleCanImport() {
  return currentUserRoleCode() === "SUPER_ADMIN";
}

function roleCanViewJobs() {
  return roleCanDelete();
}

function roleCanManageUsers() {
  return currentUserRoleCode() === "SUPER_ADMIN";
}

function roleCanViewAudit() {
  const roleCode = currentUserRoleCode();
  return roleCode === "SUPER_ADMIN" || roleCode === "ADMIN";
}

function normalizePositiveInt(value, fallback = 1) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 1) {
    return fallback;
  }
  return Math.floor(n);
}

function getInputValue(el) {
  return el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement || el instanceof HTMLSelectElement
    ? String(el.value || "")
    : "";
}

function setControlValue(el, value) {
  if (el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement || el instanceof HTMLSelectElement) {
    el.value = String(value ?? "");
  }
}

function setCheckboxChecked(el, checked) {
  if (el instanceof HTMLInputElement && el.type === "checkbox") {
    el.checked = !!checked;
  }
}

function readBoolPref(key, fallback = false) {
  const raw = localStorage.getItem(key);
  if (raw === null) {
    return fallback;
  }
  return raw === "1";
}

function readPageSizePref(key, fallback, allowedSet) {
  const raw = localStorage.getItem(key);
  if (!raw) {
    return fallback;
  }
  const parsed = normalizePositiveInt(raw, fallback);
  return allowedSet.has(parsed) ? parsed : fallback;
}

function persistUiPref(key, value) {
  localStorage.setItem(key, String(value));
}

function applyTheme(theme, options = {}) {
  const persist = options.persist !== false;
  const resolvedTheme = ALLOWED_THEMES.has(theme) ? theme : "light";
  state.theme = resolvedTheme;
  document.body.setAttribute("data-theme", resolvedTheme);
  if (els.themeToggleBtn) {
    const nextModeText = resolvedTheme === "dark" ? "浅色模式" : "深色模式";
    els.themeToggleBtn.textContent = nextModeText;
    els.themeToggleBtn.title = `切换到${nextModeText}`;
    els.themeToggleBtn.setAttribute("aria-pressed", resolvedTheme === "dark" ? "true" : "false");
  }
  if (persist) {
    persistUiPref(UI_PREF_KEYS.theme, resolvedTheme);
  }
}

function toggleTheme() {
  applyTheme(state.theme === "dark" ? "light" : "dark");
}

function setLoginSceneMode(mode) {
  if (!(els.authScreen instanceof HTMLElement)) {
    return;
  }
  els.authScreen.setAttribute("data-auth-mode", String(mode || "idle"));
}

function setLoginCharacterLookOffsets(offsetX, offsetY) {
  if (!Array.isArray(els.loginCharacters)) {
    return;
  }
  const presets = [
    { x: 1, y: 1 },
    { x: 0.82, y: 0.86 },
    { x: 0.94, y: 1.04 },
    { x: 0.68, y: 0.72 },
  ];
  els.loginCharacters.forEach((item, index) => {
    if (!(item instanceof HTMLElement)) {
      return;
    }
    const preset = presets[index] || presets[0];
    item.style.setProperty("--look-x", `${(offsetX * preset.x).toFixed(2)}px`);
    item.style.setProperty("--look-y", `${(offsetY * preset.y).toFixed(2)}px`);
  });
}

function setLoginPasswordVisibility(visible) {
  if (!(els.password instanceof HTMLInputElement) || !(els.loginPasswordToggleBtn instanceof HTMLButtonElement)) {
    return;
  }
  const nextVisible = !!visible;
  els.password.type = nextVisible ? "text" : "password";
  els.loginPasswordToggleBtn.textContent = nextVisible ? "隐藏" : "显示";
  els.loginPasswordToggleBtn.setAttribute("aria-pressed", nextVisible ? "true" : "false");
  els.loginPasswordToggleBtn.setAttribute("aria-label", nextVisible ? "隐藏密码" : "显示密码");
}

function syncLoginSceneMode() {
  if (!(els.authScreen instanceof HTMLElement)) {
    return;
  }
  const usernameFilled = !!getInputValue(els.username).trim();
  const passwordFilled = !!getInputValue(els.password);
  const passwordVisible =
    els.loginPasswordToggleBtn instanceof HTMLButtonElement &&
    els.loginPasswordToggleBtn.getAttribute("aria-pressed") === "true";

  let nextMode = "idle";
  if (document.activeElement === els.password) {
    nextMode = passwordVisible ? "peek" : "secret";
  } else if (document.activeElement === els.username) {
    nextMode = "typing";
  } else if (passwordFilled) {
    nextMode = passwordVisible ? "peek" : "secret";
  } else if (usernameFilled) {
    nextMode = "typing";
  }
  setLoginSceneMode(nextMode);
  if (nextMode === "secret") {
    setLoginCharacterLookOffsets(0, 0);
    return;
  }
  if (nextMode === "peek") {
    setLoginCharacterLookOffsets(1.8, -0.8);
    return;
  }
  if (nextMode === "typing") {
    const ratio = Math.max(0, Math.min(1, getInputValue(els.username).trim().length / 14));
    const nextX = -5 + ratio * 10;
    setLoginCharacterLookOffsets(nextX, -0.6);
    return;
  }
  setLoginCharacterLookOffsets(0, 0);
}

function resetLoginCharacterLook() {
  setLoginCharacterLookOffsets(0, 0);
}

function getLoginSceneMode() {
  return els.authScreen instanceof HTMLElement ? els.authScreen.getAttribute("data-auth-mode") || "idle" : "idle";
}

function updateLoginLookFromUsernamePointer(clientX, clientY) {
  if (!(els.username instanceof HTMLInputElement)) {
    return;
  }
  const rect = els.username.getBoundingClientRect();
  if (!rect.width || !rect.height) {
    return;
  }
  const ratioX = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
  const ratioY = Math.max(0, Math.min(1, (clientY - rect.top) / rect.height));
  const offsetX = -5 + ratioX * 10;
  const offsetY = -1.6 + ratioY * 2.2;
  setLoginCharacterLookOffsets(offsetX, offsetY);
}

function updateLoginCharacterLook(clientX, clientY) {
  if (els.appScreen && !els.appScreen.hidden) {
    return;
  }
  if (!Array.isArray(els.loginCharacters) || els.loginCharacters.length === 0) {
    return;
  }
  const sceneMode = getLoginSceneMode();
  if (sceneMode === "secret") {
    return;
  }
  if (sceneMode === "typing" && document.activeElement === els.username) {
    updateLoginLookFromUsernamePointer(clientX, clientY);
    return;
  }
  if (!(els.loginCharacterStage instanceof HTMLElement)) {
    return;
  }
  const rect = els.loginCharacterStage.getBoundingClientRect();
  if (!rect.width || !rect.height) {
    return;
  }
  const ratioX = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
  const ratioY = Math.max(0, Math.min(1, (clientY - rect.top) / rect.height));
  const offsetX = -6 + ratioX * 12;
  const offsetY = -3 + ratioY * 6;
  setLoginCharacterLookOffsets(offsetX, offsetY);
}

function hydrateUiPrefs() {
  state.jobsPageSize = readPageSizePref(UI_PREF_KEYS.jobsPageSize, 20, ALLOWED_JOBS_PAGE_SIZE);
  state.auditPageSize = readPageSizePref(UI_PREF_KEYS.auditPageSize, 10, ALLOWED_AUDIT_PAGE_SIZE);
  state.jobsAutoRefresh = readBoolPref(UI_PREF_KEYS.jobsAutoRefresh, false);
  state.auditAutoRefresh = readBoolPref(UI_PREF_KEYS.auditAutoRefresh, false);
}

function nowClockText() {
  return new Date().toLocaleTimeString("zh-CN", { hour12: false });
}

function renderLoadingRow(tbodyEl, colspan, text) {
  tbodyEl.innerHTML = `<tr><td colspan='${colspan}' class='table-loading'><span class='loading-dot'></span>${escapeHtml(text)}</td></tr>`;
  if (tbodyEl === els.queryBody) {
    state.queryRowElements = [];
  }
}

function renderEmptyRow(tbodyEl, colspan, title, hint = "") {
  const hintHtml = hint ? `<div class='table-empty-hint'>${escapeHtml(hint)}</div>` : "";
  const visualHtml =
    "<div class='table-empty-visual' aria-hidden='true'>" +
    "<svg viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'>" +
    "<path d='M4 6h16'></path><path d='M4 12h16'></path><path d='M4 18h9'></path><circle cx='19' cy='18' r='1.5'></circle>" +
    "</svg></div>";
  tbodyEl.innerHTML =
    `<tr><td colspan='${colspan}' class='table-empty'>` +
    `${visualHtml}<div class='table-empty-title'>${escapeHtml(title)}</div>${hintHtml}</td></tr>`;
  if (tbodyEl === els.queryBody) {
    state.queryRowElements = [];
  }
}

function stopJobsAutoRefresh() {
  if (state.jobsAutoRefreshTimer) {
    window.clearInterval(state.jobsAutoRefreshTimer);
    state.jobsAutoRefreshTimer = null;
  }
}

function stopAuditAutoRefresh() {
  if (state.auditAutoRefreshTimer) {
    window.clearInterval(state.auditAutoRefreshTimer);
    state.auditAutoRefreshTimer = null;
  }
}

function syncAutoRefreshTimers() {
  stopJobsAutoRefresh();
  stopAuditAutoRefresh();

  if (els.appScreen.hidden) {
    return;
  }

  if (state.token && roleCanViewJobs() && state.currentTab === "jobs-tab" && state.jobsAutoRefresh && !document.hidden) {
    state.jobsAutoRefreshTimer = window.setInterval(async () => {
      try {
        await loadJobs(state.jobsPage, { silent: true });
      } catch (err) {
        stopJobsAutoRefresh();
        toast(`任务自动刷新已停止: ${err.message}`, "error");
      }
    }, AUTO_REFRESH_INTERVAL_MS);
  }

  if (state.token && roleCanViewAudit() && state.currentTab === "audit-tab" && state.auditAutoRefresh && !document.hidden) {
    state.auditAutoRefreshTimer = window.setInterval(async () => {
      try {
        await loadAuditLogs(state.auditPage, { silent: true });
      } catch (err) {
        stopAuditAutoRefresh();
        toast(`操作记录自动刷新已停止: ${err.message}`, "error");
      }
    }, AUTO_REFRESH_INTERVAL_MS);
  }
}

function setScreen(screen, options = {}) {
  const showApp = screen === "app";
  const updateHash = options.updateHash !== false;
  els.authScreen.hidden = showApp;
  els.appScreen.hidden = !showApp;
  if (!showApp) {
    cancelRequest(state.queryAbortController);
    cancelRequest(state.statsAbortController);
    cancelRequest(state.jobsAbortController);
    cancelRequest(state.auditAbortController);
    cancelRequest(state.usersAbortController);
    state.queryAbortController = null;
    state.statsAbortController = null;
    state.jobsAbortController = null;
    state.auditAbortController = null;
    state.usersAbortController = null;
    const querySubmitBtn = els.queryForm.querySelector("button[type='submit']");
    if (querySubmitBtn instanceof HTMLButtonElement) {
      querySubmitBtn.disabled = false;
    }
    stopJobsAutoRefresh();
    stopAuditAutoRefresh();
    setShortcutPanelVisible(false);
  }
  if (updateHash) {
    const targetHash = showApp ? "#/home" : "#/login";
    if (window.location.hash !== targetHash) {
      window.history.replaceState(null, "", targetHash);
    }
  }
  if (showApp) {
    setLoginSceneMode("idle");
    resetLoginCharacterLook();
  } else {
    syncLoginSceneMode();
    resetLoginCharacterLook();
  }
  if (!showApp && els.username) {
    window.setTimeout(() => {
      els.username.focus();
    }, 0);
  }
}

function applySidebarState(collapsed, options = {}) {
  const persist = options.persist !== false;
  state.sidebarCollapsed = !!collapsed;
  els.appScreen.classList.toggle("sidebar-collapsed", state.sidebarCollapsed);
  if (els.sidebarToggleBtn) {
    const nextAction = state.sidebarCollapsed ? "展开侧边栏" : "折叠侧边栏";
    els.sidebarToggleBtn.setAttribute("data-collapsed", state.sidebarCollapsed ? "1" : "0");
    els.sidebarToggleBtn.title = nextAction;
    els.sidebarToggleBtn.setAttribute("aria-label", nextAction);
    els.sidebarToggleBtn.setAttribute("aria-expanded", state.sidebarCollapsed ? "false" : "true");
  }
  els.tabs.forEach((btn) => {
    const tabId = btn.dataset.tab || "";
    if (state.sidebarCollapsed) {
      btn.title = TAB_LABELS[tabId] || "";
      return;
    }
    btn.removeAttribute("title");
  });
  if (persist) {
    persistUiPref(UI_PREF_KEYS.sidebarCollapsed, state.sidebarCollapsed ? "1" : "0");
  }
}

function setTab(tabId, options = {}) {
  const persist = options.persist !== false;
  state.currentTab = tabId;
  els.tabs.forEach((btn) => {
    const isActive = btn.dataset.tab === tabId;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive ? "true" : "false");
    btn.setAttribute("aria-current", isActive ? "page" : "false");
  });
  els.panels.forEach((panel) => {
    panel.classList.toggle("active", panel.id === tabId);
  });
  if (els.breadcrumbCurrent) {
    els.breadcrumbCurrent.textContent = TAB_LABELS[tabId] || "查询";
  }
  if (els.topbarSubtitle) {
    els.topbarSubtitle.textContent = TAB_SUBTITLES[tabId] || TAB_SUBTITLES["query-tab"];
  }
  if (els.srAnnouncer) {
    els.srAnnouncer.textContent = `已切换到${TAB_LABELS[tabId] || "查询"}模块`;
  }
  if (persist && TAB_IDS.has(tabId)) {
    persistUiPref(UI_PREF_KEYS.lastTab, tabId);
  }
  syncAutoRefreshTimers();
}

function applyRoleUiState() {
  const canImport = roleCanImport();
  const canViewJobs = roleCanViewJobs();
  const canManageUsers = roleCanManageUsers();
  const canViewAudit = roleCanViewAudit();
  els.tabs.forEach((btn) => {
    const tab = btn.dataset.tab;
    const forbidden =
      (!canImport && tab === "import-tab") ||
      (!canViewJobs && tab === "jobs-tab") ||
      (!canManageUsers && tab === "users-tab") ||
      (!canViewAudit && tab === "audit-tab");
    btn.disabled = forbidden;
    btn.classList.toggle("is-disabled", forbidden);
    btn.setAttribute("aria-disabled", forbidden ? "true" : "false");
  });
  if (
    (!canImport && state.currentTab === "import-tab") ||
    (!canViewJobs && state.currentTab === "jobs-tab") ||
    (!canManageUsers && state.currentTab === "users-tab") ||
    (!canViewAudit && state.currentTab === "audit-tab")
  ) {
    setTab("query-tab", { persist: false });
  }
  syncQuerySelectionUi();
  syncAutoRefreshTimers();
}

function updateNewPasswordVisibility(visible) {
  if (!(els.newPassword instanceof HTMLInputElement) || !(els.toggleNewPasswordBtn instanceof HTMLButtonElement)) {
    return;
  }
  const nextVisible = !!visible;
  els.newPassword.type = nextVisible ? "text" : "password";
  els.toggleNewPasswordBtn.textContent = nextVisible ? "隐藏" : "显示";
  els.toggleNewPasswordBtn.setAttribute("aria-pressed", nextVisible ? "true" : "false");
  els.toggleNewPasswordBtn.setAttribute("aria-label", nextVisible ? "隐藏密码" : "显示密码");
}

async function saveUserQuota(userId) {
  const uid = Number(userId);
  if (!uid) {
    return;
  }
  const dailyRaw = getInputValue(els.quotaEditorDaily).trim();
  const totalRaw = getInputValue(els.quotaEditorTotal).trim();
  if (!dailyRaw || !totalRaw) {
    throw new Error("请先填写每日上限和总上限");
  }
  const dailyNum = Number(dailyRaw);
  const totalNum = Number(totalRaw);
  if (!Number.isInteger(dailyNum) || dailyNum < 0 || !Number.isInteger(totalNum) || totalNum < 0) {
    throw new Error("配额必须是非负整数");
  }
  await api(`/api/v1/users/${uid}/quota`, {
    method: "PUT",
    body: { daily_limit: dailyNum, total_limit: totalNum },
  });
  toast(`用户 ${uid} 配额已更新`);
  closeQuotaEditor();
  await loadUsers();
}

function openQuotaEditor(userId, username, dailyLimit, totalLimit) {
  if (!(els.quotaEditor instanceof HTMLElement)) {
    return;
  }
  const uid = Number(userId);
  if (!uid) {
    return;
  }
  const daily = Number.isFinite(Number(dailyLimit)) ? Number(dailyLimit) : 0;
  const total = Number.isFinite(Number(totalLimit)) ? Number(totalLimit) : 0;
  if (els.quotaEditorTitle) {
    els.quotaEditorTitle.textContent = `配额设置：${username || `用户 ${uid}`} (ID=${uid})`;
  }
  setControlValue(els.quotaEditorDaily, Math.max(0, daily));
  setControlValue(els.quotaEditorTotal, Math.max(0, total));
  if (els.quotaEditorSaveBtn) {
    els.quotaEditorSaveBtn.dataset.userId = String(uid);
  }
  els.quotaEditor.hidden = false;
  if (els.quotaEditorDaily instanceof HTMLInputElement) {
    els.quotaEditorDaily.focus();
    els.quotaEditorDaily.select();
  }
}

function closeQuotaEditor() {
  if (!(els.quotaEditor instanceof HTMLElement)) {
    return;
  }
  els.quotaEditor.hidden = true;
  if (els.quotaEditorTitle) {
    els.quotaEditorTitle.textContent = "配额设置";
  }
  setControlValue(els.quotaEditorDaily, "");
  setControlValue(els.quotaEditorTotal, "");
  if (els.quotaEditorSaveBtn) {
    delete els.quotaEditorSaveBtn.dataset.userId;
  }
}

function formatQuota(meta) {
  const q = meta && meta.quota ? meta.quota : null;
  if (!q) {
    return "配额: -";
  }
  return `配额 今日 ${q.daily_used}/${q.daily_limit}，总计 ${q.total_used}/${q.total_limit}`;
}

function getQueryPageCount() {
  return Math.max(1, Math.ceil(state.queryRows.length / state.queryPageSize));
}

function updateQueryPagerUi() {
  const pageCount = getQueryPageCount();
  state.queryPage = Math.min(Math.max(1, state.queryPage), pageCount);
  if (els.queryPageMeta) {
    els.queryPageMeta.textContent = `第 ${state.queryPage}/${pageCount} 页，每页 ${state.queryPageSize} 条`;
  }
  if (els.queryPrevBtn) {
    els.queryPrevBtn.disabled = state.queryPage <= 1 || state.queryRows.length === 0;
  }
  if (els.queryNextBtn) {
    els.queryNextBtn.disabled = state.queryPage >= pageCount || state.queryRows.length === 0;
  }
}

function getQueryRowElements() {
  if (state.queryRowElements.length > 0 && state.queryRowElements.every((row) => row.isConnected)) {
    return state.queryRowElements;
  }
  state.queryRowElements = Array.from(els.queryBody.querySelectorAll("tr[data-row-id]"));
  return state.queryRowElements;
}

function syncQuerySelectionUi() {
  const rows = getQueryRowElements();
  const availableIds = new Set();
  rows.forEach((row) => {
    const rowId = Number(row.dataset.rowId || 0);
    if (!rowId) {
      return;
    }
    availableIds.add(rowId);
  });
  for (const rowId of Array.from(state.selectedQueryRowIds)) {
    if (!availableIds.has(rowId)) {
      state.selectedQueryRowIds.delete(rowId);
    }
  }

  rows.forEach((row) => {
    const rowId = Number(row.dataset.rowId || 0);
    const selected = rowId > 0 && state.selectedQueryRowIds.has(rowId);
    row.classList.toggle("selected-row", selected);
    const checkbox = row.querySelector(".query-row-check");
    if (checkbox instanceof HTMLInputElement) {
      checkbox.checked = selected;
    }
  });

  const selectedCount = state.selectedQueryRowIds.size;
  if (els.querySelectedCount) {
    els.querySelectedCount.textContent = `已选择 ${selectedCount} 条`;
  }
  if (els.queryBulkActions) {
    els.queryBulkActions.hidden = selectedCount === 0;
  }
  if (els.bulkCopyBtn) {
    els.bulkCopyBtn.disabled = selectedCount === 0;
  }
  if (els.bulkClearBtn) {
    els.bulkClearBtn.disabled = selectedCount === 0;
  }
  if (els.bulkDeleteBtn) {
    const canDelete = roleCanDelete();
    els.bulkDeleteBtn.hidden = !canDelete;
    els.bulkDeleteBtn.disabled = selectedCount === 0 || !canDelete;
  }
  if (els.querySelectAll) {
    const total = availableIds.size;
    if (total === 0) {
      els.querySelectAll.checked = false;
      els.querySelectAll.indeterminate = false;
      els.querySelectAll.disabled = true;
    } else {
      els.querySelectAll.disabled = false;
      els.querySelectAll.checked = selectedCount === total;
      els.querySelectAll.indeterminate = selectedCount > 0 && selectedCount < total;
    }
  }
}

function clearQuerySelection() {
  state.selectedQueryRowIds.clear();
  syncQuerySelectionUi();
}

function toggleQueryRowSelection(rowId, selected) {
  const targetId = Number(rowId);
  if (!targetId) {
    return;
  }
  if (selected) {
    state.selectedQueryRowIds.add(targetId);
  } else {
    state.selectedQueryRowIds.delete(targetId);
  }
  syncQuerySelectionUi();
}

function setAllQuerySelection(selected) {
  state.selectedQueryRowIds.clear();
  if (selected) {
    for (const row of getQueryRowElements()) {
      const rowId = Number(row.dataset.rowId || 0);
      if (rowId) {
        state.selectedQueryRowIds.add(rowId);
      }
    }
  }
  syncQuerySelectionUi();
}

function markQueryRowsProcessed(rowIds) {
  for (const rowId of rowIds) {
    state.processedRowIds.add(rowId);
    const rowEl = els.queryBody.querySelector(`tr[data-row-id='${rowId}']`);
    if (rowEl) {
      rowEl.classList.add("processed");
    }
  }
}

async function copyRowsByIds(rowIds) {
  const idSet = new Set(rowIds.map((item) => Number(item)).filter((item) => item > 0));
  if (idSet.size === 0) {
    return 0;
  }
  const orderedRows = state.queryRows.filter((item) => idSet.has(Number(item.id)));
  if (orderedRows.length === 0) {
    return 0;
  }
  const lines = orderedRows.map((item) =>
    [item.id, item.name ?? "", item.id_no ?? "", item.year ?? "", item.match_score ?? 0].join("\t")
  );
  await copyText(lines.join("\n"));
  markQueryRowsProcessed(orderedRows.map((item) => Number(item.id)));
  return orderedRows.length;
}

function clearActiveQueryRow() {
  state.activeQueryRowIndex = -1;
  state.activeQueryRowId = null;
  getQueryRowElements().forEach((row) => row.classList.remove("active-row"));
}

function setActiveQueryRowByIndex(index, options = {}) {
  const rows = getQueryRowElements();
  if (rows.length === 0) {
    clearActiveQueryRow();
    return false;
  }
  const scroll = options.scroll !== false;
  const bounded = Math.max(0, Math.min(rows.length - 1, index));
  state.activeQueryRowIndex = bounded;
  state.activeQueryRowId = Number(rows[bounded].dataset.rowId || 0) || null;
  rows.forEach((row, rowIndex) => {
    row.classList.toggle("active-row", rowIndex === bounded);
  });
  if (scroll) {
    rows[bounded].scrollIntoView({ block: "nearest" });
  }
  return true;
}

function moveActiveQueryRow(offset) {
  const rows = getQueryRowElements();
  if (rows.length === 0) {
    return false;
  }
  const current = state.activeQueryRowIndex >= 0 ? state.activeQueryRowIndex : 0;
  return setActiveQueryRowByIndex(current + offset);
}

async function copyQueryRow(_row, rowId) {
  const copied = await copyRowsByIds([rowId]);
  if (copied === 0) {
    throw new Error("当前无可复制记录");
  }
  toast(`记录 ${rowId} 已复制`);
}

async function copyActiveQueryRow() {
  const rows = getQueryRowElements();
  if (rows.length === 0 || state.activeQueryRowIndex < 0) {
    toast("当前无可复制记录", "error");
    return;
  }
  const row = rows[state.activeQueryRowIndex];
  const rowId = Number(row.dataset.rowId || 0);
  if (!rowId) {
    toast("当前无可复制记录", "error");
    return;
  }
  await copyQueryRow(row, rowId);
}

async function deleteActiveQueryRow() {
  if (!roleCanDelete()) {
    toast("当前角色无删除权限", "error");
    return;
  }
  const rows = getQueryRowElements();
  if (rows.length === 0 || state.activeQueryRowIndex < 0) {
    toast("当前无可删除记录", "error");
    return;
  }
  const row = rows[state.activeQueryRowIndex];
  const rowId = Number(row.dataset.rowId || 0);
  if (!rowId) {
    toast("当前无可删除记录", "error");
    return;
  }
  await deleteRow(rowId);
}

async function copySelectedQueryRows() {
  const selectedIds = Array.from(state.selectedQueryRowIds);
  if (selectedIds.length === 0) {
    toast("请先选择要复制的记录", "error");
    return;
  }
  const count = await copyRowsByIds(selectedIds);
  if (count === 0) {
    toast("当前无可复制记录", "error");
    return;
  }
  toast(`已复制 ${count} 条记录`);
}

async function deleteSelectedQueryRows() {
  if (!roleCanDelete()) {
    toast("当前角色无删除权限", "error");
    return;
  }
  const selectedIds = Array.from(state.selectedQueryRowIds);
  if (selectedIds.length === 0) {
    toast("请先选择要删除的记录", "error");
    return;
  }
  if (!window.confirm(`确认删除选中记录（${selectedIds.length} 条）？`)) {
    return;
  }
  const confirmText = window.prompt("请输入“确认批量删除”后继续");
  if (confirmText !== "确认批量删除") {
    toast("删除已取消：确认文本不匹配", "error");
    return;
  }
  let successCount = 0;
  const failedIds = [];
  for (const rowId of selectedIds) {
    try {
      await api(`/api/v1/records/${rowId}`, { method: "DELETE" });
      successCount += 1;
    } catch {
      failedIds.push(rowId);
    }
  }
  clearQuerySelection();
  if (successCount > 0) {
    toast(`已删除 ${successCount} 条记录`);
    await runQuery();
  }
  if (failedIds.length > 0) {
    toast(`删除失败 ${failedIds.length} 条: ${failedIds.join(", ")}`, "error", 5000);
  }
}

function renderQueryRows(rows, options = {}) {
  if (Array.isArray(rows)) {
    state.queryRows = rows;
  }
  const pageCount = getQueryPageCount();
  const requestedPage = Number(options.page || state.queryPage || 1);
  state.queryPage = Math.max(1, Math.min(pageCount, requestedPage));
  const start = (state.queryPage - 1) * state.queryPageSize;
  const end = start + state.queryPageSize;
  const pagedRows = state.queryRows.slice(start, end);
  state.selectedQueryRowIds.clear();
  const canDelete = roleCanDelete();
  const rowsHtml = pagedRows
    .map((row) => {
      const processedClass = state.processedRowIds.has(row.id) ? " class=\"processed\"" : "";
      const rowId = escapeHtml(row.id);
      const name = escapeHtml(row.name ?? "");
      const idNo = escapeHtml(row.id_no ?? "");
      const year = escapeHtml(row.year ?? "");
      const score = escapeHtml(row.match_score ?? 0);
      const actionButtons = [
        `<button class="btn-mini copy-row-btn" data-row-id="${rowId}">复制整行</button>`,
        canDelete ? `<button class="btn-mini delete-row-btn" data-row-id="${rowId}">删除</button>` : "",
      ].join("");
      return `
      <tr data-row-id="${rowId}"${processedClass}>
        <td class="col-select">
          <input class="query-row-check" type="checkbox" data-row-id="${rowId}" aria-label="选择记录 ${rowId}" />
        </td>
        <td>${rowId}</td>
        <td>${name}</td>
        <td>${idNo}</td>
        <td>${year}</td>
        <td>${score}</td>
        <td><div class="mini-actions">${actionButtons}</div></td>
      </tr>`;
    })
    .join("");
  if (rowsHtml) {
    els.queryBody.innerHTML = rowsHtml;
    state.queryRowElements = Array.from(els.queryBody.querySelectorAll("tr[data-row-id]"));
  } else {
    state.queryRowElements = [];
  }
  if (pagedRows.length > 0) {
    setActiveQueryRowByIndex(0, { scroll: false });
  } else {
    renderEmptyRow(els.queryBody, 7, "暂无符合条件的数据", "请调整检索条件后再试");
    clearActiveQueryRow();
  }
  updateQueryPagerUi();
  syncQuerySelectionUi();
}

async function runQuery() {
  cancelRequest(state.queryAbortController);
  const abortController = new AbortController();
  state.queryAbortController = abortController;

  const submitBtn = els.queryForm.querySelector("button[type='submit']");
  if (submitBtn instanceof HTMLButtonElement) {
    submitBtn.disabled = true;
  }
  state.queryRows = [];
  state.queryPage = 1;
  updateQueryPagerUi();
  els.queryMeta.textContent = "查询执行中...";
  renderLoadingRow(els.queryBody, 7, "正在执行查询");

  try {
    const payload = {};
    const nameKw = getInputValue(els.qName).trim();
    const idKw = getInputValue(els.qIdno).trim();
    const yearKw = getInputValue(els.qYear).trim();
    if (nameKw) {
      payload.name_keyword = nameKw;
    }
    if (idKw) {
      payload.id_no_keyword = idKw;
    }
    if (yearKw) {
      payload.year_prefix = yearKw;
    }

    const data = await api("/api/v1/query", {
      method: "POST",
      body: payload,
      signal: abortController.signal,
    });
    if (state.queryAbortController !== abortController) {
      return;
    }
    state.processedRowIds.clear();
    renderQueryRows(data.data || [], { page: 1 });
    const meta = data.meta || {};
    els.queryMeta.textContent =
      `返回条数: ${meta.returned ?? 0}\n` +
      `是否触发上限: ${meta.capped ? "是" : "否"}\n` +
      `${formatQuota(meta)}`;
  } catch (err) {
    if (isAbortError(err)) {
      return;
    }
    const message = localizeErrorMessage(err && err.message ? err.message : "查询失败，请稍后重试");
    if (els.queryMeta) {
      els.queryMeta.textContent = `查询失败：${message}`;
    }
    renderEmptyRow(els.queryBody, 7, "查询失败", message);
    throw err;
  } finally {
    if (state.queryAbortController === abortController) {
      state.queryAbortController = null;
      if (submitBtn instanceof HTMLButtonElement) {
        submitBtn.disabled = false;
      }
    }
  }
}

async function deleteRow(recordId) {
  if (!roleCanDelete()) {
    toast("当前角色无删除权限", "error");
    return;
  }
  const confirm1 = window.confirm(`确认删除记录 ID=${recordId} 吗？`);
  if (!confirm1) {
    return;
  }
  const confirmText = window.prompt("请输入“确认删除”后继续");
  if (confirmText !== "确认删除") {
    toast("删除已取消：确认文本不匹配", "error");
    return;
  }
  await api(`/api/v1/records/${recordId}`, { method: "DELETE" });
  toast(`记录 ${recordId} 已删除`);
  await runQuery();
}

function setUploadProgress(percent) {
  const val = Math.max(0, Math.min(100, percent));
  els.uploadProgress.style.width = `${val}%`;
  els.uploadProgressText.textContent = `${val.toFixed(0)}%`;
}

function uploadImport({ file, sourcePath }) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", "/api/v1/data/import");
    xhr.setRequestHeader("Authorization", `Bearer ${state.token}`);
    if (file) {
      xhr.upload.onprogress = (event) => {
        if (!event.lengthComputable) {
          return;
        }
        setUploadProgress((event.loaded / event.total) * 100);
      };
    }
    xhr.onerror = () => reject(new Error("上传失败：网络错误"));
    xhr.onload = () => {
      let payload = {};
      try {
        payload = JSON.parse(xhr.responseText || "{}");
      } catch {
        payload = {};
      }
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(payload);
        return;
      }
      const errorMessage = parseErrorPayload(payload, `上传失败(${xhr.status})`);
      if (xhr.status === 401) {
        clearSessionAndShowLogin(errorMessage, { toast: false });
      }
      reject(new Error(errorMessage));
    };
    const form = new FormData();
    if (file) {
      form.append("file", file);
    }
    if (sourcePath) {
      form.append("source_path", sourcePath);
    }
    xhr.send(form);
  });
}

async function pollImportJob(jobId) {
  if (state.jobPollTimer) {
    window.clearTimeout(state.jobPollTimer);
  }
  const run = async () => {
    try {
      const payload = await api(`/api/v1/data/import/${jobId}`);
      const job = payload.data;
      const total = Number(job.total_rows || 0);
      const done = Number(job.success_rows || 0) + Number(job.skipped_rows || 0) + Number(job.failed_rows || 0);
      if (total > 0) {
        setUploadProgress((done / total) * 100);
      }
      els.importStatus.textContent =
        `任务 ${job.id} 状态：${statusLabel(job.status)}\n` +
        `总计：${job.total_rows}，成功：${job.success_rows}，跳过：${job.skipped_rows}，失败：${job.failed_rows}`;
      if (["SUCCESS", "FAILED", "CANCELLED"].includes(job.status)) {
        if (roleCanViewJobs()) {
          await loadJobs(state.jobsPage);
        }
        toast(`导入任务 ${job.id} 结束：${statusLabel(job.status)}`, job.status === "SUCCESS" ? "success" : "error");
        return;
      }
      state.jobPollTimer = window.setTimeout(run, IMPORT_POLL_INTERVAL_MS);
    } catch (err) {
      toast(err.message, "error");
    }
  };
  await run();
}

function formatJobStats(item) {
  return `总计：${item.total_rows}，成功：${item.success_rows}，跳过：${item.skipped_rows}，失败：${item.failed_rows}`;
}

function statusBadge(status) {
  return `<span class="status-pill status-${escapeHtml(status)}">${escapeHtml(statusLabel(status))}</span>`;
}

function updateJobsPagerUi() {
  const pageCount = Math.max(1, Math.ceil(state.jobsTotal / state.jobsPageSize));
  const updated = state.jobsLastUpdated ? `，更新时间：${state.jobsLastUpdated}` : "";
  if (els.jobsMeta) {
    els.jobsMeta.textContent = `共 ${state.jobsTotal} 条，当前第 ${state.jobsPage}/${pageCount} 页，每页 ${state.jobsPageSize} 条${updated}`;
  }
  setControlValue(els.jobsPageInput, state.jobsPage);
  setControlValue(els.jobsPageSize, state.jobsPageSize);
  if (els.jobsPageInput) {
    els.jobsPageInput.disabled = false;
  }
  if (els.jobsPageSize) {
    els.jobsPageSize.disabled = false;
  }
  if (els.jobsRefreshBtn) {
    els.jobsRefreshBtn.disabled = false;
  }
  if (els.jobsJumpBtn) {
    els.jobsJumpBtn.disabled = false;
  }
  if (els.jobsAutoRefresh) {
    els.jobsAutoRefresh.disabled = false;
  }
  if (els.jobsPrevBtn) {
    els.jobsPrevBtn.disabled = state.jobsPage <= 1;
  }
  if (els.jobsNextBtn) {
    els.jobsNextBtn.disabled = state.jobsPage >= pageCount;
  }
}

function formatCount(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return "-";
  }
  return num.toLocaleString("zh-CN");
}

function renderStatsSummary(data) {
  const summary = data && typeof data === "object" ? data : {};
  if (els.statsTotalRecords) {
    els.statsTotalRecords.textContent = formatCount(summary.total_records ?? 0);
  }
  if (els.statsTotalImportJobs) {
    els.statsTotalImportJobs.textContent = formatCount(summary.total_import_jobs ?? 0);
  }
  if (els.statsRunningImportJobs) {
    els.statsRunningImportJobs.textContent = formatCount(summary.running_import_jobs ?? 0);
  }
}

function resetStatsSummary(metaText = "尚未加载统计") {
  if (els.statsTotalRecords) {
    els.statsTotalRecords.textContent = "-";
  }
  if (els.statsTotalImportJobs) {
    els.statsTotalImportJobs.textContent = "-";
  }
  if (els.statsRunningImportJobs) {
    els.statsRunningImportJobs.textContent = "-";
  }
  if (els.statsMeta) {
    els.statsMeta.textContent = metaText;
  }
}

async function loadStats(options = {}) {
  const silent = !!options.silent;
  if (!state.token) {
    resetStatsSummary("尚未登录");
    return;
  }
  if (!silent && els.statsMeta) {
    els.statsMeta.textContent = "统计加载中...";
  }

  cancelRequest(state.statsAbortController);
  const abortController = new AbortController();
  state.statsAbortController = abortController;
  try {
    const payload = await api("/api/v1/stats/summary", { signal: abortController.signal });
    if (state.statsAbortController !== abortController) {
      return;
    }
    renderStatsSummary(payload.data || {});
    if (els.statsMeta) {
      const updated = nowClockText();
      els.statsMeta.textContent = `统计已更新：${updated}`;
    }
  } catch (err) {
    if (isAbortError(err)) {
      return;
    }
    throw err;
  } finally {
    if (state.statsAbortController === abortController) {
      state.statsAbortController = null;
    }
  }
}

async function loadJobs(page = 1, options = {}) {
  const silent = !!options.silent;
  if (!roleCanViewJobs()) {
    renderEmptyRow(els.jobsBody, 6, "当前角色无任务查看权限", "仅超级管理员和管理员可查看导入任务");
    els.jobsMeta.textContent = "仅超级管理员和管理员可查看导入任务";
    state.jobsPage = 1;
    state.jobsTotal = 0;
    state.jobsLastUpdated = "";
    state.jobsAutoRefresh = false;
    setCheckboxChecked(els.jobsAutoRefresh, false);
    setControlValue(els.jobsPageInput, 1);
    if (els.jobsPageInput) {
      els.jobsPageInput.disabled = true;
    }
    if (els.jobsPageSize) {
      els.jobsPageSize.disabled = true;
    }
    if (els.jobsRefreshBtn) {
      els.jobsRefreshBtn.disabled = true;
    }
    if (els.jobsJumpBtn) {
      els.jobsJumpBtn.disabled = true;
    }
    if (els.jobsAutoRefresh) {
      els.jobsAutoRefresh.disabled = true;
    }
    if (els.jobsPrevBtn) {
      els.jobsPrevBtn.disabled = true;
    }
    if (els.jobsNextBtn) {
      els.jobsNextBtn.disabled = true;
    }
    syncAutoRefreshTimers();
    return;
  }

  if (!silent) {
    els.jobsMeta.textContent = "任务列表加载中...";
    renderLoadingRow(els.jobsBody, 6, "正在加载任务列表");
  }

  cancelRequest(state.jobsAbortController);
  const abortController = new AbortController();
  state.jobsAbortController = abortController;

  const params = new URLSearchParams();
  const status = getInputValue(els.jobsStatus).trim();
  const filenameContains = getInputValue(els.jobsFilename).trim();
  if (status) {
    params.set("status", status);
  }
  if (filenameContains) {
    params.set("filename_contains", filenameContains);
  }
  const targetPage = normalizePositiveInt(page, 1);
  params.set("page", String(targetPage));
  params.set("page_size", String(state.jobsPageSize));

  try {
    const payload = await api(`/api/v1/data/import?${params.toString()}`, { signal: abortController.signal });
    if (state.jobsAbortController !== abortController) {
      return;
    }
    state.jobsPage = Number(payload.page || targetPage);
    state.jobsTotal = Number(payload.total || 0);
    state.jobsPageSize = Number(payload.page_size || state.jobsPageSize);
    state.jobsLastUpdated = nowClockText();
    const pageCount = Math.max(1, Math.ceil(state.jobsTotal / state.jobsPageSize));
    if (state.jobsPage > pageCount) {
      await loadJobs(pageCount, options);
      return;
    }

    const rowsHtml = (payload.data || [])
      .map((item) => {
        const canCancel = ["PENDING", "RUNNING"].includes(item.status) && roleCanViewJobs();
        return `
      <td>${escapeHtml(item.id)}</td>
      <td>${escapeHtml(item.filename)}</td>
      <td>${statusBadge(item.status)}</td>
      <td>${escapeHtml(formatJobStats(item))}</td>
      <td>${escapeHtml(item.created_at || "-")}</td>
      <td>
        <div class="mini-actions">
          ${canCancel ? `<button class="btn-mini cancel-job-btn" data-job-id="${item.id}">取消</button>` : "<span>-</span>"}
        </div>
      </td>
    `;
      })
      .map((cells) => `<tr>${cells}</tr>`)
      .join("");
    if (rowsHtml) {
      els.jobsBody.innerHTML = rowsHtml;
    } else {
      renderEmptyRow(els.jobsBody, 6, "暂无任务", "可切换筛选条件或稍后刷新");
    }
    updateJobsPagerUi();
  } catch (err) {
    if (isAbortError(err)) {
      return;
    }
    throw err;
  } finally {
    if (state.jobsAbortController === abortController) {
      state.jobsAbortController = null;
    }
  }
}

function renderUserRows(rows) {
  const rowsHtml = (rows || []).map((row) => {
    const roleCode = String(row.role || "").trim().toUpperCase();
    const isSuperAdmin = roleCode === "SUPER_ADMIN";
    const quota = row.quota || null;
    const dailyLimit = quota && roleCode === "USER" ? Number(quota.daily_limit || 0) : 0;
    const totalLimit = quota && roleCode === "USER" ? Number(quota.total_limit || 0) : 0;
    const quotaText =
      quota && roleCode === "USER"
        ? `日 ${quota.daily_used}/${quota.daily_limit} · 总 ${quota.total_used}/${quota.total_limit}`
        : "-";
    return `
      <td>${escapeHtml(row.id)}</td>
      <td>${escapeHtml(row.username)}</td>
      <td><span class="role-pill role-${roleCode}">${escapeHtml(roleLabel(roleCode))}</span></td>
      <td><span class="${row.is_active ? "state-active" : "state-inactive"}">${row.is_active ? statusLabel("ACTIVE") : statusLabel("INACTIVE")}</span></td>
      <td>${escapeHtml(row.last_login_at || "-")}</td>
      <td>${escapeHtml(quotaText)}</td>
      <td>
        <div class="mini-actions">
          <select class="user-role-select" data-user-id="${row.id}" aria-label="用户${row.id}角色">
            ${buildRoleOptions(roleCode)}
          </select>
          <button class="btn-mini user-role-save-btn" data-user-id="${row.id}" data-current-role="${roleCode}">保存角色</button>
          <button class="btn-mini user-toggle-btn" data-user-id="${row.id}" data-is-active="${row.is_active ? "1" : "0"}"${isSuperAdmin ? " disabled title=\"超级管理员不可禁用\"" : ""}>${isSuperAdmin ? "不可禁用" : row.is_active ? "禁用" : "启用"}</button>
          ${
            roleCode === "USER"
              ? `<button class="btn-mini user-quota-btn" data-user-id="${row.id}" data-username="${escapeHtml(row.username)}" data-daily-limit="${dailyLimit}" data-total-limit="${totalLimit}">配额</button>`
              : ""
          }
          <button class="btn-mini user-del-btn" data-user-id="${row.id}" data-username="${escapeHtml(row.username)}">删除</button>
        </div>
      </td>
    `;
  });
  if (rowsHtml.length > 0) {
    els.usersBody.innerHTML = rowsHtml.map((cells) => `<tr>${cells}</tr>`).join("");
  } else {
    els.usersBody.innerHTML = "";
  }
  if (!rows || rows.length === 0) {
    renderEmptyRow(els.usersBody, 7, "暂无用户", "可通过上方表单创建新用户");
  }
}

async function loadUsers() {
  if (!roleCanManageUsers()) {
    closeQuotaEditor();
    renderEmptyRow(els.usersBody, 7, "当前角色无用户管理权限", "仅超级管理员可管理用户");
    els.usersMeta.textContent = "仅超级管理员可管理用户";
    return;
  }
  cancelRequest(state.usersAbortController);
  const abortController = new AbortController();
  state.usersAbortController = abortController;
  try {
    const listResp = await api("/api/v1/users", { signal: abortController.signal });
    if (state.usersAbortController !== abortController) {
      return;
    }
    const users = listResp.data || [];
    const userRows = users.filter((u) => u.role === "USER");
    renderUserRows(users);
    els.usersMeta.textContent = `用户总数: ${users.length}（普通用户: ${userRows.length}）`;
  } catch (err) {
    if (isAbortError(err)) {
      return;
    }
    throw err;
  } finally {
    if (state.usersAbortController === abortController) {
      state.usersAbortController = null;
    }
  }
}

function normalizeDateTimeLocal(value) {
  if (!value) {
    return "";
  }
  const v = value.replace("T", " ");
  return v.length === 16 ? `${v}:00` : v;
}

function auditActionLabel(actionType) {
  const key = String(actionType || "").trim().toUpperCase();
  return AUDIT_ACTION_LABELS[key] || actionType || "-";
}

function renderAuditRows(rows) {
  const rowsHtml = (rows || [])
    .map((item) => `
      <td>${escapeHtml(item.id)}</td>
      <td>${escapeHtml(item.event_time || "-")}</td>
      <td>${escapeHtml(item.username || "-")} (${escapeHtml(roleLabel(item.user_role || "-"))})</td>
      <td>${escapeHtml(item.ip_address || "-")}</td>
      <td>${escapeHtml(auditActionLabel(item.action_type))}</td>
      <td>${statusBadge(item.action_result || "-")}</td>
    `)
    .map((cells) => `<tr>${cells}</tr>`)
    .join("");
  els.auditBody.innerHTML = rowsHtml;
  if (!rows || rows.length === 0) {
    renderEmptyRow(els.auditBody, 6, "暂无操作记录", "请调整过滤条件或扩大时间范围");
  }
}

function updateAuditPagerUi() {
  const totalKnown = state.auditTotal >= 0;
  const pageCount = totalKnown ? Math.max(1, Math.ceil(state.auditTotal / state.auditPageSize)) : "?";
  if (els.auditMeta) {
    els.auditMeta.textContent = "";
  }
  setControlValue(els.auditPageInput, state.auditPage);
  setControlValue(els.auditPageSize, state.auditPageSize);
  if (els.auditPageInput) {
    els.auditPageInput.disabled = false;
  }
  if (els.auditPageSize) {
    els.auditPageSize.disabled = false;
  }
  if (els.auditRefreshBtn) {
    els.auditRefreshBtn.disabled = false;
  }
  if (els.auditJumpBtn) {
    els.auditJumpBtn.disabled = false;
  }
  if (els.auditAutoRefresh) {
    els.auditAutoRefresh.disabled = false;
  }
  if (els.auditPrevBtn) {
    els.auditPrevBtn.disabled = state.auditPage <= 1;
  }
  if (els.auditNextBtn) {
    els.auditNextBtn.disabled = totalKnown ? state.auditPage >= Number(pageCount) : !state.auditHasMore;
  }
}

async function loadAuditLogs(page = 1, options = {}) {
  const silent = !!options.silent;
  if (!roleCanViewAudit()) {
    renderEmptyRow(els.auditBody, 6, "当前角色无查看权限", "仅超级管理员和管理员可查看操作记录");
    els.auditMeta.textContent = "";
    state.auditPage = 1;
    state.auditTotal = 0;
    state.auditHasMore = false;
    state.auditLastUpdated = "";
    state.auditAutoRefresh = false;
    setCheckboxChecked(els.auditAutoRefresh, false);
    setControlValue(els.auditPageInput, 1);
    if (els.auditPageInput) {
      els.auditPageInput.disabled = true;
    }
    if (els.auditPageSize) {
      els.auditPageSize.disabled = true;
    }
    if (els.auditRefreshBtn) {
      els.auditRefreshBtn.disabled = true;
    }
    if (els.auditJumpBtn) {
      els.auditJumpBtn.disabled = true;
    }
    if (els.auditAutoRefresh) {
      els.auditAutoRefresh.disabled = true;
    }
    if (els.auditPrevBtn) {
      els.auditPrevBtn.disabled = true;
    }
    if (els.auditNextBtn) {
      els.auditNextBtn.disabled = true;
    }
    syncAutoRefreshTimers();
    return;
  }

  if (!silent) {
    els.auditMeta.textContent = "";
    renderLoadingRow(els.auditBody, 6, "正在加载操作记录");
  }

  cancelRequest(state.auditAbortController);
  const abortController = new AbortController();
  state.auditAbortController = abortController;

  const params = new URLSearchParams();
  const from = normalizeDateTimeLocal(getInputValue(els.auditFrom).trim());
  const to = normalizeDateTimeLocal(getInputValue(els.auditTo).trim());
  const userId = getInputValue(els.auditUserId).trim();
  const actionType = getInputValue(els.auditActionType).trim();
  const actionResult = getInputValue(els.auditActionResult).trim();

  if (from) {
    params.set("from", from);
  }
  if (to) {
    params.set("to", to);
  }
  if (userId) {
    params.set("user_id", userId);
  }
  if (actionType) {
    params.set("action_type", actionType);
  }
  if (actionResult) {
    params.set("action_result", actionResult);
  }
  const targetPage = normalizePositiveInt(page, 1);
  params.set("page", String(targetPage));
  params.set("page_size", String(state.auditPageSize));
  params.set("with_total", "0");

  try {
    const payload = await api(`/api/v1/audit-logs?${params.toString()}`, { signal: abortController.signal });
    if (state.auditAbortController !== abortController) {
      return;
    }
    state.auditPage = Number(payload.page || targetPage);
    state.auditTotal = Number(payload.total ?? -1);
    state.auditHasMore = Boolean(payload.has_more);
    state.auditPageSize = Number(payload.page_size || state.auditPageSize);
    state.auditLastUpdated = nowClockText();
    const pageCount = Math.max(1, Math.ceil(state.auditTotal / state.auditPageSize));
    if (state.auditTotal >= 0 && state.auditPage > pageCount) {
      await loadAuditLogs(pageCount, options);
      return;
    }

    renderAuditRows(payload.data || []);
    updateAuditPagerUi();
  } catch (err) {
    if (isAbortError(err)) {
      return;
    }
    throw err;
  } finally {
    if (state.auditAbortController === abortController) {
      state.auditAbortController = null;
    }
  }
}

function setUserFromLogin(body) {
  const loginUser = body.user || {};
  state.user = {
    username: loginUser.username || "",
    role: String(loginUser.role || "").trim().toUpperCase(),
  };
  state.token = body.access_token;
  localStorage.setItem("access_token", state.token);
  scheduleSessionExpiry(state.token);
  els.userLabel.textContent = `${state.user.username} / ${roleLabel(state.user.role)}`;
  setControlValue(els.password, "");
  setLoginPasswordVisibility(false);
  setScreen("app");
  setTab(state.currentTab, { persist: false });
  applySidebarState(state.sidebarCollapsed, { persist: false });
  applyRoleUiState();
  toast("登录成功");
}

async function hydrateLogin() {
  if (!state.token) {
    clearSessionAndShowLogin("", { toast: false });
    return;
  }
  try {
    const me = await api("/api/v1/auth/me");
    state.user = {
      username: me.data.username,
      role: String(me.data.role || "").trim().toUpperCase(),
    };
    scheduleSessionExpiry(state.token);
    els.userLabel.textContent = `${state.user.username} / ${roleLabel(state.user.role)}`;
    setScreen("app");
    applyRoleUiState();
  } catch {
    clearSessionAndShowLogin("", { toast: false });
  }
}

function isEditableTarget(target) {
  if (!(target instanceof HTMLElement)) {
    return false;
  }
  return !!target.closest("input, textarea, select, [contenteditable=''], [contenteditable='true'], [contenteditable='plaintext-only']");
}

function setShortcutPanelVisible(visible) {
  els.shortcutPanel.hidden = !visible;
  els.shortcutPanel.setAttribute("aria-hidden", visible ? "false" : "true");
  els.shortcutToggleBtn.textContent = visible ? "收起快捷键" : "快捷键帮助";
  els.shortcutToggleBtn.setAttribute("aria-expanded", visible ? "true" : "false");
}

function toggleShortcutPanel() {
  setShortcutPanelVisible(els.shortcutPanel.hidden);
}

function getEnabledTabButtons() {
  return els.tabs.filter((tabBtn) => !tabBtn.disabled);
}

async function handleTabKeyboardNavigation(e, currentBtn) {
  const key = e.key;
  if (!["ArrowRight", "ArrowLeft", "Home", "End"].includes(key)) {
    return;
  }
  const enabledTabs = getEnabledTabButtons();
  if (enabledTabs.length === 0) {
    return;
  }
  const currentIndex = enabledTabs.indexOf(currentBtn);
  if (currentIndex < 0) {
    return;
  }
  e.preventDefault();
  let nextIndex = currentIndex;
  if (key === "ArrowRight") {
    nextIndex = (currentIndex + 1) % enabledTabs.length;
  } else if (key === "ArrowLeft") {
    nextIndex = (currentIndex - 1 + enabledTabs.length) % enabledTabs.length;
  } else if (key === "Home") {
    nextIndex = 0;
  } else if (key === "End") {
    nextIndex = enabledTabs.length - 1;
  }
  const nextBtn = enabledTabs[nextIndex];
  if (!nextBtn) {
    return;
  }
  nextBtn.focus();
  await openTabAndLoad(nextBtn.dataset.tab || "query-tab");
}

async function openTabAndLoad(tabId) {
  if (els.appScreen.hidden) {
    return;
  }
  const btn = els.tabs.find((item) => item.dataset.tab === tabId);
  if (!btn) {
    return;
  }
  if (btn.disabled) {
    toast("当前角色无访问权限", "error");
    return;
  }
  setTab(tabId);
  if (tabId === "jobs-tab") {
    await loadJobs(state.jobsPage);
    return;
  }
  if (tabId === "users-tab") {
    await loadUsers();
    return;
  }
  if (tabId === "audit-tab") {
    await loadAuditLogs(state.auditPage);
  }
}

function focusPrimaryInputForCurrentTab() {
  const focusMap = {
    "query-tab": els.qName,
    "jobs-tab": els.jobsFilename,
    "users-tab": els.newUsername,
  };
  const target = focusMap[state.currentTab];
  if (!target || target.disabled) {
    return;
  }
  target.focus();
  if (typeof target.select === "function") {
    target.select();
  }
}

function hasQueryConditions() {
  const nameKw = getInputValue(els.qName).trim();
  const idKw = getInputValue(els.qIdno).trim();
  const yearKw = getInputValue(els.qYear).trim();
  return !!(nameKw || idKw || yearKw);
}

async function refreshCurrentTabData() {
  if (state.currentTab === "query-tab") {
    if (!hasQueryConditions()) {
      toast("请先输入查询条件再刷新", "error");
      return;
    }
    await runQuery();
    return;
  }
  if (state.currentTab === "jobs-tab") {
    await loadJobs(state.jobsPage);
    return;
  }
  if (state.currentTab === "users-tab") {
    await loadUsers();
    return;
  }
  if (state.currentTab === "audit-tab") {
    await loadAuditLogs(state.auditPage);
    return;
  }
  if (state.currentTab === "import-tab" && roleCanViewJobs()) {
    await loadJobs(state.jobsPage);
    return;
  }
}

function submitCurrentTabPrimaryAction() {
  if (state.currentTab === "query-tab") {
    els.queryForm.requestSubmit();
    return;
  }
  if (state.currentTab === "import-tab") {
    if (!roleCanImport()) {
      toast("当前角色无导入权限", "error");
      return;
    }
    els.importForm.requestSubmit();
    return;
  }
  if (state.currentTab === "jobs-tab") {
    els.jobsFilterForm.requestSubmit();
    return;
  }
  if (state.currentTab === "users-tab") {
    if (!roleCanManageUsers()) {
      toast("当前角色无用户管理权限", "error");
      return;
    }
    els.createUserForm.requestSubmit();
    return;
  }
  if (state.currentTab === "audit-tab") {
    if (!roleCanViewAudit()) {
      toast("当前角色无操作记录查看权限", "error");
      return;
    }
    els.auditFilterForm.requestSubmit();
    return;
  }
}

async function handleGlobalShortcuts(e) {
  if (els.appScreen.hidden) {
    return;
  }
  const key = typeof e.key === "string" ? e.key : "";
  if (!key) {
    return;
  }
  const lowerKey = safeLower(key);
  const typing = isEditableTarget(e.target);

  if (key === "Escape" && !els.shortcutPanel.hidden) {
    e.preventDefault();
    setShortcutPanelVisible(false);
    return;
  }

  if (typing) {
    return;
  }

  if (key === "?" || (key === "/" && e.shiftKey)) {
    e.preventDefault();
    toggleShortcutPanel();
    return;
  }

  if (!e.ctrlKey && !e.metaKey && !e.altKey && key === "/") {
    e.preventDefault();
    focusPrimaryInputForCurrentTab();
    return;
  }

  if (e.altKey && !e.ctrlKey && !e.metaKey) {
    const tabMap = {
      q: "query-tab",
      i: "import-tab",
      j: "jobs-tab",
      u: "users-tab",
    };
    if (lowerKey === "b") {
      e.preventDefault();
      applySidebarState(!state.sidebarCollapsed);
      return;
    }
    const tabId = tabMap[lowerKey];
    if (tabId) {
      e.preventDefault();
      await openTabAndLoad(tabId);
      return;
    }
  }

  if (!e.ctrlKey && !e.metaKey && !e.altKey && state.currentTab === "query-tab") {
    if (lowerKey === "j" || key === "ArrowDown") {
      e.preventDefault();
      moveActiveQueryRow(1);
      return;
    }
    if (lowerKey === "k" || key === "ArrowUp") {
      e.preventDefault();
      moveActiveQueryRow(-1);
      return;
    }
    if (lowerKey === "y") {
      e.preventDefault();
      await copyActiveQueryRow();
      return;
    }
    if (lowerKey === "d") {
      e.preventDefault();
      await deleteActiveQueryRow();
      return;
    }
  }

  if ((e.ctrlKey || e.metaKey) && key === "Enter") {
    e.preventDefault();
    submitCurrentTabPrimaryAction();
    return;
  }

  if (!e.ctrlKey && !e.metaKey && !e.altKey && lowerKey === "r") {
    if (e.repeat) {
      return;
    }
    e.preventDefault();
    await refreshCurrentTabData();
  }
}

function bindEvents() {
  if (els.loginPasswordToggleBtn instanceof HTMLButtonElement) {
    els.loginPasswordToggleBtn.addEventListener("click", () => {
      const nextVisible = els.loginPasswordToggleBtn.getAttribute("aria-pressed") !== "true";
      setLoginPasswordVisibility(nextVisible);
      syncLoginSceneMode();
      if (els.password instanceof HTMLInputElement) {
        els.password.focus();
      }
    });
  }

  [els.username, els.password].forEach((field) => {
    if (!(field instanceof HTMLInputElement)) {
      return;
    }
    field.addEventListener("focus", () => {
      syncLoginSceneMode();
    });
    field.addEventListener("blur", () => {
      window.setTimeout(() => {
        syncLoginSceneMode();
      }, 0);
    });
    field.addEventListener("input", () => {
      syncLoginSceneMode();
    });
  });

  if (els.authScreen instanceof HTMLElement) {
    els.authScreen.addEventListener("pointermove", (e) => {
      updateLoginCharacterLook(e.clientX, e.clientY);
    });
    els.authScreen.addEventListener("pointerleave", () => {
      resetLoginCharacterLook();
    });
  }

  if (els.themeToggleBtn) {
    els.themeToggleBtn.addEventListener("click", () => {
      if (els.appScreen.hidden) {
        return;
      }
      toggleTheme();
    });
  }

  if (els.sidebarToggleBtn) {
    els.sidebarToggleBtn.addEventListener("click", () => {
      if (els.appScreen.hidden) {
        return;
      }
      applySidebarState(!state.sidebarCollapsed);
    });
  }

  els.tabs.forEach((btn) => {
    btn.addEventListener("click", async () => {
      if (els.appScreen.hidden) {
        return;
      }
      if (btn.dataset.tab === "jobs-tab") {
        try {
          await openTabAndLoad("jobs-tab");
        } catch (err) {
          toast(err.message, "error");
        }
        return;
      }
      if (btn.dataset.tab === "users-tab") {
        try {
          await openTabAndLoad("users-tab");
        } catch (err) {
          toast(err.message, "error");
        }
        return;
      }
      if (btn.disabled) {
        return;
      }
      setTab(btn.dataset.tab);
    });
    btn.addEventListener("keydown", (e) => {
      handleTabKeyboardNavigation(e, btn).catch((err) => {
        toast(err.message, "error");
      });
    });
  });

  els.shortcutToggleBtn.addEventListener("click", () => {
    toggleShortcutPanel();
  });

  document.addEventListener("keydown", (e) => {
    handleGlobalShortcuts(e).catch((err) => {
      toast(err.message, "error");
    });
  });

  els.loginForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      const body = await api("/api/v1/auth/login", {
        method: "POST",
        auth: false,
        body: {
          username: getInputValue(els.username).trim(),
          password: getInputValue(els.password),
        },
      });
      setUserFromLogin(body);
      if (roleCanViewJobs()) {
        await loadJobs(state.jobsPage);
      }
      if (roleCanManageUsers()) {
        await loadUsers();
      }
    } catch (err) {
      toast(err.message, "error");
    }
  });

  if (els.toggleNewPasswordBtn instanceof HTMLButtonElement) {
    els.toggleNewPasswordBtn.addEventListener("click", () => {
      updateNewPasswordVisibility(els.newPassword.type === "password");
    });
  }

  els.logoutBtn.addEventListener("click", async () => {
    if (!state.token) {
      return;
    }
    try {
      await api("/api/v1/auth/logout", { method: "POST" });
      toast("已退出");
    } catch (err) {
      toast(err.message, "error");
    } finally {
      clearSessionAndShowLogin("", { toast: false });
    }
  });

  els.queryForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await runQuery();
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.clearQueryBtn.addEventListener("click", () => {
    setControlValue(els.qName, "");
    setControlValue(els.qIdno, "");
    setControlValue(els.qYear, "");
    state.queryRows = [];
    state.queryPage = 1;
    els.queryBody.innerHTML = "";
    els.queryMeta.textContent = QUERY_IDLE_MESSAGE;
    state.processedRowIds.clear();
    state.selectedQueryRowIds.clear();
    clearActiveQueryRow();
    updateQueryPagerUi();
    syncQuerySelectionUi();
  });

  if (els.queryPrevBtn) {
    els.queryPrevBtn.addEventListener("click", () => {
      if (state.queryPage <= 1) {
        return;
      }
      renderQueryRows(null, { page: state.queryPage - 1 });
    });
  }
  if (els.queryNextBtn) {
    els.queryNextBtn.addEventListener("click", () => {
      const pageCount = getQueryPageCount();
      if (state.queryPage >= pageCount) {
        return;
      }
      renderQueryRows(null, { page: state.queryPage + 1 });
    });
  }

  if (els.querySelectAll) {
    els.querySelectAll.addEventListener("change", () => {
      setAllQuerySelection(els.querySelectAll.checked);
    });
  }

  if (els.bulkCopyBtn) {
    els.bulkCopyBtn.addEventListener("click", async () => {
      try {
        await copySelectedQueryRows();
      } catch {
        toast("批量复制失败", "error");
      }
    });
  }

  if (els.bulkDeleteBtn) {
    els.bulkDeleteBtn.addEventListener("click", async () => {
      try {
        await deleteSelectedQueryRows();
      } catch (err) {
        toast(err.message, "error");
      }
    });
  }

  if (els.bulkClearBtn) {
    els.bulkClearBtn.addEventListener("click", () => {
      clearQuerySelection();
    });
  }

  els.queryBody.addEventListener("click", async (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    const clickedRow = target.closest("tr[data-row-id]");
    if (clickedRow) {
      const rows = getQueryRowElements();
      const rowIndex = rows.indexOf(clickedRow);
      if (rowIndex >= 0) {
        setActiveQueryRowByIndex(rowIndex, { scroll: false });
      }
    }
    if (target.classList.contains("copy-row-btn")) {
      const rowId = Number(target.dataset.rowId);
      const row = target.closest("tr[data-row-id]");
      if (!row || !rowId) {
        return;
      }
      try {
        await copyQueryRow(row, rowId);
      } catch {
        toast("复制失败", "error");
      }
    }
    if (target.classList.contains("delete-row-btn")) {
      const rowId = Number(target.dataset.rowId);
      if (!rowId) {
        return;
      }
      try {
        await deleteRow(rowId);
      } catch (err) {
        toast(err.message, "error");
      }
    }
  });

  els.queryBody.addEventListener("change", (e) => {
    const target = e.target;
    if (!(target instanceof HTMLInputElement)) {
      return;
    }
    if (!target.classList.contains("query-row-check")) {
      return;
    }
    const rowId = Number(target.dataset.rowId || 0);
    if (!rowId) {
      return;
    }
    toggleQueryRowSelection(rowId, target.checked);
  });

  els.importFileInput.addEventListener("change", () => {
    state.importFile = els.importFileInput.files && els.importFileInput.files[0] ? els.importFileInput.files[0] : null;
  });

  ["dragenter", "dragover"].forEach((evtName) => {
    els.dropzone.addEventListener(evtName, (e) => {
      e.preventDefault();
      els.dropzone.classList.add("dragging");
    });
  });
  ["dragleave", "drop"].forEach((evtName) => {
    els.dropzone.addEventListener(evtName, (e) => {
      e.preventDefault();
      els.dropzone.classList.remove("dragging");
    });
  });
  els.dropzone.addEventListener("drop", (e) => {
    const files = e.dataTransfer && e.dataTransfer.files;
    if (!files || files.length === 0) {
      return;
    }
    const [file] = files;
    state.importFile = file;
    const transfer = new DataTransfer();
    transfer.items.add(file);
    els.importFileInput.files = transfer.files;
  });

  els.importForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!roleCanImport()) {
      toast("当前角色无导入权限", "error");
      return;
    }
    const file = state.importFile || (els.importFileInput.files && els.importFileInput.files[0]);
    const sourcePath = getInputValue(els.importSourcePath).trim();
    if (!file && !sourcePath) {
      toast("请选择 XLSX 文件或填写服务器本地文件路径", "error");
      return;
    }
    if (file && sourcePath) {
      toast("请二选一：上传文件或填写服务器本地文件路径", "error");
      return;
    }
    setUploadProgress(0);
    els.importStatus.textContent = file ? "上传中..." : "服务端路径导入中...";
    try {
      const payload = await uploadImport({ file, sourcePath });
      const job = payload.data;
      els.importStatus.textContent = `上传完成，任务 ${job.id} 已创建，状态 ${job.status}`;
      toast(`任务 ${job.id} 已创建`);
      setControlValue(els.importSourcePath, "");
      state.importFile = null;
      if (els.importFileInput instanceof HTMLInputElement) {
        els.importFileInput.value = "";
      }
      await pollImportJob(job.id);
      if (roleCanViewJobs()) {
        await loadJobs(state.jobsPage);
      }
    } catch (err) {
      toast(err.message, "error");
      els.importStatus.textContent = `导入失败: ${err.message}`;
    }
  });

  els.jobsFilterForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await loadJobs(1);
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.jobsPageSize.addEventListener("change", async () => {
    state.jobsPageSize = normalizePositiveInt(getInputValue(els.jobsPageSize), 20);
    if (!ALLOWED_JOBS_PAGE_SIZE.has(state.jobsPageSize)) {
      state.jobsPageSize = 20;
      setControlValue(els.jobsPageSize, "20");
    }
    persistUiPref(UI_PREF_KEYS.jobsPageSize, state.jobsPageSize);
    try {
      await loadJobs(1);
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.jobsRefreshBtn.addEventListener("click", async () => {
    try {
      await loadJobs(state.jobsPage);
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.jobsJumpBtn.addEventListener("click", async () => {
    try {
      await loadJobs(normalizePositiveInt(getInputValue(els.jobsPageInput), state.jobsPage));
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.jobsPrevBtn.addEventListener("click", async () => {
    try {
      await loadJobs(Math.max(1, state.jobsPage - 1));
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.jobsNextBtn.addEventListener("click", async () => {
    try {
      await loadJobs(state.jobsPage + 1);
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.jobsPageInput.addEventListener("keydown", async (e) => {
    if (e.key !== "Enter") {
      return;
    }
    e.preventDefault();
    try {
      await loadJobs(normalizePositiveInt(getInputValue(els.jobsPageInput), state.jobsPage));
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.jobsAutoRefresh.addEventListener("change", () => {
    state.jobsAutoRefresh = els.jobsAutoRefresh.checked;
    persistUiPref(UI_PREF_KEYS.jobsAutoRefresh, state.jobsAutoRefresh ? "1" : "0");
    syncAutoRefreshTimers();
  });

  els.jobsBody.addEventListener("click", async (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }
    if (!target.classList.contains("cancel-job-btn")) {
      return;
    }
    const jobId = Number(target.dataset.jobId);
    if (!jobId) {
      return;
    }
    if (!window.confirm(`确认取消导入任务 ${jobId} 吗？`)) {
      return;
    }
    try {
      await api(`/api/v1/data/import/${jobId}/cancel`, { method: "POST" });
      toast(`任务 ${jobId} 已取消`);
      await loadJobs(state.jobsPage);
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.createUserForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!roleCanManageUsers()) {
      toast("当前角色无用户管理权限", "error");
      return;
    }
    const username = getInputValue(els.newUsername).trim();
    const password = getInputValue(els.newPassword);
    const role = getInputValue(els.newRole).trim().toUpperCase();
    if (!username || !password || !role) {
      toast("用户名、密码和角色不能为空", "error");
      return;
    }
    if (!USER_ROLE_OPTIONS.has(role)) {
      toast("请选择有效角色", "error");
      return;
    }
    try {
      await api("/api/v1/users", {
        method: "POST",
        body: { username, password, role },
      });
      toast(`用户 ${username} 创建成功`);
      setControlValue(els.newUsername, "");
      setControlValue(els.newPassword, "");
      updateNewPasswordVisibility(false);
      setControlValue(els.newRole, "");
      await loadUsers();
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.reloadUsersBtn.addEventListener("click", async () => {
    try {
      await loadUsers();
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.usersBody.addEventListener("click", async (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) {
      return;
    }

    if (target.classList.contains("user-toggle-btn")) {
      const userId = Number(target.dataset.userId);
      const isActive = target.dataset.isActive === "1";
      if (!userId) {
        return;
      }
      try {
        await api(`/api/v1/users/${userId}`, {
          method: "PATCH",
          body: { is_active: !isActive },
        });
        toast(`用户 ${userId} 已${isActive ? "禁用" : "启用"}`);
        await loadUsers();
      } catch (err) {
        toast(err.message, "error");
      }
    }

    if (target.classList.contains("user-role-save-btn")) {
      const userId = Number(target.dataset.userId);
      const currentRole = String(target.dataset.currentRole || "").trim().toUpperCase();
      if (!userId || !currentRole) {
        return;
      }
      const roleSelect = els.usersBody.querySelector(`.user-role-select[data-user-id="${userId}"]`);
      if (!(roleSelect instanceof HTMLSelectElement)) {
        return;
      }
      const nextRole = getInputValue(roleSelect).trim().toUpperCase();
      if (!USER_ROLE_OPTIONS.has(nextRole)) {
        toast("角色值无效，请选择超级管理员/管理员/普通用户", "error");
        return;
      }
      if (nextRole === currentRole) {
        toast("角色未变化");
        return;
      }
      try {
        await api(`/api/v1/users/${userId}`, {
          method: "PATCH",
          body: { role: nextRole },
        });
        toast(`用户 ${userId} 角色已更新为 ${roleLabel(nextRole)}`);
        await loadUsers();
      } catch (err) {
        toast(err.message, "error");
      }
    }

    if (target.classList.contains("user-quota-btn")) {
      const userId = Number(target.dataset.userId);
      const username = String(target.dataset.username || "").trim();
      const dailyLimit = Number(target.dataset.dailyLimit || 0);
      const totalLimit = Number(target.dataset.totalLimit || 0);
      try {
        openQuotaEditor(userId, username, dailyLimit, totalLimit);
      } catch (err) {
        toast(err.message, "error");
      }
    }

    if (target.classList.contains("user-del-btn")) {
      const userId = Number(target.dataset.userId);
      const username = target.dataset.username || "";
      if (!userId) {
        return;
      }
      if (!window.confirm(`确认删除用户 ${username} (${userId}) 吗？`)) {
        return;
      }
      const code = window.prompt("请输入“确认删除用户”后继续");
      if (code !== "确认删除用户") {
        toast("删除已取消：确认文本不匹配", "error");
        return;
      }
      try {
        await api(`/api/v1/users/${userId}`, { method: "DELETE" });
        toast(`用户 ${username} 已删除`);
        await loadUsers();
      } catch (err) {
        toast(err.message, "error");
      }
    }
  });

  if (els.quotaEditorSaveBtn instanceof HTMLButtonElement) {
    els.quotaEditorSaveBtn.addEventListener("click", async () => {
      const userId = Number(els.quotaEditorSaveBtn.dataset.userId || 0);
      if (!userId) {
        return;
      }
      try {
        await saveUserQuota(userId);
      } catch (err) {
        toast(err.message, "error");
      }
    });
  }

  if (els.quotaEditorCancelBtn instanceof HTMLButtonElement) {
    els.quotaEditorCancelBtn.addEventListener("click", () => {
      closeQuotaEditor();
    });
  }

  const quotaInputs = [els.quotaEditorDaily, els.quotaEditorTotal].filter((el) => el instanceof HTMLInputElement);
  for (const inputEl of quotaInputs) {
    inputEl.addEventListener("keydown", async (e) => {
      if (e.key !== "Enter") {
        return;
      }
      e.preventDefault();
      const userId = Number(els.quotaEditorSaveBtn instanceof HTMLButtonElement ? els.quotaEditorSaveBtn.dataset.userId || 0 : 0);
      if (!userId) {
        return;
      }
      try {
        await saveUserQuota(userId);
      } catch (err) {
        toast(err.message, "error");
      }
    });
  }

  els.auditFilterForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      await loadAuditLogs(1);
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.auditResetBtn.addEventListener("click", async () => {
    setControlValue(els.auditFrom, "");
    setControlValue(els.auditTo, "");
    setControlValue(els.auditUserId, "");
    setControlValue(els.auditActionType, "");
    setControlValue(els.auditActionResult, "");
    try {
      await loadAuditLogs(1);
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.auditPageSize.addEventListener("change", async () => {
    state.auditPageSize = normalizePositiveInt(getInputValue(els.auditPageSize), 10);
    if (!ALLOWED_AUDIT_PAGE_SIZE.has(state.auditPageSize)) {
      state.auditPageSize = 10;
      setControlValue(els.auditPageSize, "10");
    }
    persistUiPref(UI_PREF_KEYS.auditPageSize, state.auditPageSize);
    try {
      await loadAuditLogs(1);
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.auditRefreshBtn.addEventListener("click", async () => {
    try {
      await loadAuditLogs(state.auditPage);
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.auditJumpBtn.addEventListener("click", async () => {
    try {
      await loadAuditLogs(normalizePositiveInt(getInputValue(els.auditPageInput), state.auditPage));
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.auditPageInput.addEventListener("keydown", async (e) => {
    if (e.key !== "Enter") {
      return;
    }
    e.preventDefault();
    try {
      await loadAuditLogs(normalizePositiveInt(getInputValue(els.auditPageInput), state.auditPage));
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.auditPrevBtn.addEventListener("click", async () => {
    try {
      await loadAuditLogs(Math.max(1, state.auditPage - 1));
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.auditNextBtn.addEventListener("click", async () => {
    try {
      await loadAuditLogs(state.auditPage + 1);
    } catch (err) {
      toast(err.message, "error");
    }
  });

  els.auditAutoRefresh.addEventListener("change", () => {
    state.auditAutoRefresh = els.auditAutoRefresh.checked;
    persistUiPref(UI_PREF_KEYS.auditAutoRefresh, state.auditAutoRefresh ? "1" : "0");
    syncAutoRefreshTimers();
  });

  document.addEventListener("visibilitychange", () => {
    syncAutoRefreshTimers();
  });
}

async function bootstrap() {
  hydrateUiPrefs();
  applyTheme(state.theme, { persist: false });
  setLoginPasswordVisibility(false);
  syncLoginSceneMode();
  resetLoginCharacterLook();
  bindEvents();
  updateNewPasswordVisibility(false);
  setScreen(state.token ? "app" : "auth", { updateHash: false });
  applySidebarState(state.sidebarCollapsed, { persist: false });
  setTab(state.currentTab, { persist: false });
  setShortcutPanelVisible(false);
  setControlValue(els.jobsPageSize, state.jobsPageSize);
  setControlValue(els.jobsPageInput, state.jobsPage);
  setCheckboxChecked(els.jobsAutoRefresh, state.jobsAutoRefresh);
  setControlValue(els.auditPageSize, state.auditPageSize);
  setControlValue(els.auditPageInput, state.auditPage);
  setCheckboxChecked(els.auditAutoRefresh, state.auditAutoRefresh);
  await hydrateLogin();
  if (state.token && roleCanViewJobs()) {
    try {
      await loadJobs(state.jobsPage);
    } catch (err) {
      toast(err.message, "error");
    }
  }
  if (state.token && roleCanManageUsers()) {
    try {
      await loadUsers();
    } catch (err) {
      toast(err.message, "error");
    }
  }
}

bootstrap();
