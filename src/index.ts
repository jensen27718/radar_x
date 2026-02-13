export interface Env {
  TELEGRAM_BOT_TOKEN: string;
  TELEGRAM_WEBHOOK_SECRET_TOKEN: string;
  KV: KVNamespace;
  SITE_BASE_URL: string;
}

type ForumRef = {
  id: number;
  slug: string;
  name: string;
  url: string;
};

type UserConfig = {
  v: 1;
  chatId: number;
  createdAt: number;
  updatedAt: number;
  forums: ForumRef[];
  includeKeywords: string[];
  excludeKeywords: string[];
  prefixes: string[];
  notifyThreads: boolean;
  notifyReplies: boolean;
};

type ThreadItem = {
  id: number;
  title: string;
  url: string;
  forum?: ForumRef;
  prefix?: string;
  updatedAt?: number; // epoch seconds, best-effort
  source: "home" | "forum";
};

type ForumState = {
  v: 1;
  forumId: number;
  updatedAt: number;
  // threadId -> lastSeenUpdatedAt (epoch seconds) ; 0 when unknown
  threads: Record<string, number>;
};

type HomeState = {
  v: 1;
  updatedAt: number;
  // recent thread ids in display order, used for "new thread" detection
  threadIds: number[];
};

const USERS_INDEX_KEY = "users";
const FORUM_CACHE_KEY = "cache:forums";
const HOME_STATE_KEY = "state:home";
const FORUM_STATE_PREFIX = "state:forum:";

const DEFAULT_POLL_THREAD_LIMIT = 35;
const DEFAULT_HOME_THREAD_LIMIT = 20;
const DEFAULT_LATEST_RESULT_LIMIT = 10;

const FORUM_PATHS = ["/foros/", "/forums/"];
const THREAD_PATHS = ["/temas/", "/threads/"];

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return new Response("ok", { status: 200 });
    }

    if (url.pathname === "/telegram/webhook") {
      if (request.method !== "POST") return new Response("Method Not Allowed", { status: 405 });

      const secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token") ?? "";
      if (!env.TELEGRAM_WEBHOOK_SECRET_TOKEN || secret !== env.TELEGRAM_WEBHOOK_SECRET_TOKEN) {
        return new Response("Unauthorized", { status: 401 });
      }

      const update = (await request.json().catch(() => null)) as any;
      if (!update) return new Response("Bad Request", { status: 400 });

      ctx.waitUntil(handleTelegramUpdate(update, env));
      return new Response("ok", { status: 200 });
    }

    return new Response("Not Found", { status: 404 });
  },

  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(runScheduledPoll(env, event.scheduledTime));
  }
};

async function runScheduledPoll(env: Env, scheduledTimeMs: number): Promise<void> {
  const chatIds = await getUserChatIds(env);
  if (chatIds.length === 0) return;

  const users: UserConfig[] = [];
  for (const chatId of chatIds) {
    const u = await getUser(env, chatId);
    if (u) users.push(u);
  }
  if (users.length === 0) return;

  const globalUsers: UserConfig[] = [];
  const forumToUsers = new Map<number, { forum: ForumRef; users: UserConfig[] }>();

  for (const u of users) {
    if (u.forums && u.forums.length > 0) {
      for (const f of u.forums) {
        const bucket = forumToUsers.get(f.id);
        if (bucket) bucket.users.push(u);
        else forumToUsers.set(f.id, { forum: f, users: [u] });
      }
    } else {
      globalUsers.push(u);
    }
  }

  for (const { forum, users: subs } of forumToUsers.values()) {
    await pollForumAndNotify(env, forum, subs, scheduledTimeMs);
  }

  if (globalUsers.length > 0) {
    await pollHomeAndNotify(env, globalUsers, scheduledTimeMs);
  }
}

async function pollForumAndNotify(
  env: Env,
  forum: ForumRef,
  subscribers: UserConfig[],
  nowMs: number
): Promise<void> {
  const stateKey = `${FORUM_STATE_PREFIX}${forum.id}`;
  const existing = (await env.KV.get(stateKey, { type: "json" }).catch(() => null)) as ForumState | null;

  const html = await fetchText(forum.url);
  if (!html) return;

  const threads = parseThreadsFromHtml(html, env.SITE_BASE_URL, { source: "forum", forum }).slice(
    0,
    DEFAULT_POLL_THREAD_LIMIT
  );

  const nowSec = Math.floor(nowMs / 1000);

  // Prime without sending (prevents spamming on first run).
  if (!existing) {
    const primed: ForumState = {
      v: 1,
      forumId: forum.id,
      updatedAt: nowSec,
      threads: buildThreadStateMap(threads)
    };
    await env.KV.put(stateKey, JSON.stringify(primed));
    return;
  }

  const next: ForumState = {
    v: 1,
    forumId: forum.id,
    updatedAt: nowSec,
    threads: { ...existing.threads }
  };

  const newThreadEvents: ThreadItem[] = [];
  const updatedThreadEvents: ThreadItem[] = [];

  for (const t of threads) {
    const tid = String(t.id);
    const current = t.updatedAt ?? 0;
    const prev = existing.threads[tid];

    if (prev === undefined) {
      newThreadEvents.push(t);
      next.threads[tid] = current;
      continue;
    }

    // Avoid false "reply" notifications if we previously had no timestamp.
    if ((prev ?? 0) === 0 && current > 0) {
      next.threads[tid] = current;
      continue;
    }

    if (current > 0 && prev > 0 && current > prev) {
      updatedThreadEvents.push(t);
      next.threads[tid] = current;
      continue;
    }
  }

  next.threads = pruneThreadState(next.threads, threads, 120);

  if (!shallowEqualRecords(existing.threads, next.threads)) {
    await env.KV.put(stateKey, JSON.stringify(next));
  }

  for (const t of newThreadEvents) {
    for (const u of subscribers) {
      if (!u.notifyThreads) continue;
      if (!matchesUserFilters(u, t)) continue;
      await tgSendMessage(env, u.chatId, formatThreadNotification("Nuevo tema", t), true);
    }
  }

  for (const t of updatedThreadEvents) {
    for (const u of subscribers) {
      if (!u.notifyReplies) continue;
      if (!matchesUserFilters(u, t)) continue;
      await tgSendMessage(env, u.chatId, formatThreadNotification("Nueva respuesta", t), true);
    }
  }
}

async function pollHomeAndNotify(env: Env, subscribers: UserConfig[], nowMs: number): Promise<void> {
  const existing = (await env.KV.get(HOME_STATE_KEY, { type: "json" }).catch(() => null)) as HomeState | null;
  const html = await fetchText(env.SITE_BASE_URL);
  if (!html) return;

  const threads = parseThreadsFromHtml(html, env.SITE_BASE_URL, { source: "home" }).slice(0, DEFAULT_HOME_THREAD_LIMIT);
  const nowSec = Math.floor(nowMs / 1000);

  if (!existing) {
    const primed: HomeState = { v: 1, updatedAt: nowSec, threadIds: threads.map((t) => t.id) };
    await env.KV.put(HOME_STATE_KEY, JSON.stringify(primed));
    return;
  }

  const prevSet = new Set(existing.threadIds);
  const newOnHome = threads.filter((t) => !prevSet.has(t.id));

  const next: HomeState = { v: 1, updatedAt: nowSec, threadIds: threads.map((t) => t.id) };
  if (!arrayEqual(existing.threadIds, next.threadIds)) {
    await env.KV.put(HOME_STATE_KEY, JSON.stringify(next));
  }

  for (const t of newOnHome) {
    for (const u of subscribers) {
      if (!u.notifyThreads) continue;
      if (!matchesUserFilters(u, t)) continue;
      await tgSendMessage(env, u.chatId, formatThreadNotification("Nuevo tema", t), true);
    }
  }
}

function matchesUserFilters(u: UserConfig, t: ThreadItem): boolean {
  if (u.forums.length > 0 && t.forum) {
    const ok = u.forums.some((f) => f.id === t.forum!.id);
    if (!ok) return false;
  }

  const text = normalize(`${t.title} ${t.prefix ?? ""} ${t.forum?.name ?? ""}`);

  for (const k of u.excludeKeywords) {
    if (!k) continue;
    if (text.includes(normalize(k))) return false;
  }

  if (u.includeKeywords.length > 0) {
    const any = u.includeKeywords.some((k) => k && text.includes(normalize(k)));
    if (!any) return false;
  }

  if (u.prefixes.length > 0) {
    if (!t.prefix) return false;
    const ok = u.prefixes.some((p) => normalize(p) === normalize(t.prefix!));
    if (!ok) return false;
  }

  return true;
}

function formatThreadNotification(kind: string, t: ThreadItem): string {
  const parts: string[] = [];
  parts.push(`${kind}${t.forum?.name ? ` en ${t.forum.name}` : ""}`);
  parts.push(`${t.prefix ? `[${t.prefix}] ` : ""}${t.title}`);
  if (t.updatedAt && t.updatedAt > 0) {
    const iso = new Date(t.updatedAt * 1000).toISOString().replace(".000Z", "Z");
    parts.push(`Actualizado: ${iso}`);
  }
  parts.push(t.url);
  return parts.join("\n");
}

async function handleTelegramUpdate(update: any, env: Env): Promise<void> {
  if (update.message?.text) {
    await handleTelegramMessage(update.message, env);
    return;
  }

  if (update.callback_query) {
    await handleTelegramCallback(update.callback_query, env);
    return;
  }
}

async function handleTelegramMessage(message: any, env: Env): Promise<void> {
  const chatId = message.chat?.id;
  if (typeof chatId !== "number") return;

  const text = String(message.text ?? "").trim();
  const cmd = parseCommand(text);

  if (!cmd) {
    await tgSendMessage(env, chatId, helpText(), true);
    return;
  }

  const name = cmd.name;
  const args = cmd.args;

  if (name === "start") {
    await ensureUser(env, chatId);
    await tgSendMessage(env, chatId, startText(), true);
    return;
  }

  if (name === "help") {
    await tgSendMessage(env, chatId, helpText(), true);
    return;
  }

  const user = await ensureUser(env, chatId);

  if (name === "list" || name === "filtros") {
    await tgSendMessage(env, chatId, formatUserConfig(user), true);
    return;
  }

  if (name === "foros" || name === "forums") {
    await sendForumsPage(env, user, chatId, 0, false);
    return;
  }

  if (name === "addforum" || name === "addforo") {
    const forum = await resolveForumFromArg(env, args);
    if (!forum) {
      await tgSendMessage(
        env,
        chatId,
        "No pude reconocer ese foro. Envia la URL del foro (ej: https://doncolombia.com/foros/... ) o usa /foros.",
        true
      );
      return;
    }
    const next = addForum(user, forum);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Suscrito a: ${forum.name} (${forum.id})`, true);
    return;
  }

  if (name === "rmforum" || name === "rmforo") {
    const forumId = parseInt(args.trim(), 10);
    if (!Number.isFinite(forumId)) {
      await tgSendMessage(env, chatId, "Uso: /rmforum <id>  (ej: /rmforum 18)", true);
      return;
    }
    const next = { ...user, forums: user.forums.filter((f) => f.id !== forumId), updatedAt: nowSec() };
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Foro eliminado: ${forumId}`, true);
    return;
  }

  if (name === "addkw") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Uso: /addkw <palabra o frase>", true);
      return;
    }
    const next = addUnique(user, "includeKeywords", kw);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Keyword agregado: ${kw}`, true);
    return;
  }

  if (name === "rmkw") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Uso: /rmkw <palabra o frase>", true);
      return;
    }
    const next = removeValue(user, "includeKeywords", kw);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Keyword eliminado: ${kw}`, true);
    return;
  }

  if (name === "addnot") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Uso: /addnot <palabra o frase>", true);
      return;
    }
    const next = addUnique(user, "excludeKeywords", kw);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Exclusion agregada: ${kw}`, true);
    return;
  }

  if (name === "rmnot") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Uso: /rmnot <palabra o frase>", true);
      return;
    }
    const next = removeValue(user, "excludeKeywords", kw);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Exclusion eliminada: ${kw}`, true);
    return;
  }

  if (name === "addprefix") {
    const p = args.trim();
    if (!p) {
      await tgSendMessage(env, chatId, "Uso: /addprefix <texto del prefijo> (ej: /addprefix Reseña)", true);
      return;
    }
    const next = addUnique(user, "prefixes", p);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Prefijo agregado: ${p}`, true);
    return;
  }

  if (name === "rmprefix") {
    const p = args.trim();
    if (!p) {
      await tgSendMessage(env, chatId, "Uso: /rmprefix <texto del prefijo>", true);
      return;
    }
    const next = removeValue(user, "prefixes", p);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Prefijo eliminado: ${p}`, true);
    return;
  }

  if (name === "notify") {
    const [targetRaw, modeRaw] = args.split(/\s+/).map((s) => s.trim());
    const target = normalize(targetRaw ?? "");
    const mode = normalize(modeRaw ?? "");
    if (!target || !mode || (mode !== "on" && mode !== "off")) {
      await tgSendMessage(env, chatId, "Uso: /notify threads on|off  o  /notify replies on|off", true);
      return;
    }
    const next = { ...user, updatedAt: nowSec() };
    if (target === "threads" || target === "temas") next.notifyThreads = mode === "on";
    else if (target === "replies" || target === "respuestas") next.notifyReplies = mode === "on";
    else {
      await tgSendMessage(env, chatId, "Targets validos: threads | replies", true);
      return;
    }
    await putUser(env, next);
    await tgSendMessage(
      env,
      chatId,
      `Notificaciones: threads=${next.notifyThreads ? "on" : "off"}, replies=${next.notifyReplies ? "on" : "off"}`,
      true
    );
    return;
  }

  if (name === "latest" || name === "ultimos") {
    const items = await getLatestForUser(env, user);
    if (items.length === 0) {
      await tgSendMessage(env, chatId, "No encontre resultados con tus filtros.", true);
      return;
    }
    const lines = items.slice(0, DEFAULT_LATEST_RESULT_LIMIT).map((t) => {
      const head = `${t.forum?.name ? `[${t.forum.name}] ` : ""}${t.prefix ? `[${t.prefix}] ` : ""}${t.title}`;
      return `${head}\n${t.url}`;
    });
    await tgSendMessage(env, chatId, lines.join("\n\n"), true);
    return;
  }

  await tgSendMessage(env, chatId, helpText(), true);
}

async function handleTelegramCallback(cb: any, env: Env): Promise<void> {
  const chatId = cb.message?.chat?.id;
  if (typeof chatId !== "number") return;

  const data = String(cb.data ?? "");
  const user = await ensureUser(env, chatId);

  if (data.startsWith("forums_page:")) {
    const page = parseInt(data.split(":")[1] ?? "0", 10);
    await sendForumsPage(env, user, chatId, Number.isFinite(page) ? page : 0, true, cb.message?.message_id);
    await tgAnswerCallback(env, cb.id, "");
    return;
  }

  if (data === "refresh_forums") {
    await refreshForumCache(env, true);
    await sendForumsPage(env, user, chatId, 0, true, cb.message?.message_id);
    await tgAnswerCallback(env, cb.id, "Lista actualizada");
    return;
  }

  if (data.startsWith("sub_forum:")) {
    const forumId = parseInt(data.split(":")[1] ?? "", 10);
    const forum = await resolveForumFromArg(env, String(forumId));
    if (!forum) {
      await tgAnswerCallback(env, cb.id, "No pude resolver ese foro");
      return;
    }
    const next = addForum(user, forum);
    await putUser(env, next);
    await sendForumsPage(env, next, chatId, 0, true, cb.message?.message_id);
    await tgAnswerCallback(env, cb.id, `Suscrito: ${forum.name}`);
    return;
  }

  if (data.startsWith("unsub_forum:")) {
    const forumId = parseInt(data.split(":")[1] ?? "", 10);
    const next = { ...user, forums: user.forums.filter((f) => f.id !== forumId), updatedAt: nowSec() };
    await putUser(env, next);
    await sendForumsPage(env, next, chatId, 0, true, cb.message?.message_id);
    await tgAnswerCallback(env, cb.id, `Eliminado: ${forumId}`);
    return;
  }

  await tgAnswerCallback(env, cb.id, "");
}

async function getLatestForUser(env: Env, user: UserConfig): Promise<ThreadItem[]> {
  const results: ThreadItem[] = [];

  if (user.forums.length > 0) {
    for (const f of user.forums) {
      const html = await fetchText(f.url);
      if (!html) continue;
      const threads = parseThreadsFromHtml(html, env.SITE_BASE_URL, { source: "forum", forum: f }).slice(
        0,
        DEFAULT_POLL_THREAD_LIMIT
      );
      for (const t of threads) if (matchesUserFilters(user, t)) results.push(t);
    }
  } else {
    const html = await fetchText(env.SITE_BASE_URL);
    if (!html) return [];
    const threads = parseThreadsFromHtml(html, env.SITE_BASE_URL, { source: "home" }).slice(0, DEFAULT_HOME_THREAD_LIMIT);
    for (const t of threads) if (matchesUserFilters(user, t)) results.push(t);
  }

  results.sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0));
  return uniqueThreads(results);
}

function uniqueThreads(items: ThreadItem[]): ThreadItem[] {
  const seen = new Set<number>();
  const out: ThreadItem[] = [];
  for (const t of items) {
    if (seen.has(t.id)) continue;
    seen.add(t.id);
    out.push(t);
  }
  return out;
}

async function sendForumsPage(
  env: Env,
  user: UserConfig,
  chatId: number,
  page: number,
  edit: boolean,
  messageId?: number
): Promise<void> {
  const cache = await getForumCache(env);
  const forums = cache.forums;
  if (forums.length === 0) {
    await tgSendMessage(env, chatId, "No pude cargar la lista de foros. Intenta mas tarde.", true);
    return;
  }

  const pageSize = 8;
  const totalPages = Math.max(1, Math.ceil(forums.length / pageSize));
  const p = Math.min(Math.max(0, page), totalPages - 1);

  const start = p * pageSize;
  const slice = forums.slice(start, start + pageSize);

  const subscribed = new Set(user.forums.map((f) => f.id));

  const keyboard: any[][] = [];
  for (const f of slice) {
    const isSub = subscribed.has(f.id);
    keyboard.push([
      {
        text: `${isSub ? "Quitar" : "Suscribir"}: ${f.name}`,
        callback_data: `${isSub ? "unsub_forum" : "sub_forum"}:${f.id}`
      }
    ]);
  }

  const navRow: any[] = [];
  if (p > 0) navRow.push({ text: "Anterior", callback_data: `forums_page:${p - 1}` });
  navRow.push({ text: `${p + 1}/${totalPages}`, callback_data: `forums_page:${p}` });
  if (p < totalPages - 1) navRow.push({ text: "Siguiente", callback_data: `forums_page:${p + 1}` });
  keyboard.push(navRow);
  keyboard.push([{ text: "Actualizar lista", callback_data: "refresh_forums" }]);

  const text =
    `Foros (${forums.length}).\n` +
    `Suscripciones actuales: ${user.forums.length}\n\n` +
    `Tip: tambien puedes usar /addforum <URL> si un foro no aparece aqui.`;

  const replyMarkup = { inline_keyboard: keyboard };

  if (edit && messageId) {
    await tgEditMessage(env, chatId, messageId, text, replyMarkup);
  } else {
    await tgSendMessage(env, chatId, text, true, replyMarkup);
  }
}

function parseCommand(text: string): { name: string; args: string } | null {
  const m = text.match(/^\/([a-zA-Z0-9_]+)(?:@[\w_]+)?(?:\s+([\s\S]+))?$/);
  if (!m) return null;
  return { name: normalize(m[1] ?? ""), args: String(m[2] ?? "").trim() };
}

function startText(): string {
  return [
    "Bot listo.",
    "",
    "Este bot te ayuda a filtrar y recibir alertas de nuevas publicaciones (temas) y nuevas respuestas en foros.",
    "",
    "Empieza con:",
    "/foros  (ver lista y suscribirte)",
    "/addforum <url>  (suscribirte por URL)",
    "/addkw <palabra>  (filtrar por keyword)",
    "/list  (ver filtros)",
    "/latest  (ver ultimos resultados)"
  ].join("\n");
}

function helpText(): string {
  return [
    "Comandos:",
    "/foros",
    "/addforum <url> | /rmforum <id>",
    "/addkw <texto> | /rmkw <texto>",
    "/addnot <texto> | /rmnot <texto>   (excluir)",
    "/addprefix <texto> | /rmprefix <texto>",
    "/notify threads on|off",
    "/notify replies on|off",
    "/list",
    "/latest"
  ].join("\n");
}

function formatUserConfig(u: UserConfig): string {
  const lines: string[] = [];
  lines.push(`Chat: ${u.chatId}`);
  lines.push(`Foros: ${u.forums.length ? u.forums.map((f) => `${f.name}(${f.id})`).join(", ") : "(ninguno)"}`);
  lines.push(`Keywords: ${u.includeKeywords.length ? u.includeKeywords.join(", ") : "(ninguna)"}`);
  lines.push(`Excluir: ${u.excludeKeywords.length ? u.excludeKeywords.join(", ") : "(ninguna)"}`);
  lines.push(`Prefijos: ${u.prefixes.length ? u.prefixes.join(", ") : "(ninguno)"}`);
  lines.push(`Notificar temas: ${u.notifyThreads ? "on" : "off"}`);
  lines.push(`Notificar respuestas: ${u.notifyReplies ? "on" : "off"}`);
  return lines.join("\n");
}

function normalize(s: string): string {
  return String(s ?? "").trim().toLowerCase();
}

function nowSec(): number {
  return Math.floor(Date.now() / 1000);
}

async function ensureUser(env: Env, chatId: number): Promise<UserConfig> {
  const existing = await getUser(env, chatId);
  if (existing) return existing;

  const created = createDefaultUser(chatId);
  await putUser(env, created);

  const ids = await getUserChatIds(env);
  if (!ids.includes(chatId)) {
    ids.push(chatId);
    await env.KV.put(USERS_INDEX_KEY, JSON.stringify(ids));
  }

  return created;
}

function createDefaultUser(chatId: number): UserConfig {
  const ts = nowSec();
  return {
    v: 1,
    chatId,
    createdAt: ts,
    updatedAt: ts,
    forums: [],
    includeKeywords: [],
    excludeKeywords: [],
    prefixes: [],
    notifyThreads: true,
    notifyReplies: true
  };
}

async function getUser(env: Env, chatId: number): Promise<UserConfig | null> {
  const key = `user:${chatId}`;
  const u = (await env.KV.get(key, { type: "json" }).catch(() => null)) as UserConfig | null;
  return u;
}

async function putUser(env: Env, u: UserConfig): Promise<void> {
  const key = `user:${u.chatId}`;
  const next = { ...u, updatedAt: nowSec() };
  await env.KV.put(key, JSON.stringify(next));
}

async function getUserChatIds(env: Env): Promise<number[]> {
  const raw = await env.KV.get(USERS_INDEX_KEY);
  if (!raw) return [];
  try {
    const arr = JSON.parse(raw) as any[];
    return arr.map((x) => Number(x)).filter((n) => Number.isFinite(n));
  } catch {
    return [];
  }
}

function addUnique(u: UserConfig, field: "includeKeywords" | "excludeKeywords" | "prefixes", value: string): UserConfig {
  const v = value.trim();
  if (!v) return u;
  const existing = new Set(u[field].map((x) => normalize(x)));
  if (existing.has(normalize(v))) return u;
  return { ...u, [field]: [...u[field], v], updatedAt: nowSec() } as UserConfig;
}

function removeValue(u: UserConfig, field: "includeKeywords" | "excludeKeywords" | "prefixes", value: string): UserConfig {
  const needle = normalize(value);
  const next = u[field].filter((x) => normalize(x) !== needle);
  return { ...u, [field]: next, updatedAt: nowSec() } as UserConfig;
}

function addForum(u: UserConfig, forum: ForumRef): UserConfig {
  const exists = u.forums.some((f) => f.id === forum.id);
  if (exists) return u;
  return { ...u, forums: [...u.forums, forum], updatedAt: nowSec() };
}

async function resolveForumFromArg(env: Env, arg: string): Promise<ForumRef | null> {
  const s = arg.trim();
  if (!s) return null;

  if (s.startsWith("http://") || s.startsWith("https://")) {
    const parsed = parseForumFromUrl(s, env.SITE_BASE_URL);
    if (!parsed) return null;
    const cache = await getForumCache(env);
    const hit = cache.forums.find((f) => f.id === parsed.id);
    return hit ? hit : parsed;
  }

  const id = parseInt(s, 10);
  if (!Number.isFinite(id)) return null;
  const cache = await getForumCache(env);
  const hit = cache.forums.find((f) => f.id === id);
  return hit ?? null;
}

function parseForumFromUrl(url: string, baseUrl: string): ForumRef | null {
  let u: URL;
  try {
    u = new URL(url, baseUrl);
  } catch {
    return null;
  }
  const m = forumPathMatch(u.pathname);
  if (!m) return null;
  const full = new URL(`/foros/${m.slug}.${m.id}/`, baseUrl).toString();
  return { id: m.id, slug: m.slug, name: m.slug, url: full };
}

function forumPathMatch(pathname: string): { id: number; slug: string } | null {
  for (const base of FORUM_PATHS) {
    const idx = pathname.indexOf(base);
    if (idx === -1) continue;
    const rest = pathname.slice(idx + base.length);
    const m = rest.match(/^([^\/?#]+)\.(\d+)\//);
    if (!m) continue;
    return { slug: m[1], id: parseInt(m[2], 10) };
  }
  return null;
}

async function getForumCache(env: Env): Promise<{ fetchedAt: number; forums: ForumRef[] }> {
  const cached = (await env.KV.get(FORUM_CACHE_KEY, { type: "json" }).catch(() => null)) as
    | { fetchedAt: number; forums: ForumRef[] }
    | null;
  const now = nowSec();
  if (cached && cached.fetchedAt && now - cached.fetchedAt < 24 * 60 * 60 && Array.isArray(cached.forums)) {
    return cached;
  }
  return refreshForumCache(env, false);
}

async function refreshForumCache(env: Env, force: boolean): Promise<{ fetchedAt: number; forums: ForumRef[] }> {
  const now = nowSec();
  if (!force) {
    const cached = (await env.KV.get(FORUM_CACHE_KEY, { type: "json" }).catch(() => null)) as any;
    if (cached?.fetchedAt && now - cached.fetchedAt < 10 * 60 && Array.isArray(cached.forums)) {
      return cached;
    }
  }

  const html = await fetchText(env.SITE_BASE_URL);
  const forums = html ? parseForumsFromHtml(html, env.SITE_BASE_URL) : [];
  const payload = { fetchedAt: now, forums };
  await env.KV.put(FORUM_CACHE_KEY, JSON.stringify(payload));
  return payload;
}

function parseForumsFromHtml(html: string, baseUrl: string): ForumRef[] {
  const out: ForumRef[] = [];
  const seen = new Set<number>();

  const linkRe = /<a\b[^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi;
  let m: RegExpExecArray | null;
  while ((m = linkRe.exec(html)) !== null) {
    const href = m[1] ?? "";
    const text = htmlToText(m[2] ?? "");
    const fm = forumHrefMatch(href);
    if (!fm) continue;
    if (seen.has(fm.id)) continue;
    seen.add(fm.id);
    const url = new URL(fm.href, baseUrl).toString();
    out.push({ id: fm.id, slug: fm.slug, name: text || fm.slug, url });
  }

  return out.slice(0, 250);
}

function forumHrefMatch(href: string): { href: string; id: number; slug: string } | null {
  try {
    const u = new URL(href, "https://example.com");
    const m = forumPathMatch(u.pathname);
    if (!m) return null;
    const normalized = u.pathname.endsWith("/") ? u.pathname : `${u.pathname}/`;
    return { href: normalized, id: m.id, slug: m.slug };
  } catch {
    return null;
  }
}

function parseThreadsFromHtml(
  html: string,
  baseUrl: string,
  opts: { source: "home" | "forum"; forum?: ForumRef }
): ThreadItem[] {
  const threadHrefRe = new RegExp(
    `<a\\b[^>]*href="([^"]*(?:${THREAD_PATHS.map(escapeRe).join("|")})[^"]+)"[^>]*>([\\s\\S]*?)<\\/a>`,
    "gi"
  );

  const out: ThreadItem[] = [];
  const seen = new Set<number>();

  let m: RegExpExecArray | null;
  while ((m = threadHrefRe.exec(html)) !== null) {
    const hrefRaw = m[1] ?? "";
    const title = htmlToText(m[2] ?? "");
    const threadId = threadIdFromHref(hrefRaw);
    if (!threadId) continue;
    if (seen.has(threadId)) continue;
    seen.add(threadId);

    const absUrl = new URL(hrefRaw, baseUrl).toString();

    const matchIndex = m.index ?? 0;
    const chunkStart = Math.max(0, matchIndex - 900);
    const chunkEnd = Math.min(html.length, matchIndex + m[0].length + 3000);
    const chunk = html.slice(chunkStart, chunkEnd);
    const anchorPos = matchIndex - chunkStart;

    const updatedAt = extractLatestTimestamp(chunk);
    const prefix = extractClosestLabel(chunk, anchorPos);

    let forum = opts.forum;
    if (!forum && opts.source === "home") {
      const f = extractForumFromChunk(chunk, baseUrl);
      if (f) forum = f;
    }

    out.push({
      id: threadId,
      title: title || `Tema ${threadId}`,
      url: absUrl,
      forum,
      prefix: prefix || undefined,
      updatedAt: updatedAt || undefined,
      source: opts.source
    });
  }

  return out;
}

function extractForumFromChunk(chunk: string, baseUrl: string): ForumRef | null {
  const linkRe = /<a\b[^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi;
  let m: RegExpExecArray | null;
  while ((m = linkRe.exec(chunk)) !== null) {
    const href = m[1] ?? "";
    const text = htmlToText(m[2] ?? "");
    const fm = forumHrefMatch(href);
    if (!fm) continue;
    const url = new URL(fm.href, baseUrl).toString();
    return { id: fm.id, slug: fm.slug, name: text || fm.slug, url };
  }
  return null;
}

function threadIdFromHref(href: string): number | null {
  try {
    const u = new URL(href, "https://example.com");
    const p = u.pathname;
    for (const base of THREAD_PATHS) {
      const idx = p.indexOf(base);
      if (idx === -1) continue;
      const rest = p.slice(idx + base.length);
      const m = rest.match(/^[^\/?#]+?\.(\d+)\//);
      if (!m) continue;
      const id = parseInt(m[1], 10);
      return Number.isFinite(id) ? id : null;
    }
    return null;
  } catch {
    return null;
  }
}

function extractLatestTimestamp(chunk: string): number {
  let max = 0;

  const dtRe = /data-time="(\d{9,11})"/gi;
  let m: RegExpExecArray | null;
  while ((m = dtRe.exec(chunk)) !== null) {
    const n = parseInt(m[1], 10);
    if (Number.isFinite(n) && n > max) max = n;
  }

  const isoRe = /datetime="([^"]{10,40})"/gi;
  while ((m = isoRe.exec(chunk)) !== null) {
    const t = Date.parse(m[1]);
    if (Number.isFinite(t)) {
      const sec = Math.floor(t / 1000);
      if (sec > max) max = sec;
    }
  }

  return max;
}

function extractClosestLabel(chunk: string, anchorPos: number): string | null {
  const candidates: { text: string; idx: number }[] = [];

  const labelRe = /<(?:a|span)\b[^>]*class="[^"]*\blabel\b[^"]*"[^>]*>([\s\S]*?)<\/(?:a|span)>/gi;
  let m: RegExpExecArray | null;
  while ((m = labelRe.exec(chunk)) !== null) {
    const text = htmlToText(m[1] ?? "");
    if (!text) continue;
    candidates.push({ text, idx: m.index ?? 0 });
  }

  const shortARe = /<a\b[^>]*href="[^"]+"[^>]*>([^<]{1,20})<\/a>/gi;
  while ((m = shortARe.exec(chunk)) !== null) {
    const text = htmlToText(m[1] ?? "");
    if (!text) continue;
    if (text.length > 20) continue;
    candidates.push({ text, idx: m.index ?? 0 });
  }

  const before = candidates.filter((c) => c.idx >= 0 && c.idx < anchorPos);
  if (before.length === 0) return null;
  before.sort((a, b) => b.idx - a.idx);
  return before[0].text;
}

function buildThreadStateMap(threads: ThreadItem[]): Record<string, number> {
  const m: Record<string, number> = {};
  for (const t of threads) m[String(t.id)] = t.updatedAt ?? 0;
  return m;
}

function pruneThreadState(
  current: Record<string, number>,
  recentThreads: ThreadItem[],
  maxKeep: number
): Record<string, number> {
  const keepIds = new Set(recentThreads.slice(0, maxKeep).map((t) => String(t.id)));
  const out: Record<string, number> = {};
  for (const id of Object.keys(current)) {
    if (keepIds.has(id)) out[id] = current[id];
  }
  for (const t of recentThreads.slice(0, maxKeep)) {
    const id = String(t.id);
    if (out[id] === undefined) out[id] = t.updatedAt ?? 0;
  }
  return out;
}

function shallowEqualRecords(a: Record<string, number>, b: Record<string, number>): boolean {
  const ak = Object.keys(a);
  const bk = Object.keys(b);
  if (ak.length !== bk.length) return false;
  for (const k of ak) if (a[k] !== b[k]) return false;
  return true;
}

function arrayEqual(a: number[], b: number[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

function escapeRe(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function htmlToText(html: string): string {
  const stripped = html.replace(/<[^>]*>/g, " ");
  const decoded = decodeHtmlEntities(stripped);
  return decoded.replace(/\s+/g, " ").trim();
}

function decodeHtmlEntities(input: string): string {
  let s = input
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, '"')
    .replace(/&#039;/gi, "'")
    .replace(/&apos;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");

  s = s.replace(/&#(\d+);/g, (_, num) => {
    const code = parseInt(num, 10);
    if (!Number.isFinite(code)) return _;
    try {
      return String.fromCodePoint(code);
    } catch {
      return _;
    }
  });

  s = s.replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => {
    const code = parseInt(hex, 16);
    if (!Number.isFinite(code)) return _;
    try {
      return String.fromCodePoint(code);
    } catch {
      return _;
    }
  });

  return s;
}

async function fetchText(url: string): Promise<string | null> {
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": "radar-x-bot/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
      }
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  }
}

async function telegramApi(env: Env, method: string, payload: any): Promise<any> {
  const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload)
  });
  const data = await res.json().catch(() => null);
  if (!res.ok || !data?.ok) return null;
  return data.result;
}

async function tgSendMessage(
  env: Env,
  chatId: number,
  text: string,
  disablePreview: boolean,
  replyMarkup?: any
): Promise<void> {
  await telegramApi(env, "sendMessage", {
    chat_id: chatId,
    text,
    disable_web_page_preview: disablePreview,
    reply_markup: replyMarkup
  });
}

async function tgEditMessage(env: Env, chatId: number, messageId: number, text: string, replyMarkup: any): Promise<void> {
  await telegramApi(env, "editMessageText", {
    chat_id: chatId,
    message_id: messageId,
    text,
    disable_web_page_preview: true,
    reply_markup: replyMarkup
  });
}

async function tgAnswerCallback(env: Env, callbackQueryId: string, text: string): Promise<void> {
  await telegramApi(env, "answerCallbackQuery", {
    callback_query_id: callbackQueryId,
    text: text || undefined,
    show_alert: false
  });
}
