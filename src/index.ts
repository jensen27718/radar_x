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
  // threadId -> lastSeenUpdatedAt (epoch seconds) ; 0 when unknown
  threads: Record<string, number>;
};

type PollState = {
  v: 1;
  updatedAt: number;
  home?: HomeState;
  forums: Record<string, ForumState>;
};

const USERS_INDEX_KEY = "users";
const FORUM_CACHE_KEY = "cache:forums";
const POLL_STATE_KEY = "state:poll";

const DEFAULT_POLL_THREAD_LIMIT = 35;
const DEFAULT_HOME_THREAD_LIMIT = 20;
const DEFAULT_LATEST_RESULT_LIMIT = 10;

const FORUM_PATHS = ["/foros/", "/forums/"];
const THREAD_PATHS = ["/temas/", "/threads/"];

const CITY_PRESETS: { id: string; name: string }[] = [
  { id: "bogota", name: "Bogota" },
  { id: "medellin", name: "Medellin" },
  { id: "cali", name: "Cali" },
  { id: "barranquilla", name: "Barranquilla" },
  { id: "cartagena", name: "Cartagena" },
  { id: "bucaramanga", name: "Bucaramanga" },
  { id: "pereira", name: "Pereira" },
  { id: "manizales", name: "Manizales" },
  { id: "cucuta", name: "Cucuta" },
  { id: "santa_marta", name: "Santa Marta" },
  { id: "ibague", name: "Ibague" },
  { id: "villavicencio", name: "Villavicencio" },
  { id: "pasto", name: "Pasto" },
  { id: "armenia", name: "Armenia" },
  { id: "neiva", name: "Neiva" },
  { id: "monteria", name: "Monteria" },
  { id: "sincelejo", name: "Sincelejo" },
  { id: "valledupar", name: "Valledupar" },
  { id: "tunja", name: "Tunja" },
  { id: "popayan", name: "Popayan" },
  { id: "riohacha", name: "Riohacha" },
  { id: "quibdo", name: "Quibdo" },
  { id: "leticia", name: "Leticia" },
  { id: "san_andres", name: "San Andres" }
];

const CITY_PAGE_SIZE = 8;
const PICKS_TTL_SEC = 60 * 60;

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

async function getPollState(env: Env): Promise<PollState | null> {
  const s = (await env.KV.get(POLL_STATE_KEY, { type: "json" }).catch(() => null)) as PollState | null;
  if (!s || s.v !== 1) return null;
  const next: PollState = {
    v: 1,
    updatedAt: typeof s.updatedAt === "number" ? s.updatedAt : 0,
    forums: s.forums && typeof s.forums === "object" ? s.forums : {}
  };

  if (s.home && typeof s.home === "object") {
    const threadIds = Array.isArray((s.home as any).threadIds) ? ((s.home as any).threadIds as any[]) : [];
    const ids = threadIds.map((x) => Number(x)).filter((n) => Number.isFinite(n));
    const threads =
      (s.home as any).threads && typeof (s.home as any).threads === "object"
        ? ((s.home as any).threads as Record<string, number>)
        : {};

    // Backward compat: older state had only threadIds.
    for (const id of ids) {
      const k = String(id);
      if (threads[k] === undefined) threads[k] = 0;
    }

    next.home = { v: 1, updatedAt: typeof (s.home as any).updatedAt === "number" ? (s.home as any).updatedAt : 0, threadIds: ids, threads };
  }

  return next;
}

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

  const state = (await getPollState(env)) ?? { v: 1, updatedAt: 0, forums: {} };
  let changed = false;

  for (const { forum, users: subs } of forumToUsers.values()) {
    changed = (await pollForumAndNotify(env, state, forum, subs, scheduledTimeMs)) || changed;
  }

  if (globalUsers.length > 0) {
    changed = (await pollHomeAndNotify(env, state, globalUsers, scheduledTimeMs)) || changed;
  }

  if (changed) {
    state.updatedAt = nowSec();
    await env.KV.put(POLL_STATE_KEY, JSON.stringify(state));
  }
}

async function pollForumAndNotify(
  env: Env,
  state: PollState,
  forum: ForumRef,
  subscribers: UserConfig[],
  nowMs: number
): Promise<boolean> {
  const forumKey = String(forum.id);
  const existing = state.forums[forumKey] ?? null;

  const html = await fetchText(forum.url);
  if (!html) return false;

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
    state.forums[forumKey] = primed;
    return true;
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

  const changed = !shallowEqualRecords(existing.threads, next.threads);
  if (changed) state.forums[forumKey] = next;

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

  return changed;
}

async function pollHomeAndNotify(env: Env, state: PollState, subscribers: UserConfig[], nowMs: number): Promise<boolean> {
  const existing = state.home ?? null;
  const html = await fetchText(env.SITE_BASE_URL);
  if (!html) return false;

  const threads = parseThreadsFromHtml(html, env.SITE_BASE_URL, { source: "home" }).slice(0, DEFAULT_HOME_THREAD_LIMIT);
  const nowSec = Math.floor(nowMs / 1000);

  if (!existing) {
    const primed: HomeState = {
      v: 1,
      updatedAt: nowSec,
      threadIds: threads.map((t) => t.id),
      threads: buildThreadStateMap(threads)
    };
    state.home = primed;
    return true;
  }

  const next: HomeState = {
    v: 1,
    updatedAt: nowSec,
    threadIds: threads.map((t) => t.id),
    threads: { ...(existing.threads ?? {}) }
  };

  const newThreadEvents: ThreadItem[] = [];
  const updatedThreadEvents: ThreadItem[] = [];

  for (const t of threads) {
    const tid = String(t.id);
    const current = t.updatedAt ?? 0;
    const prev = (existing.threads ?? {})[tid];

    if (prev === undefined) {
      newThreadEvents.push(t);
      next.threads[tid] = current;
      continue;
    }

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

  const changed = !arrayEqual(existing.threadIds, next.threadIds) || !shallowEqualRecords(existing.threads ?? {}, next.threads);
  if (changed) state.home = next;

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

  return changed;
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
  parts.push(`Pilas pues: ${kind}${t.forum?.name ? ` en ${t.forum.name}` : ""}`);
  parts.push(`${t.prefix ? `[${t.prefix}] ` : ""}${t.title}`);
  if (t.updatedAt && t.updatedAt > 0) {
    const iso = new Date(t.updatedAt * 1000).toISOString().replace(".000Z", "Z");
    parts.push(`Actualizado: ${iso}`);
  }
  parts.push(`Abrir: ${t.url}`);
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
    // If the user pastes a forum/thread URL, handle it as a shortcut.
    const pastedUrl = extractFirstUrl(text);
    if (pastedUrl) {
      const user = await ensureUser(env, chatId);
      const forum = await resolveForumFromArg(env, pastedUrl);
      if (forum) {
        const next = addForum(user, forum);
        await putUser(env, next);
        await tgSendMessage(env, chatId, `Listo pues, quedo suscrito a: ${forum.name}`, true);
        await sendLatestWithButtons(env, next, chatId, 5);
        return;
      }

      const threadId = threadIdFromHref(pastedUrl);
      if (threadId) {
        await handleReadCommand(env, chatId, pastedUrl);
        return;
      }
    }

    const user = await ensureUser(env, chatId);
    await sendMainMenu(env, user, chatId, false);
    return;
  }

  const name = cmd.name;
  const args = cmd.args;

  if (name === "start") {
    const user = await ensureUser(env, chatId);
    await sendMainMenu(env, user, chatId, false);
    return;
  }

  if (name === "help") {
    const user = await ensureUser(env, chatId);
    await sendHelpPage(env, user, chatId, false);
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
        "Uy parce, no le pude pillar ese foro. Mejor use /foros y lo elige con botones (o mande la URL del foro).",
        true
      );
      return;
    }
    const next = addForum(user, forum);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Listo pues, quedo suscrito a: ${forum.name}`, true);
    await sendLatestWithButtons(env, next, chatId, 5);
    return;
  }

  if (name === "rmforum" || name === "rmforo") {
    const forumId = parseInt(args.trim(), 10);
    if (!Number.isFinite(forumId)) {
      await tgSendMessage(env, chatId, "Parce, asi es: /rmforum <id>  (ej: /rmforum 18)", true);
      return;
    }
    const next = { ...user, forums: user.forums.filter((f) => f.id !== forumId), updatedAt: nowSec() };
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Listo, quite el foro: ${forumId}`, true);
    await sendLatestWithButtons(env, next, chatId, 5);
    return;
  }

  if (name === "addkw") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Listo, pero digame la palabra o frase. Ej: /addkw bogota", true);
      return;
    }
    const next = addUnique(user, "includeKeywords", kw);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `De una, agregue: ${kw}`, true);
    await sendLatestWithButtons(env, next, chatId, 5);
    return;
  }

  if (name === "rmkw") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Listo, pero cual quitamos? Ej: /rmkw bogota", true);
      return;
    }
    const next = removeValue(user, "includeKeywords", kw);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Listo, quite: ${kw}`, true);
    await sendLatestWithButtons(env, next, chatId, 5);
    return;
  }

  if (name === "addnot") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Listo, que quiere excluir? Ej: /addnot politica", true);
      return;
    }
    const next = addUnique(user, "excludeKeywords", kw);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Hecho, ahora excluyo: ${kw}`, true);
    await sendLatestWithButtons(env, next, chatId, 5);
    return;
  }

  if (name === "rmnot") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Listo, cual exclusion quitamos? Ej: /rmnot politica", true);
      return;
    }
    const next = removeValue(user, "excludeKeywords", kw);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Listo, ya no excluyo: ${kw}`, true);
    await sendLatestWithButtons(env, next, chatId, 5);
    return;
  }

  if (name === "addprefix") {
    const p = args.trim();
    if (!p) {
      await tgSendMessage(env, chatId, "Listo, pero cual prefijo? Ej: /addprefix Resena", true);
      return;
    }
    const next = addUnique(user, "prefixes", p);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Hecho, prefijo: ${p}`, true);
    await sendLatestWithButtons(env, next, chatId, 5);
    return;
  }

  if (name === "rmprefix") {
    const p = args.trim();
    if (!p) {
      await tgSendMessage(env, chatId, "Listo, cual prefijo quitamos? Ej: /rmprefix Resena", true);
      return;
    }
    const next = removeValue(user, "prefixes", p);
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Listo, quite el prefijo: ${p}`, true);
    await sendLatestWithButtons(env, next, chatId, 5);
    return;
  }

  if (name === "notify") {
    const [targetRaw, modeRaw] = args.split(/\s+/).map((s) => s.trim());
    const target = normalize(targetRaw ?? "");
    const mode = normalize(modeRaw ?? "");
    if (!target || !mode || (mode !== "on" && mode !== "off")) {
      await tgSendMessage(env, chatId, "Asi es: /notify threads on|off  o  /notify replies on|off", true);
      return;
    }
    const next = { ...user, updatedAt: nowSec() };
    if (target === "threads" || target === "temas") next.notifyThreads = mode === "on";
    else if (target === "replies" || target === "respuestas") next.notifyReplies = mode === "on";
    else {
      await tgSendMessage(env, chatId, "Opciones validas: threads | replies", true);
      return;
    }
    await putUser(env, next);
    await tgSendMessage(
      env,
      chatId,
      `Listo, avisos: temas=${next.notifyThreads ? "on" : "off"}, respuestas=${next.notifyReplies ? "on" : "off"}`,
      true
    );
    return;
  }

  if (name === "read" || name === "leer") {
    const url = extractFirstUrl(args) ?? args.trim();
    if (!url) {
      await tgSendMessage(env, chatId, "Asi es: /leer <url>  (ej: /leer https://doncolombia.com/temas/... )", true);
      return;
    }
    await handleReadCommand(env, chatId, url);
    return;
  }

  if (name === "latest" || name === "ultimos") {
    await sendLatestWithButtons(env, user, chatId, 5);
    return;
  }

  await sendMainMenu(env, user, chatId, false);
}

async function handleTelegramCallback(cb: any, env: Env): Promise<void> {
  const chatId = cb.message?.chat?.id;
  if (typeof chatId !== "number") return;

  const data = String(cb.data ?? "");
  const user = await ensureUser(env, chatId);
  const messageId = cb.message?.message_id;

  if (data.startsWith("readpick:")) {
    const parts = data.split(":");
    const token = parts[1] ?? "";
    const idx = parseInt(parts[2] ?? "", 10);
    if (!token || !Number.isFinite(idx)) {
      await tgAnswerCallback(env, cb.id, "");
      return;
    }
    const pickKey = `pick:${chatId}:${token}`;
    const pick = (await env.KV.get(pickKey, { type: "json" }).catch(() => null)) as any;
    const urls: unknown = pick?.urls;
    if (!pick || pick.chatId !== chatId || !Array.isArray(urls) || idx < 0 || idx >= urls.length) {
      await tgAnswerCallback(env, cb.id, "Se vencio");
      return;
    }
    const url = String(urls[idx] ?? "");
    if (!url) {
      await tgAnswerCallback(env, cb.id, "");
      return;
    }
    await tgAnswerCallback(env, cb.id, "Leyendo...");
    await handleReadCommand(env, chatId, url);
    return;
  }

  if (data.startsWith("notify_set:")) {
    const parts = data.split(":");
    const target = parts[1] ?? "";
    const mode = parts[2] ?? "";
    const next = { ...user, updatedAt: nowSec() };
    if (target === "threads") next.notifyThreads = mode === "on";
    else if (target === "replies") next.notifyReplies = mode === "on";
    await putUser(env, next);
    if (messageId) await sendNotifyPage(env, next, chatId, true, messageId);
    await tgAnswerCallback(env, cb.id, "Listo");
    return;
  }

  if (data.startsWith("city_toggle:")) {
    const parts = data.split(":");
    const cityId = parts[1] ?? "";
    const page = parseInt(parts[2] ?? "0", 10);
    const city = CITY_PRESETS.find((c) => c.id === cityId);
    if (!city) {
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    const isOn = user.includeKeywords.some((k) => normalize(k) === normalize(city.name));
    let next = isOn ? removeValue(user, "includeKeywords", city.name) : addUnique(user, "includeKeywords", city.name);

    // Best-effort: if there is exactly one forum whose name contains the city, auto-subscribe.
    if (!isOn) {
      const cache = await getForumCache(env);
      const hits = cache.forums.filter((f) => normalize(f.name).includes(normalize(city.name)));
      if (hits.length === 1) next = addForum(next, hits[0]);
    }

    await putUser(env, next);
    if (messageId) await sendCitiesPage(env, next, chatId, Number.isFinite(page) ? page : 0, true, messageId);
    await tgAnswerCallback(env, cb.id, "De una");
    await sendLatestWithButtons(env, next, chatId, 5);
    return;
  }

  if (data.startsWith("wiz:")) {
    const parts = data.split(":");
    const action = parts[1] ?? "";
    const arg = parts[2] ?? "";

    if (action === "main") {
      if (messageId) await sendMainMenu(env, user, chatId, true, messageId);
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    if (action === "help") {
      if (messageId) await sendHelpPage(env, user, chatId, true, messageId);
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    if (action === "me") {
      if (messageId) await sendMePage(env, user, chatId, true, messageId);
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    if (action === "notify") {
      if (messageId) await sendNotifyPage(env, user, chatId, true, messageId);
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    if (action === "cities") {
      const page = parseInt(arg ?? "0", 10);
      if (messageId) await sendCitiesPage(env, user, chatId, Number.isFinite(page) ? page : 0, true, messageId);
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    if (action === "forums") {
      const page = parseInt(arg ?? "0", 10);
      if (messageId) await sendForumsPage(env, user, chatId, Number.isFinite(page) ? page : 0, true, messageId);
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    if (action === "latest") {
      await tgAnswerCallback(env, cb.id, "Ya casi...");
      await sendLatestWithButtons(env, user, chatId, 5);
      return;
    }

    if (action === "reset") {
      const next: UserConfig = {
        ...user,
        forums: [],
        includeKeywords: [],
        excludeKeywords: [],
        prefixes: [],
        notifyThreads: true,
        notifyReplies: true,
        updatedAt: nowSec()
      };
      await putUser(env, next);
      if (messageId) await sendMainMenu(env, next, chatId, true, messageId);
      await tgAnswerCallback(env, cb.id, "Listo");
      await sendLatestWithButtons(env, next, chatId, 5);
      return;
    }
  }

  if (data.startsWith("forums_page:")) {
    const page = parseInt(data.split(":")[1] ?? "0", 10);
    await sendForumsPage(env, user, chatId, Number.isFinite(page) ? page : 0, true, cb.message?.message_id);
    await tgAnswerCallback(env, cb.id, "");
    return;
  }

  if (data === "refresh_forums") {
    await refreshForumCache(env, true);
    await sendForumsPage(env, user, chatId, 0, true, cb.message?.message_id);
    await tgAnswerCallback(env, cb.id, "Listo, actualizado");
    return;
  }

  if (data.startsWith("sub_forum:")) {
    const forumId = parseInt(data.split(":")[1] ?? "", 10);
    const forum = await resolveForumFromArg(env, String(forumId));
    if (!forum) {
      await tgAnswerCallback(env, cb.id, "No pude encontrar ese foro");
      return;
    }
    const next = addForum(user, forum);
    await putUser(env, next);
    await sendForumsPage(env, next, chatId, 0, true, cb.message?.message_id);
    await tgAnswerCallback(env, cb.id, "Listo, suscrito");
    await sendLatestWithButtons(env, next, chatId, 5);
    return;
  }

  if (data.startsWith("unsub_forum:")) {
    const forumId = parseInt(data.split(":")[1] ?? "", 10);
    const next = { ...user, forums: user.forums.filter((f) => f.id !== forumId), updatedAt: nowSec() };
    await putUser(env, next);
    await sendForumsPage(env, next, chatId, 0, true, cb.message?.message_id);
    await tgAnswerCallback(env, cb.id, "Listo, quitado");
    await sendLatestWithButtons(env, next, chatId, 5);
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

async function handleReadCommand(env: Env, chatId: number, urlRaw: string): Promise<void> {
  let url: URL;
  try {
    url = new URL(urlRaw, env.SITE_BASE_URL);
  } catch {
    await tgSendMessage(env, chatId, "Uy parce, ese link esta raro. Pruebe: /leer <url>", true);
    return;
  }

  // SSRF/abuse guard: only fetch the configured site.
  const base = new URL(env.SITE_BASE_URL);
  if (url.hostname !== base.hostname) {
    await tgSendMessage(env, chatId, `Pilas: solo leo enlaces de ${base.hostname}`, true);
    return;
  }

  const html = await fetchText(url.toString());
  if (!html) {
    await tgSendMessage(env, chatId, "Uy parce, no pude abrir esa pagina. Intente mas tarde.", true);
    return;
  }

  const title =
    extractMetaProperty(html, "og:title") ??
    extractTitleTag(html) ??
    url.toString();

  const ogDesc = extractMetaProperty(html, "og:description");
  const body = extractFirstPostText(html);
  const content = preferLonger(ogDesc, body);

  const text = content
    ? `${title}\n\n${content}\n\n${url.toString()}`
    : `${title}\n\n${url.toString()}`;

  await tgSendLongMessage(env, chatId, text, true);
}

function extractMetaProperty(html: string, property: string): string | null {
  const p = escapeRe(property);
  const re1 = new RegExp(`<meta\\b[^>]*property=["']${p}["'][^>]*content=["']([^"']+)["'][^>]*>`, "i");
  const m1 = re1.exec(html);
  if (m1?.[1]) return decodeHtmlEntities(m1[1]).trim();

  const re2 = new RegExp(`<meta\\b[^>]*content=["']([^"']+)["'][^>]*property=["']${p}["'][^>]*>`, "i");
  const m2 = re2.exec(html);
  if (m2?.[1]) return decodeHtmlEntities(m2[1]).trim();

  return null;
}

function extractTitleTag(html: string): string | null {
  const m = /<title[^>]*>([\s\S]*?)<\/title>/i.exec(html);
  if (!m?.[1]) return null;
  const t = htmlToText(m[1]);
  return t || null;
}

function extractFirstPostText(html: string): string | null {
  // XenForo commonly wraps message content in a "bbWrapper" div.
  const m = /class="bbWrapper"[^>]*>([\s\S]*?)<\/div>/i.exec(html);
  if (!m?.[1]) return null;
  const t = htmlToText(m[1]);
  return t || null;
}

function preferLonger(a: string | null, b: string | null): string | null {
  const aa = (a ?? "").trim();
  const bb = (b ?? "").trim();
  if (!aa && !bb) return null;
  if (!aa) return bb;
  if (!bb) return aa;
  return aa.length >= bb.length ? aa : bb;
}

async function sendMainMenu(
  env: Env,
  user: UserConfig,
  chatId: number,
  edit: boolean,
  messageId?: number
): Promise<void> {
  const selectedTopics = user.includeKeywords.length ? user.includeKeywords.slice(0, 3).join(", ") : "(ninguno)";
  const text = [
    "Quiubo, parce.",
    "Que quiere mirar hoy?",
    "",
    `Ciudades/temas: ${selectedTopics}`,
    `Foros: ${user.forums.length}`,
    "",
    "Pilas: toque un boton y yo le muestro."
  ].join("\n");

  const replyMarkup = {
    inline_keyboard: [
      [{ text: "Elegir ciudad", callback_data: "wiz:cities:0" }],
      [{ text: "Elegir foros", callback_data: "wiz:forums:0" }],
      [{ text: "Ultimas 5", callback_data: "wiz:latest" }],
      [{ text: "Mis filtros", callback_data: "wiz:me" }],
      [{ text: "Notificaciones", callback_data: "wiz:notify" }],
      [{ text: "Ayuda", callback_data: "wiz:help" }],
      [{ text: "Limpiar todo", callback_data: "wiz:reset" }]
    ]
  };

  if (edit && messageId) await tgEditMessage(env, chatId, messageId, text, replyMarkup);
  else await tgSendMessage(env, chatId, text, true, replyMarkup);
}

async function sendHelpPage(env: Env, user: UserConfig, chatId: number, edit: boolean, messageId?: number): Promise<void> {
  const text = [
    "Ayuda (facil, sin enredos)",
    "",
    "1) Toque 'Elegir ciudad' y escoja su ciudad (o varias).",
    "2) O toque 'Elegir foros' si quiere afinar mas.",
    "3) Cada vez que cambie un filtro, le mando las ultimas 5 con Abrir/Leer.",
    "4) Y cuando salgan temas nuevos o respuestas, le aviso solito.",
    "",
    "Tip: si pega un link de un tema, el bot tambien le saca un extracto."
  ].join("\n");

  const replyMarkup = { inline_keyboard: [[{ text: "Volver al menu", callback_data: "wiz:main" }]] };
  if (edit && messageId) await tgEditMessage(env, chatId, messageId, text, replyMarkup);
  else await tgSendMessage(env, chatId, text, true, replyMarkup);
}

async function sendMePage(env: Env, user: UserConfig, chatId: number, edit: boolean, messageId?: number): Promise<void> {
  const cities = user.includeKeywords.length ? user.includeKeywords.join(", ") : "(ninguno)";
  const forums = user.forums.length ? user.forums.map((f) => f.name).slice(0, 6).join(", ") : "(ninguno)";
  const text = [
    "Listo, asi va la vuelta:",
    "",
    `Ciudades/temas: ${cities}`,
    `Foros: ${forums}`,
    "",
    `Avisos: temas=${user.notifyThreads ? "on" : "off"}, respuestas=${user.notifyReplies ? "on" : "off"}`
  ].join("\n");

  const replyMarkup = {
    inline_keyboard: [
      [{ text: "Ultimas 5", callback_data: "wiz:latest" }],
      [{ text: "Editar ciudades", callback_data: "wiz:cities:0" }],
      [{ text: "Editar foros", callback_data: "wiz:forums:0" }],
      [{ text: "Volver al menu", callback_data: "wiz:main" }]
    ]
  };

  if (edit && messageId) await tgEditMessage(env, chatId, messageId, text, replyMarkup);
  else await tgSendMessage(env, chatId, text, true, replyMarkup);
}

async function sendNotifyPage(env: Env, user: UserConfig, chatId: number, edit: boolean, messageId?: number): Promise<void> {
  const text = [
    "Avisos",
    "",
    `Temas nuevos: ${user.notifyThreads ? "ON" : "OFF"}`,
    `Respuestas/actualizaciones: ${user.notifyReplies ? "ON" : "OFF"}`,
    "",
    "Prenda o apague aca:"
  ].join("\n");

  const replyMarkup = {
    inline_keyboard: [
      [
        { text: `Temas: ${user.notifyThreads ? "ON" : "OFF"}`, callback_data: `notify_set:threads:${user.notifyThreads ? "off" : "on"}` },
        { text: `Respuestas: ${user.notifyReplies ? "ON" : "OFF"}`, callback_data: `notify_set:replies:${user.notifyReplies ? "off" : "on"}` }
      ],
      [{ text: "Volver al menu", callback_data: "wiz:main" }]
    ]
  };

  if (edit && messageId) await tgEditMessage(env, chatId, messageId, text, replyMarkup);
  else await tgSendMessage(env, chatId, text, true, replyMarkup);
}

async function sendCitiesPage(
  env: Env,
  user: UserConfig,
  chatId: number,
  page: number,
  edit: boolean,
  messageId?: number
): Promise<void> {
  const totalPages = Math.max(1, Math.ceil(CITY_PRESETS.length / CITY_PAGE_SIZE));
  const p = Math.min(Math.max(0, page), totalPages - 1);
  const start = p * CITY_PAGE_SIZE;
  const slice = CITY_PRESETS.slice(start, start + CITY_PAGE_SIZE);

  const selected = new Set(user.includeKeywords.map((k) => normalize(k)));

  const keyboard: any[][] = [];
  for (const c of slice) {
    const on = selected.has(normalize(c.name));
    keyboard.push([{ text: `${on ? "[x]" : "[ ]"} ${c.name}`, callback_data: `city_toggle:${c.id}:${p}` }]);
  }

  const navRow: any[] = [];
  if (p > 0) navRow.push({ text: "Anterior", callback_data: `wiz:cities:${p - 1}` });
  navRow.push({ text: `${p + 1}/${totalPages}`, callback_data: `wiz:cities:${p}` });
  if (p < totalPages - 1) navRow.push({ text: "Siguiente", callback_data: `wiz:cities:${p + 1}` });
  keyboard.push(navRow);
  keyboard.push([{ text: "Ultimas 5", callback_data: "wiz:latest" }]);
  keyboard.push([{ text: "Volver al menu", callback_data: "wiz:main" }]);

  const selectedLabel = user.includeKeywords.length ? user.includeKeywords.slice(0, 6).join(", ") : "(ninguna)";
  const text = [
    "Ciudades",
    "",
    "Escoja una o varias. Cuando salga algo de esa ciudad, le aviso.",
    "",
    `Seleccionadas: ${selectedLabel}`
  ].join("\n");

  const replyMarkup = { inline_keyboard: keyboard };
  if (edit && messageId) await tgEditMessage(env, chatId, messageId, text, replyMarkup);
  else await tgSendMessage(env, chatId, text, true, replyMarkup);
}

async function sendLatestWithButtons(env: Env, user: UserConfig, chatId: number, limit: number): Promise<void> {
  const items = (await getLatestForUser(env, user)).slice(0, limit);
  if (items.length === 0) {
    await tgSendMessage(env, chatId, "Uy parce, por ahora no me sale nada con esos filtros.", true, {
      inline_keyboard: [[{ text: "Volver al menu", callback_data: "wiz:main" }]]
    });
    return;
  }

  const token = randomTokenHex(6);
  const pickKey = `pick:${chatId}:${token}`;
  await env.KV.put(pickKey, JSON.stringify({ chatId, urls: items.map((t) => t.url) }), { expirationTtl: PICKS_TTL_SEC });

  for (let i = 0; i < items.length; i++) {
    const t = items[i];
    const text = formatLatestItemText(t, i + 1, items.length);
    const keyboard = {
      inline_keyboard: [[
        { text: "Abrir", url: t.url },
        { text: "Leer", callback_data: `readpick:${token}:${i}` }
      ]]
    };
    await tgSendMessage(env, chatId, text, true, keyboard);
  }
}

function formatThreadListLine(t: ThreadItem): string {
  const head: string[] = [];
  if (t.forum?.name) head.push(`[${t.forum.name}]`);
  if (t.prefix) head.push(`[${t.prefix}]`);
  head.push(t.title);
  return truncate(head.join(" "), 140);
}

function formatLatestItemText(t: ThreadItem, idx: number, total: number): string {
  const lines: string[] = [];
  lines.push(`Pilas pues (${idx}/${total})`);
  const head: string[] = [];
  if (t.forum?.name) head.push(`[${t.forum.name}]`);
  if (t.prefix) head.push(`[${t.prefix}]`);
  head.push(t.title);
  lines.push(truncate(head.join(" "), 220));
  if (t.updatedAt && t.updatedAt > 0) {
    const iso = new Date(t.updatedAt * 1000).toISOString().replace(".000Z", "Z");
    lines.push(`Actualizado: ${iso}`);
  }
  return lines.join("\n");
}

function truncate(s: string, max: number): string {
  const text = String(s ?? "");
  if (text.length <= max) return text;
  if (max <= 3) return text.slice(0, max);
  return text.slice(0, Math.max(0, max - 3)).trimEnd() + "...";
}

function randomTokenHex(bytes: number): string {
  const buf = new Uint8Array(bytes);
  crypto.getRandomValues(buf);
  return Array.from(buf)
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
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
    await tgSendMessage(env, chatId, "Uy parce, no pude cargar la lista de foros. Intente mas tarde.", true);
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
  keyboard.push([{ text: "Ultimas 5", callback_data: "wiz:latest" }]);
  keyboard.push([{ text: "Volver al menu", callback_data: "wiz:main" }]);

  const text = ["Foros", "", "Toque el foro para prenderlo o quitarlo.", `Suscritos: ${user.forums.length}`].join("\n");

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

function extractFirstUrl(text: string): string | null {
  const m = String(text ?? "").match(/https?:\/\/[^\s<>"']+/i);
  return m ? m[0] : null;
}

function startText(): string {
  return [
    "Quiubo, el bot ya quedo.",
    "",
    "Este bot le ayuda a filtrar y recibir avisos de temas nuevos y respuestas.",
    "",
    "Recomendado:",
    "/start  (menu por botones)",
    "",
    "Si quiere cacharrear con comandos:",
    "/foros",
    "/addkw <palabra>",
    "/list",
    "/latest",
    "/leer <url>"
  ].join("\n");
}

function helpText(): string {
  return [
    "Comandos (opcional):",
    "/foros",
    "/addforum <url> | /rmforum <id>",
    "/addkw <texto> | /rmkw <texto>",
    "/addnot <texto> | /rmnot <texto>   (excluir)",
    "/addprefix <texto> | /rmprefix <texto>",
    "/notify threads on|off",
    "/notify replies on|off",
    "/list",
    "/latest",
    "/leer <url>"
  ].join("\n");
}

function formatUserConfig(u: UserConfig): string {
  const lines: string[] = [];
  lines.push("Listo, estos son sus filtros:");
  lines.push(`Ciudades/temas: ${u.includeKeywords.length ? u.includeKeywords.join(", ") : "(ninguno)"}`);
  lines.push(`Foros: ${u.forums.length ? u.forums.map((f) => f.name).join(", ") : "(ninguno)"}`);
  lines.push(`Excluir: ${u.excludeKeywords.length ? u.excludeKeywords.join(", ") : "(ninguno)"}`);
  lines.push(`Prefijos: ${u.prefixes.length ? u.prefixes.join(", ") : "(ninguno)"}`);
  lines.push(`Avisos temas: ${u.notifyThreads ? "on" : "off"}`);
  lines.push(`Avisos respuestas: ${u.notifyReplies ? "on" : "off"}`);
  return lines.join("\n");
}

function normalize(s: string): string {
  const raw = String(s ?? "");
  const noMarks = stripDiacritics(raw);
  return noMarks.trim().toLowerCase().replace(/\s+/g, " ");
}

function stripDiacritics(s: string): string {
  try {
    return s.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  } catch {
    return s;
  }
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
  const searchHtml = opts.source === "home" ? sliceHomeLatestBlock(html) : html;

  const threadHrefRe = new RegExp(
    `<a\\b[^>]*href="([^"]*(?:${THREAD_PATHS.map(escapeRe).join("|")})[^"]+)"[^>]*>([\\s\\S]*?)<\\/a>`,
    "gi"
  );

  const out: ThreadItem[] = [];
  const seen = new Set<number>();

  let m: RegExpExecArray | null;
  while ((m = threadHrefRe.exec(searchHtml)) !== null) {
    const hrefRaw = m[1] ?? "";
    const title = htmlToText(m[2] ?? "");
    const threadId = threadIdFromHref(hrefRaw);
    if (!threadId) continue;
    if (seen.has(threadId)) continue;
    seen.add(threadId);

    const absUrl = new URL(hrefRaw, baseUrl).toString();

    const matchIndex = m.index ?? 0;
    const chunkStart = Math.max(0, matchIndex - 900);
    const chunkEnd = Math.min(searchHtml.length, matchIndex + m[0].length + 3000);
    const chunk = searchHtml.slice(chunkStart, chunkEnd);
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

function sliceHomeLatestBlock(html: string): string {
  const lower = html.toLowerCase();
  const markers = [
    "nuevos temas publicados",
    "nuevas publicaciones",
    "latest threads",
    "new threads"
  ];

  for (const marker of markers) {
    const idx = lower.indexOf(marker);
    if (idx === -1) continue;
    return html.slice(idx, idx + 160_000);
  }

  // Fallback: whole document.
  return html;
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
  try {
    const url = `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/${method}`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });
    const data = (await res.json().catch(() => null)) as any;
    if (!res.ok || !data || data.ok !== true) return null;
    return data.result;
  } catch {
    return null;
  }
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

async function tgSendLongMessage(env: Env, chatId: number, text: string, disablePreview: boolean): Promise<void> {
  // Telegram message limit is 4096 chars; keep a safe margin.
  const max = 3500;
  const maxParts = 3;

  let remaining = String(text ?? "").trim();
  let part = 0;
  while (remaining.length > 0 && part < maxParts) {
    let chunk = remaining;
    if (chunk.length > max) {
      chunk = remaining.slice(0, max);
      const cut = chunk.lastIndexOf("\n");
      if (cut > 0.6 * max) chunk = chunk.slice(0, cut);
    }
    chunk = chunk.trim();
    if (!chunk) break;
    await tgSendMessage(env, chatId, chunk, disablePreview);
    remaining = remaining.slice(chunk.length).trimStart();
    part++;
  }

  if (remaining.length > 0) {
    await tgSendMessage(env, chatId, "(Se me corto. Abra el link para ver todo.)", true);
  }
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
