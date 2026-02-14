export interface Env {
  TELEGRAM_BOT_TOKEN: string;
  TELEGRAM_WEBHOOK_SECRET_TOKEN: string;
  KV: KVNamespace;
  SITE_BASE_URL: string;

  // Optional public base URL for links (donations, reviews, etc).
  // Example: "https://your-worker.workers.dev"
  PUBLIC_BASE_URL?: string;

  // Wompi donations (optional)
  WOMPI_PUBLIC_KEY?: string;
  WOMPI_INTEGRITY_SECRET?: string;
  WOMPI_EVENTS_SECRET?: string;
  DONATION_MONTHLY_GOAL_CENTS?: string;
  DONATION_REFERENCE_PREFIX?: string;

  // DeepSeek (optional, used to improve review narratives)
  DEEPSEEK_API_KEY?: string;
  DEEPSEEK_BASE_URL?: string;
  DEEPSEEK_MODEL?: string;
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
  notifyReviews: boolean;
  notifyDonations: boolean;
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

type ReviewItem = {
  v: 1;
  id: string;
  createdAt: number;
  createdBy: number;
  city: string;
  title: string;
  tags: string[];
  raw: string;
  story: string;
};

type ReviewDraft = {
  v: 1;
  chatId: number;
  createdAt: number;
  updatedAt: number;
  step: "await_title" | "await_desc" | "confirm";
  city: string;
  title?: string;
  raw?: string;
  story?: string;
  tags?: string[];
};

type DonationMonth = {
  v: 1;
  month: string; // YYYY-MM
  goalInCents: number;
  totalInCents: number;
  count: number;
  updatedAt: number;
};

const USERS_INDEX_KEY = "users";
const FORUM_CACHE_KEY = "cache:forums";
const POLL_STATE_KEY = "state:poll";

const REVIEWS_INDEX_KEY = "reviews:index";
const REVIEW_KEY_PREFIX = "review:";
const REVIEW_DRAFT_KEY_PREFIX = "draft:review:";

const DONATIONS_MONTH_KEY_PREFIX = "donations:month:";
const DONATIONS_TX_KEY_PREFIX = "donations:tx:";

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

    if (request.method === "GET" && url.pathname === "/donar") {
      return handleDonateLanding(request, env);
    }

    if (request.method === "GET" && url.pathname === "/donar/gracias") {
      return handleDonateThanks(request, env);
    }

    if (request.method === "GET" && url.pathname === "/api/donations/stats") {
      return handleDonationStats(request, env);
    }

    if (request.method === "GET" && url.pathname === "/api/donations/checkout") {
      return handleDonationCheckout(request, env);
    }

    if (url.pathname === "/wompi/webhook") {
      if (request.method !== "POST") return new Response("Method Not Allowed", { status: 405 });
      return handleWompiWebhook(request, env);
    }

    if (request.method === "GET" && url.pathname.startsWith("/r/")) {
      return handleReviewPage(request, env);
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

  // Donation nudges (opt-out). Runs very occasionally to avoid spam.
  await maybeSendDonationReminders(env, users, scheduledTimeMs);
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

  if (cmd && cmd.name === "cancel") {
    await deleteReviewDraft(env, chatId);
    const user = await ensureUser(env, chatId);
    await tgSendMessage(env, chatId, "Listo, cancele esa resena.", true);
    await sendMainMenu(env, user, chatId, false);
    return;
  }

  const draft = await getReviewDraft(env, chatId);
  if (draft && !cmd) {
    await handleReviewDraftText(env, chatId, draft, text);
    return;
  }

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

      const threadInfo = threadPathInfoFromHref(pastedUrl);
      if (threadInfo) {
        const canonUrl = new URL(threadInfo.basePath, env.SITE_BASE_URL).toString();
        await handleReadCommand(env, chatId, canonUrl);
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
      await tgSendMessage(
        env,
        chatId,
        "Asi es: /notify threads on|off | replies on|off | reviews on|off | donations on|off",
        true
      );
      return;
    }
    const next = { ...user, updatedAt: nowSec() };
    if (target === "threads" || target === "temas") next.notifyThreads = mode === "on";
    else if (target === "replies" || target === "respuestas") next.notifyReplies = mode === "on";
    else if (target === "reviews" || target === "resenas") next.notifyReviews = mode === "on";
    else if (target === "donations" || target === "aportes" || target === "donar") next.notifyDonations = mode === "on";
    else {
      await tgSendMessage(env, chatId, "Opciones validas: threads | replies | reviews | donations", true);
      return;
    }
    await putUser(env, next);
    await tgSendMessage(
      env,
      chatId,
      `Listo, avisos: temas=${next.notifyThreads ? "on" : "off"}, respuestas=${next.notifyReplies ? "on" : "off"}, resenas=${
        next.notifyReviews ? "on" : "off"
      }, aportes=${next.notifyDonations ? "on" : "off"}`,
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
    else if (target === "reviews") next.notifyReviews = mode === "on";
    else if (target === "donations") next.notifyDonations = mode === "on";
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

  if (data.startsWith("rvw:citypage:")) {
    const page = parseInt(data.split(":")[2] ?? "0", 10);
    if (messageId) await sendReviewCityPage(env, chatId, Number.isFinite(page) ? page : 0, true, messageId);
    await tgAnswerCallback(env, cb.id, "");
    return;
  }

  if (data === "rvw:latest") {
    await tgAnswerCallback(env, cb.id, "Ya casi...");
    await sendLatestReviews(env, user, chatId, 5);
    return;
  }

  if (data === "rvw:new") {
    if (messageId) await sendReviewCityPage(env, chatId, 0, true, messageId);
    else await sendReviewCityPage(env, chatId, 0, false);
    await tgAnswerCallback(env, cb.id, "");
    return;
  }

  if (data.startsWith("rvw_city:")) {
    const parts = data.split(":");
    const cityId = parts[1] ?? "";
    const page = parseInt(parts[2] ?? "0", 10);
    const city = CITY_PRESETS.find((c) => c.id === cityId);
    if (!city) {
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    await putReviewDraft(env, {
      v: 1,
      chatId,
      createdAt: nowSec(),
      updatedAt: nowSec(),
      step: "await_title",
      city: city.name,
      tags: [city.name]
    });

    await tgAnswerCallback(env, cb.id, "Listo");
    await tgSendMessage(
      env,
      chatId,
      [
        "Listo pues.",
        `Ciudad: ${city.name}`,
        "",
        "Ahora mande un titulo corto (opcional).",
        "Ej: 'Resena en Chapinero'",
        "",
        "Si no quiere, toque 'Saltar'."
      ].join("\n"),
      true,
      { inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip_title" }, { text: "Cancelar", callback_data: "rvw:cancel" }]] }
    );

    if (messageId) await sendReviewCityPage(env, chatId, Number.isFinite(page) ? page : 0, true, messageId);
    return;
  }

  if (data === "rvw:skip_title") {
    const draft = await getReviewDraft(env, chatId);
    if (!draft) {
      await tgAnswerCallback(env, cb.id, "Se vencio");
      return;
    }
    const next: ReviewDraft = { ...draft, step: "await_desc", title: draft.title?.trim() || `Resena en ${draft.city}`, updatedAt: nowSec() };
    await putReviewDraft(env, next);
    await tgAnswerCallback(env, cb.id, "Dale");
    await tgSendMessage(
      env,
      chatId,
      [
        `Titulo: ${next.title}`,
        "",
        "Ahora si: escriba la resena en sus palabras (1 mensaje).",
        "Tip: sea directo, que la IA solo la organiza mejor."
      ].join("\n"),
      true,
      { inline_keyboard: [[{ text: "Cancelar", callback_data: "rvw:cancel" }]] }
    );
    return;
  }

  if (data === "rvw:cancel") {
    await deleteReviewDraft(env, chatId);
    await tgAnswerCallback(env, cb.id, "Listo");
    if (messageId) await sendReviewsMenu(env, user, chatId, true, messageId);
    return;
  }

  if (data === "rvw:edit") {
    const draft = await getReviewDraft(env, chatId);
    if (!draft) {
      await tgAnswerCallback(env, cb.id, "Se vencio");
      return;
    }
    const next: ReviewDraft = { ...draft, step: "await_desc", updatedAt: nowSec() };
    await putReviewDraft(env, next);
    await tgAnswerCallback(env, cb.id, "Dale");
    await tgSendMessage(env, chatId, "Listo, mande el texto otra vez (1 mensaje).", true, {
      inline_keyboard: [[{ text: "Cancelar", callback_data: "rvw:cancel" }]]
    });
    return;
  }

  if (data === "rvw:publish") {
    const draft = await getReviewDraft(env, chatId);
    if (!draft || draft.step !== "confirm" || !draft.city || !draft.title || !draft.raw || !draft.story) {
      await tgAnswerCallback(env, cb.id, "Se vencio");
      return;
    }

    const item: ReviewItem = {
      v: 1,
      id: randomTokenHex(8),
      createdAt: nowSec(),
      createdBy: chatId,
      city: draft.city,
      title: draft.title,
      tags: draft.tags?.length ? draft.tags : [draft.city],
      raw: draft.raw,
      story: draft.story
    };

    await saveReview(env, item);
    await deleteReviewDraft(env, chatId);

    await tgAnswerCallback(env, cb.id, "Publicada");
    const base = publicBaseUrl(env);
    const row = base
      ? [{ text: "Abrir", url: `${base}/r/${item.id}` }, { text: "Leer", callback_data: `rvw_read:${item.id}` }]
      : [{ text: "Leer", callback_data: `rvw_read:${item.id}` }];
    await tgSendMessage(env, chatId, "Listo pues, ya quedo publicada.", true, { inline_keyboard: [row] });

    await notifyUsersAboutReview(env, item);
    return;
  }

  if (data.startsWith("rvw_read:")) {
    const id = data.split(":")[1] ?? "";
    if (!id) {
      await tgAnswerCallback(env, cb.id, "");
      return;
    }
    await tgAnswerCallback(env, cb.id, "Leyendo...");
    await sendReviewRead(env, chatId, id);
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

    if (action === "reviews") {
      if (messageId) await sendReviewsMenu(env, user, chatId, true, messageId);
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    if (action === "donate") {
      if (messageId) await sendDonateInfo(env, chatId, true, messageId);
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
        notifyReviews: true,
        notifyDonations: true,
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

  const keyboard: any[][] = [
    [{ text: "Elegir ciudad", callback_data: "wiz:cities:0" }],
    [{ text: "Elegir foros", callback_data: "wiz:forums:0" }],
    [{ text: "Ultimas 5", callback_data: "wiz:latest" }],
    [{ text: "Resenas", callback_data: "wiz:reviews" }]
  ];

  const base = publicBaseUrl(env);
  if (base) keyboard.push([{ text: "Donar", url: `${base}/donar` }]);
  else keyboard.push([{ text: "Donar", callback_data: "wiz:donate" }]);

  keyboard.push([{ text: "Mis filtros", callback_data: "wiz:me" }]);
  keyboard.push([{ text: "Notificaciones", callback_data: "wiz:notify" }]);
  keyboard.push([{ text: "Ayuda", callback_data: "wiz:help" }]);
  keyboard.push([{ text: "Limpiar todo", callback_data: "wiz:reset" }]);

  const replyMarkup = { inline_keyboard: keyboard };

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
    "5) En 'Resenas' puede ver y publicar resenas por ciudad.",
    "6) Si quiere apoyar el proyecto: toque 'Donar'.",
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
    `Avisos: temas=${user.notifyThreads ? "on" : "off"}, respuestas=${user.notifyReplies ? "on" : "off"}, resenas=${
      user.notifyReviews ? "on" : "off"
    }, aportes=${user.notifyDonations ? "on" : "off"}`
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
    `Resenas: ${user.notifyReviews ? "ON" : "OFF"}`,
    `Aportes (mensajes): ${user.notifyDonations ? "ON" : "OFF"}`,
    "",
    "Prenda o apague aca:"
  ].join("\n");

  const replyMarkup = {
    inline_keyboard: [
      [
        { text: `Temas: ${user.notifyThreads ? "ON" : "OFF"}`, callback_data: `notify_set:threads:${user.notifyThreads ? "off" : "on"}` },
        { text: `Respuestas: ${user.notifyReplies ? "ON" : "OFF"}`, callback_data: `notify_set:replies:${user.notifyReplies ? "off" : "on"}` }
      ],
      [
        { text: `Resenas: ${user.notifyReviews ? "ON" : "OFF"}`, callback_data: `notify_set:reviews:${user.notifyReviews ? "off" : "on"}` },
        { text: `Aportes: ${user.notifyDonations ? "ON" : "OFF"}`, callback_data: `notify_set:donations:${user.notifyDonations ? "off" : "on"}` }
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
    "/notify reviews on|off",
    "/notify donations on|off",
    "/list",
    "/latest",
    "/leer <url>",
    "/cancel  (cancelar resena)"
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
  lines.push(`Avisos resenas: ${u.notifyReviews ? "on" : "off"}`);
  lines.push(`Avisos aportes: ${u.notifyDonations ? "on" : "off"}`);
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
    notifyReplies: true,
    notifyReviews: true,
    notifyDonations: true
  };
}

async function getUser(env: Env, chatId: number): Promise<UserConfig | null> {
  const key = `user:${chatId}`;
  const raw = (await env.KV.get(key, { type: "json" }).catch(() => null)) as any;
  if (!raw || raw.v !== 1) return null;

  const forums: ForumRef[] = Array.isArray(raw.forums) ? raw.forums : [];
  const includeKeywords: string[] = Array.isArray(raw.includeKeywords) ? raw.includeKeywords : [];
  const excludeKeywords: string[] = Array.isArray(raw.excludeKeywords) ? raw.excludeKeywords : [];
  const prefixes: string[] = Array.isArray(raw.prefixes) ? raw.prefixes : [];

  const u: UserConfig = {
    v: 1,
    chatId: typeof raw.chatId === "number" ? raw.chatId : chatId,
    createdAt: typeof raw.createdAt === "number" ? raw.createdAt : nowSec(),
    updatedAt: typeof raw.updatedAt === "number" ? raw.updatedAt : nowSec(),
    forums,
    includeKeywords,
    excludeKeywords,
    prefixes,
    notifyThreads: typeof raw.notifyThreads === "boolean" ? raw.notifyThreads : true,
    notifyReplies: typeof raw.notifyReplies === "boolean" ? raw.notifyReplies : true,
    notifyReviews: typeof raw.notifyReviews === "boolean" ? raw.notifyReviews : true,
    notifyDonations: typeof raw.notifyDonations === "boolean" ? raw.notifyDonations : true
  };
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
    `<a\\b[^>]*href=["']([^"']*(?:${THREAD_PATHS.map(escapeRe).join("|")})[^"']+)["'][^>]*>([\\s\\S]*?)<\\/a>`,
    "gi"
  );

  // Thread lists contain multiple links per thread (unread/latest/page links, etc). Keep the best candidate per id.
  const bestById = new Map<number, { score: number; index: number; item: ThreadItem }>();

  let m: RegExpExecArray | null;
  while ((m = threadHrefRe.exec(searchHtml)) !== null) {
    const hrefRaw = m[1] ?? "";
    const titleRaw = htmlToText(m[2] ?? "");
    const info = threadPathInfoFromHref(hrefRaw);
    if (!info) continue;
    const threadId = info.id;

    // Prefer the canonical thread URL (first page), not /unread or /latest.
    const absUrl = new URL(info.basePath, baseUrl).toString();

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

    const item: ThreadItem = {
      id: threadId,
      title: titleRaw || `Tema ${threadId}`,
      url: absUrl,
      forum,
      prefix: prefix || undefined,
      updatedAt: updatedAt || undefined,
      source: opts.source
    };

    const score = scoreThreadAnchor(titleRaw, m[0] ?? "");
    if (score <= 0) continue;

    const prev = bestById.get(threadId);
    if (!prev) {
      bestById.set(threadId, { score, index: matchIndex, item });
      continue;
    }

    // Keep the best title/url candidate, but merge useful metadata across matches.
    const prevUpdated = prev.item.updatedAt ?? 0;
    const nextUpdated = item.updatedAt ?? 0;
    const maxUpdated = Math.max(prevUpdated, nextUpdated);

    const best = score > prev.score ? item : prev.item;
    const bestIndex = score > prev.score ? matchIndex : prev.index;
    const bestScore = score > prev.score ? score : prev.score;

    if (!best.prefix && item.prefix) best.prefix = item.prefix;
    if (!best.forum && item.forum) best.forum = item.forum;
    if (maxUpdated > 0) best.updatedAt = maxUpdated;

    bestById.set(threadId, { score: bestScore, index: bestIndex, item: best });
  }

  return Array.from(bestById.values())
    .sort((a, b) => a.index - b.index)
    .map((x) => x.item);
}

function sliceHomeLatestBlock(html: string): string {
  const lower = html.toLowerCase();
  const markers = [
    "nuevos temas publicados",
    "nuevas publicaciones",
    "ultimos temas",
    "ultimas publicaciones",
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
  const info = threadPathInfoFromHref(href);
  return info ? info.id : null;
}

function threadPathInfoFromHref(href: string): { id: number; basePath: string } | null {
  try {
    const u = new URL(href, "https://example.com");
    const p = u.pathname;
    for (const base of THREAD_PATHS) {
      const idx = p.indexOf(base);
      if (idx === -1) continue;
      const rest = p.slice(idx + base.length);
      const m = rest.match(/^([^\/?#]+)\.(\d+)(?:\/|$)/);
      if (!m) continue;
      const id = parseInt(m[2], 10);
      if (!Number.isFinite(id)) continue;
      const slugAndId = `${m[1]}.${m[2]}`;
      const basePath = `${p.slice(0, idx + base.length)}${slugAndId}/`;
      return { id, basePath };
    }
    return null;
  } catch {
    return null;
  }
}

function scoreThreadAnchor(titleRaw: string, fullAnchorTag: string): number {
  const title = String(titleRaw ?? "").trim();
  const tag = String(fullAnchorTag ?? "");

  // Prefer the "primary" link (XenForo thread title).
  const isPrimary = /data-tp-primary\s*=\s*["']on["']/i.test(tag);

  const classMatch = /class\s*=\s*["']([^"']+)["']/i.exec(tag);
  const classes = classMatch?.[1] ?? "";

  let score = 0;
  if (title) score += Math.min(title.length, 80);
  else score -= 50;

  if (isPrimary) score += 200;

  // Avoid picking prefix labels, unread/status icons, or date links as the "title" link.
  if (/\blabel\b/i.test(classes) || /\blabelLink\b/i.test(classes)) score -= 120;
  if (/structItem-status/i.test(classes)) score -= 120;
  if (/latestDate/i.test(classes)) score -= 80;

  if (/^(unread|read)$/i.test(title)) score -= 200;
  if (title.length > 0 && title.length <= 2) score -= 60;

  return score;
}

function extractLatestTimestamp(chunk: string): number {
  let max = 0;

  const dtRe = /data-time=["'](\d{9,16})["']/gi;
  let m: RegExpExecArray | null;
  while ((m = dtRe.exec(chunk)) !== null) {
    const n = parseInt(m[1], 10);
    const sec = normalizeEpochSeconds(n);
    if (sec > max) max = sec;
  }

  const isoRe = /datetime=["']([^"']{10,40})["']/gi;
  while ((m = isoRe.exec(chunk)) !== null) {
    const t = Date.parse(m[1]);
    if (Number.isFinite(t)) {
      const sec = Math.floor(t / 1000);
      if (sec > max) max = sec;
    }
  }

  return max;
}

function normalizeEpochSeconds(n: number): number {
  if (!Number.isFinite(n) || n <= 0) return 0;
  // Heuristic: epoch seconds are ~1e9. Anything >=1e12 is almost certainly ms/us/ns.
  if (n >= 1e18) return Math.floor(n / 1e9); // ns
  if (n >= 1e15) return Math.floor(n / 1e6); // us
  if (n >= 1e12) return Math.floor(n / 1e3); // ms
  return n;
}

function extractClosestLabel(chunk: string, anchorPos: number): string | null {
  const candidates: { text: string; idx: number }[] = [];

  const labelRe = /<(?:a|span)\b[^>]*class=["'][^"']*\blabel\b[^"']*["'][^>]*>([\s\S]*?)<\/(?:a|span)>/gi;
  let m: RegExpExecArray | null;
  while ((m = labelRe.exec(chunk)) !== null) {
    const text = htmlToText(m[1] ?? "");
    if (!text) continue;
    if (text.length > 40) continue;
    candidates.push({ text, idx: m.index ?? 0 });
  }

  const labelLinkRe = /<a\b[^>]*class=["'][^"']*\blabelLink\b[^"']*["'][^>]*>([\s\S]*?)<\/a>/gi;
  while ((m = labelLinkRe.exec(chunk)) !== null) {
    const text = htmlToText(m[1] ?? "");
    if (!text) continue;
    if (text.length > 40) continue;
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

function publicBaseUrl(env: Env): string | null {
  const raw = String(env.PUBLIC_BASE_URL ?? "").trim();
  if (!raw) return null;
  try {
    const u = new URL(raw);
    const path = u.pathname.replace(/\/+$/, "");
    const base = `${u.origin}${path === "/" ? "" : path}`;
    return base || null;
  } catch {
    return null;
  }
}

function htmlResponse(body: string, status = 200): Response {
  return new Response(body, {
    status,
    headers: {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store"
    }
  });
}

function escapeHtml(s: string): string {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

// -------------------------
// Reviews (KV-backed)
// -------------------------

type ReviewIndexEntry = {
  id: string;
  createdAt: number;
  city: string;
  title: string;
};

const REVIEW_DRAFT_TTL_SEC = 2 * 60 * 60;
const REVIEW_INDEX_MAX = 400;

function reviewKey(id: string): string {
  return `${REVIEW_KEY_PREFIX}${id}`;
}

function reviewDraftKey(chatId: number): string {
  return `${REVIEW_DRAFT_KEY_PREFIX}${chatId}`;
}

async function getReviewIndex(env: Env): Promise<ReviewIndexEntry[]> {
  const raw = await env.KV.get(REVIEWS_INDEX_KEY);
  if (!raw) return [];
  try {
    const arr = JSON.parse(raw) as any[];
    if (!Array.isArray(arr)) return [];
    const out: ReviewIndexEntry[] = [];
    for (const it of arr) {
      if (!it || typeof it !== "object") continue;
      const id = String((it as any).id ?? "").trim();
      if (!id) continue;
      out.push({
        id,
        createdAt: typeof (it as any).createdAt === "number" ? (it as any).createdAt : 0,
        city: String((it as any).city ?? "").trim(),
        title: String((it as any).title ?? "").trim()
      });
    }
    out.sort((a, b) => (b.createdAt ?? 0) - (a.createdAt ?? 0));
    return out.slice(0, REVIEW_INDEX_MAX);
  } catch {
    return [];
  }
}

async function putReviewIndex(env: Env, entries: ReviewIndexEntry[]): Promise<void> {
  await env.KV.put(REVIEWS_INDEX_KEY, JSON.stringify(entries.slice(0, REVIEW_INDEX_MAX)));
}

async function getReview(env: Env, id: string): Promise<ReviewItem | null> {
  const key = reviewKey(id);
  const item = (await env.KV.get(key, { type: "json" }).catch(() => null)) as ReviewItem | null;
  if (!item || item.v !== 1 || typeof item.id !== "string") return null;
  return item;
}

async function saveReview(env: Env, item: ReviewItem): Promise<void> {
  await env.KV.put(reviewKey(item.id), JSON.stringify(item));
  const idx = await getReviewIndex(env);
  const entry: ReviewIndexEntry = { id: item.id, createdAt: item.createdAt, city: item.city, title: item.title };
  const next = [entry, ...idx.filter((e) => e.id !== item.id)];
  await putReviewIndex(env, next);
}

async function getReviewDraft(env: Env, chatId: number): Promise<ReviewDraft | null> {
  const key = reviewDraftKey(chatId);
  const draft = (await env.KV.get(key, { type: "json" }).catch(() => null)) as ReviewDraft | null;
  if (!draft || draft.v !== 1 || draft.chatId !== chatId) return null;
  if (!draft.city || !draft.step) return null;
  return draft;
}

async function putReviewDraft(env: Env, draft: ReviewDraft): Promise<void> {
  const key = reviewDraftKey(draft.chatId);
  await env.KV.put(key, JSON.stringify(draft), { expirationTtl: REVIEW_DRAFT_TTL_SEC });
}

async function deleteReviewDraft(env: Env, chatId: number): Promise<void> {
  const key = reviewDraftKey(chatId);
  await env.KV.delete(key).catch(() => undefined);
}

async function sendReviewsMenu(env: Env, user: UserConfig, chatId: number, edit: boolean, messageId?: number): Promise<void> {
  const selected = user.includeKeywords.length ? user.includeKeywords.slice(0, 4).join(", ") : "(ninguna)";
  const text = [
    "Resenas",
    "",
    "Aca puede ver y publicar resenas filtradas por sus ciudades/temas.",
    "",
    `Ciudades/temas: ${selected}`,
    "",
    "Que hacemos?"
  ].join("\n");

  const replyMarkup = {
    inline_keyboard: [
      [{ text: "Ultimas 5", callback_data: "rvw:latest" }],
      [{ text: "Crear resena", callback_data: "rvw:new" }],
      [{ text: "Editar ciudades", callback_data: "wiz:cities:0" }],
      [{ text: "Volver al menu", callback_data: "wiz:main" }]
    ]
  };

  if (edit && messageId) await tgEditMessage(env, chatId, messageId, text, replyMarkup);
  else await tgSendMessage(env, chatId, text, true, replyMarkup);
}

async function sendReviewCityPage(env: Env, chatId: number, page: number, edit: boolean, messageId?: number): Promise<void> {
  const totalPages = Math.max(1, Math.ceil(CITY_PRESETS.length / CITY_PAGE_SIZE));
  const p = Math.min(Math.max(0, page), totalPages - 1);
  const start = p * CITY_PAGE_SIZE;
  const slice = CITY_PRESETS.slice(start, start + CITY_PAGE_SIZE);

  const keyboard: any[][] = [];
  for (const c of slice) {
    keyboard.push([{ text: c.name, callback_data: `rvw_city:${c.id}:${p}` }]);
  }

  const navRow: any[] = [];
  if (p > 0) navRow.push({ text: "Anterior", callback_data: `rvw:citypage:${p - 1}` });
  navRow.push({ text: `${p + 1}/${totalPages}`, callback_data: `rvw:citypage:${p}` });
  if (p < totalPages - 1) navRow.push({ text: "Siguiente", callback_data: `rvw:citypage:${p + 1}` });
  keyboard.push(navRow);
  keyboard.push([{ text: "Cancelar", callback_data: "rvw:cancel" }]);
  keyboard.push([{ text: "Volver", callback_data: "wiz:reviews" }]);

  const text = ["Crear resena", "", "En que ciudad fue?"].join("\n");
  const replyMarkup = { inline_keyboard: keyboard };

  if (edit && messageId) await tgEditMessage(env, chatId, messageId, text, replyMarkup);
  else await tgSendMessage(env, chatId, text, true, replyMarkup);
}

async function sendLatestReviews(env: Env, user: UserConfig, chatId: number, limit: number): Promise<void> {
  if (user.includeKeywords.length === 0) {
    await tgSendMessage(env, chatId, "Pilas: primero elija una ciudad/tema para filtrar resenas.", true, {
      inline_keyboard: [[{ text: "Elegir ciudad", callback_data: "wiz:cities:0" }], [{ text: "Volver", callback_data: "wiz:reviews" }]]
    });
    return;
  }

  const idx = await getReviewIndex(env);
  const items = idx.filter((e) => matchesUserForReviewEntry(user, e)).slice(0, limit);

  if (items.length === 0) {
    await tgSendMessage(env, chatId, "Uy parce, no me salen resenas con esos filtros (todavia).", true, {
      inline_keyboard: [[{ text: "Volver", callback_data: "wiz:reviews" }]]
    });
    return;
  }

  const base = publicBaseUrl(env);
  for (let i = 0; i < items.length; i++) {
    const it = items[i];
    const text = formatLatestReviewItemText(it, i + 1, items.length);
    const row = base
      ? [{ text: "Abrir", url: `${base}/r/${it.id}` }, { text: "Leer", callback_data: `rvw_read:${it.id}` }]
      : [{ text: "Leer", callback_data: `rvw_read:${it.id}` }];
    await tgSendMessage(env, chatId, text, true, { inline_keyboard: [row] });
  }
}

function formatLatestReviewItemText(it: ReviewIndexEntry, idx: number, total: number): string {
  const lines: string[] = [];
  lines.push(`Pilas pues resenas (${idx}/${total})`);
  lines.push(`[${it.city}] ${truncate(it.title || it.id, 180)}`);
  if (it.createdAt) {
    const iso = new Date(it.createdAt * 1000).toISOString().replace(".000Z", "Z");
    lines.push(`Fecha: ${iso}`);
  }
  return lines.join("\n");
}

async function sendReviewRead(env: Env, chatId: number, id: string): Promise<void> {
  const item = await getReview(env, id);
  if (!item) {
    await tgSendMessage(env, chatId, "Uy parce, no encontre esa resena.", true);
    return;
  }

  const base = publicBaseUrl(env);
  const lines: string[] = [];
  lines.push(item.title);
  lines.push(`Ciudad: ${item.city}`);
  const iso = new Date(item.createdAt * 1000).toISOString().replace(".000Z", "Z");
  lines.push(`Fecha: ${iso}`);
  lines.push("");
  lines.push(item.story || item.raw || "(sin texto)");
  if (base) {
    lines.push("");
    lines.push(`${base}/r/${item.id}`);
  }

  await tgSendLongMessage(env, chatId, lines.join("\n"), true);
}

function matchesUserForReviewEntry(u: UserConfig, e: ReviewIndexEntry): boolean {
  const text = normalize(`${e.city} ${e.title}`);

  for (const k of u.excludeKeywords) {
    if (!k) continue;
    if (text.includes(normalize(k))) return false;
  }

  if (u.includeKeywords.length > 0) {
    const any = u.includeKeywords.some((k) => k && text.includes(normalize(k)));
    if (!any) return false;
  }

  return true;
}

async function handleReviewDraftText(env: Env, chatId: number, draft: ReviewDraft, textRaw: string): Promise<void> {
  const text = String(textRaw ?? "").trim();
  if (!text) {
    await tgSendMessage(env, chatId, "Parce, mande un texto.", true);
    return;
  }

  if (draft.step === "await_title") {
    const title = truncate(text, 120);
    const next: ReviewDraft = { ...draft, title: title || `Resena en ${draft.city}`, step: "await_desc", updatedAt: nowSec() };
    await putReviewDraft(env, next);
    await tgSendMessage(
      env,
      chatId,
      [
        `Titulo: ${next.title}`,
        "",
        "Ahora si: escriba la resena en sus palabras (1 mensaje).",
        "Tip: sea directo, que yo la organizo.",
        "",
        "Si quiere cancelar: /cancel"
      ].join("\n"),
      true,
      { inline_keyboard: [[{ text: "Cancelar", callback_data: "rvw:cancel" }]] }
    );
    return;
  }

  if (draft.step === "await_desc") {
    const raw = clipText(text, 2000);
    if (raw.length < 15) {
      await tgSendMessage(env, chatId, "Uy, eso esta muy corto. Mande un poquito mas de detalle.", true);
      return;
    }

    await tgSendMessage(env, chatId, "Deme un toque, la estoy arreglando para que quede bien contada...", true);

    const title = (draft.title ?? "").trim() || `Resena en ${draft.city}`;
    const story = await improveReviewWithDeepSeek(env, { city: draft.city, title, raw });

    const next: ReviewDraft = { ...draft, title, raw, story, step: "confirm", updatedAt: nowSec() };
    await putReviewDraft(env, next);

    const preview = [
      "Asi quedo la resena:",
      "",
      `Ciudad: ${next.city}`,
      `Titulo: ${next.title}`,
      "",
      truncate(next.story ?? "", 1400),
      "",
      "Si esta bien, toque Publicar. Si no, Editar."
    ].join("\n");

    await tgSendMessage(env, chatId, preview, true, {
      inline_keyboard: [[
        { text: "Publicar", callback_data: "rvw:publish" },
        { text: "Editar", callback_data: "rvw:edit" },
        { text: "Cancelar", callback_data: "rvw:cancel" }
      ]]
    });
    return;
  }

  await tgSendMessage(env, chatId, "Toque Publicar / Editar / Cancelar (o /cancel).", true);
}

function clipText(s: string, max: number): string {
  const text = String(s ?? "");
  if (text.length <= max) return text;
  return text.slice(0, max).trimEnd();
}

async function improveReviewWithDeepSeek(env: Env, input: { city: string; title: string; raw: string }): Promise<string> {
  const apiKey = String(env.DEEPSEEK_API_KEY ?? "").trim();
  if (!apiKey) return input.raw;

  const base = String(env.DEEPSEEK_BASE_URL ?? "https://api.deepseek.com").trim().replace(/\/+$/g, "");
  const model = String(env.DEEPSEEK_MODEL ?? "deepseek-chat").trim() || "deepseek-chat";

  const system = [
    "Eres un editor.",
    "Reescribe el texto del usuario en espanol colombiano, con buena redaccion y fluidez.",
    "No inventes datos ni agregues detalles nuevos.",
    "No uses Markdown. No uses listas largas.",
    "Mantenga palabras fuertes si el usuario las puso (no censures).",
    "Maximo 6 parrafos cortos."
  ].join(" ");

  const user = [`Ciudad: ${input.city}`, `Titulo: ${input.title}`, "", input.raw].join("\n");

  try {
    const res = await fetch(`${base}/v1/chat/completions`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "authorization": `Bearer ${apiKey}`
      },
      body: JSON.stringify({
        model,
        messages: [
          { role: "system", content: system },
          { role: "user", content: clipText(user, 1800) }
        ],
        temperature: 0.7,
        max_tokens: 260
      })
    });
    const data = (await res.json().catch(() => null)) as any;
    const out = data?.choices?.[0]?.message?.content;
    const text = typeof out === "string" ? out.trim() : "";
    return text || input.raw;
  } catch {
    return input.raw;
  }
}

async function notifyUsersAboutReview(env: Env, item: ReviewItem): Promise<void> {
  const chatIds = await getUserChatIds(env);
  if (chatIds.length === 0) return;

  const base = publicBaseUrl(env);
  const text = normalize(`${item.city} ${item.title} ${(item.tags ?? []).join(" ")}`);

  for (const chatId of chatIds) {
    if (chatId === item.createdBy) continue;
    const u = await getUser(env, chatId);
    if (!u || !u.notifyReviews) continue;
    if (u.includeKeywords.length === 0) continue;

    let ok = true;
    for (const k of u.excludeKeywords) {
      if (!k) continue;
      if (text.includes(normalize(k))) {
        ok = false;
        break;
      }
    }
    if (!ok) continue;

    if (u.includeKeywords.length > 0) {
      const any = u.includeKeywords.some((k) => k && text.includes(normalize(k)));
      if (!any) continue;
    }

    const row = base
      ? [{ text: "Abrir", url: `${base}/r/${item.id}` }, { text: "Leer", callback_data: `rvw_read:${item.id}` }]
      : [{ text: "Leer", callback_data: `rvw_read:${item.id}` }];

    await tgSendMessage(env, u.chatId, `Pilas pues: nueva resena\n[${item.city}] ${item.title}`, true, { inline_keyboard: [row] });
  }
}

async function handleReviewPage(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const parts = url.pathname.split("/").filter(Boolean);
  const id = parts[1] ?? "";
  if (!id) return htmlResponse("Not Found", 404);

  const item = await getReview(env, id);
  if (!item) return htmlResponse("Not Found", 404);

  const base = publicBaseUrl(env) ?? new URL(request.url).origin;

  const title = escapeHtml(item.title || "Resena");
  const city = escapeHtml(item.city || "");
  const story = escapeHtml(item.story || item.raw || "");
  const created = new Date((item.createdAt || 0) * 1000).toISOString().replace(".000Z", "Z");

  const html = `<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="robots" content="noindex" />
    <title>${title}</title>
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; background: #0b1220; color: #e8eefc; }
      main { max-width: 860px; margin: 0 auto; padding: 28px 18px 48px; }
      h1 { font-size: 26px; margin: 0 0 8px; }
      .meta { color: #a9b7d6; margin: 0 0 18px; }
      .card { background: #121a2e; border: 1px solid rgba(255,255,255,0.08); border-radius: 14px; padding: 18px; }
      .story { white-space: pre-wrap; line-height: 1.5; }
      a { color: #a6c8ff; }
      .top { display:flex; gap:10px; align-items:center; justify-content:space-between; flex-wrap:wrap; }
      .btn { display:inline-block; background: rgba(37,195,139,0.12); border: 1px solid rgba(37,195,139,0.35); color: #e8eefc; text-decoration:none; padding:10px 12px; border-radius: 12px; font-weight:700; }
    </style>
  </head>
  <body>
    <main>
      <div class="top">
        <div>
          <h1>${title}</h1>
          <p class="meta">Ciudad: ${city} | ${escapeHtml(created)}</p>
        </div>
        <a class="btn" href="${escapeHtml(base)}/donar">Apoyar el proyecto</a>
      </div>
      <div class="card">
        <div class="story">${story}</div>
      </div>
    </main>
  </body>
</html>`;

  return htmlResponse(html, 200);
}

// -------------------------
// Donations (Wompi)
// -------------------------

const DONATION_REMINDER_LAST_DAY_KEY = "donations:reminder:lastDay";

function donationGoalInCents(env: Env): number {
  const raw = String(env.DONATION_MONTHLY_GOAL_CENTS ?? "").trim();
  const n = parseInt(raw, 10);
  if (Number.isFinite(n) && n > 0) return n;
  // Default: 300.000 COP => 30.000.000 (amount_in_cents for COP uses *100).
  return 30_000_000;
}

function donationReferencePrefix(env: Env): string {
  const raw = String(env.DONATION_REFERENCE_PREFIX ?? "radarx").trim();
  const cleaned = raw.replace(/[^a-zA-Z0-9_-]/g, "").slice(0, 24);
  return cleaned || "radarx";
}

function monthKeyFromMs(ms: number): string {
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}

async function getDonationMonth(env: Env, month: string): Promise<DonationMonth> {
  const key = `${DONATIONS_MONTH_KEY_PREFIX}${month}`;
  const raw = (await env.KV.get(key, { type: "json" }).catch(() => null)) as DonationMonth | null;
  const goal = donationGoalInCents(env);
  if (raw && raw.v === 1 && raw.month === month) {
    return {
      v: 1,
      month,
      goalInCents: typeof raw.goalInCents === "number" ? raw.goalInCents : goal,
      totalInCents: typeof raw.totalInCents === "number" ? raw.totalInCents : 0,
      count: typeof raw.count === "number" ? raw.count : 0,
      updatedAt: typeof raw.updatedAt === "number" ? raw.updatedAt : nowSec()
    };
  }
  return { v: 1, month, goalInCents: goal, totalInCents: 0, count: 0, updatedAt: nowSec() };
}

async function putDonationMonth(env: Env, m: DonationMonth): Promise<void> {
  const key = `${DONATIONS_MONTH_KEY_PREFIX}${m.month}`;
  await env.KV.put(key, JSON.stringify({ ...m, updatedAt: nowSec() }));
}

async function recordDonation(
  env: Env,
  input: { txId: string; reference: string; amountInCents: number; createdAtMs?: number }
): Promise<boolean> {
  const txId = String(input.txId ?? "").trim();
  if (!txId) return false;

  const txKey = `${DONATIONS_TX_KEY_PREFIX}${txId}`;
  const exists = await env.KV.get(txKey);
  if (exists) return false;

  const amount = Math.floor(Number(input.amountInCents ?? 0));
  if (!Number.isFinite(amount) || amount <= 0) return false;

  const month = monthKeyFromMs(typeof input.createdAtMs === "number" ? input.createdAtMs : Date.now());
  const current = await getDonationMonth(env, month);

  // Best-effort idempotency: mark tx first.
  await env.KV.put(
    txKey,
    JSON.stringify({ v: 1, month, amountInCents: amount, reference: input.reference, recordedAt: nowSec() }),
    { expirationTtl: 200 * 24 * 60 * 60 }
  );

  const next: DonationMonth = {
    ...current,
    totalInCents: current.totalInCents + amount,
    count: current.count + 1,
    updatedAt: nowSec()
  };
  await putDonationMonth(env, next);
  return true;
}

function formatCop(amountInCents: number): string {
  const pesos = Math.floor(amountInCents / 100);
  const s = String(Math.max(0, pesos));
  const parts: string[] = [];
  for (let i = s.length; i > 0; i -= 3) {
    const start = Math.max(0, i - 3);
    parts.unshift(s.slice(start, i));
  }
  return `$${parts.join(".")}`;
}

function wompiApiBase(env: Env): string {
  const pk = String(env.WOMPI_PUBLIC_KEY ?? "").trim();
  // Wompi env inference: pub_test_... is sandbox.
  if (pk.startsWith("pub_test_")) return "https://sandbox.wompi.co/v1";
  return "https://production.wompi.co/v1";
}

async function sha256Hex(input: string): Promise<string> {
  const bytes = new TextEncoder().encode(String(input ?? ""));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function getByPath(obj: any, path: string): any {
  const parts = String(path ?? "").split(".").filter(Boolean);
  let cur: any = obj;
  for (const p of parts) {
    if (!cur || typeof cur !== "object") return undefined;
    cur = (cur as any)[p];
  }
  return cur;
}

async function handleDonateLanding(request: Request, env: Env): Promise<Response> {
  const origin = publicBaseUrl(env) ?? new URL(request.url).origin;
  const month = monthKeyFromMs(Date.now());
  const stats = await getDonationMonth(env, month);
  const configured = Boolean(String(env.WOMPI_PUBLIC_KEY ?? "").trim() && String(env.WOMPI_INTEGRITY_SECRET ?? "").trim());

  const html = renderDonateHtml({
    origin,
    month,
    goalInCents: stats.goalInCents,
    totalInCents: stats.totalInCents,
    configured
  });
  return htmlResponse(html, 200);
}

async function handleDonateThanks(request: Request, env: Env): Promise<Response> {
  const origin = publicBaseUrl(env) ?? new URL(request.url).origin;
  const url = new URL(request.url);
  const txId = String(url.searchParams.get("id") ?? "").trim();

  let status = "";
  let amountInCents = 0;
  let reference = "";

  if (txId && String(env.WOMPI_PUBLIC_KEY ?? "").trim()) {
    try {
      const res = await fetch(`${wompiApiBase(env)}/transactions/${encodeURIComponent(txId)}`);
      const data = (await res.json().catch(() => null)) as any;
      const tx = data?.data ?? null;
      if (tx) {
        status = String(tx.status ?? "");
        amountInCents = Number(tx.amount_in_cents ?? tx.amountInCents ?? 0) || 0;
        reference = String(tx.reference ?? "");
        const createdAt = Date.parse(String(tx.created_at ?? tx.createdAt ?? ""));

        const prefix = donationReferencePrefix(env);
        if (status === "APPROVED" && reference.startsWith(prefix)) {
          await recordDonation(env, { txId, reference, amountInCents, createdAtMs: Number.isFinite(createdAt) ? createdAt : undefined });
        }
      }
    } catch {
      // ignore
    }
  }

  const html = renderThanksHtml({ origin, txId, status, amountInCents });
  return htmlResponse(html, 200);
}

async function handleDonationStats(_request: Request, env: Env): Promise<Response> {
  const month = monthKeyFromMs(Date.now());
  const stats = await getDonationMonth(env, month);
  return jsonResponse(
    {
      ok: true,
      month,
      goalInCents: stats.goalInCents,
      totalInCents: stats.totalInCents,
      goalCop: Math.floor(stats.goalInCents / 100),
      totalCop: Math.floor(stats.totalInCents / 100)
    },
    200
  );
}

async function handleDonationCheckout(request: Request, env: Env): Promise<Response> {
  const pk = String(env.WOMPI_PUBLIC_KEY ?? "").trim();
  const integrity = String(env.WOMPI_INTEGRITY_SECRET ?? "").trim();
  if (!pk || !integrity) return jsonResponse({ ok: false, error: "wompi_not_configured" }, 500);

  const url = new URL(request.url);
  const amountCop = parseInt(String(url.searchParams.get("amount") ?? ""), 10);
  if (!Number.isFinite(amountCop)) return jsonResponse({ ok: false, error: "invalid_amount" }, 400);
  if (amountCop < 1000) return jsonResponse({ ok: false, error: "min_1000" }, 400);
  if (amountCop > 5_000_000) return jsonResponse({ ok: false, error: "max_5000000" }, 400);

  const amountInCents = amountCop * 100;
  const currency = "COP";

  const month = monthKeyFromMs(Date.now()).replace("-", "");
  const prefix = donationReferencePrefix(env);
  const reference = `${prefix}_${month}_${randomTokenHex(6)}`;

  const signature = await sha256Hex(`${reference}${amountInCents}${currency}${integrity}`);
  const origin = publicBaseUrl(env) ?? new URL(request.url).origin;
  const redirectUrl = `${origin}/donar/gracias`;

  return jsonResponse({ ok: true, publicKey: pk, currency, amountInCents, reference, signature, redirectUrl }, 200);
}

async function handleWompiWebhook(request: Request, env: Env): Promise<Response> {
  const secret = String(env.WOMPI_EVENTS_SECRET ?? "").trim();
  if (!secret) return new Response("Missing WOMPI_EVENTS_SECRET", { status: 500 });

  const body = (await request.json().catch(() => null)) as any;
  if (!body || typeof body !== "object") return new Response("Bad Request", { status: 400 });

  const signature = body.signature ?? null;
  const props = Array.isArray(signature?.properties) ? (signature.properties as any[]) : [];
  const checksumBody = String(signature?.checksum ?? "");
  const timestamp = String(body.timestamp ?? "");

  const checksumHeader = request.headers.get("X-Event-Checksum");
  const checksumProvided = String(checksumHeader ?? checksumBody).trim().toLowerCase();
  if (!checksumProvided) return new Response("Unauthorized", { status: 401 });

  const dataObj = body.data ?? {};
  const joined = props.map((p) => String(getByPath(dataObj, String(p)) ?? "")).join("") + timestamp + secret;
  const computed = (await sha256Hex(joined)).toLowerCase();
  if (computed !== checksumProvided) return new Response("Unauthorized", { status: 401 });

  const tx = (body.data && body.data.transaction) || null;
  if (!tx) return new Response("ok", { status: 200 });

  const status = String(tx.status ?? "");
  if (status !== "APPROVED") return new Response("ok", { status: 200 });

  const txId = String(tx.id ?? "").trim();
  const reference = String(tx.reference ?? "").trim();
  const amountInCents = Number(tx.amount_in_cents ?? tx.amountInCents ?? 0) || 0;
  const createdAt = Date.parse(String(tx.created_at ?? tx.createdAt ?? ""));

  const prefix = donationReferencePrefix(env);
  if (!reference.startsWith(prefix)) return new Response("ok", { status: 200 });

  await recordDonation(env, { txId, reference, amountInCents, createdAtMs: Number.isFinite(createdAt) ? createdAt : undefined });
  return new Response("ok", { status: 200 });
}

function renderDonateHtml(input: {
  origin: string;
  month: string;
  goalInCents: number;
  totalInCents: number;
  configured: boolean;
}): string {
  const goal = input.goalInCents;
  const total = input.totalInCents;
  const pct = goal > 0 ? Math.min(100, Math.floor((total / goal) * 100)) : 0;
  const faltan = Math.max(0, goal - total);

  const statusLine = input.configured
    ? "Pagos seguros con Wompi. No guardamos datos de tarjetas."
    : "Donaciones no configuradas todavia. Falta WOMPI_PUBLIC_KEY y WOMPI_INTEGRITY_SECRET.";

  return [
    "<!doctype html>",
    "<html lang=\"es\">",
    "<head>",
    "<meta charset=\"utf-8\"/>",
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>",
    "<meta name=\"robots\" content=\"noindex\"/>",
    "<title>Donar | Radar-X</title>",
    "<style>",
    ":root{--bg:#070a12;--card:#0d1323;--txt:#f2f6ff;--mut:#b6c0dd;--line:rgba(255,255,255,.08);--acc:#25c38b;--warn:#ffcc66;}",
    "body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial;background:radial-gradient(1000px 600px at 10% 10%,rgba(37,195,139,.18),transparent 60%),radial-gradient(900px 600px at 85% 35%,rgba(255,204,102,.14),transparent 60%),var(--bg);color:var(--txt);}",
    ".wrap{max-width:980px;margin:0 auto;padding:28px 16px 42px;}",
    ".grid{display:grid;grid-template-columns:1.2fr .8fr;gap:16px;align-items:start}",
    "@media(max-width:860px){.grid{grid-template-columns:1fr}}",
    ".card{background:linear-gradient(180deg,rgba(255,255,255,.07),rgba(255,255,255,.03));border:1px solid var(--line);border-radius:18px;padding:18px 18px 20px;box-shadow:0 14px 45px rgba(0,0,0,.38);}",
    "h1{font-size:26px;margin:0 0 6px;letter-spacing:.3px}",
    "p{margin:0 0 10px;color:var(--mut);line-height:1.5}",
    ".bar{height:14px;background:rgba(255,255,255,.06);border:1px solid var(--line);border-radius:999px;overflow:hidden}",
    ".bar > div{height:100%;width:0;background:linear-gradient(90deg,var(--acc),#4ae3ff);border-radius:999px}",
    ".nums{display:flex;gap:10px;flex-wrap:wrap;margin-top:10px;color:var(--mut);font-size:13px}",
    ".pill{border:1px solid var(--line);border-radius:999px;padding:6px 10px;background:rgba(255,255,255,.03)}",
    ".btns{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:12px}",
    "@media(max-width:420px){.btns{grid-template-columns:1fr}}",
    "button{appearance:none;border:1px solid rgba(37,195,139,.35);background:rgba(37,195,139,.12);color:var(--txt);border-radius:14px;padding:12px 12px;font-weight:700;cursor:pointer}",
    "button:hover{background:rgba(37,195,139,.18)}",
    "button.secondary{border-color:var(--line);background:rgba(255,255,255,.04)}",
    ".row{display:flex;gap:10px;align-items:center;margin-top:12px}",
    "input{flex:1;border:1px solid var(--line);background:rgba(0,0,0,.18);color:var(--txt);border-radius:12px;padding:12px 12px;font-weight:650}",
    "small{display:block;color:var(--mut);margin-top:10px;line-height:1.4}",
    ".status{margin-top:10px;color:#d7e0ff;font-size:13px}",
    "a.link{color:#cfe3ff}",
    "</style>",
    "</head>",
    "<body>",
    "<div class=\"wrap\">",
    "<div class=\"grid\">",
    "<div class=\"card\">",
    "<h1>Apoye Radar-X</h1>",
    "<p>Esto es una vaquita mensual para pagar servidores y mantener el bot funcionando sin caidas.</p>",
    `<div class=\"bar\" aria-label=\"progreso\"><div id=\"bar\" style=\"width:${pct}%\"></div></div>`,
    `<div class=\"nums\"><span class=\"pill\">Meta: <b>${formatCop(goal)}</b></span><span class=\"pill\">Vamos: <b>${formatCop(total)}</b></span><span class=\"pill\">Faltan: <b>${formatCop(faltan)}</b></span><span class=\"pill\">Mes: <b>${escapeHtml(input.month)}</b></span></div>`,
    "<div class=\"btns\">",
    "<button type=\"button\" data-amt=\"1000\">Donar $1.000</button>",
    "<button type=\"button\" data-amt=\"2000\">Donar $2.000</button>",
    "<button type=\"button\" data-amt=\"5000\">Donar $5.000</button>",
    "<button type=\"button\" data-amt=\"10000\">Donar $10.000</button>",
    "</div>",
    "<div class=\"row\">",
    "<input id=\"custom\" inputmode=\"numeric\" placeholder=\"Otro monto (COP)\"/>",
    "<button id=\"go\" type=\"button\" class=\"secondary\">Donar</button>",
    "</div>",
    `<div class=\"status\" id=\"status\">${escapeHtml(statusLine)}</div>`,
    "<small>Tip: si quiere apoyar cada mes, vuelva a esta pagina cuando le llegue el recordatorio en el bot.</small>",
    "</div>",
    "<div class=\"card\">",
    "<h1>Transparencia</h1>",
    "<p>La meta mensual se reinicia cada mes. Los pagos se procesan con Wompi y se reflejan cuando quedan <b>APPROVED</b>.</p>",
    "<p>No guardamos datos de tarjetas, todo eso lo maneja Wompi.</p>",
    `<p><a class=\"link\" href=\"${escapeHtml(input.origin)}\">Home</a></p>`,
    "</div>",
    "</div>",
    "</div>",
    "<script src=\"https://checkout.wompi.co/widget.js\"></script>",
    "<script>",
    "(function(){",
    "var configured=" + (input.configured ? "true" : "false") + ";",
    "var statusEl=document.getElementById('status');",
    "function parseCop(v){v=String(v||'').replace(/[^0-9]/g,'');return v?parseInt(v,10):0;}",
    "async function start(amountCop){",
    "  if(!configured){statusEl.textContent='Donaciones no configuradas aun.';return;}",
    "  statusEl.textContent='Abriendo Wompi...';",
    "  var res=await fetch('/api/donations/checkout?amount='+encodeURIComponent(amountCop));",
    "  var data=await res.json().catch(function(){return null;});",
    "  if(!data||data.ok!==true){statusEl.textContent='No pude iniciar el pago. Intente otra vez.';return;}",
    "  var checkout=new WidgetCheckout({",
    "    currency:data.currency,",
    "    amountInCents:data.amountInCents,",
    "    reference:data.reference,",
    "    publicKey:data.publicKey,",
    "    signature:{ integrity: data.signature },",
    "    redirectUrl:data.redirectUrl",
    "  });",
    "  checkout.open(function(){ statusEl.textContent='Listo. Si el pago queda aprobado, se refleja en unos minutos.'; });",
    "}",
    "document.querySelectorAll('button[data-amt]').forEach(function(b){b.addEventListener('click',function(){start(parseInt(b.getAttribute('data-amt'),10));});});",
    "document.getElementById('go').addEventListener('click',function(){var v=parseCop(document.getElementById('custom').value); if(!v){statusEl.textContent='Ponga un monto.';return;} start(v);});",
    "})();",
    "</script>",
    "</body>",
    "</html>"
  ].join("");
}

function renderThanksHtml(input: { origin: string; txId: string; status: string; amountInCents: number }): string {
  const ok = input.status === "APPROVED";
  const title = ok ? "Gracias por el aporte" : "Gracias";
  const subtitle = input.txId
    ? `Transaccion: ${escapeHtml(input.txId)}${input.status ? " | Estado: " + escapeHtml(input.status) : ""}`
    : "Si su pago queda aprobado, se refleja en unos minutos.";

  const amount = input.amountInCents ? formatCop(input.amountInCents) : "";

  return [
    "<!doctype html>",
    "<html lang=\"es\">",
    "<head>",
    "<meta charset=\"utf-8\"/>",
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>",
    "<meta name=\"robots\" content=\"noindex\"/>",
    "<title>Gracias | Radar-X</title>",
    "<style>",
    ":root{--bg:#070a12;--card:#0d1323;--txt:#f2f6ff;--mut:#b6c0dd;--line:rgba(255,255,255,.08);--acc:#25c38b;}",
    "body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial;background:radial-gradient(1000px 600px at 20% 10%,rgba(37,195,139,.18),transparent 60%),var(--bg);color:var(--txt);}",
    ".wrap{max-width:720px;margin:0 auto;padding:36px 16px 44px;}",
    ".card{background:linear-gradient(180deg,rgba(255,255,255,.07),rgba(255,255,255,.03));border:1px solid var(--line);border-radius:18px;padding:18px 18px 22px;box-shadow:0 14px 45px rgba(0,0,0,.38);}",
    "h1{font-size:26px;margin:0 0 10px}",
    "p{margin:0 0 10px;color:var(--mut);line-height:1.5}",
    "a.btn{display:inline-block;margin-top:10px;background:rgba(37,195,139,.14);border:1px solid rgba(37,195,139,.35);color:var(--txt);text-decoration:none;padding:10px 12px;border-radius:12px;font-weight:700}",
    "</style>",
    "</head>",
    "<body>",
    "<div class=\"wrap\">",
    "<div class=\"card\">",
    `<h1>${escapeHtml(title)}</h1>`,
    amount ? `<p>Monto: <b>${escapeHtml(amount)}</b></p>` : "",
    `<p>${subtitle}</p>`,
    `<a class=\"btn\" href=\"${escapeHtml(input.origin)}/donar\">Volver a donar</a>`,
    "</div>",
    "</div>",
    "</body>",
    "</html>"
  ].join("");
}

async function maybeSendDonationReminders(env: Env, users: UserConfig[], nowMs: number): Promise<void> {
  if (users.length === 0) return;

  const base = publicBaseUrl(env);
  if (!base) return;

  // Colombia is UTC-5, no DST.
  const local = new Date(nowMs - 5 * 60 * 60 * 1000);
  const localHour = local.getUTCHours();
  const localMin = local.getUTCMinutes();

  // Only attempt around 3:00pm local, once per day, and only on a few days.
  if (localHour !== 15 || localMin >= 5) return;

  const y = local.getUTCFullYear();
  const m = String(local.getUTCMonth() + 1).padStart(2, "0");
  const d = String(local.getUTCDate()).padStart(2, "0");
  const dayKey = `${y}-${m}-${d}`;

  const last = await env.KV.get(DONATION_REMINDER_LAST_DAY_KEY);
  if (last === dayKey) return;

  const day = local.getUTCDate();
  if (![1, 10, 20, 28].includes(day)) return;

  if (randomInt(100) >= 55) return;

  const month = `${y}-${m}`;
  const stats = await getDonationMonth(env, month);
  if (stats.totalInCents >= stats.goalInCents) return;

  const faltan = Math.max(0, stats.goalInCents - stats.totalInCents);
  const msg = [
    "Parce, si este bot le sirve, nos ayuda con un aporte?",
    `Meta del mes: ${formatCop(stats.goalInCents)}`,
    `Vamos en: ${formatCop(stats.totalInCents)}`,
    `Faltan: ${formatCop(faltan)}`,
    "",
    `Donar: ${base}/donar`
  ].join("\n");

  for (const u of users) {
    if (!u.notifyDonations) continue;
    await tgSendMessage(env, u.chatId, msg, true);
  }

  await env.KV.put(DONATION_REMINDER_LAST_DAY_KEY, dayKey);
}

function randomInt(maxExclusive: number): number {
  const buf = new Uint32Array(1);
  crypto.getRandomValues(buf);
  return maxExclusive > 0 ? buf[0] % maxExclusive : 0;
}

async function sendDonateInfo(env: Env, chatId: number, edit: boolean, messageId?: number): Promise<void> {
  const base = publicBaseUrl(env);
  if (!base) {
    const text = [
      "Donaciones",
      "",
      "Todavia no esta configurado el link publico del Worker.",
      "Falta poner PUBLIC_BASE_URL en wrangler.toml y redeploy."
    ].join("\n");
    const replyMarkup = { inline_keyboard: [[{ text: "Volver", callback_data: "wiz:main" }]] };
    if (edit && messageId) await tgEditMessage(env, chatId, messageId, text, replyMarkup);
    else await tgSendMessage(env, chatId, text, true, replyMarkup);
    return;
  }

  const month = monthKeyFromMs(Date.now());
  const stats = await getDonationMonth(env, month);
  const faltan = Math.max(0, stats.goalInCents - stats.totalInCents);

  const text = [
    "Donar (Wompi)",
    "",
    `Mes: ${stats.month}`,
    `Meta: ${formatCop(stats.goalInCents)}`,
    `Vamos: ${formatCop(stats.totalInCents)} (${stats.count})`,
    `Faltan: ${formatCop(faltan)}`,
    "",
    `Link: ${base}/donar`
  ].join("\n");

  const replyMarkup = {
    inline_keyboard: [[{ text: "Abrir pagina de donacion", url: `${base}/donar` }], [{ text: "Volver", callback_data: "wiz:main" }]]
  };

  if (edit && messageId) await tgEditMessage(env, chatId, messageId, text, replyMarkup);
  else await tgSendMessage(env, chatId, text, true, replyMarkup);
}
