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

  // Feed scheduling (best-effort). The cron runs every 5 minutes, but each user gets a random delay.
  feedNextAt?: number; // epoch seconds
  feedDelivered?: string[]; // recent event keys to avoid repeats
  feedRecentThreadIds?: number[]; // recent thread ids to avoid immediate repeats
  feedRecentReviewIds?: string[]; // recent review ids to avoid immediate repeats
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

  // Feed pool for "push" alerts (new + old items). Newest-first.
  events?: FeedEvent[];
};

type FeedEvent = {
  v: 1;
  kind: "thread" | "reply" | "seed";
  thread: ThreadItem;
  seenAt: number; // epoch seconds when detected/seeded
};

type ReviewItem = {
  v: 1;
  id: string;
  createdAt: number;
  createdBy: number;
  city: string;
  title: string;
  tags: string[];
  raw: string; // user raw text (usually "comentario general")
  story: string; // formatted/edited narrative (optional)

  // DonColombia-like structured fields (optional)
  subjectName?: string;
  age?: number;
  skinColor?: string;
  height?: string; // ex: "1.65"
  face?: string;
  breasts?: string;
  butt?: string;
  photosMatch?: string; // "Si" | "No"
  physicalScore?: number; // 1-10

  hygiene?: string;
  kisses?: string;
  oralCondition?: string;
  oralQuality?: string;
  analOffer?: string;
  analQuality?: string;
  serviceScore?: number; // 1-10

  serviceTime?: string;
  rateCop?: number;
  place?: string;
  payOnEntry?: string; // "Si" | "No"
  siteQuality?: string;
  finalScore?: number; // avg of physicalScore + serviceScore when available
  comment?: string; // alias of raw, kept for clarity

  phone?: string;
  contactLink?: string;

  // Community signals (best-effort counters, KV is not transactional)
  votesUp?: number;
  votesDown?: number;
  commentsCount?: number;
};

type ReviewDraft = {
  v: 1;
  chatId: number;
  createdAt: number;
  updatedAt: number;
  step:
    | "await_mode"
    | "await_name"
    | "await_age"
    | "pick_skin"
    | "await_height"
    | "pick_face"
    | "pick_breasts"
    | "pick_butt"
    | "pick_photos"
    | "pick_physical_score"
    | "pick_hygiene"
    | "pick_kisses"
    | "pick_oral_condition"
    | "pick_oral_quality"
    | "pick_anal_offer"
    | "pick_anal_quality"
    | "pick_service_score"
    | "pick_service_time"
    | "await_rate"
    | "pick_place"
    | "pick_pay_on_entry"
    | "pick_site_quality"
    | "await_comment"
    | "await_phone"
    | "await_contact_link"
    | "confirm";
  city: string;
  mode?: "quick" | "full";

  subjectName?: string;
  age?: number;
  skinColor?: string;
  height?: string;
  face?: string;
  breasts?: string;
  butt?: string;
  photosMatch?: string;
  physicalScore?: number;

  hygiene?: string;
  kisses?: string;
  oralCondition?: string;
  oralQuality?: string;
  analOffer?: string;
  analQuality?: string;
  serviceScore?: number;

  serviceTime?: string;
  rateCop?: number;
  place?: string;
  payOnEntry?: string;
  siteQuality?: string;
  comment?: string;
  phone?: string;
  contactLink?: string;

  title?: string;
  raw?: string;
  story?: string;
  tags?: string[];
};

type CommentDraft = {
  v: 1;
  chatId: number;
  createdAt: number;
  updatedAt: number;
  step: "await_text";
  reviewId: string;
};

type SearchDraft = {
  v: 1;
  chatId: number;
  createdAt: number;
  updatedAt: number;
  step: "await_query";
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
const COMMENT_DRAFT_KEY_PREFIX = "draft:comment:";
const SEARCH_DRAFT_KEY_PREFIX = "draft:search:";
const REVIEW_VOTE_KEY_PREFIX = "reviewvote:";
const REVIEW_COMMENT_KEY_PREFIX = "reviewcomment:";
const REVIEW_COMMENTS_INDEX_KEY_PREFIX = "reviewcomments:index:";

const DONATIONS_MONTH_KEY_PREFIX = "donations:month:";
const DONATIONS_TX_KEY_PREFIX = "donations:tx:";

const DEFAULT_POLL_THREAD_LIMIT = 35;
const DEFAULT_HOME_THREAD_LIMIT = 20;
const DEFAULT_LATEST_RESULT_LIMIT = 10;

const FORUM_PATHS = ["/foros/", "/forums/"];
const THREAD_PATHS = ["/temas/", "/threads/"];

const FEED_MIN_DELAY_MIN = 5;
const FEED_MAX_DELAY_MIN = 60;
const FEED_EVENTS_MAX = 550;
const FEED_DELIVERED_MAX = 140;
const FEED_RECENT_THREAD_MAX = 70;
const FEED_RECENT_REVIEW_MAX = 70;
const FEED_MAX_SENDS_PER_RUN = 18;

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

    if (request.method === "GET" && url.pathname === "/leer") {
      return handleReadPage(request, env);
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

  const eventsRaw = (s as any).events;
  if (Array.isArray(eventsRaw)) {
    const out: FeedEvent[] = [];
    for (const e of eventsRaw) {
      if (!e || typeof e !== "object") continue;
      const kind = String((e as any).kind ?? "");
      if (kind !== "thread" && kind !== "reply" && kind !== "seed") continue;
      const t = (e as any).thread;
      if (!t || typeof t !== "object") continue;
      const id = Number((t as any).id);
      const title = String((t as any).title ?? "");
      const url = String((t as any).url ?? "");
      if (!Number.isFinite(id) || id <= 0 || !url) continue;

      const updatedAtRaw = (t as any).updatedAt;
      const updatedAt = typeof updatedAtRaw === "number" && Number.isFinite(updatedAtRaw) ? updatedAtRaw : undefined;

      const source = (t as any).source === "forum" ? "forum" : "home";

      const forum = (t as any).forum;
      const forumOk =
        forum &&
        typeof forum === "object" &&
        typeof (forum as any).id === "number" &&
        typeof (forum as any).name === "string" &&
        typeof (forum as any).url === "string";

      out.push({
        v: 1,
        kind: kind as any,
        thread: {
          id,
          title: title || `Tema ${id}`,
          url,
          forum: forumOk ? (forum as ForumRef) : undefined,
          prefix: typeof (t as any).prefix === "string" ? (t as any).prefix : undefined,
          updatedAt,
          source
        },
        seenAt: typeof (e as any).seenAt === "number" ? (e as any).seenAt : 0
      });
    }
    if (out.length) next.events = out.slice(0, FEED_EVENTS_MAX);
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

  // User feed (push) notifications with random per-user delay (5-60 min).
  await runUserFeedNotifications(env, state, users, scheduledTimeMs);

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
    // Seed a few old items so user feeds can still send "cositas antiguas".
    addFeedEvents(
      state,
      threads.slice(0, 12).map((t) => ({ v: 1 as const, kind: "seed" as const, thread: t, seenAt: nowSec }))
    );
    return true;
  }

  // Migration/backfill: if the feed pool doesn't have items for this forum yet, add a few seeds.
  const needSeed =
    !Array.isArray(state.events) ||
    state.events.length === 0 ||
    !state.events.some((e) => e && e.thread && e.thread.forum && e.thread.forum.id === forum.id);
  const seeded = needSeed
    ? addFeedEvents(
        state,
        threads.slice(0, 12).map((t) => ({ v: 1 as const, kind: "seed" as const, thread: t, seenAt: t.updatedAt ?? nowSec }))
      )
    : false;

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

  const eventsAdded = addFeedEvents(
    state,
    [
      ...newThreadEvents.map((t) => ({ v: 1 as const, kind: "thread" as const, thread: t, seenAt: nowSec })),
      ...updatedThreadEvents.map((t) => ({ v: 1 as const, kind: "reply" as const, thread: t, seenAt: nowSec }))
    ]
  );

  return changed || eventsAdded || seeded;
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
    // Seed a few old items so user feeds can still send "cositas antiguas".
    addFeedEvents(
      state,
      threads.slice(0, 12).map((t) => ({ v: 1 as const, kind: "seed" as const, thread: t, seenAt: nowSec }))
    );
    return true;
  }

  // Migration/backfill: if the feed pool doesn't have any home items yet, add a few seeds.
  const needSeed =
    !Array.isArray(state.events) ||
    state.events.length === 0 ||
    !state.events.some((e) => e && e.thread && e.thread.source === "home");
  const seeded = needSeed
    ? addFeedEvents(
        state,
        threads.slice(0, 12).map((t) => ({ v: 1 as const, kind: "seed" as const, thread: t, seenAt: t.updatedAt ?? nowSec }))
      )
    : false;

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

  const eventsAdded = addFeedEvents(
    state,
    [
      ...newThreadEvents.map((t) => ({ v: 1 as const, kind: "thread" as const, thread: t, seenAt: nowSec })),
      ...updatedThreadEvents.map((t) => ({ v: 1 as const, kind: "reply" as const, thread: t, seenAt: nowSec }))
    ]
  );

  return changed || eventsAdded || seeded;
}

function feedEventKey(e: FeedEvent): string {
  // Key ignores kind so "seed" does not duplicate a real event of the same thread+timestamp.
  const updated = e.thread.updatedAt ?? 0;
  return `${e.thread.id}:${updated}`;
}

function feedKindPriority(kind: FeedEvent["kind"]): number {
  if (kind === "reply") return 2;
  if (kind === "thread") return 1;
  return 0; // seed
}

function addFeedEvents(state: PollState, incoming: FeedEvent[]): boolean {
  if (!incoming || incoming.length === 0) return false;
  const existing = Array.isArray(state.events) ? state.events : [];

  // Index existing by key for upgrades (seed -> thread/reply).
  const idxByKey = new Map<string, number>();
  for (let i = 0; i < existing.length; i++) idxByKey.set(feedEventKey(existing[i]), i);

  const toPrepend: FeedEvent[] = [];
  let changed = false;

  for (const e of incoming) {
    if (!e || typeof e !== "object") continue;
    if (!e.thread || typeof e.thread !== "object") continue;
    if (!Number.isFinite(e.thread.id) || e.thread.id <= 0) continue;
    if (!e.thread.url) continue;

    const key = feedEventKey(e);
    const idx = idxByKey.get(key);
    if (idx === undefined) {
      toPrepend.push(e);
      idxByKey.set(key, -1); // prevent duplicates within the same batch
      changed = true;
      continue;
    }

    if (idx < 0) continue; // already added from this batch

    const cur = existing[idx];
    const curPri = feedKindPriority(cur.kind);
    const nextPri = feedKindPriority(e.kind);

    const mergedThread: ThreadItem = {
      ...cur.thread,
      ...e.thread,
      // Keep best-effort optional fields.
      forum: e.thread.forum ?? cur.thread.forum,
      prefix: e.thread.prefix ?? cur.thread.prefix,
      updatedAt: e.thread.updatedAt ?? cur.thread.updatedAt
    };

    const merged: FeedEvent = {
      ...cur,
      kind: nextPri > curPri ? e.kind : cur.kind,
      thread: mergedThread,
      seenAt: Math.max(cur.seenAt ?? 0, e.seenAt ?? 0)
    };

    if (merged.kind !== cur.kind || merged.seenAt !== cur.seenAt || merged.thread.title !== cur.thread.title) {
      existing[idx] = merged;
      changed = true;
    }
  }

  const next = toPrepend.length ? [...toPrepend, ...existing] : existing;
  const pruned = next.slice(0, FEED_EVENTS_MAX);
  state.events = pruned;
  return changed;
}

function hasPositiveFilters(u: UserConfig): boolean {
  return (u.includeKeywords?.length ?? 0) > 0 || (u.forums?.length ?? 0) > 0 || (u.prefixes?.length ?? 0) > 0;
}

function isUserFeedEnabled(u: UserConfig): boolean {
  // If the user has no positive filters, don't spam them with the whole site.
  if (!hasPositiveFilters(u)) return false;
  return Boolean(u.notifyReviews || u.notifyThreads || u.notifyReplies);
}

function userWantsFeedEvent(u: UserConfig, e: FeedEvent): boolean {
  if (e.kind === "reply") return Boolean(u.notifyReplies);
  return Boolean(u.notifyThreads);
}

function randomIntInRange(min: number, max: number): number {
  const a = Math.ceil(min);
  const b = Math.floor(max);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return a;
  if (b <= a) return a;
  const span = b - a + 1;
  const buf = new Uint32Array(1);
  crypto.getRandomValues(buf);
  return a + (buf[0] % span);
}

function scheduleNextFeedAt(nowSec: number): number {
  const mins = randomIntInRange(FEED_MIN_DELAY_MIN, FEED_MAX_DELAY_MIN);
  return nowSec + mins * 60;
}

function scheduleFeedSoonAt(nowSec: number): number {
  const upper = Math.min(FEED_MIN_DELAY_MIN + 10, FEED_MAX_DELAY_MIN);
  const mins = randomIntInRange(FEED_MIN_DELAY_MIN, upper);
  return nowSec + mins * 60;
}

async function runUserFeedNotifications(env: Env, state: PollState, users: UserConfig[], nowMs: number): Promise<void> {
  const now = Math.floor(nowMs / 1000);
  const events = Array.isArray(state.events) ? state.events : [];
  const reviewIdx = await getReviewIndex(env);

  let sent = 0;
  for (const u of users) {
    if (sent >= FEED_MAX_SENDS_PER_RUN) break;
    if (!isUserFeedEnabled(u)) continue;

    const nextAt = typeof u.feedNextAt === "number" ? u.feedNextAt : 0;
    if (!nextAt || nextAt <= 0) {
      // First-time schedule: don't send immediately, keep the 5-60 min rhythm.
      await putUser(env, {
        ...u,
        feedNextAt: scheduleFeedSoonAt(now),
        feedDelivered: u.feedDelivered ?? [],
        feedRecentThreadIds: u.feedRecentThreadIds ?? [],
        feedRecentReviewIds: u.feedRecentReviewIds ?? []
      });
      continue;
    }

    if (nextAt > now) continue;

    const delivered = Array.isArray(u.feedDelivered) ? u.feedDelivered : [];
    const deliveredSet = new Set(delivered);
    const recent = Array.isArray(u.feedRecentThreadIds) ? u.feedRecentThreadIds : [];
    const recentSet = new Set(recent);
    const recentReviews = Array.isArray(u.feedRecentReviewIds) ? u.feedRecentReviewIds : [];
    const recentReviewSet = new Set(recentReviews);

    const nextAt2 = scheduleNextFeedAt(now);

    // 1) Prefer own reviews.
    if (u.notifyReviews && u.includeKeywords.length > 0 && reviewIdx.length > 0) {
      const matches = reviewIdx.filter((e) => matchesUserForReviewEntry(u, e));
      let pickedReview: ReviewIndexEntry | null = null;
      let repeat = false;

      for (const it of matches) {
        const key = `r:${it.id}`;
        if (deliveredSet.has(key)) continue;
        if (recentReviewSet.has(it.id)) continue;
        pickedReview = it;
        break;
      }

      if (!pickedReview && matches.length > 0) {
        const pool = matches.filter((it) => !recentReviewSet.has(it.id));
        const arr = pool.length > 0 ? pool : matches;
        pickedReview = arr[randomIntInRange(0, arr.length - 1)];
        repeat = true;
      }

      if (pickedReview) {
        await sendFeedReviewAlert(env, u.chatId, pickedReview, { isRepeat: repeat });

        const nextDelivered = [...delivered, `r:${pickedReview.id}`];
        const trimmedDelivered = nextDelivered.slice(Math.max(0, nextDelivered.length - FEED_DELIVERED_MAX));

        const nextRecentReviews = [...recentReviews, pickedReview.id];
        const trimmedRecentReviews = nextRecentReviews.slice(Math.max(0, nextRecentReviews.length - FEED_RECENT_REVIEW_MAX));

        await putUser(env, {
          ...u,
          feedNextAt: nextAt2,
          feedDelivered: trimmedDelivered,
          feedRecentThreadIds: recent.slice(-FEED_RECENT_THREAD_MAX),
          feedRecentReviewIds: trimmedRecentReviews
        });
        sent++;
        continue;
      }
    }

    // 2) Fallback: DonColombia feed events.
    if (events.length === 0) {
      await putUser(env, {
        ...u,
        feedNextAt: nextAt2,
        feedDelivered: delivered.slice(-FEED_DELIVERED_MAX),
        feedRecentThreadIds: recent.slice(-FEED_RECENT_THREAD_MAX),
        feedRecentReviewIds: recentReviews.slice(-FEED_RECENT_REVIEW_MAX)
      });
      continue;
    }

    let picked: FeedEvent | null = null;
    let isRepeat = false;

    // Prefer unseen "thread/reply" events first (newest-first).
    for (const e of events) {
      if (e.kind === "seed") continue;
      if (!userWantsFeedEvent(u, e)) continue;
      if (!matchesUserFilters(u, e.thread)) continue;
      const keyBase = feedEventKey(e);
      const key = `t:${keyBase}`;
      if (deliveredSet.has(keyBase) || deliveredSet.has(key)) continue;
      if (recentSet.has(e.thread.id)) continue;
      picked = e;
      break;
    }

    // If there were no real events, allow seeded items.
    if (!picked) {
      for (const e of events) {
        if (e.kind !== "seed") continue;
        if (!userWantsFeedEvent(u, e)) continue;
        if (!matchesUserFilters(u, e.thread)) continue;
        const keyBase = feedEventKey(e);
        const key = `t:${keyBase}`;
        if (deliveredSet.has(keyBase) || deliveredSet.has(key)) continue;
        if (recentSet.has(e.thread.id)) continue;
        picked = e;
        break;
      }
    }

    if (!picked) {
      // No new stuff for this user's filters. Send something older (but avoid immediate repeats).
      const pool: FeedEvent[] = [];
      for (const e of events) {
        if (!userWantsFeedEvent(u, e)) continue;
        if (!matchesUserFilters(u, e.thread)) continue;
        if (recentSet.has(e.thread.id)) continue;
        pool.push(e);
      }
      if (pool.length > 0) {
        picked = pool[randomIntInRange(0, pool.length - 1)];
        isRepeat = true;
      }
    }

    if (!picked) {
      // If the filter is very narrow, allow repeats even if it's the same thread again.
      const pool: FeedEvent[] = [];
      for (const e of events) {
        if (!userWantsFeedEvent(u, e)) continue;
        if (!matchesUserFilters(u, e.thread)) continue;
        pool.push(e);
      }
      if (pool.length > 0) {
        picked = pool[randomIntInRange(0, pool.length - 1)];
        isRepeat = true;
      }
    }

    if (!picked) {
      // Still nothing; just reschedule to avoid tight loops.
      await putUser(env, {
        ...u,
        feedNextAt: nextAt2,
        feedDelivered: delivered.slice(-FEED_DELIVERED_MAX),
        feedRecentThreadIds: recent.slice(-FEED_RECENT_THREAD_MAX),
        feedRecentReviewIds: recentReviews.slice(-FEED_RECENT_REVIEW_MAX)
      });
      continue;
    }

    await sendFeedThreadAlert(env, u.chatId, picked, { isRepeat });

    const nextDelivered = [...delivered, `t:${feedEventKey(picked)}`];
    const trimmedDelivered = nextDelivered.slice(Math.max(0, nextDelivered.length - FEED_DELIVERED_MAX));

    const nextRecent = [...recent, picked.thread.id];
    const trimmedRecent = nextRecent.slice(Math.max(0, nextRecent.length - FEED_RECENT_THREAD_MAX));

    await putUser(env, {
      ...u,
      feedNextAt: nextAt2,
      feedDelivered: trimmedDelivered,
      feedRecentThreadIds: trimmedRecent,
      feedRecentReviewIds: recentReviews.slice(-FEED_RECENT_REVIEW_MAX)
    });
    sent++;
  }
}

async function sendFeedThreadAlert(
  env: Env,
  chatId: number,
  e: FeedEvent,
  opts: { isRepeat: boolean }
): Promise<void> {
  const t = e.thread;

  const label =
    opts.isRepeat || e.kind === "seed" ? "Pa que no se le pase" : e.kind === "reply" ? "Nueva respuesta" : "Nuevo tema";
  const lines: string[] = [];

  lines.push(`Pilas pues: ${label}${t.forum?.name ? ` en ${t.forum.name}` : ""}`);

  const head: string[] = [];
  if (t.prefix) head.push(`[${t.prefix}]`);
  head.push(t.title);
  lines.push(truncate(head.join(" "), 220));

  if (t.updatedAt && t.updatedAt > 0) {
    lines.push(`Fecha: ${formatCoDateTimeFromSec(t.updatedAt)}`);
  }

  // Keep the URL in the message so Telegram can show previews if the photo path fails.
  lines.push(t.url);
  const text = lines.join("\n");

  const base = publicBaseUrl(env);
  const row = base
    ? [{ text: "Abrir", url: t.url }, { text: "Leer", url: `${base}/leer?u=${encodeURIComponent(t.url)}` }]
    : [{ text: "Abrir", url: t.url }];
  const keyboard = { inline_keyboard: [row] };

  const html = await fetchTextWithTimeout(t.url, 6500);
  const imageUrl = html ? extractBestImageUrl(html, t.url) : null;

  if (imageUrl) {
    const caption = truncate(text, 900);
    const ok = await tgSendPhoto(env, chatId, imageUrl, caption, keyboard);
    if (ok) return;
  }

  // Web preview ON (if the page has og:image, Telegram usually shows it).
  await tgSendMessage(env, chatId, text, false, keyboard);
}

async function sendFeedReviewAlert(
  env: Env,
  chatId: number,
  it: ReviewIndexEntry,
  opts: { isRepeat: boolean }
): Promise<void> {
  const label = opts.isRepeat ? "Pa que no se le pase" : "Resena";
  const title = truncate(it.title || it.subjectName || it.id, 220);

  const lines: string[] = [];
  lines.push(`Pilas pues: ${label}`);
  lines.push(`[${it.city}] ${title}`);
  if (it.createdAt) lines.push(`Fecha: ${formatCoDateTimeFromSec(it.createdAt)}`);

  const up = typeof it.votesUp === "number" ? it.votesUp : 0;
  const down = typeof it.votesDown === "number" ? it.votesDown : 0;
  const cc = typeof it.commentsCount === "number" ? it.commentsCount : 0;
  if (up || down || cc) lines.push(`Votos: +${up} / -${down} | Opiniones: ${cc}`);

  await tgSendMessage(env, chatId, lines.join("\n"), true, buildReviewInlineKeyboard(env, it.id));
}

function matchesUserFilters(u: UserConfig, t: ThreadItem): boolean {
  const forum = t.forum;
  if (u.forums.length > 0) {
    if (!forum) return false;
    const ok = u.forums.some((f) => f.id === forum.id);
    if (!ok) return false;
  }

  const text = normalize(`${t.title} ${t.prefix ?? ""} ${forum?.name ?? ""}`);

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
    parts.push(`Fecha: ${formatCoDateTimeFromSec(t.updatedAt)}`);
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
    await deleteCommentDraft(env, chatId);
    await deleteSearchDraft(env, chatId);
    const user = await ensureUser(env, chatId);
    await tgSendMessage(env, chatId, "Listo, cancele eso.", true);
    await sendMainMenu(env, user, chatId, false);
    return;
  }

  const searchDraft = await getSearchDraft(env, chatId);
  if (searchDraft && !cmd) {
    await handleSearchDraftText(env, chatId, searchDraft, text);
    return;
  }

  const commentDraft = await getCommentDraft(env, chatId);
  if (commentDraft && !cmd) {
    await handleCommentDraftText(env, chatId, commentDraft, text);
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
        const next = bumpFeedAfterFilterChange(addForum(user, forum));
        await putUser(env, next);
        await tgSendMessage(env, chatId, `Listo pues, quedo suscrito a: ${forum.name}\n\nYa quedo prendido. Le mando cositas cada ratico.`, true);
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
    const next = bumpFeedAfterFilterChange(addForum(user, forum));
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Listo pues, quedo suscrito a: ${forum.name}\n\nYa quedo prendido. Le mando cositas cada ratico.`, true);
    return;
  }

  if (name === "rmforum" || name === "rmforo") {
    const forumId = parseInt(args.trim(), 10);
    if (!Number.isFinite(forumId)) {
      await tgSendMessage(env, chatId, "Parce, asi es: /rmforum <id>  (ej: /rmforum 18)", true);
      return;
    }
    const next = bumpFeedAfterFilterChange({ ...user, forums: user.forums.filter((f) => f.id !== forumId), updatedAt: nowSec() });
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Listo, quite el foro: ${forumId}`, true);
    return;
  }

  if (name === "addkw") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Listo, pero digame la palabra o frase. Ej: /addkw bogota", true);
      return;
    }
    const next = bumpFeedAfterFilterChange(addUnique(user, "includeKeywords", kw));
    await putUser(env, next);
    await tgSendMessage(env, chatId, `De una, agregue: ${kw}`, true);
    return;
  }

  if (name === "rmkw") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Listo, pero cual quitamos? Ej: /rmkw bogota", true);
      return;
    }
    const next = bumpFeedAfterFilterChange(removeValue(user, "includeKeywords", kw));
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Listo, quite: ${kw}`, true);
    return;
  }

  if (name === "addnot") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Listo, que quiere excluir? Ej: /addnot politica", true);
      return;
    }
    const next = bumpFeedAfterFilterChange(addUnique(user, "excludeKeywords", kw));
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Hecho, ahora excluyo: ${kw}`, true);
    return;
  }

  if (name === "rmnot") {
    const kw = args.trim();
    if (!kw) {
      await tgSendMessage(env, chatId, "Listo, cual exclusion quitamos? Ej: /rmnot politica", true);
      return;
    }
    const next = bumpFeedAfterFilterChange(removeValue(user, "excludeKeywords", kw));
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Listo, ya no excluyo: ${kw}`, true);
    return;
  }

  if (name === "addprefix") {
    const p = args.trim();
    if (!p) {
      await tgSendMessage(env, chatId, "Listo, pero cual prefijo? Ej: /addprefix Resena", true);
      return;
    }
    const next = bumpFeedAfterFilterChange(addUnique(user, "prefixes", p));
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Hecho, prefijo: ${p}`, true);
    return;
  }

  if (name === "rmprefix") {
    const p = args.trim();
    if (!p) {
      await tgSendMessage(env, chatId, "Listo, cual prefijo quitamos? Ej: /rmprefix Resena", true);
      return;
    }
    const next = bumpFeedAfterFilterChange(removeValue(user, "prefixes", p));
    await putUser(env, next);
    await tgSendMessage(env, chatId, `Listo, quite el prefijo: ${p}`, true);
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

    next = bumpFeedAfterFilterChange(next);
    await putUser(env, next);
    if (messageId) await sendCitiesPage(env, next, chatId, Number.isFinite(page) ? page : 0, true, messageId);
    await tgAnswerCallback(env, cb.id, "De una");
    return;
  }

  if (data.startsWith("rvw:")) {
    const parts = data.split(":");
    const action = parts[1] ?? "";

    if (action === "citypage") {
      const page = parseInt(parts[2] ?? "0", 10);
      if (messageId) await sendReviewCityPage(env, chatId, Number.isFinite(page) ? page : 0, true, messageId);
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    if (action === "latest") {
      await tgAnswerCallback(env, cb.id, "Ya casi...");
      await sendLatestReviews(env, user, chatId, 8);
      return;
    }

    if (action === "search") {
      await putSearchDraft(env, { v: 1, chatId, createdAt: nowSec(), updatedAt: nowSec(), step: "await_query" });
      await tgAnswerCallback(env, cb.id, "Listo");
      await tgSendMessage(
        env,
        chatId,
        ["Listo pues. Mande lo que tenga:", "- Nombre/apodo", "- Telefono", "- Link", "- ID de la resena", "", "Si se arrepintio: /cancel"].join("\n"),
        true,
        { inline_keyboard: [[{ text: "Cancelar", callback_data: "rvw:cancel" }]] }
      );
      return;
    }

    if (action === "new") {
      if (messageId) await sendReviewCityPage(env, chatId, 0, true, messageId);
      else await sendReviewCityPage(env, chatId, 0, false);
      await tgAnswerCallback(env, cb.id, "");
      return;
    }

    if (action === "city") {
      const cityId = parts[2] ?? "";
      const page = parseInt(parts[3] ?? "0", 10);
      const city = CITY_PRESETS.find((c) => c.id === cityId);
      if (!city) {
        await tgAnswerCallback(env, cb.id, "");
        return;
      }

      const draft: ReviewDraft = {
        v: 1,
        chatId,
        createdAt: nowSec(),
        updatedAt: nowSec(),
        step: "await_mode",
        city: city.name,
        tags: [city.name]
      };
      await putReviewDraft(env, draft);

      await tgAnswerCallback(env, cb.id, "Listo");
      await sendReviewDraftPrompt(env, chatId, draft);

      if (messageId) await sendReviewCityPage(env, chatId, Number.isFinite(page) ? page : 0, true, messageId);
      return;
    }

    if (action === "mode") {
      const mode = parts[2] ?? "";
      if (mode !== "quick" && mode !== "full") {
        await tgAnswerCallback(env, cb.id, "");
        return;
      }
      const draft = await getReviewDraft(env, chatId);
      if (!draft) {
        await tgAnswerCallback(env, cb.id, "Se vencio");
        return;
      }
      const next: ReviewDraft = { ...draft, mode: mode as any, step: "await_name", updatedAt: nowSec() };
      await putReviewDraft(env, next);
      await tgAnswerCallback(env, cb.id, "Dale");
      await sendReviewDraftPrompt(env, chatId, next);
      return;
    }

    if (action === "set") {
      const key = parts[2] ?? "";
      const val = parts.slice(3).join(":");
      const draft = await getReviewDraft(env, chatId);
      if (!draft) {
        await tgAnswerCallback(env, cb.id, "Se vencio");
        return;
      }
      await tgAnswerCallback(env, cb.id, "");
      await applyReviewDraftPick(env, chatId, draft, { key, val });
      return;
    }

    if (action === "num") {
      const key = parts[2] ?? "";
      const n = parseInt(parts[3] ?? "", 10);
      const draft = await getReviewDraft(env, chatId);
      if (!draft) {
        await tgAnswerCallback(env, cb.id, "Se vencio");
        return;
      }
      if (!Number.isFinite(n)) {
        await tgAnswerCallback(env, cb.id, "");
        return;
      }
      await tgAnswerCallback(env, cb.id, "");
      await applyReviewDraftNumber(env, chatId, draft, { key, n });
      return;
    }

    if (action === "skip") {
      const draft = await getReviewDraft(env, chatId);
      if (!draft) {
        await tgAnswerCallback(env, cb.id, "Se vencio");
        return;
      }
      await tgAnswerCallback(env, cb.id, "");
      await skipReviewDraftStep(env, chatId, draft);
      return;
    }

    if (action === "publish") {
      const draft = await getReviewDraft(env, chatId);
      if (!draft || draft.step !== "confirm") {
        await tgAnswerCallback(env, cb.id, "Se vencio");
        return;
      }
      await tgAnswerCallback(env, cb.id, "Publicando...");
      await publishReviewFromDraft(env, chatId, draft);
      return;
    }

    if (action === "restart") {
      await deleteReviewDraft(env, chatId);
      await tgAnswerCallback(env, cb.id, "Listo");
      if (messageId) await sendReviewCityPage(env, chatId, 0, true, messageId);
      else await sendReviewCityPage(env, chatId, 0, false);
      return;
    }

    if (action === "read") {
      const id = parts[2] ?? "";
      if (!id) {
        await tgAnswerCallback(env, cb.id, "");
        return;
      }
      await tgAnswerCallback(env, cb.id, "Leyendo...");
      await sendReviewRead(env, chatId, id);
      return;
    }

    if (action === "vote") {
      const dir = parts[2] ?? "";
      const id = parts[3] ?? "";
      if ((dir !== "up" && dir !== "down") || !id) {
        await tgAnswerCallback(env, cb.id, "");
        return;
      }
      const out = await applyReviewVote(env, id, chatId, dir as any);
      const label = out ? `Listo. Votos: +${out.votesUp} / -${out.votesDown}` : "Uy, no pude votar en esa resena.";
      await tgAnswerCallback(env, cb.id, label);
      return;
    }

    if (action === "comment") {
      const id = parts[2] ?? "";
      if (!id) {
        await tgAnswerCallback(env, cb.id, "");
        return;
      }
      const review = await getReview(env, id);
      if (!review) {
        await tgAnswerCallback(env, cb.id, "No la encontre");
        return;
      }
      await putCommentDraft(env, { v: 1, chatId, createdAt: nowSec(), updatedAt: nowSec(), step: "await_text", reviewId: id });
      await tgAnswerCallback(env, cb.id, "Dale");
      await tgSendMessage(
        env,
        chatId,
        ["Listo. Mande su opinion en 1 mensaje.", "", `Resena: [${review.city}] ${review.title}`, "", "Para cancelar: /cancel"].join("\n"),
        true,
        { inline_keyboard: [[{ text: "Cancelar", callback_data: "rvw:cancel" }]] }
      );
      return;
    }

    if (action === "cancel") {
      await deleteReviewDraft(env, chatId);
      await deleteCommentDraft(env, chatId);
      await deleteSearchDraft(env, chatId);
      await tgAnswerCallback(env, cb.id, "Listo");
      if (messageId) await sendReviewsMenu(env, user, chatId, true, messageId);
      else await sendReviewsMenu(env, user, chatId, false);
      return;
    }
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

    if (action === "reset") {
      const next: UserConfig = bumpFeedAfterFilterChange({
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
      });
      await putUser(env, next);
      if (messageId) await sendMainMenu(env, next, chatId, true, messageId);
      await tgAnswerCallback(env, cb.id, "Listo");
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
    const next = bumpFeedAfterFilterChange(addForum(user, forum));
    await putUser(env, next);
    await sendForumsPage(env, next, chatId, 0, true, cb.message?.message_id);
    await tgAnswerCallback(env, cb.id, "Listo, suscrito");
    return;
  }

  if (data.startsWith("unsub_forum:")) {
    const forumId = parseInt(data.split(":")[1] ?? "", 10);
    const next = bumpFeedAfterFilterChange({ ...user, forums: user.forums.filter((f) => f.id !== forumId), updatedAt: nowSec() });
    await putUser(env, next);
    await sendForumsPage(env, next, chatId, 0, true, cb.message?.message_id);
    await tgAnswerCallback(env, cb.id, "Listo, quitado");
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

  const imageUrl = extractBestImageUrl(html, url.toString());
  if (imageUrl) {
    const ok = await tgSendPhoto(env, chatId, imageUrl, truncate(title, 900));
    if (!ok) {
      // As a fallback, allow Telegram to preview the image URL.
      await tgSendMessage(env, chatId, `Imagen: ${imageUrl}`, false);
    }
  }

  const ogDesc = extractMetaProperty(html, "og:description");
  const body = extractFirstPostText(html);
  const content = preferLonger(ogDesc, body);

  const text = content
    ? `${title}\n\n${content}\n\n${url.toString()}`
    : `${title}\n\n${url.toString()}`;

  await tgSendLongMessage(env, chatId, text, true);
}

async function handleReadPage(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const targetRaw = String(url.searchParams.get("u") ?? "").trim();
  if (!targetRaw) return htmlResponse("Bad Request", 400);

  let target: URL;
  try {
    target = new URL(targetRaw, env.SITE_BASE_URL);
  } catch {
    return htmlResponse("Bad Request", 400);
  }

  const base = new URL(env.SITE_BASE_URL);
  if (target.hostname !== base.hostname) return htmlResponse("Forbidden", 403);

  const html = await fetchTextWithTimeout(target.toString(), 9000);
  if (!html) return htmlResponse("No pude abrir esa pagina.", 502);

  const title =
    extractMetaProperty(html, "og:title") ??
    extractTitleTag(html) ??
    target.toString();

  const ogDesc = extractMetaProperty(html, "og:description");
  const body = extractFirstPostText(html);
  const content = preferLonger(ogDesc, body) || "";
  const imageUrl = extractBestImageUrl(html, target.toString());

  const safeTitle = escapeHtml(title);
  const safeContent = escapeHtml(content);
  const safeTarget = escapeHtml(target.toString());

  const imgHtml = imageUrl
    ? `<div class="imgWrap"><img src="${escapeHtml(imageUrl)}" alt="imagen"/></div>`
    : "";

  const page = `<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <meta name="robots" content="noindex"/>
    <title>${safeTitle}</title>
    <style>
      :root { --bg:#070a12; --card:#0d1323; --txt:#f2f6ff; --mut:#b6c0dd; --line:rgba(255,255,255,.10); --acc:#25c38b; }
      body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; background: radial-gradient(1000px 600px at 18% 10%, rgba(37,195,139,.15), transparent 60%), var(--bg); color: var(--txt); }
      main { max-width: 920px; margin: 0 auto; padding: 26px 16px 44px; }
      .card { background: linear-gradient(180deg, rgba(255,255,255,.07), rgba(255,255,255,.03)); border: 1px solid var(--line); border-radius: 18px; padding: 18px 16px 20px; box-shadow: 0 14px 45px rgba(0,0,0,.35); }
      h1 { margin:0 0 10px; font-size: 22px; letter-spacing: .2px; }
      .meta { color: var(--mut); margin: 0 0 14px; font-size: 13px; }
      .story { white-space: pre-wrap; line-height: 1.55; color: #e9efff; }
      a { color: #cfe3ff; }
      .imgWrap { margin: 0 0 14px; border-radius: 14px; overflow: hidden; border: 1px solid var(--line); background: rgba(0,0,0,.15); }
      .imgWrap img { width: 100%; height: auto; display:block; }
      .btn { display:inline-block; margin-top: 12px; background: rgba(37,195,139,.14); border: 1px solid rgba(37,195,139,.35); color: var(--txt); text-decoration:none; padding: 10px 12px; border-radius: 12px; font-weight: 800; }
    </style>
  </head>
  <body>
    <main>
      <div class="card">
        <h1>${safeTitle}</h1>
        <p class="meta">Fuente: <a href="${safeTarget}">${safeTarget}</a></p>
        ${imgHtml}
        <div class="story">${safeContent || "(sin texto)"}</div>
        <a class="btn" href="${safeTarget}">Abrir original</a>
      </div>
    </main>
  </body>
</html>`;

  return htmlResponse(page, 200);
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

function extractMetaName(html: string, name: string): string | null {
  const n = escapeRe(name);
  const re1 = new RegExp(`<meta\\b[^>]*name=["']${n}["'][^>]*content=["']([^"']+)["'][^>]*>`, "i");
  const m1 = re1.exec(html);
  if (m1?.[1]) return decodeHtmlEntities(m1[1]).trim();

  const re2 = new RegExp(`<meta\\b[^>]*content=["']([^"']+)["'][^>]*name=["']${n}["'][^>]*>`, "i");
  const m2 = re2.exec(html);
  if (m2?.[1]) return decodeHtmlEntities(m2[1]).trim();

  return null;
}

function toAbsoluteUrl(raw: string, baseUrl: string): string | null {
  const s = String(raw ?? "").trim();
  if (!s) return null;
  try {
    const u = new URL(s, baseUrl);
    if (u.protocol !== "http:" && u.protocol !== "https:") return null;
    return u.toString();
  } catch {
    return null;
  }
}

function extractFirstPostHtml(html: string): string | null {
  const m = /class="bbWrapper"[^>]*>([\s\S]*?)<\/div>/i.exec(html);
  return m?.[1] ? String(m[1]) : null;
}

function extractFirstImageUrlFromHtml(fragmentHtml: string, baseUrl: string): string | null {
  const frag = String(fragmentHtml ?? "");
  if (!frag) return null;

  const imgRe = /<img\b[^>]*>/gi;
  let m: RegExpExecArray | null;
  while ((m = imgRe.exec(frag)) !== null) {
    const tag = m[0] ?? "";
    const getAttr = (name: string): string | null => {
      const re = new RegExp(`${escapeRe(name)}\\s*=\\s*["']([^"']+)["']`, "i");
      const mm = re.exec(tag);
      return mm?.[1] ? decodeHtmlEntities(mm[1]).trim() : null;
    };

    const src =
      getAttr("data-src") ||
      getAttr("data-lazy-src") ||
      getAttr("data-original") ||
      getAttr("src");

    if (!src) continue;
    if (src.startsWith("data:")) continue;
    const abs = toAbsoluteUrl(src, baseUrl);
    if (abs) return abs;
  }

  return null;
}

function extractBestImageUrl(html: string, baseUrl: string): string | null {
  const og = extractMetaProperty(html, "og:image");
  const tw = extractMetaName(html, "twitter:image");
  const meta = og || tw;
  if (meta) {
    const abs = toAbsoluteUrl(meta, baseUrl);
    if (abs) return abs;
  }

  const firstPost = extractFirstPostHtml(html);
  if (firstPost) {
    const abs = extractFirstImageUrlFromHtml(firstPost, baseUrl);
    if (abs) return abs;
  }

  // Fallback: any image on the page.
  const abs = extractFirstImageUrlFromHtml(html, baseUrl);
  return abs;
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
    "Este bot es pa resenas: crear, buscar, votar y opinar.",
    "",
    `Ciudades/temas: ${selectedTopics}`,
    `Foros: ${user.forums.length}`,
    "",
    "Tip: si no hay resenas nuevas, le mando cositas mas viejas o de DonColombia."
  ].join("\n");

  const keyboard: any[][] = [
    [{ text: "Buscar resena", callback_data: "rvw:search" }],
    [{ text: "Crear resena", callback_data: "rvw:new" }],
    [{ text: "Elegir ciudad", callback_data: "wiz:cities:0" }],
    [{ text: "DonColombia (foros)", callback_data: "wiz:forums:0" }],
    [{ text: "Resenas (menu)", callback_data: "wiz:reviews" }]
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
    "2) Toque 'Buscar resena' o 'Crear resena' y siga los botones.",
    "3) La gente puede votar (buena/mala) y opinar en cada resena.",
    "4) Con filtros prendidos, le van llegando resenas cada ratico (entre 5 y 60 min).",
    "5) Si no hay resenas nuevas, le mando resenas viejas o contenido de DonColombia (secundario).",
    "6) Si quiere apoyar el proyecto: toque 'Donar'.",
    "",
    "Tip: si pega un link de DonColombia, el bot le saca un extracto."
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

function formatCoDateTimeFromSec(sec: number): string {
  if (!sec || !Number.isFinite(sec) || sec <= 0) return "";
  const ms = sec * 1000;

  // Colombia is UTC-5, no DST.
  const local = new Date(ms - 5 * 60 * 60 * 1000);

  const yyyy = local.getUTCFullYear();
  const mm = String(local.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(local.getUTCDate()).padStart(2, "0");

  const h24 = local.getUTCHours();
  const min = String(local.getUTCMinutes()).padStart(2, "0");
  const ampm = h24 >= 12 ? "pm" : "am";
  let h = h24 % 12;
  if (h === 0) h = 12;

  return `${dd}/${mm}/${yyyy} ${h}:${min}${ampm} COL`;
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
    lines.push(`Fecha: ${formatCoDateTimeFromSec(t.updatedAt)}`);
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

function bumpFeedAfterFilterChange(u: UserConfig): UserConfig {
  const now = nowSec();
  if (!hasPositiveFilters(u)) {
    return { ...u, feedNextAt: 0, feedDelivered: [], feedRecentThreadIds: [], feedRecentReviewIds: [] };
  }
  return { ...u, feedNextAt: scheduleFeedSoonAt(now), feedDelivered: [], feedRecentThreadIds: [], feedRecentReviewIds: [] };
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
    notifyDonations: true,
    feedNextAt: 0,
    feedDelivered: [],
    feedRecentThreadIds: [],
    feedRecentReviewIds: []
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

  const feedDelivered: string[] = Array.isArray(raw.feedDelivered) ? raw.feedDelivered.map((x: any) => String(x)) : [];
  const feedRecentThreadIds: number[] = Array.isArray(raw.feedRecentThreadIds)
    ? raw.feedRecentThreadIds.map((x: any) => Number(x)).filter((n: number) => Number.isFinite(n))
    : [];
  const feedRecentReviewIds: string[] = Array.isArray(raw.feedRecentReviewIds)
    ? raw.feedRecentReviewIds.map((x: any) => String(x)).filter((s: string) => s)
    : [];

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
    notifyDonations: typeof raw.notifyDonations === "boolean" ? raw.notifyDonations : true,
    feedNextAt: typeof raw.feedNextAt === "number" ? raw.feedNextAt : 0,
    feedDelivered: feedDelivered.slice(-FEED_DELIVERED_MAX),
    feedRecentThreadIds: feedRecentThreadIds.slice(-FEED_RECENT_THREAD_MAX),
    feedRecentReviewIds: feedRecentReviewIds.slice(-FEED_RECENT_REVIEW_MAX)
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

async function fetchTextWithTimeout(url: string, timeoutMs: number): Promise<string | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.max(1, timeoutMs));
  try {
    const res = await fetch(url, {
      headers: {
        "User-Agent": "radar-x-bot/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
      },
      signal: controller.signal
    });
    if (!res.ok) return null;
    return await res.text();
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
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

async function tgSendPhoto(env: Env, chatId: number, photoUrl: string, caption: string, replyMarkup?: any): Promise<boolean> {
  const res = await telegramApi(env, "sendPhoto", {
    chat_id: chatId,
    photo: photoUrl,
    caption: caption || undefined,
    reply_markup: replyMarkup
  });
  return Boolean(res);
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
  subjectName?: string;
  phone?: string;
  contactLink?: string;
  votesUp?: number;
  votesDown?: number;
  commentsCount?: number;
};

const REVIEW_DRAFT_TTL_SEC = 2 * 60 * 60;
const REVIEW_INDEX_MAX = 400;
const COMMENT_DRAFT_TTL_SEC = 45 * 60;
const SEARCH_DRAFT_TTL_SEC = 25 * 60;
const REVIEW_COMMENTS_INDEX_MAX = 80;

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
        title: String((it as any).title ?? "").trim(),
        subjectName: String((it as any).subjectName ?? "").trim() || undefined,
        phone: String((it as any).phone ?? "").trim() || undefined,
        contactLink: String((it as any).contactLink ?? "").trim() || undefined,
        votesUp: typeof (it as any).votesUp === "number" ? (it as any).votesUp : undefined,
        votesDown: typeof (it as any).votesDown === "number" ? (it as any).votesDown : undefined,
        commentsCount: typeof (it as any).commentsCount === "number" ? (it as any).commentsCount : undefined
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
  const entry: ReviewIndexEntry = {
    id: item.id,
    createdAt: item.createdAt,
    city: item.city,
    title: item.title,
    subjectName: item.subjectName,
    phone: item.phone,
    contactLink: item.contactLink,
    votesUp: item.votesUp,
    votesDown: item.votesDown,
    commentsCount: item.commentsCount
  };
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

function commentDraftKey(chatId: number): string {
  return `${COMMENT_DRAFT_KEY_PREFIX}${chatId}`;
}

function searchDraftKey(chatId: number): string {
  return `${SEARCH_DRAFT_KEY_PREFIX}${chatId}`;
}

async function getSearchDraft(env: Env, chatId: number): Promise<SearchDraft | null> {
  const key = searchDraftKey(chatId);
  const draft = (await env.KV.get(key, { type: "json" }).catch(() => null)) as SearchDraft | null;
  if (!draft || draft.v !== 1 || draft.chatId !== chatId) return null;
  if (draft.step !== "await_query") return null;
  return draft;
}

async function putSearchDraft(env: Env, draft: SearchDraft): Promise<void> {
  const key = searchDraftKey(draft.chatId);
  await env.KV.put(key, JSON.stringify(draft), { expirationTtl: SEARCH_DRAFT_TTL_SEC });
}

async function deleteSearchDraft(env: Env, chatId: number): Promise<void> {
  const key = searchDraftKey(chatId);
  await env.KV.delete(key).catch(() => undefined);
}

async function getCommentDraft(env: Env, chatId: number): Promise<CommentDraft | null> {
  const key = commentDraftKey(chatId);
  const draft = (await env.KV.get(key, { type: "json" }).catch(() => null)) as CommentDraft | null;
  if (!draft || draft.v !== 1 || draft.chatId !== chatId) return null;
  if (!draft.reviewId || draft.step !== "await_text") return null;
  return draft;
}

async function putCommentDraft(env: Env, draft: CommentDraft): Promise<void> {
  const key = commentDraftKey(draft.chatId);
  await env.KV.put(key, JSON.stringify(draft), { expirationTtl: COMMENT_DRAFT_TTL_SEC });
}

async function deleteCommentDraft(env: Env, chatId: number): Promise<void> {
  const key = commentDraftKey(chatId);
  await env.KV.delete(key).catch(() => undefined);
}

type ReviewCommentIndexEntry = {
  id: string;
  createdAt: number;
  createdBy: number;
};

type ReviewCommentItem = {
  v: 1;
  id: string;
  reviewId: string;
  createdAt: number;
  createdBy: number;
  text: string;
};

function reviewVoteKey(reviewId: string, chatId: number): string {
  return `${REVIEW_VOTE_KEY_PREFIX}${reviewId}:${chatId}`;
}

function reviewCommentsIndexKey(reviewId: string): string {
  return `${REVIEW_COMMENTS_INDEX_KEY_PREFIX}${reviewId}`;
}

function reviewCommentKey(reviewId: string, commentId: string): string {
  return `${REVIEW_COMMENT_KEY_PREFIX}${reviewId}:${commentId}`;
}

async function getReviewCommentsIndex(env: Env, reviewId: string): Promise<ReviewCommentIndexEntry[]> {
  const raw = await env.KV.get(reviewCommentsIndexKey(reviewId));
  if (!raw) return [];
  try {
    const arr = JSON.parse(raw) as any[];
    if (!Array.isArray(arr)) return [];
    const out: ReviewCommentIndexEntry[] = [];
    for (const it of arr) {
      if (!it || typeof it !== "object") continue;
      const id = String((it as any).id ?? "").trim();
      if (!id) continue;
      out.push({
        id,
        createdAt: typeof (it as any).createdAt === "number" ? (it as any).createdAt : 0,
        createdBy: typeof (it as any).createdBy === "number" ? (it as any).createdBy : 0
      });
    }
    out.sort((a, b) => (b.createdAt ?? 0) - (a.createdAt ?? 0));
    return out.slice(0, REVIEW_COMMENTS_INDEX_MAX);
  } catch {
    return [];
  }
}

async function putReviewCommentsIndex(env: Env, reviewId: string, entries: ReviewCommentIndexEntry[]): Promise<void> {
  await env.KV.put(reviewCommentsIndexKey(reviewId), JSON.stringify(entries.slice(0, REVIEW_COMMENTS_INDEX_MAX)));
}

async function saveReviewComment(env: Env, reviewId: string, createdBy: number, text: string): Promise<ReviewCommentItem | null> {
  const review = await getReview(env, reviewId);
  if (!review) return null;

  const trimmed = clipText(String(text ?? "").trim(), 1400);
  if (!trimmed) return null;

  const id = randomTokenHex(8);
  const item: ReviewCommentItem = {
    v: 1,
    id,
    reviewId,
    createdAt: nowSec(),
    createdBy,
    text: trimmed
  };

  await env.KV.put(reviewCommentKey(reviewId, id), JSON.stringify(item));

  const idx = await getReviewCommentsIndex(env, reviewId);
  const entry: ReviewCommentIndexEntry = { id, createdAt: item.createdAt, createdBy };
  const next = [entry, ...idx.filter((e) => e.id !== id)];
  await putReviewCommentsIndex(env, reviewId, next);

  // Update counters (best-effort).
  const nextCount = Math.max(0, Number(review.commentsCount ?? 0) + 1);
  const updated: ReviewItem = { ...review, commentsCount: nextCount };
  await saveReview(env, updated);

  return item;
}

async function getReviewComment(env: Env, reviewId: string, commentId: string): Promise<ReviewCommentItem | null> {
  const key = reviewCommentKey(reviewId, commentId);
  const item = (await env.KV.get(key, { type: "json" }).catch(() => null)) as ReviewCommentItem | null;
  if (!item || item.v !== 1 || item.reviewId !== reviewId) return null;
  return item;
}

async function listLatestReviewComments(env: Env, reviewId: string, limit: number): Promise<ReviewCommentItem[]> {
  const idx = await getReviewCommentsIndex(env, reviewId);
  const slice = idx.slice(0, Math.max(0, Math.min(limit, REVIEW_COMMENTS_INDEX_MAX)));
  const out: ReviewCommentItem[] = [];
  for (const e of slice) {
    const it = await getReviewComment(env, reviewId, e.id);
    if (it) out.push(it);
  }
  out.sort((a, b) => (b.createdAt ?? 0) - (a.createdAt ?? 0));
  return out;
}

async function sendReviewsMenu(env: Env, user: UserConfig, chatId: number, edit: boolean, messageId?: number): Promise<void> {
  const selected = user.includeKeywords.length ? user.includeKeywords.slice(0, 4).join(", ") : "(ninguna)";
  const text = [
    "Resenas",
    "",
    "Aca es donde esta lo bueno: buscar, publicar y ver resenas.",
    "",
    `Ciudades/temas: ${selected}`,
    "",
    "Que hacemos?"
  ].join("\n");

  const replyMarkup = {
    inline_keyboard: [
      [{ text: "Buscar resena", callback_data: "rvw:search" }],
      [{ text: "Ultimas resenas", callback_data: "rvw:latest" }],
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
    keyboard.push([{ text: c.name, callback_data: `rvw:city:${c.id}:${p}` }]);
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

function reviewFlowSteps(mode: "quick" | "full"): ReviewDraft["step"][] {
  const quick: ReviewDraft["step"][] = [
    "await_name",
    "pick_photos",
    "pick_physical_score",
    "pick_service_score",
    "pick_service_time",
    "await_rate",
    "pick_place",
    "await_comment",
    "await_phone",
    "await_contact_link",
    "confirm"
  ];

  const full: ReviewDraft["step"][] = [
    "await_name",
    "await_age",
    "pick_skin",
    "await_height",
    "pick_face",
    "pick_breasts",
    "pick_butt",
    "pick_photos",
    "pick_physical_score",
    "pick_hygiene",
    "pick_kisses",
    "pick_oral_condition",
    "pick_oral_quality",
    "pick_anal_offer",
    "pick_anal_quality",
    "pick_service_score",
    "pick_service_time",
    "await_rate",
    "pick_place",
    "pick_pay_on_entry",
    "pick_site_quality",
    "await_comment",
    "await_phone",
    "await_contact_link",
    "confirm"
  ];

  return mode === "full" ? full : quick;
}

function reviewTitleFromDraft(d: ReviewDraft): string {
  const name = String(d.subjectName ?? "").trim();
  if (name) return `${name} (${d.city})`;
  return `Resena en ${d.city}`;
}

function buildReviewTagsFromDraft(d: ReviewDraft): string[] {
  const tags: string[] = [];
  if (d.city) tags.push(d.city);
  const name = String(d.subjectName ?? "").trim();
  if (name) tags.push(name);
  return tags;
}

function normalizePhone(s: string): string {
  const raw = String(s ?? "");
  const digits = raw.replace(/[^0-9+]/g, "");
  // Keep only one leading +
  if (digits.startsWith("+")) return "+" + digits.slice(1).replace(/[^0-9]/g, "");
  return digits.replace(/[^0-9]/g, "");
}

function parseAge(text: string): number | null {
  const m = String(text ?? "").match(/\d{1,2}/);
  if (!m) return null;
  const n = parseInt(m[0], 10);
  if (!Number.isFinite(n)) return null;
  if (n < 18 || n > 70) return null;
  return n;
}

function parseHeight(text: string): string | null {
  const raw = String(text ?? "").trim().replace(",", ".");
  if (!raw) return null;
  // Accept "1.65" or "165"
  const m = raw.match(/(\d{1,3})(?:\.(\d{1,2}))?/);
  if (!m) return null;
  const a = m[1] ?? "";
  const b = m[2] ?? "";
  if (a.length === 3 && !b) {
    const cm = parseInt(a, 10);
    if (!Number.isFinite(cm) || cm < 130 || cm > 200) return null;
    const meters = (cm / 100).toFixed(2);
    return meters;
  }
  const meters = b ? `${a}.${b}` : a;
  const n = parseFloat(meters);
  if (!Number.isFinite(n) || n < 1.3 || n > 2.0) return null;
  return n.toFixed(2);
}

function parseCop(text: string): number | null {
  const digits = String(text ?? "").replace(/[^0-9]/g, "");
  if (!digits) return null;
  const n = parseInt(digits, 10);
  if (!Number.isFinite(n)) return null;
  if (n < 1000) return null;
  if (n > 50_000_000) return null;
  return n;
}

function parseHttpUrl(text: string): string | null {
  const s = String(text ?? "").trim();
  if (!s) return null;
  try {
    const u = new URL(s);
    if (u.protocol !== "http:" && u.protocol !== "https:") return null;
    return u.toString();
  } catch {
    return null;
  }
}

function computeFinalScore(physicalScore?: number, serviceScore?: number): number | undefined {
  const a = typeof physicalScore === "number" && Number.isFinite(physicalScore) ? physicalScore : 0;
  const b = typeof serviceScore === "number" && Number.isFinite(serviceScore) ? serviceScore : 0;
  if (!a && !b) return undefined;
  if (a && b) return Math.round(((a + b) / 2) * 10) / 10;
  return a || b || undefined;
}

function formatReviewTemplateFromItem(item: ReviewItem): string {
  const lines: string[] = [];

  const pushLine = (label: string, value: any) => {
    const v = String(value ?? "").trim();
    if (!v) return;
    lines.push(`- ${label}: ${v}`);
  };

  lines.push("Atributos Fisicos y Generales");
  pushLine("Nombre/Apodo", item.subjectName);
  pushLine("Edad", item.age ? `Aprox ${item.age} anos` : "");
  pushLine("Color de piel", item.skinColor);
  pushLine("Estatura", item.height ? `${item.height}` : "");
  pushLine("Cara", item.face);
  pushLine("Senos", item.breasts);
  pushLine("Cola", item.butt);
  pushLine("Es como las fotos", item.photosMatch);
  pushLine("Calificacion del fisico", typeof item.physicalScore === "number" ? String(item.physicalScore) : "");

  lines.push("");
  lines.push("Atributos NO Fisicos");
  pushLine("Aseo y bioseguridad", item.hygiene);
  pushLine("Da besos", item.kisses);
  pushLine("Condicion del oral", item.oralCondition);
  pushLine("Calidad del oral", item.oralQuality);
  pushLine("Ofrece anal", item.analOffer);
  pushLine("Calidad del anal", item.analQuality);
  pushLine("Calificacion del servicio", typeof item.serviceScore === "number" ? String(item.serviceScore) : "");

  lines.push("");
  lines.push("Otros Aspectos");
  pushLine("Tiempo del servicio", item.serviceTime);
  pushLine("Tarifa", typeof item.rateCop === "number" ? `${item.rateCop} COP` : "");
  pushLine("En donde fue", item.place);
  pushLine("Se paga al ingresar", item.payOnEntry);
  pushLine("Calidad del sitio", item.siteQuality);
  pushLine("Calificacion final", typeof item.finalScore === "number" ? String(item.finalScore) : "");

  // For new reviews, `story` is an edited narrative. For older stored items, `story` may contain the full template,
  // so we detect and ignore that legacy format.
  const storyText = String(item.story ?? "").trim();
  const commentText = String(item.comment ?? "").trim();
  const rawText = String(item.raw ?? "").trim();
  const looksLikeLegacyTemplate = (() => {
    if (!storyText) return false;
    const needles = ["Atributos Fisicos", "Atributos NO Fisicos", "Otros Aspectos", "Contactos"];
    let hits = 0;
    for (const n of needles) if (storyText.includes(n)) hits++;
    return hits >= 2;
  })();
  const narrative = !looksLikeLegacyTemplate && storyText ? storyText : commentText || rawText;

  if (narrative) {
    lines.push("");
    lines.push("Comentario general");
    lines.push(narrative);
  }

  const phone = String(item.phone ?? "").trim();
  const link = String(item.contactLink ?? "").trim();
  if (phone || link) {
    lines.push("");
    lines.push("Contactos");
    if (phone) pushLine("Telefono", phone);
    if (link) pushLine("Link", link);
  }

  return lines.join("\n").trim();
}

function formatReviewTemplateFromDraft(draft: ReviewDraft): string {
  const item: ReviewItem = {
    v: 1,
    id: "draft",
    createdAt: draft.createdAt,
    createdBy: draft.chatId,
    city: draft.city,
    title: draft.title || reviewTitleFromDraft(draft),
    tags: draft.tags?.length ? draft.tags : buildReviewTagsFromDraft(draft),
    raw: draft.raw || draft.comment || "",
    story: draft.story || "",
    subjectName: draft.subjectName,
    age: draft.age,
    skinColor: draft.skinColor,
    height: draft.height,
    face: draft.face,
    breasts: draft.breasts,
    butt: draft.butt,
    photosMatch: draft.photosMatch,
    physicalScore: draft.physicalScore,
    hygiene: draft.hygiene,
    kisses: draft.kisses,
    oralCondition: draft.oralCondition,
    oralQuality: draft.oralQuality,
    analOffer: draft.analOffer,
    analQuality: draft.analQuality,
    serviceScore: draft.serviceScore,
    serviceTime: draft.serviceTime,
    rateCop: draft.rateCop,
    place: draft.place,
    payOnEntry: draft.payOnEntry,
    siteQuality: draft.siteQuality,
    finalScore: computeFinalScore(draft.physicalScore, draft.serviceScore),
    comment: draft.comment,
    phone: draft.phone,
    contactLink: draft.contactLink
  };
  return formatReviewTemplateFromItem(item);
}

function buildReviewInlineKeyboard(env: Env, reviewId: string): any {
  const base = publicBaseUrl(env);
  const row1 = base
    ? [{ text: "Abrir", url: `${base}/r/${reviewId}` }, { text: "Leer", callback_data: `rvw:read:${reviewId}` }]
    : [{ text: "Leer", callback_data: `rvw:read:${reviewId}` }];
  const row2 = [
    { text: "Buena (+)", callback_data: `rvw:vote:up:${reviewId}` },
    { text: "Mala (-)", callback_data: `rvw:vote:down:${reviewId}` }
  ];
  const row3 = [{ text: "Opinar", callback_data: `rvw:comment:${reviewId}` }];
  return { inline_keyboard: [row1, row2, row3] };
}

async function sendReviewDraftPrompt(env: Env, chatId: number, draft: ReviewDraft): Promise<void> {
  const header = [`Ciudad: ${draft.city}`, `Modo: ${draft.mode === "full" ? "Completa" : draft.mode === "quick" ? "Rapida" : "(sin elegir)"}`].join("\n");

  if (draft.step === "await_mode") {
    await tgSendMessage(env, chatId, [header, "", "Quiere hacer resena rapida o completa?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Rapida (recomendada)", callback_data: "rvw:mode:quick" }],
        [{ text: "Completa (plantilla)", callback_data: "rvw:mode:full" }],
        [{ text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "await_name") {
    await tgSendMessage(env, chatId, [header, "", "Mande el nombre/apodo (1 mensaje).", "Ej: Luna"].join("\n"), true, {
      inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
    });
    return;
  }

  if (draft.step === "await_age") {
    await tgSendMessage(env, chatId, [header, "", "Edad aproximada? (solo numero)", "Ej: 23"].join("\n"), true, {
      inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
    });
    return;
  }

  if (draft.step === "pick_skin") {
    await tgSendMessage(env, chatId, [header, "", "Color de piel?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Blanca", callback_data: "rvw:set:skin:Blanca" }, { text: "Morena", callback_data: "rvw:set:skin:Morena" }],
        [{ text: "Triguena", callback_data: "rvw:set:skin:Triguena" }, { text: "Negra", callback_data: "rvw:set:skin:Negra" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "await_height") {
    await tgSendMessage(env, chatId, [header, "", "Estatura? (ej: 1.65 o 165)"].join("\n"), true, {
      inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
    });
    return;
  }

  if (draft.step === "pick_face") {
    await tgSendMessage(env, chatId, [header, "", "Cara?"].join("\n"), true, {
      inline_keyboard: [
        [
          { text: "Bonita", callback_data: "rvw:set:face:Bonita" },
          { text: "Regular", callback_data: "rvw:set:face:Regular" },
          { text: "Fea", callback_data: "rvw:set:face:Fea" }
        ],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_breasts") {
    await tgSendMessage(env, chatId, [header, "", "Senos?"].join("\n"), true, {
      inline_keyboard: [
        [
          { text: "Pequenos", callback_data: "rvw:set:breasts:Pequenos" },
          { text: "Grandes", callback_data: "rvw:set:breasts:Grandes" }
        ],
        [
          { text: "Operados", callback_data: "rvw:set:breasts:Operados" },
          { text: "Naturales", callback_data: "rvw:set:breasts:Naturales" }
        ],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_butt") {
    await tgSendMessage(env, chatId, [header, "", "Cola?"].join("\n"), true, {
      inline_keyboard: [
        [
          { text: "Redonda", callback_data: "rvw:set:butt:Redonda" },
          { text: "Plana", callback_data: "rvw:set:butt:Plana" }
        ],
        [
          { text: "Operada", callback_data: "rvw:set:butt:Operada" },
          { text: "Natural", callback_data: "rvw:set:butt:Natural" }
        ],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_photos") {
    await tgSendMessage(env, chatId, [header, "", "Es como las fotos?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Si", callback_data: "rvw:set:photos:Si" }, { text: "No", callback_data: "rvw:set:photos:No" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_physical_score") {
    const row1 = [1, 2, 3, 4, 5].map((n) => ({ text: String(n), callback_data: `rvw:num:physical:${n}` }));
    const row2 = [6, 7, 8, 9, 10].map((n) => ({ text: String(n), callback_data: `rvw:num:physical:${n}` }));
    await tgSendMessage(env, chatId, [header, "", "Calificacion del fisico (1-10)?"].join("\n"), true, {
      inline_keyboard: [row1, row2, [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
    });
    return;
  }

  if (draft.step === "pick_hygiene") {
    await tgSendMessage(env, chatId, [header, "", "Aseo y bioseguridad?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Aseada", callback_data: "rvw:set:hyg:Aseada" }, { text: "Desarreglada", callback_data: "rvw:set:hyg:Desarreglada" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_kisses") {
    await tgSendMessage(env, chatId, [header, "", "Da besos?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Si", callback_data: "rvw:set:kisses:Si" }, { text: "No", callback_data: "rvw:set:kisses:No" }],
        [{ text: "Con lengua", callback_data: "rvw:set:kisses:Con lengua" }, { text: "Solo picos", callback_data: "rvw:set:kisses:Solo picos" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_oral_condition") {
    await tgSendMessage(env, chatId, [header, "", "Condicion del oral?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Preservativo", callback_data: "rvw:set:oralcond:Preservativo" }, { text: "Natural", callback_data: "rvw:set:oralcond:Natural" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_oral_quality") {
    await tgSendMessage(env, chatId, [header, "", "Calidad del oral?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Bueno", callback_data: "rvw:set:oralq:Bueno" }, { text: "Regular", callback_data: "rvw:set:oralq:Regular" }, { text: "Malo", callback_data: "rvw:set:oralq:Malo" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_anal_offer") {
    await tgSendMessage(env, chatId, [header, "", "Ofrece anal?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Si", callback_data: "rvw:set:anal:Si" }, { text: "No", callback_data: "rvw:set:anal:No" }, { text: "Adicional", callback_data: "rvw:set:anal:Adicional" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_anal_quality") {
    await tgSendMessage(env, chatId, [header, "", "Calidad del anal?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Bueno", callback_data: "rvw:set:analq:Bueno" }, { text: "Regular", callback_data: "rvw:set:analq:Regular" }, { text: "Malo", callback_data: "rvw:set:analq:Malo" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_service_score") {
    const row1 = [1, 2, 3, 4, 5].map((n) => ({ text: String(n), callback_data: `rvw:num:service:${n}` }));
    const row2 = [6, 7, 8, 9, 10].map((n) => ({ text: String(n), callback_data: `rvw:num:service:${n}` }));
    await tgSendMessage(env, chatId, [header, "", "Calificacion del servicio (1-10)?"].join("\n"), true, {
      inline_keyboard: [row1, row2, [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
    });
    return;
  }

  if (draft.step === "pick_service_time") {
    await tgSendMessage(env, chatId, [header, "", "Tiempo del servicio?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Rato", callback_data: "rvw:set:time:Rato" }, { text: "1/2 hora", callback_data: "rvw:set:time:1/2 Hora" }],
        [{ text: "1 hora", callback_data: "rvw:set:time:1 Hora" }, { text: "Otro", callback_data: "rvw:set:time:Otro" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "await_rate") {
    await tgSendMessage(env, chatId, [header, "", "Tarifa? (solo numero, COP)", "Ej: 150000"].join("\n"), true, {
      inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
    });
    return;
  }

  if (draft.step === "pick_place") {
    await tgSendMessage(env, chatId, [header, "", "En donde fue el servicio?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Hotel", callback_data: "rvw:set:place:Hotel" }, { text: "Motel", callback_data: "rvw:set:place:Motel" }],
        [{ text: "Apartamento", callback_data: "rvw:set:place:Apartamento" }, { text: "Domicilio", callback_data: "rvw:set:place:Domicilio" }],
        [{ text: "Otro", callback_data: "rvw:set:place:Otro" }, { text: "Saltar", callback_data: "rvw:skip" }],
        [{ text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_pay_on_entry") {
    await tgSendMessage(env, chatId, [header, "", "Se paga al ingresar?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Si", callback_data: "rvw:set:pay:Si" }, { text: "No", callback_data: "rvw:set:pay:No" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "pick_site_quality") {
    await tgSendMessage(env, chatId, [header, "", "Calidad del sitio?"].join("\n"), true, {
      inline_keyboard: [
        [{ text: "Bueno", callback_data: "rvw:set:site:Bueno" }, { text: "Regular", callback_data: "rvw:set:site:Regular" }, { text: "Malo", callback_data: "rvw:set:site:Malo" }],
        [{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]
      ]
    });
    return;
  }

  if (draft.step === "await_comment") {
    await tgSendMessage(env, chatId, [header, "", "Comentario general (en sus palabras).", "Tip: 1 mensaje, sin pena."].join("\n"), true, {
      inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
    });
    return;
  }

  if (draft.step === "await_phone") {
    await tgSendMessage(env, chatId, [header, "", "Telefono (opcional).", "Ej: 3001234567"].join("\n"), true, {
      inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
    });
    return;
  }

  if (draft.step === "await_contact_link") {
    await tgSendMessage(env, chatId, [header, "", "Link web/contacto (opcional).", "Ej: https://..."].join("\n"), true, {
      inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
    });
    return;
  }

  if (draft.step === "confirm") {
    const preview = formatReviewTemplateFromDraft(draft);
    await tgSendMessage(env, chatId, ["Asi va quedando:", "", truncate(preview, 3200), "", "Si esta bien, publique."].join("\n"), true, {
      inline_keyboard: [[
        { text: "Publicar", callback_data: "rvw:publish" },
        { text: "Empezar de nuevo", callback_data: "rvw:restart" },
        { text: "Cancelar", callback_data: "rvw:cancel" }
      ]]
    });
    return;
  }

  await tgSendMessage(env, chatId, "Toque un boton o /cancel.", true, { inline_keyboard: [[{ text: "Cancelar", callback_data: "rvw:cancel" }]] });
}

function nextDraftStep(draft: ReviewDraft): ReviewDraft["step"] {
  const mode = draft.mode === "full" ? "full" : "quick";
  const flow = reviewFlowSteps(mode);
  const idx = flow.indexOf(draft.step);
  if (idx === -1) return "confirm";
  const next = flow[idx + 1];
  return next ?? "confirm";
}

async function advanceReviewDraft(env: Env, chatId: number, draft: ReviewDraft): Promise<void> {
  const next: ReviewDraft = { ...draft, step: nextDraftStep(draft), updatedAt: nowSec() };
  next.title = reviewTitleFromDraft(next);
  next.tags = buildReviewTagsFromDraft(next);

  // Best-effort: improve the free-text comment with DeepSeek right before confirmation,
  // so the preview shows the final narrative.
  if (next.step === "confirm") {
    const apiKey = String(env.DEEPSEEK_API_KEY ?? "").trim();
    const raw = String(next.raw ?? next.comment ?? "").trim();
    const hasStory = String(next.story ?? "").trim();
    if (apiKey && raw && !hasStory) {
      const improved = await improveReviewWithDeepSeek(env, { city: next.city, title: next.title || "", raw });
      const story = clipText(String(improved ?? "").trim(), 2000);
      if (story) next.story = story;
    }
  }

  await putReviewDraft(env, next);
  await sendReviewDraftPrompt(env, chatId, next);
}

async function skipReviewDraftStep(env: Env, chatId: number, draft: ReviewDraft): Promise<void> {
  await advanceReviewDraft(env, chatId, draft);
}

async function applyReviewDraftPick(
  env: Env,
  chatId: number,
  draft: ReviewDraft,
  input: { key: string; val: string }
): Promise<void> {
  const key = String(input.key ?? "");
  const val = String(input.val ?? "").trim();
  if (!val) {
    await sendReviewDraftPrompt(env, chatId, draft);
    return;
  }

  const next: ReviewDraft = { ...draft, updatedAt: nowSec() };
  if (key === "skin") next.skinColor = val;
  else if (key === "face") next.face = val;
  else if (key === "breasts") next.breasts = val;
  else if (key === "butt") next.butt = val;
  else if (key === "photos") next.photosMatch = val;
  else if (key === "hyg") next.hygiene = val;
  else if (key === "kisses") next.kisses = val;
  else if (key === "oralcond") next.oralCondition = val;
  else if (key === "oralq") next.oralQuality = val;
  else if (key === "anal") next.analOffer = val;
  else if (key === "analq") next.analQuality = val;
  else if (key === "time") next.serviceTime = val;
  else if (key === "place") next.place = val;
  else if (key === "pay") next.payOnEntry = val;
  else if (key === "site") next.siteQuality = val;

  next.step = nextDraftStep(next);
  next.title = reviewTitleFromDraft(next);
  next.tags = buildReviewTagsFromDraft(next);
  await putReviewDraft(env, next);
  await sendReviewDraftPrompt(env, chatId, next);
}

async function applyReviewDraftNumber(
  env: Env,
  chatId: number,
  draft: ReviewDraft,
  input: { key: string; n: number }
): Promise<void> {
  const key = String(input.key ?? "");
  const n = Math.floor(Number(input.n ?? 0));
  if (!Number.isFinite(n)) {
    await sendReviewDraftPrompt(env, chatId, draft);
    return;
  }
  if (n < 1 || n > 10) {
    await sendReviewDraftPrompt(env, chatId, draft);
    return;
  }

  const next: ReviewDraft = { ...draft, updatedAt: nowSec() };
  if (key === "physical") next.physicalScore = n;
  else if (key === "service") next.serviceScore = n;

  next.step = nextDraftStep(next);
  next.title = reviewTitleFromDraft(next);
  next.tags = buildReviewTagsFromDraft(next);
  await putReviewDraft(env, next);
  await sendReviewDraftPrompt(env, chatId, next);
}

async function publishReviewFromDraft(env: Env, chatId: number, draft: ReviewDraft): Promise<void> {
  const title = reviewTitleFromDraft(draft);
  const tags = buildReviewTagsFromDraft(draft);
  const raw = String(draft.comment ?? draft.raw ?? "").trim();
  const story = clipText(String(draft.story ?? "").trim(), 2000);

  const item: ReviewItem = {
    v: 1,
    id: randomTokenHex(8),
    createdAt: nowSec(),
    createdBy: chatId,
    city: draft.city,
    title,
    tags,
    raw: raw,
    story: story,
    subjectName: draft.subjectName,
    age: draft.age,
    skinColor: draft.skinColor,
    height: draft.height,
    face: draft.face,
    breasts: draft.breasts,
    butt: draft.butt,
    photosMatch: draft.photosMatch,
    physicalScore: draft.physicalScore,
    hygiene: draft.hygiene,
    kisses: draft.kisses,
    oralCondition: draft.oralCondition,
    oralQuality: draft.oralQuality,
    analOffer: draft.analOffer,
    analQuality: draft.analQuality,
    serviceScore: draft.serviceScore,
    serviceTime: draft.serviceTime,
    rateCop: draft.rateCop,
    place: draft.place,
    payOnEntry: draft.payOnEntry,
    siteQuality: draft.siteQuality,
    finalScore: computeFinalScore(draft.physicalScore, draft.serviceScore),
    comment: draft.comment,
    phone: draft.phone,
    contactLink: draft.contactLink,
    votesUp: 0,
    votesDown: 0,
    commentsCount: 0
  };

  await saveReview(env, item);
  await deleteReviewDraft(env, chatId);

  await tgSendMessage(env, chatId, "Listo pues, resena publicada.", true, buildReviewInlineKeyboard(env, item.id));
  await notifyUsersAboutReview(env, item);
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
    const keyboard = buildReviewInlineKeyboard(env, it.id);
    await tgSendMessage(env, chatId, text, true, keyboard);
  }
}

function formatLatestReviewItemText(it: ReviewIndexEntry, idx: number, total: number): string {
  const lines: string[] = [];
  lines.push(`Pilas pues resenas (${idx}/${total})`);
  lines.push(`[${it.city}] ${truncate(it.title || it.subjectName || it.id, 180)}`);
  if (it.createdAt) {
    lines.push(`Fecha: ${formatCoDateTimeFromSec(it.createdAt)}`);
  }
  const up = typeof it.votesUp === "number" ? it.votesUp : 0;
  const down = typeof it.votesDown === "number" ? it.votesDown : 0;
  const cc = typeof it.commentsCount === "number" ? it.commentsCount : 0;
  if (up || down || cc) lines.push(`Votos: +${up} / -${down} | Opiniones: ${cc}`);
  return lines.join("\n");
}

async function sendReviewRead(env: Env, chatId: number, id: string): Promise<void> {
  const item = await getReview(env, id);
  if (!item) {
    await tgSendMessage(env, chatId, "Uy parce, no encontre esa resena.", true);
    return;
  }

  const votesUp = Number(item.votesUp ?? 0);
  const votesDown = Number(item.votesDown ?? 0);
  const commentsCount = Number(item.commentsCount ?? 0);

  const lines: string[] = [];
  lines.push(item.title);
  lines.push(`Ciudad: ${item.city}`);
  lines.push(`Fecha: ${formatCoDateTimeFromSec(item.createdAt)}`);
  lines.push(`Votos: +${votesUp} / -${votesDown} | Opiniones: ${commentsCount}`);
  lines.push("");
  lines.push(formatReviewTemplateFromItem(item));

  const comments = await listLatestReviewComments(env, item.id, 5);
  if (comments.length > 0) {
    lines.push("");
    lines.push("Opiniones (ultimas):");
    for (const c of comments) {
      lines.push(`- ${formatCoDateTimeFromSec(c.createdAt)}: ${truncate(c.text, 260)}`);
    }
  }

  await tgSendLongMessage(env, chatId, lines.join("\n"), true);
  await tgSendMessage(env, chatId, "Quiere votar u opinar?", true, buildReviewInlineKeyboard(env, item.id));
}

async function applyReviewVote(
  env: Env,
  reviewId: string,
  chatId: number,
  dir: "up" | "down"
): Promise<{ votesUp: number; votesDown: number } | null> {
  const review = await getReview(env, reviewId);
  if (!review) return null;

  const key = reviewVoteKey(reviewId, chatId);
  const prev = String((await env.KV.get(key).catch(() => "")) ?? "").trim();

  let deltaUp = 0;
  let deltaDown = 0;

  if (prev === dir) {
    // Toggle off
    await env.KV.delete(key).catch(() => undefined);
    if (dir === "up") deltaUp -= 1;
    else deltaDown -= 1;
  } else if (prev === "up" || prev === "down") {
    await env.KV.put(key, dir);
    if (prev === "up") deltaUp -= 1;
    if (prev === "down") deltaDown -= 1;
    if (dir === "up") deltaUp += 1;
    if (dir === "down") deltaDown += 1;
  } else {
    await env.KV.put(key, dir);
    if (dir === "up") deltaUp += 1;
    else deltaDown += 1;
  }

  const votesUp = Math.max(0, Number(review.votesUp ?? 0) + deltaUp);
  const votesDown = Math.max(0, Number(review.votesDown ?? 0) + deltaDown);

  const updated: ReviewItem = { ...review, votesUp, votesDown };
  await saveReview(env, updated);

  return { votesUp, votesDown };
}

async function handleCommentDraftText(env: Env, chatId: number, draft: CommentDraft, textRaw: string): Promise<void> {
  const text = String(textRaw ?? "").trim();
  if (!text) {
    await tgSendMessage(env, chatId, "Parce, mande un texto.", true);
    return;
  }

  const item = await saveReviewComment(env, draft.reviewId, chatId, text);
  await deleteCommentDraft(env, chatId);

  if (!item) {
    await tgSendMessage(env, chatId, "Uy, no pude guardar esa opinion. Intente otra vez.", true);
    return;
  }

  await tgSendMessage(env, chatId, "Listo, opinion guardada.", true, buildReviewInlineKeyboard(env, draft.reviewId));

  const review = await getReview(env, draft.reviewId);
  if (review && review.createdBy && review.createdBy !== chatId) {
    const snippet = truncate(item.text, 240);
    await tgSendMessage(
      env,
      review.createdBy,
      `Pilas: le dejaron una opinion en su resena\n[${review.city}] ${review.title}\n\n"${snippet}"`,
      true,
      buildReviewInlineKeyboard(env, review.id)
    );
  }
}

async function handleSearchDraftText(env: Env, chatId: number, draft: SearchDraft, textRaw: string): Promise<void> {
  const q = String(textRaw ?? "").trim();
  if (!q || q.length < 2) {
    await tgSendMessage(env, chatId, "Mande mas info pues (nombre/telefono/link).", true);
    return;
  }

  await deleteSearchDraft(env, chatId);

  const url = extractFirstUrl(q) ?? (q.startsWith("http://") || q.startsWith("https://") ? q : "");
  if (url) {
    // Our review link: .../r/<id>
    const m = String(url).match(/\/r\/([0-9a-fA-F]{6,40})/);
    if (m?.[1]) {
      const id = m[1].toLowerCase();
      await sendReviewRead(env, chatId, id);
      return;
    }

    // DonColombia thread/forum shortcut.
    const threadInfo = threadPathInfoFromHref(url);
    if (threadInfo) {
      const canonUrl = new URL(threadInfo.basePath, env.SITE_BASE_URL).toString();
      await handleReadCommand(env, chatId, canonUrl);
      return;
    }
  }

  // Raw id
  if (/^[0-9a-fA-F]{16}$/.test(q)) {
    await sendReviewRead(env, chatId, q.toLowerCase());
    return;
  }

  const idx = await getReviewIndex(env);
  const needleDigits = q.replace(/[^0-9]/g, "");
  const needle = normalize(q);

  let hits: ReviewIndexEntry[] = [];
  if (needleDigits.length >= 7) {
    hits = idx.filter((e) => String(e.phone ?? "").replace(/[^0-9]/g, "").includes(needleDigits));
  } else {
    hits = idx.filter((e) =>
      normalize(`${e.city} ${e.title} ${e.subjectName ?? ""} ${e.phone ?? ""} ${e.contactLink ?? ""}`).includes(needle)
    );
  }

  hits = hits.slice(0, 8);
  if (hits.length === 0) {
    await tgSendMessage(env, chatId, "Uy, no encontre resenas con eso. Pruebe con otro dato.", true, {
      inline_keyboard: [[{ text: "Volver", callback_data: "wiz:reviews" }]]
    });
    return;
  }

  await tgSendMessage(env, chatId, `Listo, encontre ${hits.length}.`, true, { inline_keyboard: [[{ text: "Menu resenas", callback_data: "wiz:reviews" }]] });
  for (let i = 0; i < hits.length; i++) {
    const it = hits[i];
    const text = formatLatestReviewItemText(it, i + 1, hits.length);
    await tgSendMessage(env, chatId, text, true, buildReviewInlineKeyboard(env, it.id));
  }
}

function matchesUserForReviewEntry(u: UserConfig, e: ReviewIndexEntry): boolean {
  if (u.includeKeywords.length === 0) return false;
  const text = normalize(`${e.city} ${e.title} ${e.subjectName ?? ""} ${e.phone ?? ""} ${e.contactLink ?? ""}`);

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

  if (draft.step === "await_name") {
    const name = truncate(text, 60);
    const next: ReviewDraft = { ...draft, subjectName: name, updatedAt: nowSec() };
    await putReviewDraft(env, next);
    await advanceReviewDraft(env, chatId, next);
    return;
  }

  if (draft.step === "await_age") {
    const age = parseAge(text);
    if (!age) {
      await tgSendMessage(env, chatId, "Mande un numero valido (18-70) o toque Saltar.", true, {
        inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
      });
      return;
    }
    const next: ReviewDraft = { ...draft, age, updatedAt: nowSec() };
    await putReviewDraft(env, next);
    await advanceReviewDraft(env, chatId, next);
    return;
  }

  if (draft.step === "await_height") {
    const height = parseHeight(text);
    if (!height) {
      await tgSendMessage(env, chatId, "Asi: 1.65 o 165 (cm). O toque Saltar.", true, {
        inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
      });
      return;
    }
    const next: ReviewDraft = { ...draft, height, updatedAt: nowSec() };
    await putReviewDraft(env, next);
    await advanceReviewDraft(env, chatId, next);
    return;
  }

  if (draft.step === "await_rate") {
    const rateCop = parseCop(text);
    if (!rateCop) {
      await tgSendMessage(env, chatId, "Mande un numero (COP). Ej: 150000. O toque Saltar.", true, {
        inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
      });
      return;
    }
    const next: ReviewDraft = { ...draft, rateCop, updatedAt: nowSec() };
    await putReviewDraft(env, next);
    await advanceReviewDraft(env, chatId, next);
    return;
  }

  if (draft.step === "await_comment") {
    const comment = clipText(text, 2000);
    const next: ReviewDraft = { ...draft, comment, raw: comment, updatedAt: nowSec() };
    await putReviewDraft(env, next);
    await advanceReviewDraft(env, chatId, next);
    return;
  }

  if (draft.step === "await_phone") {
    const phone = normalizePhone(text);
    const digits = phone.replace(/[^0-9]/g, "");
    if (digits.length > 0 && digits.length < 7) {
      await tgSendMessage(env, chatId, "Ese numero esta muy corto. Mande el telefono bien o toque Saltar.", true, {
        inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
      });
      return;
    }
    const next: ReviewDraft = { ...draft, phone: phone || undefined, updatedAt: nowSec() };
    await putReviewDraft(env, next);
    await advanceReviewDraft(env, chatId, next);
    return;
  }

  if (draft.step === "await_contact_link") {
    const link = parseHttpUrl(text);
    if (!link) {
      await tgSendMessage(env, chatId, "Mande un link valido (http/https) o toque Saltar.", true, {
        inline_keyboard: [[{ text: "Saltar", callback_data: "rvw:skip" }, { text: "Cancelar", callback_data: "rvw:cancel" }]]
      });
      return;
    }
    const next: ReviewDraft = { ...draft, contactLink: link, updatedAt: nowSec() };
    await putReviewDraft(env, next);
    await advanceReviewDraft(env, chatId, next);
    return;
  }

  await tgSendMessage(env, chatId, "Toque un boton o /cancel.", true, { inline_keyboard: [[{ text: "Cancelar", callback_data: "rvw:cancel" }]] });
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

    const keyboard = buildReviewInlineKeyboard(env, item.id);
    const votesUp = Number(item.votesUp ?? 0);
    const votesDown = Number(item.votesDown ?? 0);
    await tgSendMessage(
      env,
      u.chatId,
      `Pilas pues: nueva resena\n[${item.city}] ${item.title}\nVotos: +${votesUp} / -${votesDown}`,
      true,
      keyboard
    );
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
  const votesUp = Math.max(0, Number(item.votesUp ?? 0));
  const votesDown = Math.max(0, Number(item.votesDown ?? 0));
  const commentsCount = Math.max(0, Number(item.commentsCount ?? 0));
  const story = escapeHtml(formatReviewTemplateFromItem(item));
  const created = formatCoDateTimeFromSec(item.createdAt || 0);

  const comments = await listLatestReviewComments(env, item.id, 20);
  const commentsHtml =
    comments.length === 0
      ? `<p class="meta">Todavia no hay opiniones.</p>`
      : `<div class="comments">${comments
          .map((c) => {
            const when = escapeHtml(formatCoDateTimeFromSec(c.createdAt));
            const body = escapeHtml(c.text || "");
            return `<div class="c"><div class="cmeta">${when}</div><div class="cbody">${body}</div></div>`;
          })
          .join("")}</div>`;

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
      h2 { font-size: 18px; margin: 0 0 10px; }
      .meta { color: #a9b7d6; margin: 0 0 18px; }
      .card { background: #121a2e; border: 1px solid rgba(255,255,255,0.08); border-radius: 14px; padding: 18px; }
      .story { white-space: pre-wrap; line-height: 1.5; }
      a { color: #a6c8ff; }
      .top { display:flex; gap:10px; align-items:center; justify-content:space-between; flex-wrap:wrap; }
      .btn { display:inline-block; background: rgba(37,195,139,0.12); border: 1px solid rgba(37,195,139,0.35); color: #e8eefc; text-decoration:none; padding:10px 12px; border-radius: 12px; font-weight:700; }
      .split { display:grid; grid-template-columns: 1fr; gap: 12px; margin-top: 12px; }
      .comments { display:flex; flex-direction:column; gap:10px; }
      .c { border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; padding: 12px; background: rgba(255,255,255,0.03); }
      .cmeta { color:#a9b7d6; font-size:12px; margin-bottom:6px; }
      .cbody { white-space: pre-wrap; line-height: 1.45; }
    </style>
  </head>
  <body>
    <main>
      <div class="top">
        <div>
          <h1>${title}</h1>
          <p class="meta">Ciudad: ${city} | ${escapeHtml(created)} | Votos: +${votesUp} / -${votesDown} | Opiniones: ${commentsCount}</p>
        </div>
        <a class="btn" href="${escapeHtml(base)}/donar">Apoyar el proyecto</a>
      </div>
      <div class="split">
        <div class="card">
          <div class="story">${story}</div>
        </div>
        <div class="card">
          <h2>Opiniones</h2>
          ${commentsHtml}
        </div>
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
