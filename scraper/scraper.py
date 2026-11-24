# ©️ Asmahri Abderrahman 2026. All rights reserved.
"""Structured job feeds scraper (v6.35.0).

Runs on GitHub Actions (public-repo unlimited minutes). CONFIG-DRIVEN
since v6.34.0: the source list AND the AI key pool come from the site —

    GET {SITE_URL}/api/ingest?config=1    (Bearer INGEST_SECRET)

—which serves exactly what the dashboard saves: the Data Sources panel's
feed list (the state_rows 'feeds' row) and the AI Center's key pool
(ai_keys table rows, up to 4 slots per provider). One paste in the
dashboard lights up both the edge pipeline and this scraper. If the
config channel is down, scraper/sources.json (the same list, mirrored)
is the fallback.

The scraper mirrors the site's own AI discipline (functions/api/_lib/
ai.js) for messy sources and thin descriptions: the same provider pool
(Gemini / Groq / Mistral / Z.AI / Puter / OpenRouter / Cerebras /
NVIDIA), the same per-provider model ladders, key-slot rotation on
429/503/529 (honoring Retry-After) and dead-key rotation on 401/402/403,
provider cooldowns after consecutive failures — then a run-end
pool_report POST ships the per-provider/per-slot verdicts back into
ai_pool_state (the admin AI-status panel renders them). The optional
Workers-AI REST polish (CF_API_TOKEN) stays as the final fallback lane.

Feed types (the dashboard's vocabulary — config.js DEFAULT_FEEDS and
the Data Sources panel):
    json-api    generic jobicy-style JSON (jobs[] or a bare array)
    remotive    remotive.com JSON (labeled Location/Type/Salary bits)
    rss / gnews generic RSS 2.0 / Atom with the edge's geo-hint
                (channel title "…jobs in X") + URL-slug company logic
    greenhouse  ATS board by id (boards-api.greenhouse.io)
    lever       ATS board by company (api.lever.co)            [v6.34.0]
    websearch   Z.AI web-search discovery (zai pool keys)      [v6.34.0]
    parallel    parallel.ai search (parallel pool keys). A query
                that starts with http(s) is FETCHED as a feed
                endpoint instead of searched (the whatjobs entry)
                                                                    [v6.34.0]
Plus the legacy dedicated mappers (remoteok / himalayas / wwr / jobicy)
for older sources.json entries.

Scrape order is RANDOM each run (the owner's design: consecutive runs
must not hammer the sources in the same sequence). SCRAPER_SEED pins
the shuffle for reproducible test runs.

Ship contract:
    POST {SITE_URL}/api/ingest  Authorization: Bearer {INGEST_SECRET}
    {"jobs": [...]}             (max 50/batch)
    POST {SITE_URL}/api/ingest  {"pool_report": {...}}  (run-end telemetry)

Why outside Cloudflare: the edge pipeline spends ~50 subrequests per run
plus AI chains; here GitHub pays the compute and the edge's cost per
50-job batch is ONE request.
"""

import collections
import html
import itertools
import json
import os
import random
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests

SITE_URL = (os.environ.get("SITE_URL") or "").rstrip("/")
# PUBLIC COPY: no default URL — SITE_URL comes from the repo variable.
if not SITE_URL:
    print("FATAL: SITE_URL is not set (repo variable or env)", file=sys.stderr)
    sys.exit(1)
INGEST_SECRET = os.environ.get("INGEST_SECRET") or ""
CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID") or ""
CF_API_TOKEN = os.environ.get("CF_API_TOKEN") or ""
# Polish fallback lane (Workers AI REST). @cf/moonshotai/kimi-k2.7-code
# answered 410 Gone on 2026-11-24 — the default is the edge's own
# verified live chain head. CF_MODEL pins any other catalog model.
CF_MODEL = os.environ.get("CF_MODEL") or "@cf/meta/llama-3.3-70b-instruct-fp8-fast"
CF_POLISH_CHAIN = [
    "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
    "@cf/meta/llama-4-scout-17b-16e-instruct",
    "@cf/meta/llama-3.2-3b-instruct",
]

MAX_BATCH = 50          # the API lane's hard cap (cron.js handleIngest)
HTTP_TIMEOUT = 25
POLISH_LIMIT = int(os.environ.get("POLISH_LIMIT") or 12)   # max AI polish calls per run
ENRICH_LIMIT = int(os.environ.get("ENRICH_LIMIT") or 60)   # max AI extraction calls per run
# (60 since v6.34.2: the v6.34.1 run finished 40/40 in seconds — the pool
# has headroom, and 100 rows were being dropped with the tail lanes never
# seeing AI budget under the old FIFO queue)
AI_BUDGET_S = int(os.environ.get("AI_BUDGET_S") or 300)    # wall clock for the whole AI phase
PUBLISHED_WITHIN_DAYS = 30
MAX_PER_SOURCE = 60     # the edge slices every feed at 60 items
KEY_COOLDOWN_CAP_S = 300  # Retry-After is honored but never trusted past 5 min

CATEGORIES = [
    "Software Development", "Design", "Marketing", "Sales",
    "Customer Support", "Product", "Data", "DevOps", "Writing",
    "Finance", "Other",
]

CATEGORY_KEYWORDS = {
    "Software Development": ["software", "developer", "engineer", "programmer", "full stack", "fullstack", "frontend", "backend", "web developer", "mobile", "ios", "android", "devtools"],
    "Design": ["designer", "design", "ux", "ui", "graphic", "illustrator", "brand"],
    "Marketing": ["marketing", "seo", "growth", "content strategist", "social media", "brand manager", "copywriter"],
    "Sales": ["sales", "account executive", "account manager", "business development", "bdm", "sdr", "bdr", "revenue"],
    "Customer Support": ["support", "customer service", "success manager", "help desk", "community manager"],
    "Product": ["product manager", "product owner", "product "],
    "Data": ["data", "analyst", "analytics", "scientist", "machine learning", "ml engineer", "bi "],
    "DevOps": ["devops", "sre", "site reliability", "infrastructure", "platform engineer", "cloud engineer", "sysadmin"],
    "Writing": ["writer", "technical writer", "content writer", "editor", "documentation"],
    "Finance": ["finance", "accountant", "accounting", "bookkeep", "financial analyst", "payroll"],
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; job-feed-bot/6.34)"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# ── The provider pool (a faithful port of ai.js callAIRaw + callAIKeys) ────
# Endpoints, model ladders and failure classes mirror the site's own pool
# (the TESTED table in config.js + the live branches in ai.js). Rotation
# rule (the owner's words: "if 1st is limit reached run the 2nd"):
#   • 429/503/529 (rate limit / overload) → honor Retry-After on that key
#     slot, one backoff retry, then the next key in the chain
#   • 401/402/403 (dead / paywalled key)  → next key (slot marked dead)
#   • 404/400 (model gone)                → next model in the ladder
#   • timeout                             → next model (a hanging model is
#     not a dead endpoint — ai.js v6.19.2 doctrine)
#   • any other error                     → next PROVIDER
# Two consecutive provider failures park it for 90s so the healthy
# members keep working (ai.js PROVIDER_COOLDOWN_*).

PROVIDERS = {
    "gemini": {"style": "gemini", "models": [
        "gemini-3.8-flash", "gemini-3.6-flash", "gemini-3.5-flash-lite",
        "gemini-3.1-flash-lite", "gemini-2.5-flash"]},
    "groq": {"url": "https://api.groq.com/openai/v1/chat/completions", "json_mode": True, "models": ["openai/gpt-oss-120b"]},
    "mistral": {"url": "https://api.mistral.ai/v1/chat/completions", "json_mode": True, "models": ["open-mistral-7b", "ministral-8b-latest"]},
    "zai": {"url": "https://api.z.ai/api/paas/v4/chat/completions", "models": ["glm-3.5-flash", "glm-5.3-flash"]},
    "puter": {"url": "https://api.puter.com/puterai/openai/v1/chat/completions", "models": ["gpt-5.4-nano", "gemini-3.5-flash-lite", "grok-4.7"]},
    "openrouter": {"url": "https://openrouter.ai/api/v1/chat/completions", "models": ["minimax/minimax-m3.1-flash:free", "google/gemma-4-31b-it:free"]},
    "cerebras": {"url": "https://api.cerebras.ai/v1/chat/completions", "models": ["gpt-oss-120b", "qwen-3.8-27b"]},
    "nvidia": {"url": "https://integrate.api.nvidia.com/v1/chat/completions", "max_tokens": 2048,
               "models": ["nvidia/nemotron-3.5-bolt-e4b", "poolside/laguna-xs-2.1", "nvidia/nemotron-3-ultra-550b-a55b"]},
}
PROVIDER_ORDER = ["gemini", "groq", "mistral", "zai", "puter", "openrouter", "cerebras", "nvidia"]
PROVIDER_COOLDOWN_AFTER = 2
PROVIDER_COOLDOWN_MS = 90 * 1000
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"

EXTRACT_SYSTEM = "You output only valid JSON. No prose, no markdown fences."
# NOTE: literal JSON braces are DOUBLED ({{ }}) so .format() leaves them
# alone — the live 2026-11-24 run crashed with KeyError: '"title"' because
# the single braces collided with the {categories}/{raw} placeholders.
EXTRACT_USER = (
    "Extract the job posting fields from the raw text below. Reply with ONLY a JSON object:\n"
    '{{"title": string (<=120 chars), "company": string (<=80 chars), "location": string (<=90 chars), '
    '"salary": string (<=40 chars, empty if unknown), "category": one of the categories below, '
    '"tags": array of up to 8 short skill strings, "description": one clean English paragraph '
    '(max 80 words) describing the role}}\n'
    "Every value must be in ENGLISH (translate Arabic/French/other source languages).\n"
    "Category must be exactly one of: {categories}.\n"
    "Raw text:\n{raw}"
)
POLISH_SYSTEM = ("You rewrite job descriptions. Reply with ONE clean English paragraph (max 80 words) "
                 "describing the role. No lists, no headers, no commentary.")


def guess_category(title: str, extra: str = "") -> str:
    """Map a title (+ optional feed category string) to a site category."""
    text = (title + " " + extra).lower()
    for cat, keywords in CATEGORY_KEYWORDS.items():
        if any(k in text for k in keywords):
            return cat
    return "Other"


def clean(text, limit: int) -> str:
    """Strip HTML tags + collapse whitespace, cap length."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(text))
    text = re.sub(r"&[a-z#0-9]+;", " ", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:limit]


def country_from_location(location: str) -> str:
    """Derive a country hint from a free-text location (the site runs the
    real normalizeCountry pass server-side; this only pre-hints it)."""
    if not location:
        return "GLOBAL"
    low = location.lower()
    if "worldwide" in low or "anywhere" in low:
        return "GLOBAL"
    return location.strip()[:90]


def extract_json(text: str):
    """Tolerant JSON object extraction (models wrap fences despite the
    prompt). Returns a dict or None."""
    if not text:
        return None
    text = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    try:
        v = json.loads(text)
        if isinstance(v, dict):
            return v
    except Exception:  # noqa: BLE001 — fall through to brace scan
        pass
    start, depth = -1, 0
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                try:
                    v = json.loads(text[start:i + 1])
                    if isinstance(v, dict):
                        return v
                except Exception:  # noqa: BLE001
                    start = -1
    return None


class ProviderPool:
    """The site's multi-key provider pool, in Python. chains comes from
    the config channel ({provider: [key1, key2, ...]})."""

    def __init__(self, chains: dict | None):
        self.chains = {p: [k for k in ks if k] for p, ks in (chains or {}).items() if ks}
        self.pos: dict[str, int] = {}            # sticky slot (last that answered)
        self.key_cooldown: dict[tuple, float] = {}   # (pid, slot) -> epoch until
        self.provider_cooldown: dict[str, float] = {}
        self.fails: dict[str, int] = {}
        self.retired_models: set[tuple] = set()  # (pid, model) retired this run
        self.stats = {p: {"calls": 0, "ok": 0, "rate_limited": 0, "key_errors": 0,
                          "parked": False, "slots": {}} for p in PROVIDER_ORDER}
        # v6.35.0 — audit H9: "parallel" is a pseudo-provider (the web-search
        # discovery lane, not a chat model), so it never rode PROVIDER_ORDER —
        # but source_parallel reaches into stats["parallel"] / _slot("parallel")
        # on the first 429/503/401/403, a KeyError that killed the whole
        # discovery feed for the run (429s are routine on these shared keys).
        # Seed it here like any other provider.
        self.stats["parallel"] = {"calls": 0, "ok": 0, "rate_limited": 0,
                                  "key_errors": 0, "parked": False, "slots": {}}
        self.enrich = {"calls": 0, "ok": 0}
        self.polish = {"calls": 0, "ok": 0, "dead_models": []}

    # -- slot bookkeeping ---------------------------------------------------
    def _slot(self, pid: str, idx: int) -> dict:
        return self.stats[pid]["slots"].setdefault(str(idx + 1), {"ok": 0, "rate_limited": 0, "dead": False})

    def _retry_after(self, resp) -> float:
        """Retry-After (seconds or HTTP-date), capped — never trust a
        provider's 3600s header past KEY_COOLDOWN_CAP_S."""
        raw = (resp.headers.get("Retry-After") or "").strip()
        until = 0.0
        if raw:
            try:
                until = time.time() + min(float(raw), KEY_COOLDOWN_CAP_S)
            except ValueError:
                try:
                    dt = datetime.strptime(raw, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
                    until = min(dt.timestamp(), time.time() + KEY_COOLDOWN_CAP_S)
                except Exception:  # noqa: BLE001 — unparseable header: default cooldown
                    until = 0.0
        return until

    # -- one provider call: model ladder + failure classification ----------
    def _call(self, pid: str, key: str, idx: int, system: str, user: str,
              want_json: bool, deadline: float) -> str | None:
        spec = PROVIDERS[pid]
        st, slot = self.stats[pid], self._slot(pid, idx)
        for model in spec["models"]:
            if (pid, model) in self.retired_models:
                continue
            backoff_done = False
            while True:  # one backoff retry on the rate-limit class
                if time.time() > deadline:
                    return None
                st["calls"] += 1
                try:
                    if pid == "gemini":
                        resp = SESSION.post(
                            GEMINI_URL.format(model=model, key=key),
                            json={"contents": [{"role": "user", "parts": [{"text": system + "\n\n" + user}]}],
                                  "generationConfig": {"temperature": 0.1, "maxOutputTokens": 4096,
                                                       **({"response_mime_type": "application/json"} if want_json else {})}},
                            timeout=20)
                    else:
                        body = {"model": model,
                                "messages": [{"role": "system", "content": system},
                                             {"role": "user", "content": user}],
                                "temperature": 0.1}
                        if spec.get("json_mode") and want_json:
                            body["response_format"] = {"type": "json_object"}
                        if spec.get("max_tokens"):
                            body["max_tokens"] = spec["max_tokens"]
                        resp = SESSION.post(spec["url"], json=body,
                                            headers={"authorization": f"Bearer {key}",
                                                     "content-type": "application/json"}, timeout=20)
                except requests.Timeout:
                    # a hanging model is not a dead endpoint — next model
                    break
                except requests.RequestException:
                    return None  # endpoint-level: another key cannot fix it
                if resp.status_code in (429, 503, 529):
                    st["rate_limited"] += 1
                    slot["rate_limited"] += 1
                    until = self._retry_after(resp)
                    if until:
                        self.key_cooldown[(pid, idx)] = until
                    if not backoff_done and pid != "nvidia":
                        backoff_done = True
                        time.sleep(0.8)  # one 800ms backoff retry (ai.js v6.3.1)
                        continue
                    return None  # rotate to the next key
                if resp.status_code in (401, 402, 403):
                    st["key_errors"] += 1
                    slot["dead"] = True
                    return None  # dead/paywalled key — next key
                if resp.status_code in (400, 404):
                    self.retired_models.add((pid, model))
                    break  # model gone — next model in the ladder
                if resp.status_code != 200:
                    return None  # other HTTP — next provider
                try:
                    data = resp.json()
                except Exception:  # noqa: BLE001 — bad body: next provider
                    return None
                if pid == "gemini":
                    parts = (((data.get("candidates") or [{}])[0].get("content") or {}).get("parts")) or []
                    text = "".join(p.get("text") or "" for p in parts).strip()
                else:
                    text = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
                if text:
                    st["ok"] += 1
                    slot["ok"] += 1
                    return text
                return None  # empty response — next provider
        return None

    # -- the walk: providers → keys (sticky start) → models ----------------
    def chat(self, system: str, user: str, want_json: bool = False,
             deadline: float | None = None) -> str | None:
        if not self.chains:
            return None
        if deadline is None:
            deadline = time.time() + AI_BUDGET_S
        now = time.time()
        for pid in PROVIDER_ORDER:
            chain = self.chains.get(pid)
            if not chain or self.provider_cooldown.get(pid, 0) > now:
                if self.provider_cooldown.get(pid, 0) > now:
                    self.stats[pid]["parked"] = True
                continue
            start = self.pos.get(pid, 0) % len(chain)
            answered = False
            for ki in range(len(chain)):
                if time.time() > deadline:
                    return None
                idx = (start + ki) % len(chain)
                if self.key_cooldown.get((pid, idx), 0) > time.time():
                    continue
                text = self._call(pid, chain[idx], idx, system, user, want_json, deadline)
                if text is not None:
                    self.pos[pid] = idx
                    self.fails[pid] = 0
                    answered = True
                    return text
            if not answered:
                self.fails[pid] = self.fails.get(pid, 0) + 1
                if self.fails[pid] >= PROVIDER_COOLDOWN_AFTER:
                    self.provider_cooldown[pid] = time.time() + PROVIDER_COOLDOWN_MS
                    self.stats[pid]["parked"] = True
        return None

    def report(self) -> dict:
        providers = {p: s for p, s in self.stats.items() if s["calls"] or s["parked"]}
        return {"providers": providers, "polish": self.polish, "enrich": self.enrich,
                "at": int(time.time() * 1000)}


# ── Workers AI REST polish (the CF_API_TOKEN fallback lane) ────────────────

_dead_models: set[str] = set()  # 410/404 answers — retired for this run


def _cf_chain() -> list[str]:
    chain = [CF_MODEL] + CF_POLISH_CHAIN
    out, seen = [], set()
    for m in chain:
        if m not in seen:
            seen.add(m)
            if m not in _dead_models:
                out.append(m)
    return out


def cf_polish(job: dict) -> bool:
    """The pre-v6.34 lane, kept as the final fallback: Workers AI REST,
    off the edge's request wall. A 410/404 answer retires the model for
    the rest of the run (the 2026-11-24 kimi incident)."""
    if not (CF_ACCOUNT_ID and CF_API_TOKEN):
        return False
    for model in _cf_chain():
        try:
            resp = requests.post(
                f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/ai/run/{model}",
                headers={"Authorization": f"Bearer {CF_API_TOKEN}"},
                json={"messages": [
                    {"role": "system", "content": POLISH_SYSTEM},
                    {"role": "user", "content": f"Title: {job['title']}\nCompany: {job['company']}\nRaw description: {job['description'][:1200]}"},
                ]},
                timeout=HTTP_TIMEOUT,
            )
            if resp.status_code in (404, 410):
                _dead_models.add(model)
                print(f"    polish model retired ({resp.status_code}): {model} — trying next", file=sys.stderr)
                continue
            resp.raise_for_status()
            text = clean((resp.json().get("result") or {}).get("response") or "", 600)
            if len(text) > 60:
                job["description"] = text
                return True
            return False
        except Exception:  # noqa: BLE001 — polish is best-effort
            # v6.35.0 — audit M32: requests exception text embeds the full
            # REST URL (the CF account id rides it) — never print it to
            # the PUBLIC Actions logs; the exception class is enough.
            print(f"    cf polish failed ({job['url'][:60]}): {type(exc).__name__}", file=sys.stderr)
            return False
    return False


def ai_polish(job: dict, pool: ProviderPool, deadline: float) -> None:
    """Rewrite a messy description. POOL FIRST (the site's own keys),
    then the CF REST fallback, then ship unpolished — never fail a row."""
    if len(job["description"]) > 150 and job["description"].endswith((".", "!", "?")):
        return  # already a clean sentence-shaped description
    pool.polish["calls"] += 1
    if time.time() <= deadline:
        text = pool.chat(POLISH_SYSTEM,
                         f"Title: {job['title']}\nCompany: {job['company']}\n"
                         f"Raw description: {job['description'][:1200]}",
                         deadline=deadline)
        if text:
            text = clean(text, 600)
            if len(text) > 60:
                job["description"] = text
                pool.polish["ok"] += 1
                return
    if cf_polish(job):
        pool.polish["ok"] += 1


# ── Shared fetch / parse helpers ───────────────────────────────────────────

# A browser UA for the one-shot 403 retry — some feeds (wadifa-info.com)
# sit behind a Cloudflare challenge that hard-blocks the scraper UA but
# lets a plain browser UA through. One retry only: a real JS challenge
# can never be solved here, and hammering it just earns an IP block.
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")


def _get_with_ua_retry(url: str, params: dict | None = None):
    """GET with the scraper UA; on a 403, one retry with a browser UA.
    Returns the LAST response either way (raise_for_status is the
    caller's call — the error message should name the real status)."""
    r = SESSION.get(url, timeout=HTTP_TIMEOUT, params=params)
    if r.status_code == 403:
        r2 = SESSION.get(url, timeout=HTTP_TIMEOUT, params=params,
                         headers={"User-Agent": BROWSER_UA})
        if r2.status_code == 200:
            print(f"    {url[:60]}: 403 under the scraper UA — fetched with a browser UA", file=sys.stderr)
            return r2
        # still blocked → a real challenge; report it as what it is
        if "just a moment" in r2.text[:2000].lower() or "cloudflare" in r2.text[:2000].lower():
            raise requests.HTTPError(
                f"403 Cloudflare challenge (browser UA blocked too — the site "
                f"requires a JS-capable client; remove this source or proxy it)", response=r2)
        return r2
    return r


def fetch_json(url: str, params: dict | None = None):
    r = _get_with_ua_retry(url, params)
    r.raise_for_status()
    return r.json()


def fetch_text(url: str) -> str:
    r = _get_with_ua_retry(url)
    r.raise_for_status()
    return r.text


def is_fresh(iso_date) -> bool:
    """Accept 'now' or a date within PUBLISHED_WITHIN_DAYS (feeds mix
    ISO-8601, RFC-822 and epoch formats)."""
    if not iso_date:
        return True
    try:
        text = str(iso_date)
        if text.isdigit():
            dt = datetime.fromtimestamp(int(text) / (1000 if len(text) > 10 else 1), tz=timezone.utc)
        else:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).days <= PUBLISHED_WITHIN_DAYS
    except Exception:  # noqa: BLE001 — unparseable dates stay admitted
        return True


def map_job(title, company, url, location="", description="", salary="", tags=None,
            category_hint="", date=None) -> dict | None:
    title, company, url = clean(title, 120), clean(company, 80), (url or "").strip()
    if not (title and company and url.startswith("http")):
        return None
    if date is not None and not is_fresh(date):
        return None
    return {
        "title": title,
        "company": company,
        "url": url,
        "location": clean(location, 90) or "Worldwide",
        "country_code": country_from_location(location),
        "category": guess_category(title, category_hint),
        "description": clean(description, 600) or f"{title} at {company}.",
        "salary_range": clean(salary, 40) or "N/A",
        "tags": [clean(t, 30) for t in (tags or []) if clean(t, 30)][:8],
    }


def raw_item(title="", company="", url="", location="", description="",
             salary="", tags=None, category_hint="", date=None) -> dict:
    """A pre-map row: company may be empty (the messy-source case) — main()
    sends those through the AI enrich lane instead of dropping them."""
    return {"title": title or "", "company": company or "", "url": (url or "").strip(),
            "location": location or "", "description": description or "",
            "salary": salary or "", "tags": tags or [], "category_hint": category_hint or "",
            "date": date}


# The edge's URL-slug company logic (feed.js v6.1): ATS boards and
# LinkedIn company pages carry the employer in the URL slug.
SLUG_COMPANY_RES = [
    re.compile(r"linkedin\.com/company/([a-z0-9-]{2,60})", re.I),
    re.compile(r"boards\.greenhouse\.io/([a-z0-9-]{2,60})", re.I),
    re.compile(r"jobs\.lever\.co/([a-z0-9-]{2,60})", re.I),
    re.compile(r"jobs\.ashbyhq\.com/([a-z0-9-]{2,60})", re.I),
    re.compile(r"/companies/([a-z0-9-]{2,60})/", re.I),
]


def slug_company(url: str) -> str:
    for rx in SLUG_COMPANY_RES:
        m = rx.search(url or "")
        if m:
            return " ".join(w[:1].upper() + w[1:] for w in m.group(1).split("-"))[:80]
    return ""


def split_title_company(title: str) -> tuple[str, str]:
    """Structural company fallbacks when neither feed metadata nor the URL
    slug named one (the edge pipeline sends these to AI extraction; here
    the deterministic patterns run FIRST so only truly messy rows pay AI
    budget):
      1. 'Company: Role'   — the WWR/Himalayas/aggregator convention
      2. 'Role at Company' — rwfa's convention ('Product Engineer at
         SafetyWing'); requires a real company-shaped tail (2-4 words,
         no location/remote noise) so 'Engineer at Remote Company in
         Morocco' splits at the LAST clean 'at', not the first noise one.
    Returns (company, title) — ('' , title) when nothing splits."""
    if ":" in title:
        head, tail = title.split(":", 1)
        if 1 < len(head.strip()) <= 60 and len(tail.strip()) > 3:
            return head.strip(), tail.strip()
    # 'Role at Company' — try the LAST ' at ' whose tail looks like a
    # company name (short, capitalized word(s), no location/remote words).
    noise = ("remote", "anywhere", "worldwide", "home", "office", "hiring",
             "full-time", "part-time", "contract", "freelance", "in ", "for ")
    parts = title.split(" at ")
    for i in range(len(parts) - 1, 0, -1):
        tail = parts[i].strip().rstrip(".,;!")
        words = tail.split()
        if 1 <= len(words) <= 4 and 2 < len(tail) <= 48:
            if any(w[0].isupper() for w in words) and not any(n in tail.lower() for n in noise):
                role = " at ".join(parts[:i]).strip()
                if len(role) > 3:
                    return tail, role
    return "", title


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _rss_items_et(root):
    """Strict-XML item walk (the fast path for well-formed feeds)."""
    meta_tags = ("region", "country", "state", "city", "location", "salary",
                 "type", "company", "skills")
    items = []
    for el in root.iter():
        if _local(el.tag) not in ("item", "entry"):
            continue
        pick, meta, categories = {}, {}, []
        link = ""
        for child in el:
            name = _local(child.tag)
            txt = (child.text or "").strip() if child.text else ""
            if name == "link":
                link = txt or child.attrib.get("href", "") or link
            elif name in ("description", "summary", "content"):
                pick.setdefault("description", html.unescape(txt))
            elif name == "title":
                pick["title"] = html.unescape(txt)
            elif name == "category":
                if txt:
                    categories.append(html.unescape(txt))
            elif name in meta_tags:
                if txt:
                    meta.setdefault(name, html.unescape(txt))
        if link and not link.startswith("http"):
            link = ""
        items.append({"title": pick.get("title", ""), "link": link,
                      "description": pick.get("description", ""),
                      "meta": {**meta, "categories": categories}})
    return items


def parse_rss_like(text: str):
    """Namespace-tolerant RSS 2.0 / Atom parse — a port of feed.js
    parseRSS: returns (items, channel_title) where each item carries
    title/link/description plus the meta fields the edge folds into the
    extraction text (region/country/state/city/location/salary/type/
    company/skills/categories).

    THE LIVE 2026-11-24 LESSON: realworld feeds ship malformed XML
    (realworkfromanywhere.com's RSS carries an unescaped '&' inside a URL
    attribute — line 109, col 82) and a STRICT parser answers zero items
    for a feed that has 128. The edge's parseRSS is regex-based for
    exactly this reason; the port now walks the same way: strict ET
    first (namespaced fast path), regex block-scan fallback (the edge's
    own grammar) when ET rejects the document."""
    raw = text.encode("utf-8") if isinstance(text, str) else text
    items, channel_title = [], ""
    try:
        root = ET.fromstring(raw)
        items = _rss_items_et(root)
        for el in root.iter():
            if _local(el.tag) == "title" and (el.text or "").strip():
                channel_title = html.unescape((el.text or "").strip())[:120]
                break
        if items or channel_title:
            return items, channel_title
    except ET.ParseError:
        pass  # malformed XML — the regex lane below

    # The edge's regex grammar (feed.js parseRSS): <item>/<entry> blocks,
    # channel title from the first <title> under <channel>, CDATA stripped,
    # entities decoded, Atom link@href accepted.
    def _strip_cdata(s: str) -> str:
        return re.sub(r"^\s*<!\[CDATA\[|\]\]>\s*$", "", s)

    src = raw.decode("utf-8", "replace")
    chan = re.search(r"<channel[\s\S]*?<title[^>]*>([\s\S]*?)</title>", src, re.I)
    channel_title = html.unescape(_strip_cdata(chan.group(1)).strip())[:120] if chan else ""
    meta_tags = ("region", "country", "state", "city", "location", "salary",
                 "type", "company", "skills")
    for block in re.findall(r"<(?:item|entry)[\s\S]*?</(?:item|entry)>", src, re.I):
        def pick(tag, blk=block):
            m = re.search(r"<" + tag + r"[^>]*>([\s\S]*?)</" + tag + ">", blk, re.I)
            return html.unescape(_strip_cdata(m.group(1)).strip()) if m else ""
        link = pick("link")
        if not link:
            m = re.search(r"<link[^>]*href=[\"']([^\"']+)[\"']", block, re.I)
            link = html.unescape(m.group(1)) if m else ""
        if link and not link.startswith("http"):
            link = ""
        meta, categories = {}, []
        for tag in meta_tags:
            v = pick(tag)
            if v:
                meta[tag] = v
        for m in re.finditer(r"<category[^>]*>([\s\S]*?)</category>", block, re.I):
            v = html.unescape(_strip_cdata(m.group(1)).strip())
            if v:
                categories.append(v)
        items.append({
            "title": pick("title"),
            "link": link,
            "description": pick("description") or pick("summary") or pick("content"),
            "meta": {**meta, "categories": categories},
        })
    return items, channel_title


def geo_hint_from_channel(channel_title: str) -> str:
    """feed.js v6.1: '…jobs in United States' channel titles are the only
    location signal per-country LinkedIn aggregates carry."""
    m = re.search(r"jobs?\s+in\s+(.{2,40})$", channel_title or "", re.I)
    cand = m and re.sub(r'["|(),].*$', "", m.group(1)).strip()
    if cand and re.fullmatch(r"[a-zA-Z ,]{2,40}", cand):
        return cand
    return ""


def labeled_bits(meta: dict, geo_hint: str) -> str:
    """The 'Field: value' grammar every structured source uses (feed.js
    v6.2.1: unlabeled joins made country/salary extraction unreliable)."""
    bits = []
    loc = (meta.get("region") or meta.get("location")
           or ((meta.get("city") + ", " + meta["country"]) if meta.get("city") and meta.get("country") else "")
           or meta.get("country") or meta.get("city") or geo_hint)
    if loc:
        bits.append("Location: " + clean(loc, 90))
    if meta.get("type"):
        bits.append("Type: " + clean(meta["type"], 30))
    if meta.get("salary"):
        bits.append("Salary: " + clean(meta["salary"], 40))
    if meta.get("categories"):
        bits.append("Keywords: " + clean(", ".join(meta["categories"]), 200))
    if meta.get("skills"):
        bits.append("Skills: " + clean(meta["skills"], 160))
    return " · ".join(bits)


# ── Feed mappers ───────────────────────────────────────────────────────────

def source_rss_generic(url: str):
    """rss / gnews: generic RSS with the edge's geo-hint + slug-company
    + 'Company: Role' title-split fallbacks. Rows with no company at all
    go to the AI enrich lane (main handles raw_item rows). A 200 that
    carries HTML instead of XML (dreamjob.ma) names itself instead of
    silently yielding zero items."""
    text = fetch_text(url)
    items, channel_title = parse_rss_like(text)
    if not items and not channel_title:
        stripped = text.lstrip()[:15].lower()
        if stripped.startswith(("<!doctype", "<html")):
            print(f"    {url[:60]}: 200 but serves HTML, not a feed — "
                  f"the url is a page, not an RSS endpoint (fix it in the Data Sources panel)", file=sys.stderr)
            return
    geo_hint = geo_hint_from_channel(channel_title)
    for it in items[:MAX_PER_SOURCE]:
        title, link = clean(it["title"], 160), it["link"]
        if not (title and link):
            continue
        meta = it["meta"]
        company = clean(meta.get("company"), 80) or slug_company(link)
        head, tail = ("", title)
        if not company:
            head, tail = split_title_company(title)
            company, title = head, tail
        desc = "\n".join(b for b in [labeled_bits(meta, geo_hint),
                                     clean(it["description"], 1600)] if b)[:2000]
        yield raw_item(title, company, link, meta.get("location") or geo_hint, desc,
                       meta.get("salary"), meta.get("categories", [])[:4],
                       " ".join(meta.get("categories", [])[:4]), None)


def source_json_api(url: str):
    """json-api: the jobicy-style generic (feed.js v6 fix): jobs[] or a
    bare array, v1 (position/company/description) AND v2 (jobTitle/
    companyName/jobDescription) layouts, list-shaped jobIndustry AND
    jobType (the 2026-11-24 str+list crash), labeled geo/level/type
    bits, salary fields folded in."""
    data = fetch_json(url)
    jobs = data if isinstance(data, list) else (data.get("jobs") or [])
    for j in jobs[:MAX_PER_SOURCE]:
        if not isinstance(j, dict):
            continue
        title = j.get("jobTitle") or j.get("position") or j.get("title") or j.get("role")
        url_j = j.get("url") or j.get("link") or j.get("jobUrl") or j.get("absolute_url")
        if not (title and url_j):
            continue
        company = (j.get("companyName") or j.get("company")
                   or j.get("company_name") or j.get("organization") or slug_company(url_j))
        industry = j.get("jobIndustry") or j.get("industry")
        industry = " ".join(industry) if isinstance(industry, list) else (industry or "")
        jtype = j.get("jobType") or j.get("job_type") or j.get("type")
        jtype = " ".join(jtype) if isinstance(jtype, list) else (jtype or "")
        bits = [clean(x, 60) for x in (j.get("jobGeo"), j.get("candidate_required_location"),
                                       j.get("candidate_location"), j.get("location"),
                                       j.get("jobLevel"), jtype, industry,
                                       j.get("jobCategory") or j.get("category")) if x]
        salary = j.get("salary") or ""
        if not salary and (j.get("salaryMin") or j.get("salaryMax")):
            salary = f"{j.get('salaryCurrency') or 'USD'} {j.get('salaryMin') or '?'}-{j.get('salaryMax') or '?'}" \
                     + (f" / {j.get('salaryPeriod')}" if j.get("salaryPeriod") else "")
        desc = "\n".join(b for b in [" · ".join(bits),
                                     clean(j.get("jobExcerpt") or j.get("summary"), 600),
                                     clean(j.get("jobDescription") or j.get("description"), 1600)] if b)[:2000]
        tags = j.get("tags") if isinstance(j.get("tags"), list) else []
        yield raw_item(title, company, url_j, j.get("jobGeo") or j.get("candidate_location") or j.get("location"),
                       desc, salary, tags[:8], " ".join(x for x in [industry, j.get("jobCategory") or ""] if x),
                       j.get("pubDate") or j.get("publication_date") or j.get("published_at") or j.get("updated_at"))


def source_remotive(url: str = "https://remotive.com/api/remote-jobs", limit: int = 50):
    data = fetch_json(url, {"limit": limit} if "remotive.com/api/remote-jobs" in url else None)
    for j in (data.get("jobs") or [])[:MAX_PER_SOURCE]:
        jt = re.sub(r"[_-]+", " ", str(j.get("job_type") or ""))
        bits = []
        if j.get("candidate_required_location"):
            bits.append("Location: " + clean(j["candidate_required_location"], 90))
        if jt:
            bits.append("Type: " + clean(jt, 30))
        if j.get("category"):
            bits.append("Category: " + clean(j["category"], 60))
        if j.get("salary"):
            bits.append("Salary: " + clean(j["salary"], 40))
        if isinstance(j.get("tags"), list) and j["tags"]:
            bits.append("Skills: " + clean(", ".join(j["tags"]), 220))
        desc = "\n".join(b for b in [" · ".join(bits), clean(j.get("description"), 1600)] if b)[:2000]
        yield raw_item(j.get("title"), j.get("company_name"), j.get("url"),
                       j.get("candidate_required_location"), desc, j.get("salary"),
                       (j.get("tags") or [])[:8], j.get("category"), j.get("publication_date"))


def source_greenhouse(board: str):
    data = fetch_json(f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs")
    for j in (data.get("jobs") or [])[:MAX_PER_SOURCE]:
        yield raw_item(j.get("title"), j.get("company_name") or board, j.get("absolute_url"),
                       (j.get("location") or {}).get("name"), "", "", [], "software",
                       j.get("first_published"))


def source_lever(company: str):
    """lever (v6.34.0): api.lever.co/v0/postings/{company}?mode=json —
    the edge's Lever adapter shape: text/hostedUrl/categories.location/
    commitment/team + createdAt (epoch ms). The API answers a bare
    array; an error dict (bad board, 429 JSON) must not crash the walk."""
    data = fetch_json(f"https://api.lever.co/v0/postings/{company}", {"mode": "json"})
    for j in (data if isinstance(data, list) else [])[:MAX_PER_SOURCE]:
        if not isinstance(j, dict):
            continue
        cats = j.get("categories") or {}
        yield raw_item(j.get("text"), company, j.get("hostedUrl"),
                       cats.get("location"), "", "", [], cats.get("team"),
                       j.get("createdAt"))


# QPS pacing for the discovery lanes (zai free tier: ONE concurrent
# request per key; parallel.ai similar). Shared across both lanes so a
# zai query followed instantly by a parallel query on the same provider
# family still honors the gap.
_DISCOVERY_LAST_CALL = 0.0
_DISCOVERY_MIN_GAP_S = 1.1


def _discovery_pace():
    global _DISCOVERY_LAST_CALL
    wait = _DISCOVERY_LAST_CALL + _DISCOVERY_MIN_GAP_S - time.time()
    if wait > 0:
        time.sleep(wait)
    _DISCOVERY_LAST_CALL = time.time()


def source_zai_websearch(feed: dict, pool: ProviderPool):
    """websearch: Z.AI web-search discovery on the dedicated Tool API —
    the SAME zai key chain as the site's source (feed.js v6.3/v6.19.8):
    key-slot rotation on 429/503/401/403, sticky slot, search-prime
    engine, count 20, recency filter. URL-shaped queries are FETCHED as
    feed endpoints instead of searched (the dashboard's whatjobs-style
    entries ride this).

    THE LIVE 2026-11-24 LESSON ('no zai key answered', zero items): the
    free tier allows ONE concurrent request per key and throttles QPS
    bursts — three queries back-to-back on one key is a guaranteed 429.
    The edge serializes through withZaiGate for exactly this reason; the
    port paces ≥1.1s between zai calls (shared with the parallel lane
    via _discovery_pace) and names WHY each key failed so the log
    separates a throttled key from a dead one.

    THE SECOND LIVE LESSON (v6.34.1 run, same day): both keys 429'd on
    the FIRST query despite pacing — these keys are SHARED with the
    site's edge, which enriches continuously between scraper runs. When
    the run lands mid-edge-burst every key is already hot, and pacing
    only spaces the scraper's own calls. The edge's withZaiGate answer
    is backoff-and-retry; the port now does the same: when every key
    answers 429/503, sleep (Retry-After or a growing backoff, capped at
    20s) and re-poll the chain — up to 2 extra rounds. 401/403 keys are
    dead and exempt from the retry; only throttled keys get re-tried."""
    chains = pool.chains.get("zai") or []
    queries = feed.get("queries") or ([feed["query"]] if feed.get("query") else [])
    recency = feed.get("recency") if feed.get("recency") in ("oneDay", "oneWeek", "oneMonth", "oneYear", "noLimit") else "oneWeek"
    for q in queries:
        if q.startswith("http"):
            yield from feed_endpoint_jobs(q)
            continue
        if not chains:
            print("    zai-websearch: no zai key in the pool (add one in AI Center)", file=sys.stderr)
            return
        _discovery_pace()
        data = None
        why = []
        throttled_rounds = 0
        while data is None:
            start = pool.pos.get("zai", 0) % len(chains)
            throttled = 0
            for ki in range(len(chains)):
                idx = (start + ki) % len(chains)
                key = chains[idx]
                # skip keys under cooldown from THIS run's earlier rounds —
                # but a skipped key still counts as throttled so the retry
                # round below keeps waiting for its cooldown to expire
                # (otherwise a full cooldown sweep breaks the loop early
                # and the lane gives up one round too soon)
                if pool.key_cooldown.get(("zai", idx), 0) > time.time():
                    why.append(f"key#{idx + 1}: cooldown")
                    throttled += 1
                    continue
                try:
                    resp = SESSION.post("https://api.z.ai/api/paas/v4/web_search",
                                        json={"search_engine": "search-prime", "search_query": q,
                                              "count": 20, "search_recency_filter": recency},
                                        headers={"authorization": f"Bearer {key}",
                                                 "content-type": "application/json",
                                                 "accept-language": "en-US,en"}, timeout=15)
                except requests.RequestException as exc:
                    print(f"    zai-websearch query failed: {exc}", file=sys.stderr)
                    return
                if resp.status_code == 200:
                    pool.pos["zai"] = idx
                    data = resp.json()
                    break
                if resp.status_code in (429, 503, 401, 403):
                    slot = pool._slot("zai", idx)  # noqa: SLF001 — same-module bookkeeping
                    reason = (resp.headers.get("Retry-After") and f"429 retry-after {resp.headers['Retry-After']}s") \
                        or f"http_{resp.status_code}"
                    why.append(f"key#{idx + 1}: {reason}")
                    if resp.status_code in (429, 503):
                        pool.stats["zai"]["rate_limited"] += 1
                        slot["rate_limited"] += 1
                        throttled += 1
                        until = pool._retry_after(resp)  # noqa: SLF001
                        if until:
                            pool.key_cooldown[("zai", idx)] = until
                    else:
                        pool.stats["zai"]["key_errors"] += 1
                        slot["dead"] = True
                    continue
                print(f"    zai-websearch: http_{resp.status_code} (endpoint problem — no key can fix it)", file=sys.stderr)
                return
            if data is not None:
                break
            if throttled and throttled_rounds < 2:
                # every reachable key answered 429/503 — the keys are
                # shared with the edge and already hot. Back off and
                # re-poll: honor the longest remaining key cooldown
                # (Retry-After), else 3s→6s, capped at 20s.
                throttled_rounds += 1
                delay = min(3.0 * throttled_rounds, 20.0)
                for (pid_cd, idx), until in list(pool.key_cooldown.items()):
                    if pid_cd == "zai" and until > time.time():
                        delay = max(delay, min(until - time.time(), 20.0))
                print(f"    zai-websearch: all keys throttled — backing off {delay:.0f}s "
                      f"(round {throttled_rounds}/2)", file=sys.stderr)
                time.sleep(delay)
                _discovery_pace()
                why.clear()
                continue
            break
        if not data:
            print(f"    zai-websearch: no zai key answered ({'; '.join(why) or 'unknown'}) — "
                  f"keys are throttled (QPS) or invalid; the AI Center slot report tells which", file=sys.stderr)
            return
        seen = set()
        for s in (data.get("search_result") or data.get("results") or [])[:MAX_PER_SOURCE]:
            url_s = s.get("link") or ""
            if not (url_s.startswith("http") and url_s not in seen):
                continue
            seen.add(url_s)
            bits = []
            if s.get("media"):
                bits.append("Source: " + clean(s["media"], 60))
            if s.get("publish_date"):
                bits.append("Published: " + clean(s["publish_date"], 30))
            desc = ("\n".join(bits) + ("\n" if bits else "") + clean(s.get("content"), 1000))[:1200]
            yield raw_item(s.get("title"), slug_company(url_s), url_s, "", desc,
                           "", [], "", None)


def source_parallel(feed: dict, pool: ProviderPool):
    """parallel: parallel.ai search discovery (feed.js v6.5/v6.19.8) —
    x-api-key chain rotation, mode 'fast', the feed's own objective.
    URL-shaped queries are FETCHED as feed endpoints instead (same rule
    as the websearch lane)."""
    chains = pool.chains.get("parallel") or []
    queries = feed.get("queries") or ([feed["query"]] if feed.get("query") else [])
    objective = clean(feed.get("objective"), 200) or "Find currently open job postings matching the search queries"
    for q in queries:
        if q.startswith("http"):
            yield from feed_endpoint_jobs(q)
            continue
        if not chains:
            print("    parallel: no parallel key in the pool (add one in AI Center)", file=sys.stderr)
            return
        _discovery_pace()
        start = pool.pos.get("parallel", 0) % len(chains)
        data = None
        why = []
        for ki in range(len(chains)):
            idx = (start + ki) % len(chains)
            key = chains[idx]
            try:
                resp = SESSION.post("https://api.parallel.ai/v1/search",
                                    json={"mode": "fast", "objective": objective, "search_queries": [q]},
                                    headers={"x-api-key": key, "content-type": "application/json"}, timeout=15)
            except requests.RequestException as exc:
                print(f"    parallel query failed: {exc}", file=sys.stderr)
                return
            if resp.status_code == 200:
                pool.pos["parallel"] = idx
                data = resp.json()
                break
            if resp.status_code in (429, 503, 401, 403):
                slot = pool._slot("parallel", idx)  # noqa: SLF001
                reason = (resp.headers.get("Retry-After") and f"429 retry-after {resp.headers['Retry-After']}s") \
                    or f"http_{resp.status_code}"
                why.append(f"key#{idx + 1}: {reason}")
                if resp.status_code in (429, 503):
                    pool.stats["parallel"]["rate_limited"] += 1
                    slot["rate_limited"] += 1
                    until = pool._retry_after(resp)  # noqa: SLF001
                    if until:
                        pool.key_cooldown[("parallel", idx)] = until
                else:
                    pool.stats["parallel"]["key_errors"] += 1
                    slot["dead"] = True
                continue
            print(f"    parallel: http_{resp.status_code} (endpoint problem — no key can fix it)", file=sys.stderr)
            return
        if not data:
            print(f"    parallel: no parallel key answered ({'; '.join(why) or 'unknown'}) — "
                  f"keys are throttled or invalid; the AI Center slot report tells which", file=sys.stderr)
            return
        seen = set()
        for s in (data.get("results") or [])[:MAX_PER_SOURCE]:
            url_s = s.get("url") or ""
            if not (url_s.startswith("http") and url_s not in seen):
                continue
            seen.add(url_s)
            bits = []
            if s.get("publish_date"):
                bits.append("Published: " + clean(s["publish_date"], 30))
            excerpts = s.get("excerpts") if isinstance(s.get("excerpts"), list) else []
            desc = ("\n".join(bits) + ("\n" if bits else "")
                    + clean(" ".join(excerpts), 1000))[:1200]
            yield raw_item(s.get("title"), slug_company(url_s), url_s, "", desc,
                           "", [], "", None)


def feed_endpoint_jobs(url: str):
    """A URL-shaped 'query' (the dashboard's whatjobs entry) is a feed
    ENDPOINT, not a search string: fetch it and map RSS/Atom/generic-XML
    or JSON with the generic mappers."""
    try:
        text = fetch_text(url)
    except Exception as exc:  # noqa: BLE001 — one dead endpoint never kills the run
        print(f"    endpoint query failed ({url[:60]}): {exc}", file=sys.stderr)
        return
    stripped = text.lstrip()[:1]
    if stripped == "<":
        items, _channel = parse_rss_like(text)
        if items:
            for it in items[:MAX_PER_SOURCE]:
                title, link = clean(it["title"], 160), it["link"]
                if not (title and link):
                    continue
                meta = it["meta"]
                company = clean(meta.get("company"), 80) or slug_company(link)
                head, tail = ("", title)
                if not company:
                    head, tail = split_title_company(title)
                    company, title = head, tail
                desc = "\n".join(b for b in [labeled_bits(meta, ""),
                                             clean(it["description"], 1600)] if b)[:2000]
                yield raw_item(title, company, link, meta.get("location"), desc,
                               meta.get("salary"), meta.get("categories", [])[:4], "", None)
            return
        # generic <job>-shaped XML (whatjobs-style APIs): not RSS — walk
        # every element whose local name looks like a posting record.
        yield from _generic_xml_jobs(text)
        return
    try:
        data = json.loads(text)
    except Exception:  # noqa: BLE001
        print(f"    endpoint query: unparseable content from {url[:60]}", file=sys.stderr)
        return
    if isinstance(data, dict):
        data = data.get("jobs") or data.get("results") or []
    if isinstance(data, list):
        for j in data[:MAX_PER_SOURCE]:
            if isinstance(j, dict):
                yield raw_item(j.get("title") or j.get("jobTitle") or j.get("position"),
                               j.get("company") or j.get("company_name") or j.get("companyName"),
                               j.get("url") or j.get("link"),
                               j.get("location") or j.get("city") or j.get("jobGeo"),
                               clean(j.get("description") or j.get("jobDescription"), 1600),
                               j.get("salary"), [], j.get("category") or "",
                               j.get("pubDate") or j.get("publication_date"))


def _generic_xml_jobs(text: str):
    try:
        root = ET.fromstring(text.encode("utf-8"))
    except ET.ParseError:
        return
    record_tags = {"job", "position", "vacancy", "posting", "ad"}
    fields = {"title": ("title", "jobtitle", "job_title", "position"),
              "company": ("company", "companyname", "company_name", "organization", "employer"),
              "url": ("url", "link", "joburl", "apply_url"),
              "location": ("location", "city", "jobgeo", "candidate_location"),
              "salary": ("salary", "salaryrange", "pay"),
              "description": ("description", "jobdescription", "summary", "content"),
              "date": ("pubdate", "published", "published_date", "publication_date", "created")}
    for el in root.iter():
        if _local(el.tag) not in record_tags:
            continue
        row = {}
        for child in el:
            name = _local(child.tag)
            for field, names in fields.items():
                if name in names and field not in row and (child.text or "").strip():
                    row[field] = html.unescape(child.text.strip())
        if row.get("title") and row.get("url"):
            company = row.get("company") or slug_company(row["url"])
            head, tail = ("", row["title"])
            if not company:
                head, tail = split_title_company(row["title"])
                company = head
            yield raw_item(tail, company, row["url"], row.get("location"),
                           clean(row.get("description"), 1600), row.get("salary"),
                           [], "", row.get("date"))


# Legacy dedicated mappers (pre-config sources.json entries) — kept for
# backward compatibility; the dashboard drives the generic types above.

def source_jobicy(count: int = 50):
    yield from source_json_api(f"https://jobicy.com/api/v2/remote-jobs?count={count}")


def source_remoteok():
    data = fetch_json("https://remoteok.com/api")
    for j in (data[1:] if isinstance(data, list) else [])[:MAX_PER_SOURCE]:
        if not isinstance(j, dict):
            continue
        salary = f"${j.get('salary_min')}-${j.get('salary_max')}" if j.get("salary_min") and j.get("salary_max") else ""
        yield raw_item(j.get("position"), j.get("company"), j.get("url"), j.get("location"),
                       clean(j.get("description"), 1600), salary, (j.get("tags") or [])[:8],
                       " ".join((j.get("tags") or [])[:5]), j.get("date") or j.get("epoch"))


def source_himalayas():
    yield from source_rss_generic("https://himalayas.app/jobs/rss")


def source_wwr():
    yield from source_rss_generic("https://weworkremotely.com/categories/remote-programming-jobs.rss")


LEGACY_SOURCES = {
    "jobicy": lambda feed: source_jobicy(),
    "remoteok": lambda feed: source_remoteok(),
    "himalayas": lambda feed: source_himalayas(),
    "wwr": lambda feed: source_wwr(),
}


def feed_to_jobs(feed: dict, pool: ProviderPool):
    """Dispatch one dashboard feed entry by its type — the same type
    vocabulary the Data Sources panel saves."""
    typ = feed.get("type") or ""
    fid = feed.get("id") or typ
    if typ in ("rss", "gnews"):
        if not feed.get("url"):
            raise ValueError("rss/gnews source needs a url")
        yield from source_rss_generic(feed["url"])
    elif typ == "json-api":
        if not feed.get("url"):
            raise ValueError("json-api source needs a url")
        yield from source_json_api(feed["url"])
    elif typ == "remotive":
        yield from source_remotive(feed.get("url") or "https://remotive.com/api/remote-jobs")
    elif typ == "greenhouse":
        yield from source_greenhouse(feed.get("board") or fid)
    elif typ == "lever":
        yield from source_lever(feed.get("company") or fid)
    elif typ == "websearch":
        yield from source_zai_websearch(feed, pool)
    elif typ == "parallel":
        yield from source_parallel(feed, pool)
    elif typ in LEGACY_SOURCES:
        yield from LEGACY_SOURCES[typ](feed)
    else:
        raise ValueError(f"unsupported feed type: {typ}")


# ── The config channel + the offline fallback ──────────────────────────────

def fetch_config() -> dict | None:
    """GET {SITE_URL}/api/ingest?config=1 — the dashboard's feeds list
    (state_rows 'feeds') + the AI Center's key pool (ai_keys), exactly
    what the site's own pipeline uses. None on any failure (offline
    sources.json fallback)."""
    try:
        resp = SESSION.get(f"{SITE_URL}/api/ingest", params={"config": "1"},
                           headers={"Authorization": f"Bearer {INGEST_SECRET}"}, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("success") and isinstance(data.get("feeds"), list):
                return data
        print(f"config channel: HTTP {resp.status_code} — falling back to sources.json", file=sys.stderr)
    except Exception as exc:  # noqa: BLE001 — the fallback must always engage
        print(f"config channel unreachable ({exc}) — falling back to sources.json", file=sys.stderr)
    return None


def load_feeds(cfg: dict | None) -> list[dict]:
    if cfg:
        feeds = [f for f in cfg["feeds"] if isinstance(f, dict) and f.get("enabled", True)]
        print(f"config: {len(feeds)} enabled feeds from the dashboard ({cfg.get('feeds_source')})")
    else:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "sources.json"), encoding="utf-8") as fh:
            feeds = [f for f in json.load(fh).get("sources", []) if f.get("enabled", True)]
        print(f"config: offline — {len(feeds)} enabled feeds from sources.json")
    # RANDOM scrape order (the owner's design): consecutive runs must not
    # hammer the sources in the same sequence. SCRAPER_SEED pins it for
    # reproducible test runs.
    # v6.35.0 — a non-integer SCRAPER_SEED repo variable used to crash
    # load_feeds with a raw traceback before any scraping; fall back to
    # the unseeded shuffle.
    try:
        random.Random(int(os.environ["SCRAPER_SEED"])).shuffle(feeds)
    except (KeyError, ValueError):
        random.shuffle(feeds)
    return feeds


def enrich_row(raw: dict, pool: ProviderPool, deadline: float) -> dict | None:
    """The messy-source lane (what the website does with its AI pool):
    extract title/company/location/salary/category/tags + an English
    description from raw posting text. Rows without this stay dropped —
    the site's own pipeline has the same company-required contract."""
    pool.enrich["calls"] += 1
    if time.time() > deadline:
        return None
    body = "\n".join(x for x in [f"Title: {raw['title']}" if raw["title"] else "",
                                 f"Location: {raw['location']}" if raw["location"] else "",
                                 clean(raw["description"], 1200)] if x)
    text = pool.chat(EXTRACT_SYSTEM,
                     EXTRACT_USER.format(categories=", ".join(CATEGORIES), raw=body),
                     want_json=True, deadline=deadline)
    if not text:
        return None
    data = extract_json(text)
    if not data:
        return None
    job = map_job(data.get("title") or raw["title"], data.get("company"), raw["url"],
                  data.get("location") or raw["location"],
                  data.get("description") or raw["description"],
                  data.get("salary") or raw["salary"],
                  data.get("tags") if isinstance(data.get("tags"), list) else raw["tags"],
                  data.get("category") or raw["category_hint"], None)
    if job and data.get("category") in CATEGORIES:
        job["category"] = data["category"]
    if job:
        pool.enrich["ok"] += 1
    return job


def post_pool_report(report: dict) -> None:
    """Run-end telemetry: the per-provider/per-slot verdicts land in
    ai_pool_state.scraper_pool on the edge (the admin AI-status panel
    renders them). Best-effort — never fails the run."""
    try:
        resp = SESSION.post(f"{SITE_URL}/api/ingest",
                            headers={"Authorization": f"Bearer {INGEST_SECRET}"},
                            json={"pool_report": report}, timeout=30)
        merged = ""
        try:
            merged = f", {resp.json().get('merged')} providers merged"
        except Exception:  # noqa: BLE001
            pass
        print(f"pool report: HTTP {resp.status_code}{merged}")
    except Exception as exc:  # noqa: BLE001
        print(f"pool report failed (non-fatal): {exc}", file=sys.stderr)


def fair_share_deal(queue: list[dict]) -> list[dict]:
    """TRUE round-robin interleave of the enrich queue per source lane:
    row 0 of every lane, then row 1 of every lane, … (zip_longest) — NOT
    per-lane concatenation, which would just re-create the FIFO
    starvation. Strips the _src lane tag so it never reaches the ingest
    payload. THE v6.34.1 LESSON: FIFO let gnews-linkedin's 60 rows eat
    the whole ENRICH_LIMIT while smaller lanes never saw AI budget."""
    lanes = collections.OrderedDict()
    for raw in queue:
        lanes.setdefault(raw.get("_src") or "?", []).append(raw)
    dealt = [row for tup in itertools.zip_longest(*lanes.values())
             for row in tup if row is not None]
    for raw in dealt:
        raw.pop("_src", None)
    return dealt


def main() -> int:
    if not INGEST_SECRET:
        print("FATAL: INGEST_SECRET is not set (GitHub repo secret)", file=sys.stderr)
        return 1
    cfg = fetch_config()
    feeds = load_feeds(cfg)
    pool = ProviderPool((cfg or {}).get("pool"))
    if cfg and cfg.get("pool"):
        print("pool: " + ", ".join(f"{p}×{len(ks)}" for p, ks in sorted(cfg["pool"].items())))
    else:
        print("pool: no provider keys configured — AI lanes idle (structured feeds unaffected)")

    seen_urls: set[str] = set()
    all_jobs: list[dict] = []
    enrich_queue: list[dict] = []
    failures: list[str] = []

    for feed in feeds:
        sid = feed.get("id") or feed.get("type")
        got = enqueued = 0
        try:
            for raw in feed_to_jobs(feed, pool):
                if not raw["url"] or raw["url"] in seen_urls:
                    continue
                if raw["company"]:
                    job = map_job(raw["title"], raw["company"], raw["url"], raw["location"],
                                  raw["description"], raw["salary"], raw["tags"],
                                  raw["category_hint"], raw["date"])
                    if job:
                        seen_urls.add(raw["url"])
                        all_jobs.append(job)
                        got += 1
                else:
                    # messy source row (search discovery, aggregators) —
                    # the AI enrich lane is its only path to a company.
                    seen_urls.add(raw["url"])
                    raw["_src"] = sid  # fair-share lane tag (see below)
                    enrich_queue.append(raw)
                    enqueued += 1
        except Exception as exc:  # noqa: BLE001 — one dead feed never kills the run
            failures.append(f"{sid}: {exc}")
            print(f"  source {sid} FAILED: {exc}", file=sys.stderr)
        print(f"{sid}: {got} jobs" + (f", {enqueued} queued for AI enrich" if enqueued else ""))

    # ── AI phase (bounded by ENRICH_LIMIT/POLISH_LIMIT + wall clock) ──────
    # THE LIVE 2026-11-24 LESSON: one bad row in this loop killed the whole
    # run (KeyError on the prompt template) AFTER the feeds were already
    # scraped — every per-row AI op is now individually guarded so a bug
    # costs ONE row, never the batch ship.
    #
    # THE SAME-RUN FAIR-SHARE LESSON (v6.34.1, 40/40 enriched, 100 dropped):
    # a FIFO queue lets the biggest single source (gnews-linkedin's 60 rows)
    # eat the whole ENRICH_LIMIT every run while smaller lanes (feed-17,
    # parallel-websearch) NEVER get AI budget. The queue is now dealt
    # round-robin per source: every lane advances together, and the biggest
    # source only takes its leftover share at the end.
    if enrich_queue:
        enrich_queue[:] = fair_share_deal(enrich_queue)
    deadline = time.time() + AI_BUDGET_S
    enriched = 0
    enrich_errors = 0
    for raw in enrich_queue[:ENRICH_LIMIT]:
        try:
            job = enrich_row(raw, pool, deadline)
        except Exception as exc:  # noqa: BLE001 — one bad row never kills the ship
            enrich_errors += 1
            print(f"    enrich failed ({raw['url'][:60]}): {exc}", file=sys.stderr)
            continue
        if job:
            all_jobs.append(job)
            enriched += 1
    if enrich_queue:
        skipped = max(0, len(enrich_queue) - enriched)
        print(f"enrich: {enriched}/{min(len(enrich_queue), ENRICH_LIMIT)} rows extracted, "
              f"{skipped} dropped" + (f", {enrich_errors} errors" if enrich_errors else ""))

    thin = [j for j in all_jobs if len(j["description"]) < 120][:POLISH_LIMIT]
    polished = 0
    for job in thin:
        try:
            ai_polish(job, pool, deadline)
            polished += 1 if len(job["description"]) >= 120 else 0  # v6.35.0 — count only real polish (the old `or pool.polish["ok"]` over-counted failures)
        except Exception as exc:  # noqa: BLE001 — polish is best-effort per row
            print(f"    polish failed ({job['url'][:60]}): {exc}", file=sys.stderr)

    print(f"\ntotal: {len(all_jobs)} unique jobs, {len(failures)} failed sources, "
          f"{enriched} AI-enriched, {pool.polish['ok']}/{len(thin)} polished")
    if not all_jobs:
        # v6.35.0 — audit M28: post the pool report BEFORE the zero-jobs
        # return — when every enrich row fails (all keys dead, the exact
        # scenario the AI-status panel exists to surface) the per-slot
        # verdicts used to never ship.
        report = pool.report()
        report["polish"]["dead_models"] = sorted(_dead_models)[:8]
        post_pool_report(report)
        return 0 if not failures else 2

    stored = duplicates = rejected = 0
    for i in range(0, len(all_jobs), MAX_BATCH):
        batch = all_jobs[i:i + MAX_BATCH]
        resp = None
        out = {}
        # v6.35.0 — audit M27: a 429 (the edge enforces 20/min; the old
        # fixed sleep(2) admitted ~30/min) DROPPED the batch for the run —
        # up to 50 jobs lost per hit. Retry with backoff instead; the lane
        # is idempotent (url dedupe + the API-side UNIQUE contract), so a
        # re-sent batch is always safe. Non-dict JSON bodies no longer
        # crash the loop either.
        for attempt in range(3):
            try:
                resp = SESSION.post(f"{SITE_URL}/api/ingest",
                                    headers={"Authorization": f"Bearer {INGEST_SECRET}"},
                                    json={"jobs": batch}, timeout=60)
                try:
                    body = resp.json()
                    out = body if isinstance(body, dict) else {}
                except Exception:  # noqa: BLE001
                    out = {}
                if resp.status_code == 429 and attempt < 2:
                    wait = 3.5 * (attempt + 1)
                    print(f"batch {i // MAX_BATCH + 1}: 429 — backing off {wait:.0f}s (attempt {attempt + 1}/3)", file=sys.stderr)
                    time.sleep(wait)
                    continue
                break
            except Exception as exc:  # noqa: BLE001
                print(f"batch {i // MAX_BATCH + 1}: transport failure ({type(exc).__name__}) — attempt {attempt + 1}/3", file=sys.stderr)
                if attempt < 2:
                    time.sleep(3.5 * (attempt + 1))
                    continue
                return 3
        if resp is None:
            return 3
        if resp.status_code == 200 and out.get("success"):
            n = (out.get("inserted") or 0) + (out.get("sb_inserted") or 0)
            stored += n
            duplicates += out.get("duplicates") or 0
            rejected += out.get("rejected") or 0
            print(f"batch {i // MAX_BATCH + 1}/{(len(all_jobs) + MAX_BATCH - 1) // MAX_BATCH}: "
                  f"+{n} stored, {out.get('duplicates') or 0} dup, {out.get('rejected') or 0} rejected"
                  + (f" [{out.get('error')}]" if out.get("error") else ""))
        else:
            print(f"batch {i // MAX_BATCH + 1}: HTTP {resp.status_code} {out.get('error')}", file=sys.stderr)
            if resp.status_code == 403:
                return 4  # secret mismatch — fail loud, never silently
            if resp.status_code not in (200, 409, 429):
                return 3
        time.sleep(3.2)  # v6.35.0 — ≥ the edge's 20/min ingest limiter (2s admitted 30/min and tail batches 429'd)

    # pool telemetry (best-effort, after the batches so a ship failure
    # still exits with its loud code first)
    report = pool.report()
    report["polish"]["dead_models"] = sorted(_dead_models)[:8]
    if any(s.get("calls") for s in report["providers"].values()) or report["polish"]["calls"] or report["enrich"]["calls"]:
        post_pool_report(report)

    print(f"\nDONE: {stored} stored, {duplicates} duplicates, {rejected} rejected, {len(failures)} failed sources")
    return 0


if __name__ == "__main__":
    sys.exit(main())
