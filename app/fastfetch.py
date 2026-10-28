# ============================================================
# app/fastfetch.py
# 高速・高信頼 YouTube 取得コア (動画情報 / ストリーム / コメント)
# ------------------------------------------------------------
# 設計:
#   1. ノード・ヘルススコア + サーキットブレーカ (2回失敗で60秒隔離)
#   2. ヘッジ付きレース (0/150/300ms... の段階発火、最速の有効応答を採用)
#   3. single-flight 重複排除 + TTL/ステイル再検証キャッシュ (連打に強い)
#   4. コメントは継続トークンで複数ページ取得 → commentId で重複排除
#   5. ストリームは muxed / videoOnly / audio / HLS に正規化
#      最終手段として nocookie 埋め込み URL を必ず返す
#   6. どのプロバイダが落ちても次へ自動降格。失敗は即スコアに反映
# ============================================================
from __future__ import annotations

import asyncio
import os
import sys
import time
from functools import partial
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

router = APIRouter()

UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)

NOCOOKIE = "https://www.youtube-nocookie.com/embed/"
THUMB = "https://i.ytimg.com/vi/{}/hqdefault.jpg"

INSTANCE_LIST_URLS = (
    "https://raw.githubusercontent.com/ikirikittsuhao-ctrl/Invidious-check/refs/heads/main/lists/video.json",
    "https://raw.githubusercontent.com/ikirikittsuhao-ctrl/Invidious-check/refs/heads/main/lists/search.json",
    "https://raw.githubusercontent.com/kuru-bana/yt-data/refs/heads/main/list/injidious.json",
)

SEED_INSTANCES = [
    "https://invidious.ritoge.com",
    "https://yt.omada.cafe",
    "https://invidious.f5.si",
    "https://y.com.sb",
    "https://invidious.nerdvpn.de",
    "https://inv.zoomerville.com",
]

PIPED_INSTANCES = [
    "https://pipedapi.wireway.ch",
    "https://api.piped.private.coffee",
    "https://pipedapi.winscloud.net",
]

SIA_BASE = os.environ.get("SIA_API_BASE", "https://siatube.com")
SENNIN_BASE = os.environ.get(
    "SENNIN_API_BASE",
    "https://discerning-adventure-production-ebfc.up.railway.app",
)
RAPIDAPI_HOST = "ytstream-download-youtube-videos.p.rapidapi.com"

INST_TTL = 600.0
HEDGE_DELAY = 0.15
DEFAULT_BUDGET = 6.0
COOLDOWN = 60.0

TTL = {"video": 300.0, "streams": 120.0, "comments": 120.0}
STALE = {"video": 3600.0, "streams": 1800.0, "comments": 1800.0}

_LIMITS = httpx.Limits(
    max_connections=300, max_keepalive_connections=100, keepalive_expiry=30.0
)
_TIMEOUT = httpx.Timeout(connect=2.0, read=4.5, write=4.5, pool=2.0)

_client: Optional[httpx.AsyncClient] = None


class FetchError(Exception):
    """全プロバイダ失敗時の例外。"""


def client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=_TIMEOUT,
            limits=_LIMITS,
            follow_redirects=True,
            headers={"User-Agent": UA},
            http2=False,
        )
    return _client


async def close() -> None:
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
    _client = None


# ------------------------------------------------------------
# ヘルススコア / サーキットブレーカ
# ------------------------------------------------------------
_health: Dict[str, Dict[str, float]] = {}


def _mark_ok(label: str, dt: float) -> None:
    st = _health.setdefault(
        label, {"fail": 0.0, "ok": 0.0, "lat": 0.0, "open_until": 0.0}
    )
    st["ok"] += 1
    st["fail"] = max(0.0, st["fail"] - 1)
    st["lat"] = dt if st["lat"] <= 0 else st["lat"] * 0.7 + dt * 0.3
    st["open_until"] = 0.0


def _mark_fail(label: str) -> None:
    st = _health.setdefault(
        label, {"fail": 0.0, "ok": 0.0, "lat": 0.0, "open_until": 0.0}
    )
    st["fail"] += 1
    if st["fail"] >= 2:
        st["open_until"] = time.monotonic() + COOLDOWN


def _score(label: str) -> float:
    st = _health.get(label)
    if not st:
        return 50.0
    now = time.monotonic()
    if st["open_until"] > now:
        return 1000.0 + (st["open_until"] - now)
    lat = st["lat"] if st["lat"] > 0 else 3.0
    return lat + st["fail"] * 2.0


Entry = Tuple[str, Callable[[], Awaitable[Any]]]


async def _run(label: str, factory: Entry[1], delay: float) -> Any:
    if delay:
        await asyncio.sleep(delay)
    return await factory()


async def _hedge(
    entries: List[Entry],
    budget: float = DEFAULT_BUDGET,
    cap: int = 8,
) -> Any:
    """ヘッジ付きレース。最速の有効応答を返し、残りは即キャンセル。"""
    ranked = sorted(entries, key=lambda e: _score(e[0]))[:cap]
    if not ranked:
        raise FetchError("no nodes available")

    loop = asyncio.get_running_loop()
    tasks = {
        loop.create_task(_run(label, factory, i * HEDGE_DELAY))
        for i, (label, factory) in enumerate(ranked)
    }
    pending = set(tasks)
    errors: List[str] = []
    deadline = time.monotonic() + budget
    try:
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            done, pending = await asyncio.wait(
                pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                break
            for t in done:
                if t.cancelled():
                    continue
                exc = t.exception()
                if exc is not None:
                    errors.append(str(exc))
                    continue
                res = t.result()
                if res is not None:
                    return res
        detail = "; ".join(errors[:4]) if errors else "budget exceeded"
        raise FetchError(detail)
    finally:
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def _get_json(
    label: str,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: Optional[httpx.Timeout] = None,
) -> Any:
    t0 = time.perf_counter()
    try:
        r = await client().get(
            url, params=params, headers=headers, timeout=timeout or _TIMEOUT
        )
    except Exception as exc:
        _mark_fail(label)
        raise FetchError(f"{label}: {type(exc).__name__}") from None
    if r.status_code != 200:
        _mark_fail(label)
        raise FetchError(f"{label}: HTTP {r.status_code}")
    try:
        data = r.json()
    except Exception:
        _mark_fail(label)
        raise FetchError(f"{label}: bad json") from None
    _mark_ok(label, time.perf_counter() - t0)
    return data


# ------------------------------------------------------------
# キャッシュ (TTL + ステイル再検証 + single-flight)
# ------------------------------------------------------------
_cache: Dict[str, Dict[str, Any]] = {}
_inflight: Dict[str, asyncio.Future] = {}
_CACHE_MAX = 3000


def _evict() -> None:
    now = time.monotonic()
    for k in [k for k, v in _cache.items() if v["hard"] < now]:
        _cache.pop(k, None)
    while len(_cache) > _CACHE_MAX:
        oldest = min(_cache, key=lambda k: _cache[k]["at"])
        _cache.pop(oldest, None)


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    e = _cache.get(key)
    if not e:
        return None
    if time.monotonic() > e["hard"]:
        _cache.pop(key, None)
        return None
    return e


def _cache_put(key: str, val: Any, ttl: float, stale: float) -> None:
    if len(_cache) > _CACHE_MAX:
        _evict()
    now = time.monotonic()
    _cache[key] = {"val": val, "fresh": now + ttl, "hard": now + ttl + stale, "at": now}


def _resolve(fut: asyncio.Future, val: Any = None, exc: Optional[BaseException] = None) -> None:
    if fut.done():
        return
    if exc is not None:
        fut.set_exception(exc)
        try:
            fut.exception()
        except BaseException:
            pass
    else:
        fut.set_result(val)


async def _refresh(key: str, factory: Entry[1], ttl: float, stale: float) -> None:
    if key in _inflight:
        return
    fut = asyncio.get_running_loop().create_future()
    _inflight[key] = fut
    try:
        val = await factory()
        _cache_put(key, val, ttl, stale)
        _resolve(fut, val)
    except BaseException as exc:
        _resolve(fut, exc=exc)
    finally:
        _inflight.pop(key, None)


async def cached(key: str, factory: Entry[1], kind: str) -> Any:
    e = _cache_get(key)
    if e:
        if time.monotonic() <= e["fresh"]:
            return e["val"]
        asyncio.create_task(_refresh(key, factory, TTL[kind], STALE[kind]))
        return e["val"]

    fut = _inflight.get(key)
    if fut is not None:
        return await asyncio.shield(fut)

    fut = asyncio.get_running_loop().create_future()
    _inflight[key] = fut
    try:
        val = await factory()
        _cache_put(key, val, TTL[kind], STALE[kind])
        _resolve(fut, val)
        return val
    except BaseException as exc:
        _resolve(fut, exc=exc)
        raise
    finally:
        _inflight.pop(key, None)


# ------------------------------------------------------------
# インスタンス登録簿
# ------------------------------------------------------------
_instances: List[str] = []
_instances_at = 0.0
_inst_lock = asyncio.Lock()


def _clean_base(u: str) -> str:
    u = u.strip().rstrip("/")
    if u and not u.startswith("http"):
        u = "https://" + u
    return u


def _parse_list(data: Any) -> List[str]:
    out: List[str] = []
    if not isinstance(data, list):
        return out
    for item in data:
        if isinstance(item, str):
            out.append(_clean_base(item))
        elif isinstance(item, dict):
            for k in ("instance", "url", "base", "api"):
                v = item.get(k)
                if isinstance(v, str) and v:
                    out.append(_clean_base(v))
                    break
    return [u for u in out if u.startswith("http")]


async def instances() -> List[str]:
    global _instances, _instances_at
    async with _inst_lock:
        if _instances and time.monotonic() - _instances_at < INST_TTL:
            return _instances
        found: List[str] = []
        for url in INSTANCE_LIST_URLS:
            try:
                r = await client().get(url, timeout=4.0)
                if r.status_code != 200:
                    continue
                found.extend(_parse_list(r.json()))
            except Exception:
                continue
        merged = list(dict.fromkeys([u for u in found + SEED_INSTANCES if u]))
        if merged:
            _instances = merged
            _instances_at = time.monotonic()
        return _instances or list(SEED_INSTANCES)


# ------------------------------------------------------------
# 正規化ヘルパ
# ------------------------------------------------------------
def _as_text(v: Any) -> str:
    if isinstance(v, dict):
        return str(v.get("text") or v.get("formatted") or v.get("html") or "")
    return str(v or "")


def _first_icon(d: Dict[str, Any]) -> str:
    for key in ("authorIcon", "uploaderAvatar", "avatar", "authorThumbnail"):
        v = d.get(key)
        if isinstance(v, str) and v:
            return v
    thumbs = d.get("authorThumbnails")
    if isinstance(thumbs, list):
        for t in reversed(thumbs):
            if isinstance(t, dict) and t.get("url"):
                return str(t["url"])
    return ""


def _int(v: Any) -> int:
    if isinstance(v, dict):
        v = v.get("count")
    try:
        return int(v or 0)
    except Exception:
        return 0


def _player(video_id: str) -> Dict[str, str]:
    return {"embed": NOCOOKIE + video_id, "thumbnail": THUMB.format(video_id)}


def normalize_video(d: Dict[str, Any], video_id: str, source: str) -> Dict[str, Any]:
    author = d.get("author") or d.get("uploader") or ""
    if isinstance(author, dict):
        author = author.get("name", "")
    desc = _as_text(d.get("description"))
    return {
        "videoId": d.get("videoId") or video_id,
        "title": d.get("title") or "",
        "author": author,
        "authorId": d.get("authorId") or "",
        "authorIcon": _first_icon(d),
        "subCountText": str(d.get("subCountText") or ""),
        "viewCount": _int(d.get("viewCount") or d.get("views")),
        "likeCount": _int(d.get("likeCount") or d.get("likes")),
        "lengthSeconds": _int(d.get("lengthSeconds") or d.get("duration")),
        "publishedText": d.get("publishedText") or d.get("published") or d.get("uploadedDate") or "",
        "description": desc,
        "descriptionHtml": desc.replace("\n", "<br>"),
        "thumbnail": d.get("thumbnail") or d.get("thumbnailUrl") or THUMB.format(video_id),
        "isLive": bool(d.get("liveNow") or d.get("isLive")),
        "source": source,
        "player": _player(video_id),
    }


def _stream_item(url: str, quality: str, mime: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    item = {
        "url": url,
        "resolution": quality or "auto",
        "mime": mime or "",
        "format": "mp4",
        "audioUrl": "",
    }
    if extra:
        item.update(extra)
    return item


def _extract_invidious_streams(d: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    muxed, video_only, audio = [], [], []
    for f in d.get("formatStreams") or []:
        if not isinstance(f, dict) or not f.get("url"):
            continue
        muxed.append(_stream_item(f["url"], f.get("qualityLabel") or f.get("quality", ""), f.get("type", "")))
    for f in d.get("adaptiveFormats") or []:
        if not isinstance(f, dict) or not f.get("url"):
            continue
        mime = str(f.get("type") or f.get("mimeType") or "")
        if "audio" in mime:
            audio.append(_stream_item(f["url"], f.get("audioQuality", ""), mime))
        elif "video" in mime:
            video_only.append(_stream_item(f["url"], f.get("qualityLabel") or f.get("quality", ""), mime))
    return {"muxed": muxed, "videoOnly": video_only, "audio": audio}


def _extract_piped_streams(d: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    muxed, video_only, audio, hls = [], [], [], []
    if d.get("hls"):
        hls.append({"url": d["hls"], "resolution": "HLS", "mime": "application/vnd.apple.mpegurl", "format": "m3u8", "audioUrl": ""})
    for s in d.get("videoStreams") or []:
        if not isinstance(s, dict) or not s.get("url"):
            continue
        item = _stream_item(s["url"], s.get("quality", ""), s.get("mimeType", ""), {"bitrate": s.get("bitrate", 0)})
        (video_only if s.get("videoOnly", True) else muxed).append(item)
    for s in d.get("audioStreams") or []:
        if isinstance(s, dict) and s.get("url"):
            audio.append(_stream_item(s["url"], s.get("quality", ""), s.get("mimeType", ""), {"bitrate": s.get("bitrate", 0)}))
    return {"muxed": muxed, "videoOnly": video_only, "audio": audio, "hls": hls}


def _extract_generic_streams(d: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Sia / Sennin / RapidAPI など揺れのある形をゆるく拾う。"""
    muxed, video_only, audio, hls = [], [], [], []
    buckets: List[Any] = []
    for key in ("formats", "adaptiveFormats", "muxed", "videoOnly", "audioOnly", "streams"):
        v = d.get(key)
        if isinstance(v, list):
            buckets.extend(v)
        elif isinstance(v, dict):
            for sub in v.values():
                if isinstance(sub, list):
                    buckets.extend(sub)
    for key in ("hls", "m3u8", "manifestUrl"):
        v = d.get(key)
        if isinstance(v, str) and v:
            hls.append({"url": v, "resolution": "HLS", "mime": "application/vnd.apple.mpegurl", "format": "m3u8", "audioUrl": ""})
        elif isinstance(v, dict):
            for sub in v.get("list") or []:
                if isinstance(sub, dict) and sub.get("streamUrl"):
                    q = sub.get("height") or sub.get("formatNote") or "HLS"
                    hls.append({"url": sub["streamUrl"], "resolution": str(q), "mime": "application/vnd.apple.mpegurl", "format": "m3u8", "audioUrl": ""})
    for s in buckets:
        if not isinstance(s, dict):
            continue
        url = s.get("url") or s.get("streamUrl")
        if not url:
            continue
        mime = str(s.get("mimeType") or s.get("type") or "")
        q = s.get("qualityLabel") or s.get("quality") or s.get("formatNote") or ""
        if mime.startswith("audio") or s.get("audioCodec") and not s.get("videoCodec"):
            audio.append(_stream_item(url, q, mime))
        elif s.get("videoOnly"):
            video_only.append(_stream_item(url, q, mime))
        else:
            muxed.append(_stream_item(url, q, mime))
    return {"muxed": muxed, "videoOnly": video_only, "audio": audio, "hls": hls}


def build_stream_payload(video_id: str, source: str, parts: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    audio_url = parts["audio"][0]["url"] if parts.get("audio") else ""
    for item in parts.get("videoOnly", []):
        item["audioUrl"] = audio_url
    best = ""
    if parts.get("muxed"):
        best = parts["muxed"][-1]["url"]
    elif parts.get("hls"):
        best = parts["hls"][0]["url"]
    elif parts.get("videoOnly"):
        best = parts["videoOnly"][0]["url"]
    return {
        "videoId": video_id,
        "source": source,
        "muxed": parts.get("muxed", []),
        "videoOnly": parts.get("videoOnly", []),
        "audio": parts.get("audio", []),
        "hls": parts.get("hls", []),
        "best": best,
        "hasVideo": bool(parts.get("muxed") or parts.get("videoOnly") or parts.get("hls")),
        "player": _player(video_id),
    }


def normalize_comment(c: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(c, dict):
        return None
    author = c.get("author")
    icon = ""
    if isinstance(author, dict):
        icon = author.get("avatar") or author.get("authorIcon") or ""
        author = author.get("name", "")
    author = author or c.get("authorName") or ""
    icon = icon or _first_icon(c)
    text = c.get("content") or c.get("text") or c.get("message") or c.get("commentText") or ""
    cid = c.get("commentId") or c.get("id") or ""
    if not cid and not text:
        return None
    replies = c.get("replies")
    if isinstance(replies, dict):
        replies = replies.get("count")
    return {
        "commentId": str(cid),
        "author": str(author),
        "authorId": c.get("authorId") or "",
        "authorIcon": icon,
        "text": str(text),
        "publishedText": c.get("publishedText") or c.get("publishedTime") or c.get("commentedTime") or "",
        "likeCount": _int(c.get("likeCount") or c.get("likes")),
        "replyCount": _int(replies or c.get("replyCount")),
        "isCreator": bool(c.get("isCreator") or c.get("creator")),
        "isPinned": bool(c.get("isPinned") or c.get("pinned")),
    }


def dedupe_comments(items: List[Dict[str, Any]], sort: str) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for c in items:
        key = c["commentId"] or c["text"][:120]
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    if sort == "top":
        out.sort(key=lambda c: (c["isPinned"], c["likeCount"]), reverse=True)
    return out


# ------------------------------------------------------------
# 動画情報
# ------------------------------------------------------------
async def _inv_video(video_id: str) -> Dict[str, Any]:
    nodes = await instances()

    async def one(base: str) -> Dict[str, Any]:
        d = await _get_json(base, f"{base}/api/v1/videos/{video_id}")
        if not isinstance(d, dict) or d.get("error") or not d.get("title"):
            _mark_fail(base)
            raise FetchError(f"{base}: invalid")
        return normalize_video(d, video_id, "invidious")

    return await _hedge([(b, partial(one, b)) for b in nodes], cap=5)


async def _piped_video(video_id: str) -> Dict[str, Any]:
    async def one(base: str) -> Dict[str, Any]:
        d = await _get_json(base, f"{base}/streams/{video_id}")
        if not isinstance(d, dict) or d.get("error") or not d.get("title"):
          
