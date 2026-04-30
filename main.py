import asyncio
import jwt
import time
import httpx
import re
import json
import os
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from filelock import FileLock

app = FastAPI()

# --- CONFIGS ---
JWKS_URL = "https://nudfmlkthqgtrxbigcss.supabase.co/auth/v1/jwks"
CACHE_FILE = "/tmp/cache.json"
RATE_FILE = "/tmp/rate.json"
JWKS_FILE = "/tmp/jwks.json"
ERRORS_FILE = "/tmp/errors.json"

# --- FILE OPS (non-blocking) ---
def _sync_file_op(file_path, mode, data=None):
    lock = FileLock(file_path + ".lock")
    with lock:
        if mode == "read":
            if not os.path.exists(file_path):
                return {}
            try:
                with open(file_path, "r") as f:
                    return json.load(f)
            except:
                return {}
        elif mode == "write":
            tmp = file_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(data, f)
            os.replace(tmp, file_path)
            return data

async def safe_file_op(file_path, mode, data=None):
    return await asyncio.to_thread(_sync_file_op, file_path, mode, data)

# --- JWT ---
async def get_jwks(force_refresh=False):
    cache = await safe_file_op(JWKS_FILE, "read")
    now = time.time()
    if force_refresh or "keys" not in cache or now > cache.get("expire", 0):
        async with httpx.AsyncClient() as client:
            resp = await client.get(JWKS_URL)
            keys = resp.json()["keys"]
            cache = {"keys": keys, "expire": now + 86400}
            await safe_file_op(JWKS_FILE, "write", cache)
    return cache["keys"]

async def verify_jwt(token: str):
    if not token:
        raise HTTPException(status_code=401, detail="Token ausente")
    keys = await get_jwks()
    header = jwt.get_unverified_header(token)
    key_data = next((k for k in keys if k["kid"] == header["kid"]), None)
    if not key_data:
        keys = await get_jwks(force_refresh=True)
        key_data = next((k for k in keys if k["kid"] == header["kid"]), None)
        if not key_data:
            raise HTTPException(status_code=401, detail="Chave nao encontrada")
    pub_key = jwt.algorithms.RSAAlgorithm.from_jwk(key_data)
    try:
        payload = jwt.decode(token, pub_key, algorithms=["RS256"], audience="authenticated")
        return payload["sub"]
    except Exception:
        raise HTTPException(status_code=401, detail="Token invalido")

# --- RATE LIMIT ---
async def check_rate(user_id: str):
    rates = await safe_file_op(RATE_FILE, "read")
    now = time.time()
    user_history = [t for t in rates.get(user_id, []) if now - t < 60]
    if len(user_history) >= 30:
        raise HTTPException(status_code=429, detail="Muitas requisicoes")
    rates[user_id] = user_history + [now]
    await safe_file_op(RATE_FILE, "write", rates)

# --- CACHE ---
async def get_cached_url(video_id: str):
    cache = await safe_file_op(CACHE_FILE, "read")
    item = cache.get(video_id)
    if item and item.get("expire", 0) > time.time():
        return item["url"]
    return None

async def set_cached_url(video_id: str, url: str):
    cache = await safe_file_op(CACHE_FILE, "read")
    cache[video_id] = {"url": url, "expire": time.time() + 18000}
    await safe_file_op(CACHE_FILE, "write", cache)

# --- ALERT ---
async def alert_if_needed():
    errors = await safe_file_op(ERRORS_FILE, "read")
    now = time.time()
    recent = [t for t in errors.get("times", []) if now - t < 60]
    recent.append(now)
    await safe_file_op(ERRORS_FILE, "write", {"times": recent})

# --- CLEAN TITLE ---
def clean_title(title: str) -> str:
    title = title.lower()
    title = re.sub(r"\(.*?\)|\[.*?\]", "", title)
    title = re.sub(r"official video|lyrics|ft\..*|feat\..*|prod\..*", "", title)
    return re.sub(r"\s+", " ", title).strip()

# --- YTDLP ---
async def extract_url(video_id: str):
    url = f"https://www.youtube.com/watch?v={video_id}"
    cmd = [
        "yt-dlp",
        "--no-warnings",
        "--print", "%(url)s\x1f%(title)s\x1f%(uploader)s",
        "-f", "ba[ext=m4a]/ba[ext=mp4]/ba",
        "--no-playlist",
        url
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=20.0)
        if proc.returncode == 0:
            parts = stdout.decode().strip().split("\x1f")
            if len(parts) >= 1:
                stream_url = parts[0].strip()
                title = parts[1].strip() if len(parts) > 1 else ""
                uploader = parts[2].strip() if len(parts) > 2 else ""
                return stream_url, title, uploader
    except asyncio.TimeoutError:
        proc.kill()
    await alert_if_needed()
    return None, None, None

# --- DEEZER FALLBACK ---
async def get_deezer_preview(query: str):
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://api.deezer.com/search?q={query}",
                timeout=10.0
            )
            data = resp.json()
            for item in data.get("data", []):
                if item.get("preview"):
                    return item["preview"]
    except:
        pass
    return None

# --- RESOLVE URL FINAL (evita perda de Range no redirect) ---
async def resolve_final_url(url: str) -> str:
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            r = await client.head(url, timeout=10.0)
            return str(r.url)
    except:
        return url

# --- ENDPOINTS ---

@app.get("/health")
async def health():
    try:
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp", "--version",
            stdout=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        version = stdout.decode().strip()
        return {"status": "ok", "yt_dlp_version": version}
    except:
        return {"status": "error", "yt_dlp_version": "unknown"}

@app.get("/stream")
async def stream(
    request: Request,
    video_id: str = Query(...),
    token: str = Query(...),
    title: str = Query(""),
    artist: str = Query("")
):
    user_id = await verify_jwt(token)
    await check_rate(user_id)

    # Verifica cache
    cached_url = await get_cached_url(video_id)
    stream_url = cached_url
    meta_title = title
    meta_artist = artist

    # Extrai se nao tem cache
    if not stream_url:
        stream_url, meta_title, meta_artist = await extract_url(video_id)
        if stream_url:
            await set_cached_url(video_id, stream_url)

    # Fallback Deezer
    if not stream_url:
        query = clean_title(f"{title} {artist}") if title else video_id
        stream_url = await get_deezer_preview(query)
        if not stream_url:
            raise HTTPException(status_code=404, detail="Musica nao disponivel")

    # Resolve URL final (evita perda de Range em redirects)
    final_url = await resolve_final_url(stream_url)

    # Repassa Range request (para seek funcionar)
    headers = {"User-Agent": "Mozilla/5.0"}
    range_header = request.headers.get("range")
    if range_header:
        headers["Range"] = range_header

    async def generate():
        async with httpx.AsyncClient() as client:
            async with client.stream("GET", final_url, headers=headers) as r:
                async for chunk in r.aiter_bytes(chunk_size=131072):
                    yield chunk

    response_headers = {}
    try:
        async with httpx.AsyncClient() as client:
            head_r = await client.head(final_url, headers=headers, timeout=10.0)
            if "content-range" in head_r.headers:
                response_headers["Content-Range"] = head_r.headers["content-range"]
            if "content-length" in head_r.headers:
                response_headers["Content-Length"] = head_r.headers["content-length"]
            response_headers["Accept-Ranges"] = "bytes"
    except:
        pass

    status_code = 206 if range_header else 200
    return StreamingResponse(
        generate(),
        status_code=status_code,
        media_type="audio/mp4",
        headers=response_headers
    )

@app.post("/prefetch")
async def prefetch(
    request: Request,
    token: str = Query(...)
):
    await verify_jwt(token)
    body = await request.json()
    video_ids = body.get("ids", [])[:3]

    async def _prefetch_sequential():
        for vid in video_ids:
            cached = await get_cached_url(vid)
            if not cached:
                url, _, _ = await extract_url(vid)
                if url:
                    await set_cached_url(vid, url)
                await asyncio.sleep(0.5)

    asyncio.create_task(_prefetch_sequential())
    return {"status": "prefetch iniciado"}

@app.get("/wake")
async def wake():
    return {"status": "servidor acordado"}
