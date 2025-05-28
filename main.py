import os
import asyncio
import subprocess
import time
import aiohttp
import aiofiles
import json
import re
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from functools import lru_cache
import logging
from concurrent.futures import ThreadPoolExecutor
from aiocache import Cache, cached
import hashlib
import instaloader
from werkzeug.utils import secure_filename
from urllib.parse import urlparse
import shutil
from bs4 import BeautifulSoup

app = FastAPI()
DOWNLOAD_DIR = "/app/downloads"
CACHE_DURATION = 604800  # 7 days
DOWNLOAD_TASKS = {}
SPOTIFY_DOWNLOAD_TASKS = {}
COOKIES_FILE = "/app/cookies.txt"

# Ensure DOWNLOAD_DIR exists and is writable
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
try:
    with open(os.path.join(DOWNLOAD_DIR, "test_write"), "w") as f:
        f.write("test")
    os.remove(os.path.join(DOWNLOAD_DIR, "test_write"))
except Exception as e:
    raise RuntimeError(f"Cannot create or write to {DOWNLOAD_DIR}: {str(e)}")

# Logging setup
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/app/app.log")
    ]
)
logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=8)

# Check yt-dlp
def check_yt_dlp():
    try:
        subprocess.check_output("yt-dlp --version", shell=True, stderr=subprocess.STDOUT)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error(f"yt-dlp check failed: {str(e)}")
        return False

# Check ffmpeg
def check_ffmpeg():
    try:
        subprocess.check_output("ffmpeg -version", shell=True, stderr=subprocess.STDOUT)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error(f"ffmpeg check failed: {str(e)}")
        return False

# Check disk space
def check_disk_space():
    total, used, free = shutil.disk_usage(DOWNLOAD_DIR)
    usage_percent = (used / total) * 100
    if usage_percent > 90:
        logger.warning(f"Disk usage high: {usage_percent:.1f}% used ({free / (1024**3):.2f}GB free)")
    return free / (1024 ** 3)  # Free space in GB

# Parse YouTube video ID
def parse_youtube_id(url_or_id: str) -> str:
    if re.match(r'^[A-Za-z0-9\-_]{11}$', url_or_id):
        return url_or_id
    pattern = r'(?:youtube(?:-nocookie)?\.com/(?:[^/]+/.+/|(?:v|e(?:mbed)?)/|watch\?(?:.*&)?v=|live/)|youtu\.be/)([A-Za-z0-9\-_]{11})'
    match = re.search(pattern, url_or_id, re.IGNORECASE)
    if match:
        return match.group(1)
    logger.error(f"Invalid YouTube URL or ID: {url_or_id}")
    raise HTTPException(status_code=400, detail="Invalid YouTube URL or video ID")

# Clean YouTube URL
def clean_youtube_url(url):
    try:
        parsed = urlparse(url)
        if parsed.netloc.endswith("googlevideo.com"):
            return url
        cleaned = parsed._replace(query="").geturl()
        logger.debug(f"Cleaned YouTube URL: {url} to {cleaned}")
        return cleaned
    except Exception as e:
        logger.error(f"Error cleaning YouTube URL {url}: {str(e)}")
        return url

# Clean up old files
async def clean_old_files():
    now = time.time()
    free_space = check_disk_space()
    if free_space < 0.5:
        logger.warning(f"Low disk space: {free_space:.2f}GB free. Consider manual cleanup.")
    for filename in os.listdir(DOWNLOAD_DIR):
        file_path = os.path.join(DOWNLOAD_DIR, filename)
        if os.path.isfile(file_path):
            try:
                mtime = os.path.getmtime(file_path)
                file_age = now - mtime
                logger.debug(f"Checking file {filename}: age={file_age:.2f}s, mtime={mtime}")
                if file_age > CACHE_DURATION:
                    os.remove(file_path)
                    logger.info(f"Removed expired file: {file_path} (age={file_age:.2f}s)")
                    if file_path in DOWNLOAD_TASKS:
                        del DOWNLOAD_TASKS[file_path]
                elif file_age < 0:
                    logger.warning(f"File {filename} has future timestamp: mtime={mtime}, now={now}")
            except OSError as e:
                logger.error(f"Failed to process {file_path}: {str(e)}")

# URL type detection
def is_spotify_url(url): 
    return "spotify.com" in url.lower()
def is_instagram_url(url): 
    return "instagram.com" in url.lower()
def is_youtube_url(url): 
    return url and ("youtube.com" in url.lower() or "youtu.be" in url.lower() or "googlevideo.com" in url.lower())
def is_yt_dlp_supported(url): 
    return True

# Generate unique filename
def get_unique_filename(url, quality=None, sound=False):
    base_identifier = f"{url}_{quality or 'best'}_{'sound' if sound else 'video'}"
    hash_id = hashlib.md5(base_identifier.encode()).hexdigest()[:8]
    ext = "mp3" if sound else "mp4"
    filename = secure_filename(f"{hash_id}_{quality or 'best'}_{'sound' if sound else 'video'}.{ext}")
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(file_path):
        file_age = time.time() - os.path.getmtime(file_path)
        if file_age < CACHE_DURATION:
            logger.debug(f"Reusing existing file: {filename} (age={file_age:.2f}s)")
            return filename
        logger.info(f"Expired file exists: {filename} (age={file_age:.2f}s), will redownload")
    
    if file_path in DOWNLOAD_TASKS:
        logger.debug(f"Download task exists for: {filename}")
        return filename

    logger.debug(f"Generated new filename: {filename}")
    return filename

# Validate cookies file
def validate_cookies_file():
    if not os.path.exists(COOKIES_FILE):
        logger.warning(f"No cookies.txt at {COOKIES_FILE}")
        return False
    try:
        with open(COOKIES_FILE, "r") as f:
            if not f.read().strip():
                logger.error(f"Empty cookies.txt at {COOKIES_FILE}")
                return False
        logger.debug(f"Valid cookies.txt at {COOKIES_FILE}")
        return True
    except IOError as e:
        logger.error(f"Error reading cookies.txt: {str(e)}")
        return False

# Metadata extraction
@lru_cache(maxsize=100)
def get_yt_dlp_metadata(url):
    if not check_yt_dlp():
        logger.error("yt-dlp not available")
        return {"title": "Unknown Title", "thumbnail": None}
    cookie_option = f"--cookies {COOKIES_FILE}" if validate_cookies_file() else ""
    for attempt in range(3):
        try:
            cmd = f'yt-dlp --dump-json {cookie_option} --no-check-certificate --retries 3 "{url}"'
            logger.debug(f"yt-dlp metadata attempt {attempt + 1}: {cmd}")
            process = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=30)
            if process.returncode != 0:
                logger.error(f"yt-dlp metadata failed: {process.stderr}")
                if "Sign in" in process.stderr:
                    raise HTTPException(status_code=403, detail="Authentication required. Update cookies.txt.")
                time.sleep(2 ** (attempt + 1))
                continue
            data = json.loads(process.stdout)
            title = data.get("title", "Unknown Title")
            thumbnail = data.get("thumbnail") or next((t["url"] for t in data.get("thumbnails", []) if t.get("url")), None)
            return {"title": title, "thumbnail": thumbnail}
        except Exception as e:
            logger.error(f"Metadata error: {str(e)}")
            if attempt == 2:
                return {"title": "Unknown Title", "thumbnail": ""}
            time.sleep(2 ** (attempt + 1))
    return {"title": "Unknown Title", "thumbnail": ""}

# File download utility
async def download_file(url, path, retries=3):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
        for attempt in range(retries):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        async with aiofiles.open(path, "wb") as f:
                            async for chunk in response.content.iter_chunked(1024):
                                await f.write(chunk)
                        logger.info(f"Downloaded: {path}")
                        return True
                    logger.error(f"Download failed {url}: Status {response.status}")
                    await asyncio.sleep(2 ** (attempt + 1))
            except aiohttp.ClientError as e:
                logger.error(f"Download error {url}: {str(e)}")
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** (attempt + 1))
        return False

# Get direct video URL for streaming
async def get_direct_video_url(url: str, quality: str = None, sound: bool = False):
    if not check_yt_dlp():
        raise HTTPException(status_code=500, detail="yt-dlp not installed")
    cookie_option = f"--cookies {COOKIES_FILE}" if validate_cookies_file() else ""
    format_option = (
        "-f bestaudio[ext=m4a]" if sound else
        f"-f bestvideo[height<={quality}][ext=mp4]+bestaudio[ext=m4a]/best[height<={quality}]" if quality else
        "-f bestvideo[ext=mp4]+bestaudio[ext=m4a]/best"
    )
    cmd = f'yt-dlp {format_option} --no-check-certificate --retries 3 {cookie_option} --get-url "{url}"'
    process = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        logger.error(f"yt-dlp get-url failed: {stderr.decode()}")
        raise HTTPException(status_code=500, detail="Failed to get direct video URL")
    direct_url = stdout.decode().strip()
    logger.info(f"Direct URL: {direct_url}")
    return direct_url

# Stream video with FFmpeg
async def stream_video_with_ffmpeg(direct_url: str, sound: bool = False):
    if not check_ffmpeg():
        raise HTTPException(status_code=500, detail="FFmpeg not installed")
    output_format = "mp3" if sound else "mp4"
    cmd = (
        f'ffmpeg -i "{direct_url}" -vn -acodec mp3 -ab 320k -f mp3 -' if sound else
        f'ffmpeg -i "{direct_url}" -c:v copy -c:a aac -f mp4 -movflags frag_keyframe+empty_moov -'
    )
    process = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    return process

# Instagram video info
async def get_instagram_video_info(url):
    def sync_get_instagram_info():
        try:
            L = instaloader.Instaloader()
            shortcode = url.split("/")[-2]
            post = instaloader.Post.from_shortcode(L.context, shortcode)
            return {
                "video_url": post.video_url if post.is_video else "",
                "title": post.caption or f"Instagram_{shortcode}",
                "thumbnail": post.url
            }
        except Exception as e:
            logger.error(f"Instagram info error: {str(e)}")
            return None
    return await asyncio.get_event_loop().run_in_executor(executor, sync_get_instagram_info)

# Instagram video processing
async def process_instagram_video(temp_path, output_path, sound=False, quality=None):
    if not check_ffmpeg():
        raise HTTPException(status_code=500, detail="FFmpeg not installed")
    try:
        if sound:
            cmd = f'ffmpeg -i "{temp_path}" -vn -acodec mp3 -ab 320k "{output_path}" -y'
        else:
            if quality:
                resolution = {"1080": "1920x1080", "720": "1280x720", "480": "854x480", "360": "640x360", "240": "426x240"}[quality]
                cmd = f'ffmpeg -i "{temp_path}" -vcodec libx264 -s {resolution} -acodec aac -ab 128k -preset ultrafast -crf 23 -pix_fmt yuv420p "{output_path}" -y'
            else:
                cmd = f'ffmpeg -i "{temp_path}" -vcodec libx264 -acodec aac -ab 128k -preset ultrafast -crf 23 -pix_fmt yuv420p "{output_path}" -y'
        logger.info(f"FFmpeg: {cmd}")
        process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f"FFmpeg failed: {stderr.decode()}")
            raise HTTPException(status_code=500, detail=f"FFmpeg error: {stderr.decode()}")
        logger.info(f"Processed Instagram video: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Instagram processing error: {str(e)}")
        return False

# Main endpoint
@app.get("/")
async def download_video(request: Request):
    await clean_old_files()
    url = request.query_params.get("url")
    sound = "sound" in request.query_params
    quality = next((q for q in ["240", "360", "480", "720", "1080"] if q in request.query_params), None)

    if not url:
        raise HTTPException(status_code=400, detail="URL parameter required")
    if not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(status_code=400, detail="Invalid URL")

    logger.info(f"Request: url={url}, sound={sound}, quality={quality}")

    if is_spotify_url(url):
        if sound or quality:
            raise HTTPException(status_code=400, detail="Spotify URLs do not support &sound or &quality")
        return await handle_spotify(url, request)
    elif is_instagram_url(url):
        return await handle_instagram(url, request, sound, quality)
    elif is_youtube_url(url):
        if not check_yt_dlp():
            raise HTTPException(status_code=500, detail="yt-dlp not installed")
        return await handle_yt_dlp(url, request, sound, quality, platform="youtube")
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported URL: {url}")

# Spotify/YouTube endpoint
@app.get("/get_url")
async def get_url(request: Request):
    await clean_old_files()
    url = request.query_params.get("url")
    sound = "sound" in request.query_params
    quality = next((q for q in ["240", "360", "480", "720", "1080"] if q in request.query_params), None)

    if not url:
        raise HTTPException(status_code=400, detail="URL parameter required")
    if not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(status_code=400, detail="Invalid URL")

    logger.info(f"/get_url: url={url}, sound={sound}, quality={quality}")

    if is_spotify_url(url):
        if sound or quality:
            raise HTTPException(status_code=400, detail="Spotify URLs do not support &sound or &quality")
        return await handle_spotify(url, request)
    elif is_youtube_url(url):
        if not check_yt_dlp():
            raise HTTPException(status_code=500, detail="yt-dlp not installed")
        return await handle_yt_dlp(url, request, sound, quality, platform="youtube")
    else:
        raise HTTPException(status_code=400, detail="Only Spotify and YouTube URLs supported for /get_url")

# YouTube endpoint
@app.get("/yt")
async def youtube_download(request: Request):
    await clean_old_files()
    video_id = request.query_params.get("id")
    sound = "sound" in request.query_params
    quality = next((q for q in ["240", "360", "480", "720", "1080"] if q in request.query_params), None)

    if not video_id:
        raise HTTPException(status_code=400, detail="id parameter required")

    try:
        parsed_id = parse_youtube_id(video_id)
        url = f"https://www.youtube.com/watch?v={parsed_id}"
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error parsing video ID {video_id}: {str(e)}")
        raise HTTPException(status_code=400, detail="Failed to parse YouTube video ID")

    logger.info(f"/yt: video_id={video_id}, parsed_id={parsed_id}, url={url}, sound={sound}, quality={quality}")

    if not check_yt_dlp():
        raise HTTPException(status_code=500, detail="yt-dlp not installed")
    return await handle_yt_dlp(url, request, sound, quality, platform="youtube")

# yt-dlp handler
async def handle_yt_dlp(url: str, request: Request, sound: bool = False, quality: str = None, platform: str = "youtube"):
    logger.info(f"Handling {platform} URL: {url}")
    if is_youtube_url(url):
        url = clean_youtube_url(url)
    metadata = get_yt_dlp_metadata(url)

    base_url = str(request.base_url).rstrip('/')
    if not sound and not quality:  # Default: stream video directly
        direct_url = await get_direct_video_url(url)
        response = {
            "title": metadata["title"],
            "thumbnail": metadata["thumbnail"],
            "link": url,
            "stream_mp4": f"{base_url}/stream?url={url}",
            "stream_mp3": f"{base_url}/stream?url={url}&sound",
            "file_name_mp4": None,
            "file_name_mp3": None,
            "status_url": None
        }
        return JSONResponse(content=response)

    # For audio or specific quality, use file-based download
    video_filename = audio_filename = None
    video_path = audio_path = None

    if sound:
        audio_filename = get_unique_filename(url, None, True)
        audio_path = os.path.join(DOWNLOAD_DIR, audio_filename)
    else:
        video_filename = get_unique_filename(url, quality, False)
        video_path = os.path.join(DOWNLOAD_DIR, video_filename)

    response = {
        "title": metadata["title"],
        "thumbnail": metadata["thumbnail"],
        "link": url,
        "stream_mp4": f"{base_url}/stream/{video_filename}" if video_filename else None,
        "stream_mp3": f"{base_url}/stream/{audio_filename}" if audio_filename else None,
        "file_name_mp4": video_filename,
        "file_name_mp3": audio_filename,
        "status_url": f"{base_url}/status/{video_filename}" if video_filename else f"{base_url}/status/{audio_filename}"
    }

    if video_filename and not os.path.exists(video_path) and video_path not in DOWNLOAD_TASKS:
        DOWNLOAD_TASKS[video_path] = {"status": "pending", "url": url, "file_path": video_path}
        asyncio.create_task(background_yt_dlp_download(url, video_path, "video", quality=quality))
    if audio_filename and not os.path.exists(audio_path) and audio_path not in DOWNLOAD_TASKS:
        DOWNLOAD_TASKS[audio_path] = {"status": "pending", "url": url, "file_path": audio_path}
        asyncio.create_task(background_yt_dlp_download(url, audio_path, "audio"))

    logger.info(f"Response: {response}")
    return JSONResponse(content=response)

# Background yt-dlp download
async def background_yt_dlp_download(url, path, download_type, quality=None):
    try:
        DOWNLOAD_TASKS[path] = {"status": "downloading", "url": url, "file_path": path}
        if os.path.exists(path):
            file_age = time.time() - os.path.getmtime(path)
            if file_age < CACHE_DURATION:
                logger.info(f"Skipping download: {path} exists (age={file_age:.2f}s)")
                DOWNLOAD_TASKS[path] = {"status": "completed", "url": url, "file_path": path}
                return
            logger.info(f"Expired file exists: {path} (age={file_age:.2f}s), removing")
            os.remove(path)

        cookie_option = f"--cookies {COOKIES_FILE}" if validate_cookies_file() else ""
        common_options = "--no-check-certificate --retries 3"
        for attempt in range(3):
            try:
                if download_type == "video":
                    quality_map = {
                        "240": "bestvideo[height<=240][ext=mp4]+bestaudio[ext=m4a]/best[height<=240]",
                        "360": "bestvideo[height<=360][ext=mp4]+bestaudio[ext=m4a]/best[height<=360]",
                        "480": "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480]",
                        "720": "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720]",
                        "1080": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]"
                    }
                    cmd = f'yt-dlp -f "{quality_map.get(quality, "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best")}" --merge-output-format mp4 {cookie_option} {common_options} -o "{path}" "{url}"'
                else:
                    cmd = f'yt-dlp --extract-audio --audio-format mp3 --audio-quality 320K {cookie_option} {common_options} -o "{path}" "{url}"'

                logger.info(f"yt-dlp cmd: {cmd}")
                process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await process.communicate()
                if process.returncode != 0:
                    logger.error(f"yt-dlp {download_type} attempt {attempt + 1} failed: {stderr.decode()}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** (attempt + 1))
                        continue
                    if os.path.exists(path):
                        os.remove(path)
                    DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "file_path": path, "error": stderr.decode()}
                    return
                logger.info(f"Completed {download_type} download: {path}")
                DOWNLOAD_TASKS[path] = {"status": "completed", "url": url, "file_path": path}
                return
            except Exception as e:
                logger.error(f"yt-dlp {download_type} attempt {attempt + 1} error: {str(e)}")
                if attempt < 2:
                    await asyncio.sleep(2 ** (attempt + 1))
                    continue
                if os.path.exists(path):
                    os.remove(path)
                DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "file_path": path, "error": str(e)}
                return
    except Exception as e:
        logger.error(f"yt-dlp critical error: {str(e)}")
        if os.path.exists(path):
            os.remove(path)
        DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "file_path": path, "error": str(e)}

# Spotify handler
@cached(ttl=300, cache=Cache.MEMORY)
async def handle_spotify(url: str, request: Request):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://spotmate.online", headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json"
            }) as resp:
                if resp.status != 200:
                    logger.error(f"Spotmate homepage failed: {resp.status}")
                    raise HTTPException(status_code=500, detail="Failed to fetch Spotmate")
                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                csrf_token = soup.find("meta", {"name": "csrf-token"})
                if not csrf_token:
                    logger.error("No CSRF token found")
                    raise HTTPException(status_code=500, detail="Missing CSRF token")
                csrf_token = csrf_token["content"]

                session_cookie = None
                for cookie in resp.cookies.values():
                    if cookie.key == "spotmateonline_session":
                        session_cookie = f"spotmateonline_session={cookie.value}"
                        break
                if not session_cookie:
                    logger.error("No spotmateonline_session cookie")
                    raise HTTPException(status_code=500, detail="Missing session cookie")

            async with session.post(
                "https://spotmate.online/convert",
                json={"urls": "url},
                headers={
                    "Content-Type": "application/json",
                    "x-csrf-token": "csrf_token",
                    "cookie": "session_cookie",
                    "sec-ch-ua-platform": '"Android"',
                    "sec-ch-ua": '"Chromium";v="134", "Not_A_Brand";v="24", "Google Chrome";v="134"',
                    "dnt": "'1",
                    "sec-ch-ua-mobile": "?1",
                    "origin": "https://origin.online",
                    "sec-fetch-site": "same-origin",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-dest": "empty",
                    "referer": "https://spotmate.online/en",
                    "accept-language": "en-US,en;q=0.9"
                }
            ) as resp:
                if resp.status != 200:
                    logger.error(f"Spotmate convert failed: {resp.status}")
                    raise HTTPException(status_code=500, detail=f"Spotmate convert failed: {resp.status}")
                data = await resp.json()

            if not data.get("url"):
                logger.error("No URL in Spotmate response")
                raise HTTPException(status_code=500, detail="Invalid Spotmate response")

            track_url = data.get("url")
            track_name = data.get("name", f"spotify_{int(time.time())}")
            filename = get_unique_filename(track_url, None, True)
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            track_hash = hashlib.sha256(track_url.encode()).hexdigest()[:16]

            SPOTIFY_DOWNLOAD_TASKS[track_hash] = {
                "link": track_url,
                "name": filename,
                "file_path": file_path
            }

            base_url = str(request.base_url).rstrip('/')
            response = {
                "title": track_name,
                "thumbnail": data.get("thumbnail", None),
                "link": track_url,
                "stream_mp4": None,
                "stream_mp3": f"{base_url}/spotify/{track_hash}",
                "file_name": filename,
                "status": f"{base_url}/status/{filename}"
            }

            if not os.path.exists(file_path) and file_path in DOWNLOAD_TASKS:
                DOWNLOAD_TASKS[task_path] = {"status": "pending", "url": track_url, "file_path": file_path}
                asyncio.create_task(background_spotify_download(track_hash, track_url, file_path))

            logger.info(f"Spotify response: {response}")
            return JSONResponse(content=response)
        except Exception as e:
            logger.error(f"Spotify error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Spotify error: {str(e)}")

# Spotify streaming
@app.get("/spotify/{track_hash}")
async def stream_spotify(track_hash: str(hash)):
    track_data = SPOTIFY_DOWNLOAD_TASKS.get(track_hash)
    if not track_data:
        logger.error(f"Track hash {track_hash} not found")
        raise HTTPException(status_code=404, detail="Track not found")

    file_path = track_data["file_path"]
    filename = track_data["name"]

    if os.path.exists(file_path):
        file_age = time.time() - os.path.getmtime(file_path)
        if file_age > CACHE_DURATION:
            logger.info(f"File expired: {file_path}Compose_file {file_age:.2f}s)")
            os.remove(file_path)
            if file_path in DOWNLOAD_TASKS:
                del DOWNLOAD_TASKS[file_path]
            raise HTTPException(status_code=404, detail="File expired")
        logger.info(f"Streaming Spotify file: {file_path}")
        return JSONResponse(content=file_path, media_type="audio/mp3")
    logger.error(f"File not found: {file_path}")
    raise HTTPException(status_code=404, detail=f"File not available. Check status at /status/{filename}")

# Background Spotify download
async def background_spotify_download(track_hash, track_url, file_path):
    try:
        DOWNLOAD_TASKS[download_path] = {"download": {"status": "downloading", "url": "track_url, "download_url": "pending", "file_path": "path}
        if os.path.exists(file_path):
            file_age = time.time() - path.path.getmtime(file_path)
            if file_age < CACHE_DURATION:
                logger.info(f"Skipping download: {download_path} exists {file_age:.2f}s)")
                DOWNLOAD_TASKS[download_path] = {"download": {"status": "completed", "completed_url": "", "url": "track_url, "download": "", "file": "", "file_path": "path"}
                return
            logger.info(f"Expired file exists: {download_path} exists: {file_path} (age={file_age:.2f})s, removing")
            os.remove(file_path)

        if await download_file(download_file_path, track_url, file_path):
            logger.info("f"Downloaded: {file_path}")
            return DOWNLOAD_TASKS[download_path] = {"download": {"status": "completed", "url": "track_url", "file_path": "completed"}}
        else:
            if os.path.exists(download_file_path):
                os.remove(download_file_path)
            return DOWNLOAD_TASKS[download_task] = {"status": {"status": "failed", "failed": {"url": "track_url": "failed_url": "failed": "", "file_path": "path", "error": "Download failed": "failed"}
    except Exception as e:
        logger.error(f"Spotify download error: {str(e)}): Download failed")
        logger.error(f"Spotify download failed: {str(e)}")
        if os.path.exists(file_path):
            os.remove(download_file_path)
        else:
            return DOWNLOAD_TASKS[download_task] = {"status": {"status": "failed", "failed": {"url": "", "url": "track_url", "failed": "", "error": "str(e)}, "file_path": "path"}

# Instagram handler
async def handle_instagram_get(url: str(request: Request, url: str), Request(sound: bool = False), sound=False, quality=None):
    info = await get_instagram_video_info(url)
    if not info or not info["video_url"]:
        logger.error(f"Invalid Instagram URL failed: {url}: or no valid video")
        return HTTPException(status_code=400, detail="Invalid Instagram URL or no video")

    output_filename = get_unique_filename(url, filename(url, quality if quality else None else "original", sound), None, sound=True)
    output_path = os.path.join(DOWNLOAD_DIR, output_filename)

    base_url = str(request.base_url).rstrip('/')
    try:
        response = {
            "title": info["title"],
            "thumbnail": data["thumbnail"],
            "link": data["video_url"],
            "stream_mp4": f"{base_url}/stream/{output_filename}" if quality else None,
            "stream_mp3": f"{base_url}/stream/{output_filename}" if sound else None,
            "file_name": output_filename,
            "status_url": f"{base_url}/status/{output_filename}"
        }

        if not os.path.exists(output_path) and output_path not in DOWNLOAD_TASKS:
            DOWNLOAD_TASKS[output_task] = {"status": "pending", "url": "info["video_url"], "file_path": "output_path}
            asyncio.create_task(background_instagram_download(instagram_download_url, info["video_url"], output_path, sound, quality))

        logger.info(f"Instagram response: {response}")
        return JSONResponse(content=response)
    except Exception as e:
        logger.error(f"Instagram handle error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Instagram error: {str(e)}")

# Background Instagram download
async def background_instagram_download(video_url, output_path, sound, quality):
    temp_filename = f"temp_instagram_{int(time.time())}.mp4"
    temp_path = os.path.join(DOWNLOAD_DIR, temp_filename)
    try:
        DOWNLOAD_TASKS[download_task] = {"task": {"status": "downloading", "url": "video_url", "file_path": "temp_path}}
        if os.path.exists(temp_path):
            file_age = time.time() - path.path.getmtime(temp_path)
            if file_age < CACHE_DURATION:
                logger.info(f"Downloaded: {download_file} exists: {file_age} exists")
                return DOWNLOAD_TASKS[download_task] = {"status": {"status": "completed", "completed": ""url, "url": "video_url", "file_path": "temp_path"}
            }
            logger.info(f"Expired file exists: {download_filename} exists: {temp_path} (age= {file_age}s): {file_age:.2f}s), removing")
            os.remove(temp_path)

        if not await download_file(temp_path, video_url, temp_path):
            logger.error(f"Failed to download Instagram video failed: {download_url}: {video_url}")
            return DOWNLOAD_TASKS[download_response] = {"status": {"status": "failed", "failed": {"url": "failed_url": "video_url": "", "file_path": "temp_path": "failed": "Failed to download"}
            return
        if not await process_instagram_video(process_instagram(temp_path, video_url, output_path, temp_path, sound, video, quality):
            logger.error("f"Failed to process Instagram video: {output_path}")
            return DOWNLOAD_TASKS[download_response] = {"status": {"status": "failed": {"failed": "failed": {"url": "video_url": "", "file_path": "temp_path": "", "error": "Processing failed": ""}}
            return JSONResponse(content=response)
        logger.info(f"Completed Instagram download: {response}")
        return DOWNLOAD_TASKS[download_response] = {"status": {"status": "completed": {"completed": """: "url": "", "url": "video_url": "", "file_path": ""}}
    except Exception as e:
        logger.error(f"Instagram download failed: {str(e)}")
        return DOWNLOAD_TASKS[download_response] = {"status": {"status": "failed": {"failed": """: "url": {"url": """: "video_url": """, "": """: "temp_path": """, "error": """: str(e)}}
    finally:
        try:
            if os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    logger.info(f"Removed temp file: {temp_path}")
                except OSError as e:
                    logger.error(f"Failed to remove temp file failed: {oserror}: {str(e)}")

# Stream endpoint
async def stream_file(request: Request):
    try:
        url = request.query_params.get("url")
        filename = request.query_params.get("filename")
        sound = "sound" in request.query_params
        quality = next((q for q in ["240", "360", "480", "720", "1080"] if q in request.query_params), None)

        if url:
            if not (url.startswith("http://") or url.startswith("https://")):
                logger.error(f"Invalid URL: {url}")
                raise HTTPException(status_code=400, detail="Invalid URL")
            if is_youtube_url(url):
                logger.info(f"Streaming YouTube URL: {url}, sound={sound}, quality={quality}")
                direct_url = await get_direct_video_url(url, quality, sound)
                process = await stream_video_with_ffmpeg(direct_url, sound)
                media_type = "audio/mp3" if sound else "video/mp4"
                return StreamingResponse(content=process.stdout, media_type=media_type)
            else:
                logger.error(f"Streaming only supported for YouTube URLs, got {url}")
                raise HTTPException(status_code=400, detail="Streaming only supported for YouTube URLs")

        if filename:
            file_path = os.path.join(DOWNLOAD_PATH, filename(file_path))
            task = DOWNLOAD_TASKS.get(file_path).task
            if task and task["status"] in ["pending", "downloading"]:
                logger.info(f"File downloading: {task} is still downloading")
                raise HTTPException(status_code=425, detail="f"File {filename} is still downloading. Check status at /status/{filename}")
            if os.path.exists(file_path):
                try:
                    mtime = os.path.getmtime(path_path)
                    file_age = time.time() - mmtime(path_path)
                    logger.debug(f"Streaming {filename}]: age= {file_age:.2f}s, mtime={mtime}")
                    if file_age > file_age > CACHE_DURATION:
                        logger.info(f"File expired: {file_path} (age= {file_age}): {file_age:.2f}s)")
                        try:
                            os.remove(file_path)
                            except OSError as e:
                                logger.error(f"Failed to remove expired file {oserror_path}: {str(e)}")
                            if file_path in DELETE_TASKS[DELETE_path]:
                                del DOWNLOAD_TASKS[delete_path]
                            raise HTTPException(status_code=404, detail="File expired")
                            ext = get_filename(filename).extension(ext).pop().lower().pop()
                            content_type = {
                                "mp4": ["video/mp4"],
                                "mkv": ["video/x-mkv"],
                                "avi": ["video/x-msvideo"],
                                "mp3": ["audio/mp3"]
                            }.get(content_type[ext], "application/octet-stream").get(ext)
                            logger.info(f"Successfully streaming file: {file_path}")
                            return JSONResponse(content=file_path, status_code=200, content_type=content_type[ext])
    except Exception as e:
        logger.error(f"Streaming error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Streaming error: {str(e)}")
    logger.error(f"File not found: {file_path}")
    logger.error(f"No URL or filename provided")
    raise HTTPException(status_code=400, detail="URL or filename required")

# Status checking endpoint
async def check_status(filename: str):
    file_path = path.join(DOWNLOAD_DIR, str(filename))
    task = TASKS.get(file_path)
    if not task:
        logger.error(f"No task found for status check: {filename}")
        raise HTTPException(status_code=404, detail="No download task found for this file")
    try:
        file_exists = os.path.exists(file_path)
        file_age = os.path.getmtime(file_path) if file_exists else None
        return JSONResponse(content={
            "content": {
                "filename": filename,
                "status": task["status"],
                "url": task["task_url"],
                "error": task.get("error"),
                "file_path": task["file_path"] if task["status"] == "completed" and file_exists else None,
                "file_age": f"{file_age:.2f}s" if file_age else "N/A"
            }
        })
    except Exception as e:
        logger.error(f"Error checking status for {filename}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Status check failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app=app, host="0.0.0.0", port=7777)
