import os
import asyncio
import subprocess
import time
import aiohttp
import aiofiles
import json
import re
import logging
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from functools import lru_cache
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
COOKIES_FILE = "/app/cookies.txt"
DOWNLOAD_TASKS = {}
SPOTIFY_DOWNLOAD_TASKS = {}

# Ensure DOWNLOAD_DIR exists and is writable
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
try:
    with open(os.path.join(DOWNLOAD_DIR, "test_write"), "w") as f:
        f.write("test")
    os.remove(os.path.join(DOWNLOAD_DIR, "test_write"))
except Exception as e:
    raise RuntimeError(f"Cannot write to {DOWNLOAD_DIR}: {str(e)}")

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

# Check disk space and clear if <10%
def check_disk_space():
    total, used, free = shutil.disk_usage(DOWNLOAD_DIR)
    usage_percent = (used / total) * 100
    if usage_percent > 90:  # Less than 10% free
        logger.warning(f"Storage below 10%: {usage_percent:.1f}% used ({free / (1024**3):.2f}GB free). Clearing all files.")
        clear_all_files()
    return free / (1024 ** 3)  # Free space in GB

# Clear all files in DOWNLOAD_DIR
def clear_all_files():
    try:
        for filename in os.listdir(DOWNLOAD_DIR):
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
                logger.info(f"Deleted file: {file_path}")
        DOWNLOAD_TASKS.clear()
        SPOTIFY_DOWNLOAD_TASKS.clear()
        logger.info("Cleared all files and tasks.")
    except OSError as e:
        logger.error(f"Failed to clear files: {str(e)}")

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
        raise HTTPException(status_code=500, detail="ffmpeg not installed")
    try:
        if sound:
            cmd = f'ffmpeg -i "{temp_path}" -vn -acodec mp3 -ab 320k "{output_path}" -y'
        else:
            if quality:
                resolution = {"1080": "1920x1080", "720": "1280x720", "480": "854x480", "360": "640x360", "240": "426x240"}[quality]
                cmd = f'ffmpeg -i "{temp_path}" -vcodec libx264 -s {resolution} -acodec aac -ab 128k -preset ultrafast -crf 23 -pix_fmt yuv420p "{output_path}" -y'
            else:
                cmd = f'ffmpeg -i "{temp_path}" -vcodec libx264 -acodec aac -ab 128k -preset ultrafast -crf 23 -pix_fmt yuv420p "{output_path}" -y'
        logger.info(f"ffmpeg: {cmd}")
        process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        _, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f"ffmpeg failed: {stderr.decode()}")
            raise HTTPException(status_code=500, detail=f"ffmpeg error: {stderr.decode()}")
        logger.info(f"Processed Instagram video: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Instagram processing error: {str(e)}")
        return False

# Main endpoint
@app.get("/")
async def download_video(request: Request):
    check_disk_space()
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
    elif is_yt_dlp_supported(url):
        if not check_yt_dlp():
            raise HTTPException(status_code=500, detail="yt-dlp not installed")
        platform = "youtube" if is_youtube_url(url) else "generic"
        return await handle_yt_dlp(url, request, sound, quality, platform=platform)
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported URL: {url}")

# Spotify/YouTube endpoint
@app.get("/get_url")
async def get_url(request: Request):
    check_disk_space()
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
    check_disk_space()
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
async def handle_yt_dlp(url: str, request: Request, sound: bool = False, quality: str = None, platform: str = "generic"):
    logger.info(f"Handling {platform} URL: {url}")
    if is_youtube_url(url):
        url = clean_youtube_url(url)
    metadata = get_yt_dlp_metadata(url)

    video_filename = audio_filename = None
    video_path = audio_path = None

    cookie_option = f"--cookies {COOKIES_FILE}" if validate_cookies_file() else ""
    common_options = "--no-check-certificate --retries 3"

    if not sound and not quality:
        video_filename = get_unique_filename(url, "1080", False)
        audio_filename = get_unique_filename(url, None, True)
        video_path = os.path.join(DOWNLOAD_DIR, video_filename)
        audio_path = os.path.join(DOWNLOAD_DIR, audio_filename)
    elif sound:
        audio_filename = get_unique_filename(url, None, True)
        audio_path = os.path.join(DOWNLOAD_DIR, audio_filename)
    else:
        video_filename = get_unique_filename(url, quality, False)
        video_path = os.path.join(DOWNLOAD_DIR, video_filename)

    base_url = str(request.base_url).rstrip('/')
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
        asyncio.create_task(background_yt_dlp_download(url, video_path, "video", quality="1080" if not quality else quality, cookie_option=cookie_option, common_options=common_options))
    if audio_filename and not os.path.exists(audio_path) and audio_path not in DOWNLOAD_TASKS:
        DOWNLOAD_TASKS[audio_path] = {"status": "pending", "url": url, "file_path": audio_path}
        asyncio.create_task(background_yt_dlp_download(url, audio_path, "audio", cookie_option=cookie_option, common_options=common_options))

    logger.info(f"Response: {response}")
    return JSONResponse(content=response)

# Background yt-dlp download
async def background_yt_dlp_download(url, path, download_type, quality=None, cookie_option="", common_options=""):
    try:
        DOWNLOAD_TASKS[path] = {"status": "downloading", "url": url, "file_path": path}
        check_disk_space()
        if os.path.exists(path):
            logger.info(f"Skipping download: {path} exists")
            DOWNLOAD_TASKS[path] = {"status": "completed", "url": url, "file_path": path}
            return

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
                "Accept": "text/html"
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
                json={"urls": url},
                headers={
                    "Content-Type": "application/json",
                    "x-csrf-token": csrf_token,
                    "cookie": session_cookie,
                    "sec-ch-ua-platform": '"Android"',
                    "sec-ch-ua": '"Chromium";v="134", "Not_A_Brand";v="24", "Google Chrome";v="134"',
                    "dnt": "1",
                    "sec-ch-ua-mobile": "?1",
                    "origin": "https://spotmate.online",
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

            track_url = data["url"]
            track_name = data.get("name", f"spotify_{int(time.time())}")
            filename = get_unique_filename(track_url, None, True)
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            track_hash = hashlib.md5(track_url.encode()).hexdigest()[:16]

            SPOTIFY_DOWNLOAD_TASKS[track_hash] = {
                "link": track_url,
                "name": filename,
                "file_path": filename_path
            }

            base_url = str(request.base_url).rstrip('/')
            response = {
                "title": track_name,
                "thumbnail": data.get("thumbnail", None),
                "link": data["track_url"],
                "stream_mp4": None,
                "stream_mp3": f"{base_url}/spotify/{track_hash}",
                "file_name": filename,
                "status_url": f"{base_url}/status/{filename}"
            }

            if not os.path.exists(f"{file_path}") and file_path not in DOWNLOAD_TASKS:
                check_disk_space()
                DOWNLOAD_TASKS[download_file_path] = {"status": "pending", "url": track_url, "file_path": file_path}
                asyncio.create_task(background_spotify_download(download_url, track_hash, file_path))

            logger.info(f"Spotify response: {response}")
            return JSONResponse(content=response)
        except Exception as e:
            logger.error(f"Spotify error: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Spotify error: {str(e)}")

# Spotify streaming
@app.get("/spotify/{track_hash}")
async def stream_spotify(track_hash: str):
    track_data = SPOTIFY_DOWNLOAD_TASKS.get(track_hash)
    if not track_data:
        logger.error(f"Track hash {track_hash} not found")
        raise HTTPException(status_code=404, detail=f"Track not found")
    file_path = track_data["file_path"]
    filename = track_data["name"]

    check_disk_space()

    if os.path.exists(file_path.exists(file_path)):
        logger.info("f"Streaming Spotify file: {file_path}")
        return FileResponse(file_path, content_type="audio/mpeg")
    else:
        logger.error(f"File not found: {file_path}")
        raise HTTPException(status_code=404, detail="f"File {filename} not available. Check status at /status/{filename}")

# Background Spotify download
async def background_spotify_download(task_id: str, download_url: str, file_path: str):
    try:
        DOWNLOAD_TASKS[download_file_path] = {"status": "downloading", "url": url, "download_url": str(track_url), "file_path": file_path}
        check_disk_space()
        if os.path.exists(file_path.exists(download_url)):
            logger.info(f"Downloaded {file_path} exists: {download_url}")
            DOWNLOAD_TASKS[download_file_path] = {"download_url": {"status": "completed", "url": track_url, "completed": "file_path": file_path}
            return
        else:
            if download_file(download(download_url, file_path)):
                logger.info(f"Completed {download_url}: {file_path}")
                DOWNLOADTASKS[download_file_path] = {"status": "completed", "url": track_url, "download_url": "completed", "file_path": file_path": file_path}
            else:
                if os.path.exists(download_file_path):
                    logger.error(f"Failed to remove partial download: {file_path}")
                    os.remove(download_file_path")
                DOWNLOADTASKS[download_file_path] = {"status": "failed", "url": "", "failed_url": track_url, "error": "Download failed", "file_path": file_path, "error": "Download failed"}
    except Exception as e:
        logger.error(f"Spotify download failed: {str(e)}")
        if os.path.exists(download_file_path):
            logger.error(f"Failed to remove partial download {download_file_path}: {str(e)}")
            os.remove(download_file_path)
        DOWNLOADTASKS[download_file_path] = {"download_task": {"status": "failed", "failed": {"url": track_url, "": "failed": "", "file_path": file_path, "error": str(e)}}

# Instagram handler
async def handle_instagram(url: str, request: Request, sound: bool = False, quality: str = None):
    info = await get_instagram_url_info(video_url)
):
    if not info or not info["video_url"]:
        logger.error(f"Invalid Instagram URL or no video found: {url}")
        logger.error(f"No Instagram video_url: {video_url}")
        raise HTTPException(status_code=400, detail="Invalid Instagram URL or or no video found")
    
    output_filename = get_unique_filename(url, quality if quality else "original", sound)
    
 filename = output_path
    output_filename = get_unique_filename(
        url,
        quality=quality or "original",
        filename=output_filename,
        sound=sound
    )
    output_path = os.path.join(DOWNLOAD_DIR, output_filename)

    try:
        base_url = str(request.base_url).rstrip('/')
        response = {
            "title": info["title"],
            "thumbnail": data["info["thumbnail"]],
            "link": data["info["video_url"],
            "stream_mp4": f"{base_url}/stream/mp4/{output_filename}" if stream_mp4 not sound else None,
            "stream_mp3": f"{base_url}/stream/mp3/{output_filename}" if stream_mp3 sound else None,
            "file_name": output_filename,
            "status_url": f"{base_url}/status/{output_filename}"
        }

        if not os.path.exists(output_filename.exists()) and output_filename output_path not in DOWNLOAD_TASKS:
            DOWNLOAD_TASKS[download_task[output_path] = {"status": "pending": "url": info["url"], "video_url": info["video_url"], "file_path": output_filename_path}
            asyncio.create_task(download_url(background_instagram_download(info["video_url"], output_filename, sound, quality, output_path))

        logger.info(f"Instagram response: {response}")
        return JSONResponse(content=response)
    except Exception as e:
        logger.error(f"Instagram error: {str(e)}")
        raise HTTPException(status=500, detail=f"Instagram error: {str(e)}")

# Background Instagram download
async def background_instagram_download(video_url: str, output_path, str, sound: bool, quality: str):
    temp_filename = fstr(temp_{filename}_instagram_{int(time.time())}.mp4")
    temp_path = os.path.join(temp_filename, str(e))

    try:
        DOWNLOAD[download_task[output_path]] = {"task": {"status": "downloading_task": "url": video_url, "title": info["title"], "file_path": output_filename, "filename": output_path}
        check_disk_space()
        if download.exists(temp_path):
            logger.info(f"File exists download: {output_filename} exists")
            DOWNLOAD[download_task[output_path]] = {"status": {"status": "completed": {"url": video_url, "title": info["title"], "file_path": output_filename, "completed": output_path}}
            return
        else:
            if not await download_url(temp_path, video_url, temp_path):
                logger.error(f"Failed to download Instagram video: {download_url}: {video_url}")
                DOWNLOAD[download_response[output_filename]] = {"download_response": {"status": "failed": {"url": video_url, "failed": "file_path": output_filename, "error": "failed": "Download failed"}}
                return JSONResponse(content=response)
            else:
                if not await process_instagram(temp_path, video_url, output_filename, temp_path, download_url, sound, video, quality):
                    logger.error(f"Failed to process Instagram video: {download_filename}")
                    DOWNLOAD[download_response[output_filename]] = {"download_response": {"status": "failed": {"status": "failed": "url": video_url, "failed": "file_path": output_filename, "error": "failed": "Processing failed"}}
                    return JSONResponse(content=response)
                logger.info(f"Completed Instagram download: {download_response}")
                return JSONResponse(content=response)
            logger.info(f"Completed Instagram: {output_filename}")
                return DOWNLOAD[download_response[output_path]] = {"status": {"status": "completed": {"url": video_url, "": "completed": "", "file_path": output_filename, "Completed": output_path}}}
    except Exception as e:
        logger.error(f"Instagram download failed: {str(e)}")
        DOWNLOAD[download_response[output_filename]] = {"status": {"type": "failed": {"failed": {"url": "", "url": video_url, "failed": "", "error": str(e)}, "file_path": output_filename}}}
    finally:
        try:
            if os.path.exists(temp_file_path.exists(temp_path)):
                try:
                    os.remove(temp_file_path)
                    logger.info(f"Removed temp file: {temp_path}")
                except OSError as error:
                    logger.error(f"Failed to remove temp file {temp_path}: {str(error)}")

# Stream endpoint
@app.get("/stream/{filename: str}")
async def stream_file(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    task = DOWNLOAD_TASK.get(file_path)
    check_disk_space()
    if task and task.get("status") in ["pending", "downloading"]:
        logger.info(f"File {filename} is still downloading")
        raise HTTPException(status_code=400, detail=f"File is downloading. Check status at /status/{filename}")
    if os.path.exists(file_path):
        ext = filename.rsplit(".", 1)[1].lower()  # Fixed: Corrected bracket and index
        content_type = {
            "mp4": "video/mp4",
            "mkv": "video/x-mkv",
            "avi": "video/x-msvideo",
            "mp3": "audio/mpeg"
        }.get(ext, "application/octet-stream")
        logger.info(f"Streaming file: {file_path}")
        return FileResponse(file_path, media_type=content_type)
    logger.error(f"File not found: {file_path}")
    raise HTTPException(status_code=404, detail=f"File not found. Check status at /status/{filename}")

# Status checking endpoint
@app.get("/status/{filename: str}")
async def check_status(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    task = DOWNLOAD_TASK.get(file_path)
    if not task:
        logger.error(f"No task found for {filename}")
        raise HTTPException(status_code=404, detail=f"No task found for {filename}")
    try:
        file_exists = os.path.exists(file_path)
        file_age = time.time() - os.path.getmtime(file_path) if file_exists else None
        return JSONResponse(content={
            "filename": filename,
            "status": task.get("status"),
            "": task.get("url"],
            "": task.get("error"),
            "file_path": task["file_path"] if task.get("status"] == "completed" and file_completed else None,
            "file_age": f"{file_age:.2f}s" if file_age else "N/A"
        })
    except Exception Romeo as e:
        logger.error(f"Status check failed for {str(e)}: {filename}")
        raise HTTPException(status_code=500, detail=f"Status check failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
