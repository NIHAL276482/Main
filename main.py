import os
import asyncio
import subprocess
import time
import aiohttp
import aiofiles
import json
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from functools import lru_cache
import logging
from concurrent.futures import ThreadPoolExecutor
from aiocache import Cache, cached
import base64
import hashlib
import instaloader
from werkzeug.utils import secure_filename
from urllib.parse import urlparse, urlunparse
import uuid
from bs4 import BeautifulSoup
from http.cookiejar import MozillaCookieJar

app = FastAPI()
DOWNLOAD_DIR = "/app/downloads"
CACHE_DURATION = 43200  # 12 hours
DOWNLOAD_TASKS = {}
SPOTIFY_DOWNLOAD_TASKS = {}
COOKIES_FILE = "/app/cookies.txt"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

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

# Clean YouTube URL
def clean_youtube_url(url):
    try:
        parsed = urlparse(url)
        if parsed.netloc.endswith("googlevideo.com"):
            return url
        cleaned = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
        logger.debug(f"Cleaned YouTube URL: {url} to {cleaned}")
        return cleaned
    except Exception as e:
        logger.error(f"Error cleaning YouTube URL {url}: {str(e)}")
        return url

# Clean up old files
async def clean_old_files():
    now = time.time()
    for filename in os.listdir(DOWNLOAD_DIR):
        file_path = os.path.join(DOWNLOAD_DIR, filename)
        if os.path.isfile(file_path):
            file_age = now - min(os.path.getmtime(file_path), os.path.getctime(file_path))
            if file_age > CACHE_DURATION:
                try:
                    os.remove(file_path)
                    logger.info(f"Removed old file: {file_path}")
                    if file_path in DOWNLOAD_TASKS:
                        del DOWNLOAD_TASKS[file_path]
                except OSError as e:
                    logger.error(f"Failed to remove {file_path}: {str(e)}")

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
    file_path = None
    for existing_path, task in DOWNLOAD_TASKS.items():
        if task["url"] == url and base_identifier in existing_path:
            file_path = existing_path
            break
    if file_path and os.path.exists(file_path):
        file_age = time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path))
        if file_age < CACHE_DURATION:
            logger.debug(f"Reusing filename: {os.path.basename(file_path)}")
            return os.path.basename(file_path)
    
    unique_id = str(uuid.uuid4())[:8]
    identifier = f"{base_identifier}_{unique_id}"
    hash_id = hashlib.md5(identifier.encode()).hexdigest()[:8]
    ext = "mp3" if sound else "mp4"
    filename = secure_filename(f"{hash_id}_{quality or 'best'}_{'sound' if sound else 'video'}.{ext}")
    logger.debug(f"Generated filename: {filename}")
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
                    raise HTTPException(403, "Authentication required. Update cookies.txt.")
                if attempt == 2:
                    return {"title": "Unknown Title", "thumbnail": None}
                time

.sleep(2 ** (attempt + 1))
                continue
            data = json.loads(process.stdout)
            title = data.get("title", "Unknown Title")
            thumbnail = data.get("thumbnail") or next((t["url"] for t in data.get("thumbnails", []) if t.get("url")), None)
            return {"title": title, "thumbnail": thumbnail}
        except Exception as e:
            logger.error(f"Metadata error: {str(e)}")
            return {"title": "Unknown Title", "thumbnail": None}

# File download utility
async def download_file(url, path):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    async with aiofiles.open(path, "wb") as f:
                        async for chunk in response.content.iter_chunked(1024):
                            await f.write(chunk)
                    logger.info(f"Downloaded: {path}")
                    return True
                logger.error(f"Download failed {url}: Status {response.status}")
                return False
        except aiohttp.ClientError as e:
            logger.error(f"Download error {url}: {str(e)}")
            return False

# Instagram video info
async def get_instagram_video_info(post_url):
    def sync_get_instagram_info():
        try:
            L = instaloader.Instaloader()
            shortcode = post_url.split("/")[-2]
            post = instaloader.Post.from_shortcode(L.context, shortcode)
            return {
                "video_url": post.video_url if post.is_video else None,
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
        raise HTTPException(500, "ffmpeg not installed")
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
            raise HTTPException(500, f"ffmpeg error: {stderr.decode()}")
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
        raise HTTPException(400, "URL parameter required")
    if not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(400, "Invalid URL")

    logger.info(f"Request: url={url}, sound={sound}, quality={quality}")

    if is_spotify_url(url):
        if sound or quality:
            raise HTTPException(400, "Spotify URLs do not support &sound or &quality")
        return await handle_spotify(url, request)
    elif is_instagram_url(url):
        return await handle_instagram(url, request, sound, quality)
    elif is_yt_dlp_supported(url):
        if not check_yt_dlp():
            raise HTTPException(500, "yt-dlp not installed")
        platform = "youtube" if is_youtube_url(url) else "generic"
        return await handle_yt_dlp(url, request, sound, quality, platform=platform)
    else:
        raise HTTPException(400, f"Unsupported URL: {url}")

# Spotify/YouTube endpoint
@app.get("/get_url")
async def get_url(request: Request):
    await clean_old_files()
    url = request.query_params.get("url")
    sound = "sound" in request.query_params
    quality = next((q for q in ["240", "360", "480", "720", "1080"] if q in request.query_params), None)

    if not url:
        raise HTTPException(400, "URL parameter required")
    if not (url.startswith("http://") or url.startswith("https://")):
        raise HTTPException(400, "Invalid URL")

    logger.info(f"/get_url: url={url}, sound={sound}, quality={quality}")

    if is_spotify_url(url):
        if sound or quality:
            raise HTTPException(400, "Spotify URLs do not support &sound or &quality")
        return await handle_spotify(url, request)
    elif is_youtube_url(url):
        if not check_yt_dlp():
            raise HTTPException(500, "yt-dlp not installed")
        return await handle_yt_dlp(url, request, sound, quality, platform="youtube")
    else:
        raise HTTPException(400, "Only Spotify and YouTube URLs supported for /get_url")

# YouTube endpoint
@app.get("/yt")
async def youtube_download(request: Request):
    await clean_old_files()
    video_id = request.query_params.get("id")
    sound = "sound" in request.query_params
    quality = next((q for q in ["240", "360", "480", "720", "1080"] if q in request.query_params), None)

    if not video_id:
        raise HTTPException(400, "id parameter required")

    # Check if video_id is a full URL or just ID
    if video_id.startswith("http"):
        url = video_id
    else:
        url = f"https://www.youtube.com/watch?v={video_id}"

    logger.info(f"/yt: video_id={video_id}, url={url}, sound={sound}, quality={quality}")

    if not check_yt_dlp():
        raise HTTPException(500, "yt-dlp not installed")
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
        "file_name_mp3": audio_filename
    }

    if video_filename and not os.path.exists(video_path) and video_path not in DOWNLOAD_TASKS:
        DOWNLOAD_TASKS[video_path] = {"status": "pending", "url": url}
        asyncio.create_task(background_yt_dlp_download(url, video_path, "video", quality="1080" if not quality else quality, cookie_option=cookie_option, common_options=common_options))
    if audio_filename and not os.path.exists(audio_path) and audio_path not in DOWNLOAD_TASKS:
        DOWNLOAD_TASKS[audio_path] = {"status": "pending", "url": url}
        asyncio.create_task(background_yt_dlp_download(url, audio_path, "audio", cookie_option=cookie_option, common_options=common_options))

    logger.info(f"Response: {response}")
    return JSONResponse(response)

# Background yt-dlp download
async def background_yt_dlp_download(url, path, download_type, quality=None, cookie_option="", common_options=""):
    try:
        DOWNLOAD_TASKS[path] = {"status": "downloading", "url": url}
        if os.path.exists(path) and time.time() - min(os.path.getmtime(path), os.path.getctime(path)) < CACHE_DURATION:
            logger.info(f"Skipping download: {path} exists")
            DOWNLOAD_TASKS[path] = {"status": "completed", "url": url}
            return

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
        _, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f"yt-dlp {download_type} failed: {stderr.decode()}")
            if os.path.exists(path):
                os.remove(path)
            DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "error": stderr.decode()}
        else:
            logger.info(f"Completed {download_type} download: {path}")
            DOWNLOAD_TASKS[path] = {"status": "completed", "url": url}
    except Exception as e:
        logger.error(f"yt-dlp error: {str(e)}")
        if os.path.exists(path):
            os.remove(path)
        DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "error": str(e)}

# Spotify handler
@cached(ttl=300, cache=Cache.MEMORY)
async def handle_spotify(url: str, request: Request):
    async with aiohttp.ClientSession() as session:
        try:
            # Fetch spotmate.online homepage
            async with session.get("https://spotmate.online", headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "text/html"
            }) as resp:
                if resp.status != 200:
                    logger.error(f"Spotmate homepage failed: {resp.status}")
                    raise HTTPException(500, "Failed to fetch Spotmate")
                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")
                csrf_token = soup.find("meta", {"name": "csrf-token"})["content"]

                # Extract session cookie
                session_cookie = None
                for cookie in resp.cookies.values():
                    if cookie.key == "spotmateonline_session":
                        session_cookie = f"spotmateonline_session={cookie.value}"
                        break
                if not session_cookie:
                    logger.error("No spotmateonline_session cookie")
                    raise HTTPException(500, "Missing session cookie")

            # POST to convert endpoint
            async with session.post(
                "https://spotmate.online/convert",
                json={"urls": url},
                headers={
                    "Content-Type": "application/json",
                    "x-csrf-token": csrf_token,
                    "cookie": session_cookie,
                    "sec-ch-ua-platform": '"Android"',
                    "sec-ch-ua": '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
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
                    raise HTTPException(500, f"Spotmate convert failed: {resp.status}")
                data = await resp.json()

            if not data.get("url"):
                logger.error("No URL in Spotmate response")
                raise HTTPException(500, "Invalid Spotmate response")

            track_url = data["url"]
            track_name = data.get("name", f"spotify_{int(time.time())}")
            filename = get_unique_filename(track_url, None, True)
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            track_hash = base64.urlsafe_b64encode(hashlib.md5(track_url.encode()).digest()).decode()[:24]

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
                "file_name": filename
            }

            if not os.path.exists(file_path) and file_path not in DOWNLOAD_TASKS:
                DOWNLOAD_TASKS[file_path] = {"status": "pending", "url": track_url}
                asyncio.create_task(background_spotify_download(track_hash, track_url, file_path))

            logger.info(f"Spotify response: {response}")
            return JSONResponse(response)
        except Exception as e:
            logger.error(f"Spotify error: {str(e)}")
            raise HTTPException(500, f"Spotify error: {str(e)}")

# Spotify streaming
@app.get("/spotify/{track_hash}")
async def stream_spotify(track_hash: str):
    track_data = SPOTIFY_DOWNLOAD_TASKS.get(track_hash)
    if not track_data:
        raise HTTPException(404, "Track not found")

    file_path = track_data["file_path"]
    filename = track_data["name"]

    if os.path.exists(file_path):
        file_age = time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path))
        if file_age > CACHE_DURATION:
            os.remove(file_path)
            raise HTTPException(404, "File expired")
        return FileResponse(file_path, media_type="audio/mpeg")
    raise HTTPException(404, "File not available")

# Background Spotify download
async def background_spotify_download(track_hash, track_url, file_path):
    try:
        DOWNLOAD_TASKS[file_path] = {"status": "downloading", "url": track_url}
        if os.path.exists(file_path) and time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path)) < CACHE_DURATION:
            logger.info(f"Skipping download: {file_path} exists")
            DOWNLOAD_TASKS[file_path] = {"status": "completed", "url": track_url}
            return

        if await download_file(track_url, file_path):
            logger.info(f"Downloaded: {file_path}")
            DOWNLOAD_TASKS[file_path] = {"status": "completed", "url": track_url}
        else:
            if os.path.exists(file_path):
                os.remove(file_path)
            DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": track_url, "error": "Download failed"}
    except Exception as e:
        logger.error(f"Spotify download error: {str(e)}")
        if os.path.exists(file_path):
            os.remove(file_path)
        DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": track_url, "error": str(e)}

# Instagram handler
async def handle_instagram(url: str, request: Request, sound: bool = False, quality: str = None):
    info = await get_instagram_video_info(url)
    if not info or not info["video_url"]:
        raise HTTPException(400, "Invalid Instagram URL or no video")

    output_filename = get_unique_filename(url, quality if quality else "original", sound)
    output_path = os.path.join(DOWNLOAD_DIR, output_filename)

    base_url = str(request.base_url).rstrip('/')
    response = {
        "title": info["title"],
        "thumbnail": info["thumbnail"],
        "link": info["video_url"],
        "stream_mp4": f"{base_url}/stream/{output_filename}" if not sound else None,
        "stream_mp3": f"{base_url}/stream/{output_filename}" if sound else None,
        "file_name": output_filename
    }

    if not os.path.exists(output_path) and output_path not in DOWNLOAD_TASKS:
        DOWNLOAD_TASKS[output_path] = {"status": "pending", "url": info["video_url"]}
        asyncio.create_task(background_instagram_download(info["video_url"], output_path, sound, quality))

    return JSONResponse(response)

# Background Instagram download
async def background_instagram_download(video_url, output_path, sound, quality):
    temp_filename = f"temp_instagram_{int(time.time())}.mp4"
    temp_path = os.path.join(DOWNLOAD_DIR, temp_filename)
    try:
        DOWNLOAD_TASKS[output_path] = {"status": "downloading", "url": video_url}
        if os.path.exists(output_path) and time.time() - min(os.path.getmtime(output_path), os.path.getctime(output_path)) < CACHE_DURATION:
            logger.info(f"Skipping download: {output_path} exists")
            DOWNLOAD_TASKS[output_path] = {"status": "completed", "url": video_url}
            return

        if not await download_file(video_url, temp_path):
            logger.error(f"Failed to download Instagram video: {video_url}")
            DOWNLOAD_TASKS[output_path] = {"status": "failed", "url": video_url, "error": "Download failed"}
            return
        if not await process_instagram_video(temp_path, output_path, sound, quality):
            logger.error(f"Failed to process Instagram video: {output_path}")
            DOWNLOAD_TASKS[output_path] = {"status": "failed", "url": video_url, "error": "Processing failed"}
            return
        logger.info(f"Completed Instagram download: {output_path}")
        DOWNLOAD_TASKS[output_path] = {"status": "completed", "url": video_url}
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
                logger.info(f"Removed temp file: {temp_path}")
            except OSError as e:
                logger.error(f"Failed to remove temp file {temp_path}: {str(e)}")

# Streaming endpoint
@app.get("/stream/{filename}")
async def stream_file(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    if os.path.exists(file_path):
        file_age = time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path))
        if file_age > CACHE_DURATION:
            os.remove(file_path)
            if file_path in DOWNLOAD_TASKS:
                del DOWNLOAD_TASKS[file_path]
            raise HTTPException(404, "File expired")
        ext = filename.rsplit(".", 1)[-1].lower()
        content_type = {
            "mp4": "video/mp4",
            "mkv": "video/x-matroska",
            "avi": "video/x-msvideo",
            "mp3": "audio/mpeg"
        }.get(ext, "application/octet-stream")
        return FileResponse(file_path, media_type=content_type)
    raise HTTPException(404, "File not available")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7777)
