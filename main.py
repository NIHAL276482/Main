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

app = FastAPI()
DOWNLOAD_DIR = "downloads"
CACHE_DURATION = 43200  # 12 hours
TERABOX_LINKS = {}
DOWNLOAD_TASKS = {}  # Tracks all download tasks
SPOTIFY_DOWNLOAD_TASKS = {}
COOKIES_FILE = "cookies.txt"
API_KEY = "spotify"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Configure logging with DEBUG level
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log")
    ]
)
logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=8)

# Check if yt-dlp is available
def check_yt_dlp():
    try:
        subprocess.check_output("yt-dlp --version", shell=True, stderr=subprocess.STDOUT)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"yt-dlp is not installed or not in PATH: {e.output}")
        return False
    except FileNotFoundError:
        logger.error("yt-dlp command not found. Ensure yt-dlp is installed and in PATH.")
        return False

# Check if ffmpeg is available
def check_ffmpeg():
    try:
        subprocess.check_output("ffmpeg -version", shell=True, stderr=subprocess.STDOUT)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg is not installed or not in PATH: {e.output}")
        return False
    except FileNotFoundError:
        logger.error("ffmpeg command not found. Ensure ffmpeg is installed and in PATH.")
        return False

# Clean YouTube URL
def clean_youtube_url(url):
    try:
        parsed = urlparse(url)
        if parsed.netloc.endswith("googlevideo.com"):
            return url
        cleaned = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
        logger.info(f"Cleaned YouTube URL from {url} to {cleaned}")
        return cleaned
    except Exception as e:
        logger.error(f"Failed to clean YouTube URL {url}: {e}")
        return url

# Clean up old files
async def clean_old_files():
    now = time.time()
    for filename in os.listdir(DOWNLOAD_DIR):
        file_path = os.path.join(DOWNLOAD_DIR, filename)
        if os.path.isfile(file_path):
            mtime = os.path.getmtime(file_path)
            ctime = os.path.getctime(file_path)
            file_age = now - min(mtime, ctime)  # Use earliest timestamp
            logger.debug(f"Checking file {file_path}: mtime={mtime}, ctime={ctime}, now={now}, age={file_age:.2f} seconds")
            if file_age > CACHE_DURATION:
                try:
                    os.remove(file_path)
                    logger.info(f"Removed old file: {file_path} (age: {file_age:.2f} seconds)")
                    if file_path in DOWNLOAD_TASKS:
                        del DOWNLOAD_TASKS[file_path]
                        logger.debug(f"Cleared expired task for {file_path} from DOWNLOAD_TASKS")
                except OSError as e:
                    logger.error(f"Failed to remove file {file_path}: {e}")
            else:
                logger.debug(f"Skipping file {file_path} (age: {file_age:.2f} seconds, within cache duration)")

# URL type detection
def is_terabox_url(url): 
    return any(domain in url.lower() for domain in ["terabox.com", "1024tera.com", "terabox.app", "terabox.club"])
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
            logger.debug(f"Reusing filename for {url}: {os.path.basename(file_path)} (age: {file_age:.2f} seconds)")
            return os.path.basename(file_path)
    
    unique_id = str(uuid.uuid4())[:8]
    identifier = f"{base_identifier}_{unique_id}"
    hash_id = hashlib.md5(identifier.encode()).hexdigest()[:8]
    ext = "mp3" if sound else "mp4"
    filename = secure_filename(f"{hash_id}_{quality or 'best'}_{'sound' if sound else 'video'}.{ext}")
    logger.debug(f"Generated new unique filename for {url}: {filename}")
    return filename

# Validate cookies file
def validate_cookies_file():
    if not os.path.exists(COOKIES_FILE):
        logger.warning(f"No cookies.txt found at {COOKIES_FILE}. Some platforms may require authentication.")
        return False
    try:
        with open(COOKIES_FILE, "r") as f:
            content = f.read().strip()
            if not content:
                logger.error(f"cookies.txt at {COOKIES_FILE} is empty.")
                return False
            if not any(line.startswith("youtube.com") for line in content.splitlines()):
                logger.warning(f"cookies.txt at {COOKIES_FILE} lacks YouTube cookies.")
            logger.debug(f"Valid cookies.txt found at {COOKIES_FILE} with content length: {len(content)}")
            return True
    except IOError as e:
        logger.error(f"Failed to read cookies.txt at {COOKIES_FILE}: {e}")
        return False

# Generic metadata extraction using yt-dlp
@lru_cache(maxsize=100)
def get_yt_dlp_metadata(url):
    if not check_yt_dlp():
        logger.error("Cannot fetch metadata: yt-dlp is not available")
        return {"title": "Unknown Title", "thumbnail": None}
    
    cookie_option = f"--cookies {COOKIES_FILE}" if validate_cookies_file() else ""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            cmd = f'yt-dlp --dump-json {cookie_option} --no-playlist --no-check-certificate --retries 3 "{url}"'
            logger.debug(f"Executing yt-dlp metadata command (attempt {attempt + 1}/{max_retries}): {cmd}")
            process = subprocess.run(
                cmd, shell=True, text=True, capture_output=True, timeout=30
            )
            if process.returncode != 0:
                logger.error(f"yt-dlp metadata failed (attempt {attempt + 1}/{max_retries}): {process.stderr}")
                if "Sign in to confirm you’re not a bot" in process.stderr:
                    raise HTTPException(
                        status_code=403,
                        detail="Authentication required. Please update cookies.txt with valid YouTube cookies."
                    )
                if attempt == max_retries - 1:
                    logger.error(f"Exhausted retries for {url}. Returning fallback metadata.")
                    return {"title": "Unknown Title", "thumbnail": None}
                time.sleep(2 ** (attempt + 1))
                continue
            data = json.loads(process.stdout)
            title = data.get("title", "Unknown Title")
            thumbnail = data.get("thumbnail") or next((t["url"] for t in data.get("thumbnails", []) if t.get("url")), None)
            logger.debug(f"Metadata for {url}: title={title}, thumbnail={thumbnail}")
            return {"title": title, "thumbnail": thumbnail}
        except subprocess.TimeoutExpired as e:
            logger.error(f"yt-dlp timed out for {url} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return {"title": "Unknown Title", "thumbnail": None}
            time.sleep(2 ** (attempt + 1))
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse yt-dlp JSON for {url}: {e}")
            return {"title": "Unknown Title", "thumbnail": None}
        except Exception as e:
            logger.error(f"Unexpected error fetching metadata for {url}: {e}")
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
                    logger.info(f"Successfully downloaded file: {path}")
                    return True
                logger.error(f"Failed to download {url}: Status {response.status}")
                return False
        except aiohttp.ClientError as e:
            logger.error(f"Error downloading file {url}: {e}")
            return False

# Instagram video URL and title extraction
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
            logger.error(f"Error fetching Instagram info: {e}")
            return None
    return await asyncio.get_event_loop().run_in_executor(executor, sync_get_instagram_info)

# Instagram video processing using ffmpeg
async def process_instagram_video(temp_path, output_path, sound=False, quality=None):
    if not check_ffmpeg():
        logger.error("Cannot process Instagram video: ffmpeg is not available")
        raise HTTPException(status_code=500, detail="ffmpeg is not installed or not in PATH")
    try:
        if sound:
            cmd = f'ffmpeg -i "{temp_path}" -vn -acodec mp3 -ab 320k "{output_path}" -y'
        else:
            if quality:
                resolution = {
                    "1080": "1920x1080",
                    "720": "1280x720",
                    "480": "854x480",
                    "360": "640x360",
                    "240": "426x240"
                }[quality]
                cmd = f'ffmpeg -i "{temp_path}" -vcodec libx264 -s {resolution} -acodec aac -ab 128k -preset ultrafast -crf 23 -pix_fmt yuv420p "{output_path}" -y'
            else:
                cmd = f'ffmpeg -i "{temp_path}" -vcodec libx264 -acodec aac -ab 128k -preset ultrafast -crf 23 -pix_fmt yuv420p "{output_path}" -y'
        logger.info(f"Executing ffmpeg command: {cmd}")
        process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f"ffmpeg failed: {stderr.decode()}")
            raise HTTPException(status_code=500, detail=f"ffmpeg processing failed: {stderr.decode()}")
        logger.info(f"Processed Instagram video: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error processing Instagram video: {e}")
        return False

# Main endpoint
@app.get("/")
async def download_video(request: Request):
    await clean_old_files()
    url = request.query_params.get("url")
    sound = "sound" in request.query_params
    quality = next((q for q in ["240", "360", "480", "720", "1080"] if q in request.query_params), None)

    if not url:
        logger.error("No URL provided in request")
        raise HTTPException(status_code=400, detail="URL parameter is required")

    if not (url.startswith("http://") or url.startswith("https://")):
        logger.error(f"Invalid URL: {url}. Must be a valid HTTP/HTTPS URL")
        raise HTTPException(status_code=400, detail="Invalid URL. Please provide a valid HTTP/HTTPS URL")

    api_key = request.headers.get("X-API-Key")
    is_authenticated = api_key == API_KEY
    if is_authenticated:
        logger.info("Authenticated request with X-API-Key 'spotify'")
    else:
        logger.info("Unauthenticated request (no or invalid X-API-Key)")

    logger.info(f"Received request for url: {url}, sound: {sound}, quality: {quality}, authenticated: {is_authenticated}")

    if is_spotify_url(url):
        if sound or quality:
            logger.error("Spotify URLs do not support &sound or &quality parameters")
            raise HTTPException(status_code=400, detail="Spotify URLs do not support &sound or &quality parameters")
        return await handle_spotify(url, request, is_authenticated)
    elif is_terabox_url(url):
        if sound or quality:
            logger.error("Terabox URLs do not support &sound or &quality parameters")
            raise HTTPException(status_code=400, detail="Terabox URLs do not support &sound or &quality parameters")
        return await handle_terabox(url, request, is_authenticated)
    elif is_instagram_url(url):
        return await handle_instagram(url, request, sound, quality, is_authenticated)
    elif is_yt_dlp_supported(url):
        if not check_yt_dlp():
            logger.error("yt-dlp is not available for processing")
            raise HTTPException(status_code=500, detail="yt-dlp is not installed or not in PATH")
        platform = "youtube" if is_youtube_url(url) else "generic"
        return await handle_yt_dlp(url, request, sound, quality, platform=platform, is_authenticated=is_authenticated)
    else:
        logger.error(f"Unsupported URL type: {url}")
        raise HTTPException(status_code=400, detail=f"Unsupported URL type: {url}")

# Generic yt-dlp handler
async def handle_yt_dlp(url: str, request: Request, sound: bool = False, quality: str = None, platform: str = "generic", is_authenticated: bool = False):
    logger.info(f"Handling {platform} URL: {url}, authenticated: {is_authenticated}")
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

    logger.info(f"Returning response for {url}: {response}")
    return JSONResponse(response)

# Background download for yt-dlp
async def background_yt_dlp_download(url, path, download_type, quality=None, cookie_option="", common_options=""):
    try:
        DOWNLOAD_TASKS[path] = {"status": "downloading", "url": url}
        if os.path.exists(path):
            file_age = time.time() - min(os.path.getmtime(path), os.path.getctime(path))
            if file_age < CACHE_DURATION:
                logger.info(f"Skipping download for {path}: File exists and is within cache duration ({file_age:.2f} seconds)")
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
        
        logger.info(f"Executing background yt-dlp command: {cmd}")
        process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            logger.error(f"yt-dlp {download_type} download failed for {url}: {stderr.decode()}")
            if os.path.exists(path) and (time.time() - min(os.path.getmtime(path), os.path.getctime(path))) > CACHE_DURATION:
                os.remove(path)
                logger.info(f"Removed partial file: {path} (outside cache duration)")
            DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "error": stderr.decode()}
        else:
            logger.info(f"Completed background {download_type} download for {path}")
            DOWNLOAD_TASKS[path] = {"status": "completed", "url": url}
    except Exception as e:
        logger.error(f"Background {download_type} download error for {url}: {e}")
        if os.path.exists(path) and (time.time() - min(os.path.getmtime(path), os.path.getctime(path))) > CACHE_DURATION:
            os.remove(path)
            logger.info(f"Removed partial file: {path} (outside cache duration)")
        DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "error": str(e)}

# Terabox handler
@cached(ttl=300, cache=Cache.MEMORY)
async def handle_terabox(url: str, request: Request, is_authenticated: bool):
    api_url = f"http://127.0.0.1:8000/?url={url}"
    max_retries = 3
    retry_status_codes = [400, 404, 405, 504]
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                logger.info(f"Attempt {attempt + 1}/{max_retries} to fetch Terabox API: {api_url}")
                async with session.get(api_url) as response:
                    response_text = await response.text()
                    logger.info(f"Terabox API response status: {response.status}")
                    if response.status == 200:
                        data = await response.json()
                        direct_link = data.get("direct_link")
                        if direct_link:
                            link_hash = base64.urlsafe_b64encode(hashlib.md5(direct_link.encode()).digest()).decode()[:24]
                            filename = data.get("name", f"terabox_{link_hash}.mp4")
                            file_path = os.path.join(DOWNLOAD_DIR, filename)
                            TERABOX_LINKS[link_hash] = {
                                "link": direct_link,
                                "name": filename
                            }
                            base_url = str(request.base_url).rstrip('/')
                            response_data = {
                                "title": data.get("name", "Terabox Video"),
                                "thumbnail": data.get("thumbnail"),
                                "link": direct_link,
                                "stream_mp4": f"{base_url}/tb/{link_hash}",
                                "stream_mp3": None,
                                "file_name": filename
                            }
                            if not os.path.exists(file_path) and file_path not in DOWNLOAD_TASKS:
                                DOWNLOAD_TASKS[file_path] = {"status": "pending", "url": direct_link}
                                asyncio.create_task(background_download(link_hash, direct_link, file_path))
                            logger.info(f"Terabox response: {response_data}")
                            return JSONResponse(response_data)
                        logger.error("No direct_link provided in Terabox API response")
                        raise HTTPException(status_code=500, detail="No direct_link provided")
                    elif response.status in retry_status_codes and attempt < max_retries - 1:
                        logger.warning(f"Terabox API returned {response.status}, retrying...")
                        await asyncio.sleep(2 ** (attempt + 1))
                        continue
                    logger.error(f"Terabox API returned status {response.status}: {response_text}")
                    raise HTTPException(status_code=response.status, detail=f"Terabox API error: {response_text}")
        except aiohttp.ClientError as e:
            logger.error(f"Terabox API request failed on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                raise HTTPException(status_code=500, detail=f"Terabox API failed after {max_retries} attempts: {str(e)}")
            await asyncio.sleep(2 ** (attempt + 1))
    logger.error(f"Failed to fetch Terabox data after {max_retries} attempts for URL: {url}")
    raise HTTPException(status_code=500, detail=f"Failed to fetch Terabox data after {max_retries} attempts")

# Spotify handler
@cached(ttl=300, cache=Cache.MEMORY)
async def handle_spotify(url: str, request: Request, is_authenticated: bool):
    api_url = f"http://sp.hosters.club/?url={url}"
    max_retries = 3
    retry_status_codes = [400, 404, 429, 500, 502, 503, 504]
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                logger.info(f"Attempt {attempt + 1}/{max_retries} to fetch Spotify API: {api_url}")
                async with session.get(api_url) as response:
                    response_text = await response.text()
                    logger.info(f"Spotify API response status: {response.status}, body: {response_text[:500]}...")
                    if response.status == 200:
                        data = await response.json()
                        if not data.get("error", True) and data.get("url"):
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
                                "title": data.get("name", "Spotify Track"),
                                "thumbnail": None,
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
                        logger.error("Spotify API returned an error or no URL")
                        raise HTTPException(status_code=500, detail="Spotify API returned an error or no valid URL")
                    elif response.status in retry_status_codes and attempt < max_retries - 1:
                        logger.warning(f"Spotify API returned {response.status} on attempt {attempt + 1}, retrying...")
                        await asyncio.sleep(2 ** (attempt + 1))
                        continue
                    logger.error(f"Spotify API returned status {response.status}: {response_text[:500]}...")
                    raise HTTPException(status_code=response.status, detail=f"Spotify API returned status {response.status}")
        except aiohttp.ClientError as e:
            logger.error(f"Spotify API request failed on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                raise HTTPException(status_code=500, detail=f"Spotify API request failed after {max_retries} attempts: {str(e)}")
            await asyncio.sleep(2 ** (attempt + 1))
    logger.error(f"Failed to fetch Spotify data after {max_retries} attempts for URL: {url}")
    raise HTTPException(status_code=500, detail=f"Failed to fetch Spotify data after {max_retries} attempts")

# Spotify streaming
@app.get("/spotify/{track_hash}")
async def stream_spotify(track_hash: str, request: Request):
    track_data = SPOTIFY_DOWNLOAD_TASKS.get(track_hash)
    if not track_data:
        logger.error(f"Spotify track not found for hash: {track_hash}")
        raise HTTPException(status_code=404, detail="Track not found")

    filename = track_data["name"]
    file_path = track_data["file_path"]

    if os.path.exists(file_path):
        now = time.time()
        mtime = os.path.getmtime(file_path)
        ctime = os.path.getctime(file_path)
        file_age = now - min(mtime, ctime)
        logger.debug(f"Streaming Spotify file {file_path}: now={now}, mtime={mtime}, ctime={ctime}, age={file_age:.2f} seconds")
        if file_age > CACHE_DURATION or file_age < -3600:
            logger.warning(f"File {file_path} has invalid age ({file_age:.2f} seconds), checking DOWNLOAD_TASKS")
            if file_path in DOWNLOAD_TASKS and DOWNLOAD_TASKS[file_path]["status"] == "completed":
                logger.info(f"File {file_path} is marked as completed in DOWNLOAD_TASKS, serving anyway")
            else:
                logger.warning(f"File {file_path} is older than cache duration ({file_age:.2f} seconds), removing")
                os.remove(file_path)
                raise HTTPException(status_code=404, detail="File no longer available (expired)")
        ext = filename.rsplit(".", 1)[-1].lower()
        content_type = {"mp3": "audio/mpeg"}.get(ext, "application/octet-stream")
        logger.info(f"Serving existing Spotify file: {file_path}")
        return FileResponse(file_path, media_type=content_type)
    
    logger.error(f"Spotify file not yet available: {file_path}")
    raise HTTPException(status_code=404, detail="File not yet available")

# Background download for Spotify
async def background_spotify_download(track_hash, track_url, file_path):
    try:
        DOWNLOAD_TASKS[file_path] = {"status": "downloading", "url": track_url}
        if os.path.exists(file_path):
            file_age = time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path))
            if file_age < CACHE_DURATION:
                logger.info(f"Skipping download for {file_path}: File exists and is within cache duration ({file_age:.2f} seconds)")
                DOWNLOAD_TASKS[file_path] = {"status": "completed", "url": track_url}
                return

        logger.info(f"Started background download for Spotify: {track_url}")
        if await download_file(track_url, file_path):
            logger.info(f"Completed background download for {file_path}")
            DOWNLOAD_TASKS[file_path] = {"status": "completed", "url": track_url}
        else:
            if os.path.exists(file_path) and (time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path))) > CACHE_DURATION:
                os.remove(file_path)
                logger.info(f"Removed partial file: {file_path} (outside cache duration)")
            DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": track_url, "error": "Download failed"}
            logger.error(f"Background download failed for {track_url}")
    except Exception as e:
        if os.path.exists(file_path) and (time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path))) > CACHE_DURATION:
            os.remove(file_path)
            logger.info(f"Removed partial file: {file_path} (outside cache duration)")
        DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": track_url, "error": str(e)}
        logger.error(f"Background download error for {track_url}: {e}")

# Instagram handler
async def handle_instagram(url: str, request: Request, sound: bool = False, quality: str = None, is_authenticated: bool = False):
    logger.info(f"Handling Instagram URL: {url}, authenticated: {is_authenticated}")
    info = await get_instagram_video_info(url)
    if not info or not info["video_url"]:
        logger.error(f"Invalid Instagram URL or no video found: {url}")
        raise HTTPException(status_code=400, detail="Invalid Instagram URL or no video found")

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

    logger.info(f"Instagram response: {response}")
    return JSONResponse(response)

# Background download for Instagram
async def background_instagram_download(video_url, output_path, sound, quality):
    temp_filename = f"temp_instagram_{int(time.time())}.mp4"
    temp_path = os.path.join(DOWNLOAD_DIR, temp_filename)
    try:
        DOWNLOAD_TASKS[output_path] = {"status": "downloading", "url": video_url}
        if os.path.exists(output_path):
            file_age = time.time() - min(os.path.getmtime(output_path), os.path.getctime(output_path))
            if file_age < CACHE_DURATION:
                logger.info(f"Skipping download for {output_path}: File exists and is within cache duration ({file_age:.2f} seconds)")
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
        logger.info(f"Completed background Instagram download: {output_path}")
        DOWNLOAD_TASKS[output_path] = {"status": "completed", "url": video_url}
    except Exception as e:
        logger.error(f"Background Instagram download error: {e}")
        if os.path.exists(output_path) and (time.time() - min(os.path.getmtime(output_path), os.path.getctime(output_path))) > CACHE_DURATION:
            os.remove(output_path)
            logger.info(f"Removed partial file: {output_path} (outside cache duration)")
        DOWNLOAD_TASKS[output_path] = {"status": "failed", "url": video_url, "error": str(e)}
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
                logger.info(f"Removed temp file: {temp_path}")
            except OSError:
                logger.error(f"Failed to remove temp file {temp_path}")

# Streaming endpoint
@app.get("/stream/{filename}")
async def stream_file(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    if os.path.exists(file_path):
        now = time.time()
        mtime = os.path.getmtime(file_path)
        ctime = os.path.getctime(file_path)
        file_age = now - min(mtime, ctime)
        logger.debug(f"Streaming file {file_path}: now={now}, mtime={mtime}, ctime={ctime}, age={file_age:.2f} seconds")
        if file_age > CACHE_DURATION or file_age < -3600:
            logger.warning(f"File {file_path} has invalid age ({file_age:.2f} seconds), checking DOWNLOAD_TASKS")
            if file_path in DOWNLOAD_TASKS and DOWNLOAD_TASKS[file_path]["status"] == "completed":
                logger.info(f"File {file_path} is marked as completed in DOWNLOAD_TASKS, serving anyway")
            else:
                logger.warning(f"File {file_path} is older than cache duration ({file_age:.2f} seconds), removing")
                os.remove(file_path)
                if file_path in DOWNLOAD_TASKS:
                    del DOWNLOAD_TASKS[file_path]
                    logger.debug(f"Cleared expired task for {file_path} from DOWNLOAD_TASKS")
                raise HTTPException(status_code=404, detail="File no longer available (expired)")
        ext = filename.rsplit(".", 1)[-1].lower()
        content_type = {
            "mp4": "video/mp4",
            "mkv": "video/x-matroska",
            "avi": "video/x-msvideo",
            "mp3": "audio/mpeg"
        }.get(ext, "application/octet-stream")
        logger.info(f"Streaming file: {file_path}, Content-Type: {content_type}")
        return FileResponse(file_path, media_type=content_type)
    logger.error(f"File not found: {file_path}")
    raise HTTPException(status_code=404, detail="File not yet available")

# New status check endpoint
@app.get("/status/{filename}")
async def check_status(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    if os.path.exists(file_path):
        return JSONResponse({"status": "completed"})
    task = DOWNLOAD_TASKS.get(file_path)
    if task:
        return JSONResponse({"status": task["status"], "error": task.get("error")})
    raise HTTPException(status_code=404, detail="No download task found for this file")

# Terabox streaming
@app.get("/tb/{link_hash}")
async def stream_terabox(link_hash: str, request: Request):
    link_data = TERABOX_LINKS.get(link_hash)
    if not link_data:
        logger.error(f"Terabox link not found for hash: {link_hash}")
        raise HTTPException(status_code=404, detail="Link not found")

    filename = link_data["name"]
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(file_path):
        now = time.time()
        mtime = os.path.getmtime(file_path)
        ctime = os.path.getctime(file_path)
        file_age = now - min(mtime, ctime)
        logger.debug(f"Streaming Terabox file {file_path}: now={now}, mtime={mtime}, ctime={ctime}, age={file_age:.2f} seconds")
        if file_age > CACHE_DURATION or file_age < -3600:
            logger.warning(f"File {file_path} has invalid age ({file_age:.2f} seconds), checking DOWNLOAD_TASKS")
            if file_path in DOWNLOAD_TASKS and DOWNLOAD_TASKS[file_path]["status"] == "completed":
                logger.info(f"File {file_path} is marked as completed in DOWNLOAD_TASKS, serving anyway")
            else:
                logger.warning(f"File {file_path} is older than cache duration ({file_age:.2f} seconds), removing")
                os.remove(file_path)
                if file_path in DOWNLOAD_TASKS:
                    del DOWNLOAD_TASKS[file_path]
                    logger.debug(f"Cleared expired task for {file_path} from DOWNLOAD_TASKS")
                raise HTTPException(status_code=404, detail="File no longer available (expired)")
        ext = filename.rsplit(".", 1)[-1].lower()
        content_type = {
            "mp4": "video/mp4",
            "mkv": "video/x-matroska",
            "avi": "video/x-msvideo",
            "mp3": "audio/mpeg"
        }.get(ext, "application/octet-stream")
        logger.info(f"Serving existing Terabox file: {file_path}")
        return FileResponse(file_path, media_type=content_type)

    logger.error(f"Terabox file not yet available: {file_path}")
    raise HTTPException(status_code=404, detail="File not yet available")

# Terabox status check
@app.get("/tb_status/{link_hash}")
async def check_download_status(link_hash: str, request: Request):
    link_data = TERABOX_LINKS.get(link_hash)
    if not link_data:
        logger.error(f"No Terabox link found for hash: {link_hash}")
        raise HTTPException(status_code=404, detail="No download task found")
    file_path = os.path.join(DOWNLOAD_DIR, link_data["name"])
    task = DOWNLOAD_TASKS.get(file_path)
    if not task:
        logger.error(f"No download task found for file: {file_path}")
        raise HTTPException(status_code=404, detail="No download task found")
    base_url = str(request.base_url).rstrip('/')
    logger.info(f"Terabox status for {link_hash}: {task['status']}")
    return JSONResponse({
        "status": task["status"],
        "stream_link": f"{base_url}/stream/{os.path.basename(task['file_path'])}" if task["status"] == "completed" else None,
        "error": task.get("error")
    })

# Background download for Terabox
async def background_download(link_hash, link, file_path):
    try:
        DOWNLOAD_TASKS[file_path] = {"status": "downloading", "url": link}
        if os.path.exists(file_path):
            file_age = time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path))
            if file_age < CACHE_DURATION:
                logger.info(f"Skipping download for {file_path}: File exists and is within cache duration ({file_age:.2f} seconds)")
                DOWNLOAD_TASKS[file_path] = {"status": "completed", "url": link}
                return

        logger.info(f"Started background download for {link}")
        if await download_file(link, file_path):
            logger.info(f"Completed background download for {file_path}")
            DOWNLOAD_TASKS[file_path] = {"status": "completed", "url": link}
        else:
            if os.path.exists(file_path) and (time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path))) > CACHE_DURATION:
                os.remove(file_path)
                logger.info(f"Removed partial file: {file_path} (outside cache duration)")
            DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": link, "error": "Download failed"}
            logger.error(f"Background download failed for {link}")
    except Exception as e:
        if os.path.exists(file_path) and (time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path))) > CACHE_DURATION:
            os.remove(file_path)
            logger.info(f"Removed partial file: {file_path} (outside cache duration)")
        DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": link, "error": str(e)}
        logger.error(f"Background download error for {link}: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7777)
