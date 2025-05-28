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
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

app = FastAPI()
DOWNLOAD_DIR = "downloads"
CACHE_DURATION = 7200  # 2 hours
TERABOX_LINKS = {}
DOWNLOAD_TASKS = {}
COOKIES_FILE = "cookies.txt"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log")
    ]
)
logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=4)

# Check if yt-dlp is available
def check_yt_dlp():
    try:
        output = subprocess.check_output("yt-dlp --version", shell=True, stderr=subprocess.STDOUT, text=True)
        logger.info(f"yt-dlp version: {output.strip()}")
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
        output = subprocess.check_output("ffmpeg -version", shell=True, stderr=subprocess.STDOUT, text=True)
        logger.info(f"ffmpeg version: {output.splitlines()[0]}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"ffmpeg is not installed or not in PATH: {e.output}")
        return False
    except FileNotFoundError:
        logger.error("ffmpeg command not found. Ensure ffmpeg is installed and in PATH.")
        return False

# Check DOWNLOAD_DIR permissions
def check_download_dir():
    try:
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        test_file = os.path.join(DOWNLOAD_DIR, "test.txt")
        with open(test_file, "w") as f:
            f.write("test")
        os.remove(test_file)
        logger.info(f"Download directory {DOWNLOAD_DIR} is writable")
    except Exception as e:
        logger.error(f"Download directory {DOWNLOAD_DIR} is not writable: {e}")
        raise RuntimeError(f"Cannot write to {DOWNLOAD_DIR}: {e}")

# Clean YouTube URL
def clean_youtube_url(url):
    try:
        parsed = urlparse(url)
        if parsed.netloc.endswith("googlevideo.com"):
            return url
        query_params = parse_qs(parsed.query)
        cleaned_query = {k: v[0] for k, v in query_params.items() if k == 'v' or k == 'si'}
        cleaned = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', urlencode(cleaned_query), ''))
        logger.info(f"Cleaned YouTube URL from {url} to {cleaned}")
        return cleaned
    except Exception as e:
        logger.error(f"Failed to clean YouTube URL {url}: {e}")
        return url

# Clean up old files
async def clean_old_files():
    now = time.time()
    active_files = {task["file_path"] for task in DOWNLOAD_TASKS.values() if task["status"] == "downloading"}
    for filename in os.listdir(DOWNLOAD_DIR):
        file_path = os.path.join(DOWNLOAD_DIR, filename)
        if file_path in active_files:
            logger.info(f"Skipping cleanup for active file: {file_path}")
            continue
        if os.path.isfile(file_path) and (now - os.path.getmtime(file_path)) > CACHE_DURATION:
            try:
                os.remove(file_path)
                logger.info(f"Removed old file: {file_path}")
            except OSError as e:
                logger.error(f"Failed to remove file {file_path}: {e}")

# URL type detection
def is_terabox_url(url): 
    return any(domain in url.lower() for domain in ["terabox.com", "1024tera.com", "terabox.app", "terabox.club"])
def is_spotify_url(url): 
    return "spotify.com" in url.lower()
def is_youtube_url(url): 
    return url and ("youtube.com" in url.lower() or "youtu.be" in url.lower() or "googlevideo.com" in url.lower())
def is_youtube_music_url(url):
    return "music.youtube.com" in url.lower()
def is_jiosaavn_url(url):
    return "jiosaavn.com" in url.lower()
def is_music_platform_url(url):
    return is_spotify_url(url) or is_youtube_music_url(url) or is_jiosaavn_url(url)
def is_instagram_url(url): 
    return "instagram.com" in url.lower()
def is_yt_dlp_supported(url): 
    return True  # All URLs are potentially supported by yt-dlp, except specialized cases

# Generate unique filename
def get_unique_filename(url, quality=None, sound=False):
    identifier = f"{url}_{quality or 'best'}_{'sound' if sound else 'video'}"
    hash_id = hashlib.md5(identifier.encode()).hexdigest()[:8]
    ext = "mp3" if sound else "mp4"
    filename = secure_filename(f"{hash_id}_{quality or 'best'}_{'sound' if sound else 'video'}.{ext}")
    logger.info(f"Generated filename for URL {url}: {filename}")
    return filename

# Validate cookies file
def validate_cookies_file():
    if os.path.exists(COOKIES_FILE):
        try:
            with open(COOKIES_FILE, "r") as f:
                f.read(1)
            logger.info(f"Valid cookies.txt found at {COOKIES_FILE}")
            return True
        except IOError as e:
            logger.error(f"Failed to read cookies file: {e}")
            return False
    logger.warning("No cookies.txt found; some platforms may require authentication")
    return False

# Generic metadata extraction using yt-dlp
@lru_cache(maxsize=100)
def get_yt_dlp_metadata(url):
    if not check_yt_dlp():
        logger.error("Cannot fetch metadata: yt-dlp is not available")
        return {"title": "Unknown Title", "thumbnail": None}
    cookie_option = f"--cookies {COOKIES_FILE}" if validate_cookies_file() else ""
    max_retries = 2
    for attempt in range(max_retries):
        try:
            cmd = f'yt-dlp --dump-json {cookie_option} "{url}"'
            logger.info(f"Executing yt-dlp metadata command (attempt {attempt + 1}/{max_retries}): {cmd}")
            output = subprocess.check_output(cmd, shell=True, text=True, stderr=subprocess.STDOUT, timeout=30)
            data = json.loads(output)
            title = data.get("title", "Unknown Title")
            thumbnail = data.get("thumbnail") or next((t["url"] for t in data.get("thumbnails", []) if t.get("url")), None)
            return {"title": title, "thumbnail": thumbnail}
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to get metadata for {url} (attempt {attempt + 1}/{max_retries}): {e.output}")
            if "Sign in to confirm you’re not a bot" in e.output:
                raise HTTPException(
                    status_code=403,
                    detail="Authentication required. Please provide a valid cookies.txt file. See https://github.com/yt-dlp/yt-dlp/wiki/FAQ#how-do-i-pass-cookies-to-yt-dlp for details."
                )
            if attempt == max_retries - 1:
                return {"title": "Unknown Title", "thumbnail": None}
            time.sleep(1)
        except subprocess.TimeoutExpired as e:
            logger.error(f"yt-dlp timed out fetching metadata for {url} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                return {"title": "Unknown Title", "thumbnail": None}
            time.sleep(1)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse yt-dlp JSON output for {url}: {e}")
            return {"title": "Unknown Title", "thumbnail": None}

# File download utility
async def download_file(url, path):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=600)) as session:
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

# Generic yt-dlp handler
async def handle_yt_dlp(url: str, request: Request, sound: bool = False, quality: str = None, platform: str = "generic"):
    logger.info(f"Handling {platform} URL: {url}")
    if is_youtube_url(url):
        url = clean_youtube_url(url)
    metadata = get_yt_dlp_metadata(url)
    video_filename = audio_filename = None
    video_path = audio_path = None
    cookie_option = f"--cookies {COOKIES_FILE}" if validate_cookies_file() else ""
    common_options = "--no-check-certificate --retries 3"

    try:
        if not sound and not quality:
            video_filename = get_unique_filename(url, "1080", False)
            audio_filename = get_unique_filename(url, None, True)
            video_path = os.path.join(DOWNLOAD_DIR, video_filename)
            audio_path = os.path.join(DOWNLOAD_DIR, audio_filename)

            if not os.path.exists(video_path):
                cmd = f'yt-dlp -f "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best" --merge-output-format mp4 {cookie_option} {common_options} -o "{video_path}" "{url}"'
                logger.info(f"Executing yt-dlp video command: {cmd}")
                process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await process.communicate()
                if process.returncode != 0 or not os.path.exists(video_path):
                    logger.error(f"yt-dlp video download failed: {stderr.decode()}")
                    raise HTTPException(status_code=500, detail=f"Video download failed: {stderr.decode()}")
                logger.info(f"Video downloaded successfully: {video_path}")

            if not os.path.exists(audio_path):
                cmd = f'yt-dlp --extract-audio --audio-format mp3 --audio-quality 320K {cookie_option} {common_options} -o "{audio_path}" "{url}"'
                logger.info(f"Executing yt-dlp audio command: {cmd}")
                process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await process.communicate()
                if process.returncode != 0 or not os.path.exists(audio_path):
                    logger.error(f"yt-dlp audio download failed: {stderr.decode()}")
                    raise HTTPException(status_code=500, detail=f"Audio download failed: {stderr.decode()}")
                logger.info(f"Audio downloaded successfully: {audio_path}")

        elif sound:
            audio_filename = get_unique_filename(url, None, True)
            audio_path = os.path.join(DOWNLOAD_DIR, audio_filename)
            if not os.path.exists(audio_path):
                cmd = f'yt-dlp --extract-audio --audio-format mp3 --audio-quality 320K {cookie_option} {common_options} -o "{audio_path}" "{url}"'
                logger.info(f"Executing yt-dlp audio command: {cmd}")
                process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await process.communicate()
                if process.returncode != 0 or not os.path.exists(audio_path):
                    logger.error(f"yt-dlp audio download failed: {stderr.decode()}")
                    raise HTTPException(status_code=500, detail=f"Audio download failed: {stderr.decode()}")
                logger.info(f"Audio downloaded successfully: {audio_path}")

        else:
            quality_map = {
                "240": "bestvideo[height<=240][ext=mp4]+bestaudio[ext=m4a]/best[height<=240]",
                "360": "bestvideo[height<=360][ext=mp4]+bestaudio[ext=m4a]/best[height<=360]",
                "480": "bestvideo[height<=480][ext=mp4]+bestaudio[ext=m4a]/best[height<=480]",
                "720": "bestvideo[height<=720][ext=mp4]+bestaudio[ext=m4a]/best[height<=720]",
                "1080": "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080]"
            }
            video_filename = get_unique_filename(url, quality, False)
            video_path = os.path.join(DOWNLOAD_DIR, video_filename)
            if not os.path.exists(video_path):
                cmd = f'yt-dlp -f "{quality_map.get(quality, "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best")}" --merge-output-format mp4 {cookie_option} {common_options} -o "{video_path}" "{url}"'
                logger.info(f"Executing yt-dlp video command: {cmd}")
                process = await asyncio.create_subprocess_shell(cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, stderr = await process.communicate()
                if process.returncode != 0 or not os.path.exists(video_path):
                    logger.error(f"yt-dlp video download failed: {stderr.decode()}")
                    raise HTTPException(status_code=500, detail=f"Video download failed: {stderr.decode()}")
                logger.info(f"Video downloaded successfully: {video_path}")

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
        logger.info(f"Returning response for {url}: {response}")
        return JSONResponse(response)

    except subprocess.CalledProcessError as e:
        logger.error(f"yt-dlp failed for {url}: {e.stderr.decode()}")
        raise HTTPException(status_code=500, detail=f"Download failed: {e.stderr.decode()}")

# Terabox handler
@cached(ttl=300, cache=Cache.MEMORY)
async def handle_terabox(url: str, request: Request):
    api_url = f"http://127.0.0.1:8000/?url={url}"
    max_retries = 3
    retry_status_codes = [400, 404, 405, 504]
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                logger.info(f"Attempt {attempt + 1}/{max_retries} to fetch Terabox API: {api_url}")
                async with session.get(api_url) as response:
                    response_text = await response.text()
                    logger.info(f"Terabox API response status: {response.status}, body: {response_text[:1000]}...")
                    if response.status == 200:
                        data = await response.json()
                        direct_link = data.get("direct_link")
                        if direct_link:
                            link_hash = base64.urlsafe_b64encode(hashlib.md5(direct_link.encode()).digest()).decode()[:24]
                            TERABOX_LINKS[link_hash] = {
                                "link": direct_link,
                                "name": data.get("name", f"terabox_{link_hash}.mp4")
                            }
                            base_url = str(request.base_url).rstrip('/')
                            response_data = {
                                "title": data.get("name", "Terabox Video"),
                                "thumbnail": data.get("thumbnail"),
                                "bytes": data.get("bytes"),
                                "size": data.get("size"),
                                "link": direct_link,
                                "stream_mp4": f"{base_url}/tb/{link_hash}",
                                "stream_mp3": None,
                                "file_name": data.get("name", f"terabox_{link_hash}.mp4")
                            }
                            logger.info(f"Terabox response: {response_data}")
                            return JSONResponse(response_data)
                        logger.error("No direct_link provided in Terabox API response")
                        raise HTTPException(status_code=500, detail="No direct_link provided in API response")
                    elif response.status in retry_status_codes and attempt < max_retries - 1:
                        logger.warning(f"Terabox API returned {response.status}, retrying...")
                        await asyncio.sleep(2 ** (attempt + 1))
                        continue
                    logger.error(f"Terabox API returned status {response.status}: {response_text[:500]}...")
                    raise HTTPException(status_code=response.status, detail=f"Terabox API returned status {response.status}")
        except aiohttp.ClientError as e:
            logger.error(f"Terabox API request failed on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                raise HTTPException(status_code=500, detail=f"Terabox API request failed after {max_retries} attempts: {str(e)}")
            await asyncio.sleep(2 ** (attempt + 1))
    logger.error(f"Failed to fetch Terabox data after {max_retries} attempts for URL: {url}")
    raise HTTPException(status_code=500, detail=f"Failed to fetch Terabox data after {max_retries} attempts")

# Spotify handler
@cached(ttl=300, cache=Cache.MEMORY)
async def handle_spotify(url: str, request: Request):
    api_url = f"https://sp.hosters.club/?url={url}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(api_url) as response:
                if response.status == 200:
                    data = await response.json()
                    if not data.get("error", True):
                        base_url = str(request.base_url).rstrip('/')
                        response = {
                            "title": data.get("name", "Spotify Track"),
                            "thumbnail": None,
                            "link": data["url"],
                            "stream_mp4": None,
                            "stream_mp3": f"{base_url}/stream/{os.path.basename(data['url'])}" if data["url"] else None,
                            "file_name": os.path.basename(data["url"]) if data["url"] else None
                        }
                        logger.info(f"Spotify response: {response}")
                        return JSONResponse(response)
                    logger.error("Spotify API returned an error")
                    raise HTTPException(status_code=500, detail="Spotify API returned an error")
                logger.error(f"Spotify API returned status {response.status}")
                raise HTTPException(status_code=500, detail=f"Spotify API returned status {response.status}")
        except aiohttp.ClientError as e:
            logger.error(f"Spotify API request failed: {e}")
            raise HTTPException(status_code=500, detail=f"Spotify API request failed: {str(e)}")

# JioSaavn handler
@cached(ttl=300, cache=Cache.MEMORY)
async def handle_jiosaavn(url: str, request: Request):
    if not check_yt_dlp():
        logger.error("yt-dlp is not available for JioSaavn processing")
        raise HTTPException(status_code=500, detail="yt-dlp is not installed or not in PATH")
    return await handle_yt_dlp(url, request, sound=True, platform="jiosaavn")

# Instagram handler
async def handle_instagram(url: str, request: Request, sound: bool = False, quality: str = None):
    logger.info(f"Handling Instagram URL: {url}")
    info = await get_instagram_video_info(url)
    if not info or not info["video_url"]:
        logger.error(f"Invalid Instagram URL or no video found: {url}")
        raise HTTPException(status_code=400, detail="Invalid Instagram URL or no video found")

    temp_filename = f"temp_instagram_{int(time.time())}.mp4"
    output_filename = get_unique_filename(url, quality if quality else "original", sound)
    temp_path = os.path.join(DOWNLOAD_DIR, temp_filename)
    output_path = os.path.join(DOWNLOAD_DIR, output_filename)

    try:
        if not await download_file(info["video_url"], temp_path):
            logger.error(f"Failed to download Instagram video: {info['video_url']}")
            raise HTTPException(status_code=500, detail="Failed to download Instagram video")
        if not await process_instagram_video(temp_path, output_path, sound, quality):
            logger.error(f"Failed to process Instagram video: {output_path}")
            raise HTTPException(status_code=500, detail="Failed to process Instagram video")
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
                logger.info(f"Removed temp file: {temp_path}")
            except OSError:
                logger.error(f"Failed to remove temp file {temp_path}")

    base_url = str(request.base_url).rstrip('/')
    response = {
        "title": info["title"],
        "thumbnail": info["thumbnail"],
        "link": info["video_url"],
        "stream_mp4": f"{base_url}/stream/{output_filename}" if not sound else None,
        "stream_mp3": f"{base_url}/stream/{output_filename}" if sound else None,
        "file_name": output_filename
    }
    logger.info(f"Instagram response: {response}")
    return JSONResponse(response)

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

    logger.info(f"Received request for url: {url}, sound: {sound}, quality: {quality}")

    if is_spotify_url(url):
        if sound or quality:
            logger.error("Spotify URLs do not support &sound or &quality parameters")
            raise HTTPException(status_code=400, detail="Spotify URLs do not support &sound or &quality parameters")
        return await handle_spotify(url, request)
    elif is_jiosaavn_url(url):
        if not sound or quality:
            logger.error("JioSaavn URLs only support audio extraction without quality parameters")
            raise HTTPException(status_code=400, detail="JioSaavn URLs only support audio extraction without quality parameters")
        return await handle_jiosaavn(url, request)
    elif is_terabox_url(url):
        if sound or quality:
            logger.error("Terabox URLs do not support &sound or &quality parameters")
            raise HTTPException(status_code=400, detail="Terabox URLs do not support &sound or &quality parameters")
        return await handle_terabox(url, request)
    elif is_instagram_url(url):
        return await handle_instagram(url, request, sound, quality)
    elif is_yt_dlp_supported(url):
        if not check_yt_dlp():
            logger.error("yt-dlp is not available for processing")
            raise HTTPException(status_code=500, detail="yt-dlp is not installed or not in PATH")
        platform = "youtube" if is_youtube_url(url) else "generic"
        return await handle_yt_dlp(url, request, sound, quality, platform=platform)
    else:
        logger.error(f"Unsupported URL type: {url}")
        raise HTTPException(status_code=400, detail=f"Unsupported URL type: {url}")

# New YouTube ID endpoint
@app.get("/yt")
async def download_youtube_by_id(request: Request):
    video_id = request.query_params.get("id")
    si = request.query_params.get("si")
    sound = "sound" in request.query_params
    quality = next((q for q in ["240", "360", "480", "720", "1080"] if q in request.query_params), None)

    if not video_id:
        logger.error("No video ID provided in /yt request")
        raise HTTPException(status_code=400, detail="Video ID parameter is required")

    # Construct YouTube URL
    query_params = {"v": video_id}
    if si:
        query_params["si"] = si
    url = urlunparse(("https", "www.youtube.com", "/watch", "", urlencode(query_params), ""))
    logger.info(f"Constructed YouTube URL: {url}, sound: {sound}, quality: {quality}")

    if not check_yt_dlp():
        logger.error("yt-dlp is not available for YouTube processing")
        raise HTTPException(status_code=500, detail="yt-dlp is not installed or not in PATH")

    return await handle_yt_dlp(url, request, sound, quality, platform="youtube")

# New music platform endpoint
@app.get("/{dummy:get_url}")
async def download_music(request: Request):
    url = request.query_params.get("get_url")
    if not url:
        logger.error("No URL provided in /get_url request")
        raise HTTPException(status_code=400, detail="URL parameter is required")

    if not (url.startswith("http://") or url.startswith("https://")):
        logger.error(f"Invalid URL: {url}. Must be a valid HTTP/HTTPS URL")
        raise HTTPException(status_code=400, detail="Invalid URL. Please provide a valid HTTP/HTTPS URL")

    if not is_music_platform_url(url):
        logger.error(f"Unsupported music platform URL: {url}. Only Spotify, YouTube Music, and JioSaavn are supported")
        raise HTTPException(status_code=400, detail="Only Spotify, YouTube Music, and JioSaavn URLs are supported")

    logger.info(f"Received music request for url: {url}")

    if is_spotify_url(url):
        return await handle_spotify(url, request)
    elif is_jiosaavn_url(url):
        return await handle_jiosaavn(url, request)
    elif is_youtube_music_url(url):
        if not check_yt_dlp():
            logger.error("yt-dlp is not available for YouTube Music processing")
            raise HTTPException(status_code=500, detail="yt-dlp is not installed or not in PATH")
        return await handle_yt_dlp(url, request, sound=True, platform="youtube_music")
    else:
        logger.error(f"Unexpected music platform URL: {url}")
        raise HTTPException(status_code=400, detail="Unexpected music platform URL")

# Streaming endpoint
@app.get("/stream/{filename}")
async def stream_file(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    if not os.path.exists(file_path):
        logger.error(f"Stream endpoint: File not found at {file_path}")
        raise HTTPException(status_code=404, detail=f"File {filename} not found in {DOWNLOAD_DIR}")
    ext = filename.rsplit(".", 1)[-1].lower()
    content_type = {
        "mp4": "video/mp4",
        "mkv": "video/x-matroska",
        "avi": "video/x-msvideo",
        "mp3": "audio/mpeg"
    }.get(ext, "application/octet-stream")
    logger.info(f"Streaming file: {file_path}, Content-Type: {content_type}")
    return FileResponse(file_path, media_type=content_type)

# Terabox streaming
@app.get("/tb/{link_hash}")
async def stream_terabox(link_hash: str, request: Request):
    link_data = TERABOX_LINKS.get(link_hash)
    if not link_data:
        logger.error(f"Terabox link not found for hash: {link_hash}")
        raise HTTPException(status_code=404, detail="Link not found")

    link = link_data["link"]
    filename = link_data["name"]
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(file_path):
        ext = filename.rsplit(".", 1)[-1].lower()
        content_type = {
            "mp4": "video/mp4",
            "mkv": "video/x-matroska",
            "avi": "video/x-msvideo",
            "mp3": "audio/mpeg"
        }.get(ext, "application/octet-stream")
        logger.info(f"Serving existing Terabox file: {file_path}")
        return FileResponse(file_path, media_type=content_type)

    task = DOWNLOAD_TASKS.get(link_hash)
    if task:
        if task["status"] == "completed":
            if os.path.exists(file_path):
                ext = filename.rsplit(".", 1)[-1].lower()
                content_type = {
                    "mp4": "video/mp4",
                    "mkv": "video/x-matroska",
                    "avi": "video/x-msvideo",
                    "mp3": "audio/mpeg"
                }.get(ext, "application/octet-stream")
                logger.info(f"Serving completed Terabox file: {file_path}")
                return FileResponse(file_path, media_type=content_type)
            else:
                logger.error(f"Task marked completed but file missing: {file_path}")
                raise HTTPException(status_code=500, detail="File missing despite completed task")
        elif task["status"] == "downloading":
            for _ in range(30):
                await asyncio.sleep(1)
                if os.path.exists(file_path):
                    ext = filename.rsplit(".", 1)[-1].lower()
                    content_type = {
                        "mp4": "video/mp4",
                        "mkv": "video/x-matroska",
                        "avi": "video/x-msvideo",
                        "mp3": "audio/mpeg"
                    }.get(ext, "application/octet-stream")
                    logger.info(f"Serving Terabox file after waiting: {file_path}")
                    return FileResponse(file_path, media_type=content_type)
            logger.info(f"Terabox download still in progress for: {link_hash}")
            base_url = str(request.base_url).rstrip('/')
            return JSONResponse({"status": "downloading", "stream_link": f"{base_url}/stream/{filename}"}, status_code=202)
        elif task["status"] == "failed":
            logger.error(f"Previous download failed for {link_hash}: {task.get('error')}")
            raise HTTPException(status_code=500, detail=f"Previous download failed: {task.get('error')}")

    logger.info(f"Starting background download for Terabox: {link_hash}")
    asyncio.create_task(background_download(link_hash, link, file_path))
    base_url = str(request.base_url).rstrip('/')
    return JSONResponse({"status": "started", "stream_link": f"{base_url}/stream/{filename}"}, status_code=202)

# Terabox status check
@app.get("/tb_status/{link_hash}")
async def check_download_status(link_hash: str, request: Request):
    task = DOWNLOAD_TASKS.get(link_hash)
    if not task:
        logger.error(f"No download task found for hash: {link_hash}")
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
        DOWNLOAD_TASKS[link_hash] = {"status": "downloading", "file_path": file_path}
        logger.info(f"Started background download for {link}")
        if await download_file(link, file_path):
            DOWNLOAD_TASKS[link_hash] = {"status": "completed", "file_path": file_path}
            logger.info(f"Completed background download for {file_path}")
        else:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Removed partial file: {file_path}")
            DOWNLOAD_TASKS[link_hash] = {"status": "failed", "error": "Download failed"}
            logger.error(f"Background download failed for {link}")
    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Removed partial file: {file_path}")
        DOWNLOAD_TASKS[link_hash] = {"status": "failed", "error": str(e)}
        logger.error(f"Background download error for {link}: {e}")

if __name__ == "__main__":
    check_download_dir()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7777)
