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
import logging.handlers
from concurrent.futures import ThreadPoolExecutor
from aiocache import Cache, cached
import base64
import hashlib
import instaloader
from werkzeug.utils import secure_filename
from urllib.parse import urlparse, urlunparse
import uuid
import re
from typing import Optional
import weakref

app = FastAPI()
DOWNLOAD_DIR = "downloads"
CACHE_DURATION = 43200  # 12 hours
TERABOX_LINKS = {}
DOWNLOAD_TASKS = {}
SPOTIFY_DOWNLOAD_TASKS = {}
COOKIES_FILE = "cookies.txt"
API_KEY = "spotify"

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = "7409903064:AAFSN3FrIK7TjU7vptCRMrA5h0Ywhqo5x88"
BASE_DOMAIN = "https://yt.hosters.club"

# High-performance configuration for 200k daily requests
MAX_WORKERS = 200
MAX_CONCURRENT_DOWNLOADS = 50
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
ACTIVE_DOWNLOADS = set()

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Configure logging with optimized settings
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(
            filename="app.log",
            mode='a',
            maxBytes=10*1024*1024,  # 10 MB
            backupCount=3
        )
    ]
)
logger = logging.getLogger(__name__)

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

connector = aiohttp.TCPConnector(
    limit=500,
    limit_per_host=50,
    ttl_dns_cache=300,
    use_dns_cache=True,
    keepalive_timeout=30,
    enable_cleanup_closed=True
)

def check_yt_dlp():
    try:
        subprocess.check_output("yt-dlp --version", shell=True, stderr=subprocess.STDOUT)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("yt-dlp is not available")
        return False

def check_ffmpeg():
    try:
        subprocess.check_output("ffmpeg -version", shell=True, stderr=subprocess.STDOUT)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error("ffmpeg is not available")
        return False

def clean_youtube_url(url):
    try:
        parsed = urlparse(url)
        if parsed.netloc.endswith("googlevideo.com"):
            return url
        cleaned = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
        return cleaned
    except Exception as e:
        logger.error(f"Failed to clean YouTube URL {url}: {e}")
        return url

def clean_instagram_url(url):
    try:
        parsed = urlparse(url)
        cleaned = urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
        return cleaned.rstrip('/')
    except Exception as e:
        logger.error(f"Failed to clean Instagram URL {url}: {e}")
        return url

async def clean_old_files():
    def cleanup_sync():
        now = time.time()
        cleaned_count = 0
        for filename in os.listdir(DOWNLOAD_DIR):
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            if os.path.isfile(file_path):
                try:
                    file_age = now - min(os.path.getmtime(file_path), os.path.getctime(file_path))
                    if file_age > CACHE_DURATION:
                        os.remove(file_path)
                        cleaned_count += 1
                        if file_path in DOWNLOAD_TASKS:
                            del DOWNLOAD_TASKS[file_path]
                except OSError:
                    pass
        return cleaned_count
    
    cleaned = await asyncio.get_event_loop().run_in_executor(executor, cleanup_sync)
    if cleaned > 0:
        logger.info(f"Cleaned {cleaned} old files")

def is_terabox_url(url): 
    return any(domain in url.lower() for domain in ["terabox.com", "1024tera.com", "terabox.app", "terabox.club"])
def is_spotify_url(url): 
    return "spotify.com" in url.lower()
def is_instagram_url(url): 
    return "instagram.com" in url.lower() and any(x in url.lower() for x in ["reel", "p", "tv"])
def is_youtube_url(url): 
    return url and ("youtube.com" in url.lower() or "youtu.be" in url.lower() or "googlevideo.com" in url.lower())
def is_yt_dlp_supported(url): 
    return True
def is_youtube_video_id(video_id):
    return len(video_id) == 11 and video_id.isalnum()

def get_unique_filename(url, quality=None, sound=False):
    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
    quality_str = quality or ('audio' if sound else 'video')
    ext = "mp3" if sound else "mp4"
    timestamp = int(time.time())
    filename = f"{url_hash}_{quality_str}_{timestamp}.{ext}"
    return secure_filename(filename)

def validate_cookies_file():
    return os.path.exists(COOKIES_FILE) and os.path.getsize(COOKIES_FILE) > 0

@lru_cache(maxsize=2000)
def get_yt_dlp_metadata(url):
    if not check_yt_dlp():
        return {"title": "Unknown Title", "thumbnail": None}
    
    cookie_option = f"--cookies {COOKIES_FILE}" if validate_cookies_file() else ""
    
    try:
        cmd = f'yt-dlp --dump-json {cookie_option} --no-playlist --no-check-certificate --socket-timeout 10 "{url}"'
        process = subprocess.run(
            cmd, shell=True, text=True, capture_output=True, timeout=15
        )
        if process.returncode == 0:
            data = json.loads(process.stdout)
            title = data.get("title", "Unknown Title")
            thumbnail = data.get("thumbnail") or next((t["url"] for t in data.get("thumbnails", []) if t.get("url")), None)
            return {"title": title, "thumbnail": thumbnail}
    except Exception as e:
        logger.error(f"Metadata extraction failed for {url}: {e}")
    
    return {"title": "Unknown Title", "thumbnail": None}

async def download_file(url, path, retries=1):
    async with DOWNLOAD_SEMAPHORE:
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=600),
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        ) as session:
            for attempt in range(1, retries + 1):
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            async with aiofiles.open(path, "wb") as f:
                                async for chunk in response.content.iter_chunked(16384):
                                    await f.write(chunk)
                            logger.info(f"Downloaded: {os.path.basename(path)}")
                            return True
                        else:
                            logger.error(f"Download failed {url}: Status {response.status} (Attempt {attempt}/{retries})")
                            if attempt == retries:
                                return False
                except Exception as e:
                    logger.error(f"Download error {url}: {e} (Attempt {attempt}/{retries})")
                    if attempt == retries:
                        return False
                await asyncio.sleep(1)
            return False

async def get_instagram_video_info(post_url, retries=3, delay=1):
    def sync_get_info():
        try:
            L = instaloader.Instaloader()
            L.context.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            shortcode = post_url.split("/")[-2] if post_url.endswith("/") else post_url.split("/")[-1]
            post = instaloader.Post.from_shortcode(L.context, shortcode)
            if not post.is_video:
                return None
            return {
                "video_url": post.video_url,
                "title": (post.caption or f"Instagram_{shortcode}")[:100],
                "thumbnail": post.url
            }
        except Exception as e:
            logger.error(f"Instagram info error: {e}")
            return None
    
    for attempt in range(1, retries + 1):
        try:
            result = await asyncio.get_event_loop().run_in_executor(executor, sync_get_info)
            if result:
                return result
            logger.warning(f"Failed to get Instagram info for {post_url} (Attempt {attempt}/{retries})")
            if attempt == retries:
                return None
            await asyncio.sleep(delay)
        except Exception as e:
            logger.error(f"Instagram info error for {post_url}: {e} (Attempt {attempt}/{retries})")
            if attempt == retries:
                return None
            await asyncio.sleep(delay)
    return None

async def process_instagram_video(temp_path, output_path, sound=False, quality=None):
    if not check_ffmpeg():
        raise HTTPException(status_code=500, detail="ffmpeg not available")
    
    try:
        if sound:
            if not output_path.endswith('.mp3'):
                output_path = output_path.rsplit('.', 1)[0] + '.mp3'
            cmd = f'ffmpeg -i "{temp_path}" -vn -acodec libmp3lame -b:a 320k -ar 44100 -ac 2 "{output_path}" -y -loglevel error'
        else:
            if quality:
                resolution_map = {
                    "1080": "1920:1080", "720": "1280:720", "480": "854:480", 
                    "360": "640:360", "240": "426:240"
                }
                res = resolution_map.get(quality, "1280:720")
                cmd = f'ffmpeg -i "{temp_path}" -vf "scale={res}:force_original_aspect_ratio=decrease:force_divisible_by=2" -c:v libx264 -preset fast -crf 23 -c:a aac -b:a 128k "{output_path}" -y -loglevel error'
            else:
                cmd = f'ffmpeg -i "{temp_path}" -c:v libx264 -preset fast -crf 23 -c:a aac -b:a 128k "{output_path}" -y -loglevel error'
        
        logger.info(f"Processing Instagram video with command: {cmd}")
        process = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            logger.info(f"Successfully processed Instagram video: {os.path.basename(output_path)}")
            return True
        else:
            error_msg = stderr.decode().strip()
            logger.error(f"ffmpeg failed: {error_msg}")
            return False
    except Exception as e:
        logger.error(f"Instagram processing error: {e}")
        return False

def extract_urls_from_text(text):
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    urls = re.findall(url_pattern, text)
    if not urls:
        domain_pattern = r'(?:www\.)?(?:youtube\.com|youtu\.be|instagram\.com|spotify\.com|terabox\.com)[^\s]*'
        potential_urls = re.findall(domain_pattern, text, re.IGNORECASE)
        urls = [f"https://{url}" if not url.startswith(('http://', 'https://')) else url for url in potential_urls]
    return urls

async def send_telegram_message(chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, timeout=aiohttp.ClientTimeout(total=10)) as response:
                return await response.json()
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

async def send_telegram_photo(chat_id, photo_url, caption, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    data = {
        "chat_id": chat_id,
        "photo": photo_url,
        "caption": caption,
        "parse_mode": "HTML"
    }
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, timeout=aiohttp.ClientTimeout(total=10)) as response:
                return await response.json()
    except:
        await send_telegram_message(chat_id, caption, reply_markup)

async def send_telegram_document(chat_id, file_path, caption=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    async with aiohttp.ClientSession() as session:
        form = aiohttp.FormData()
        form.add_field('chat_id', str(chat_id))
        if caption:
            form.add_field('caption', caption)
        form.add_field('document', open(file_path, 'rb'), filename=os.path.basename(file_path))
        async with session.post(url, data=form) as response:
            return await response.json()

async def wait_for_download(file_path, timeout=300, check_interval=5):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if os.path.exists(file_path):
            return True
        await asyncio.sleep(check_interval)
    return False

async def send_file_when_ready(chat_id, file_path, caption):
    try:
        if await wait_for_download(file_path):
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
            if file_size <= 50:
                await send_telegram_document(chat_id, file_path, caption)
            else:
                await send_telegram_message(chat_id, f"📁 File is larger than 50MB ({file_size:.2f}MB). Please use the stream link.")
        else:
            await send_telegram_message(chat_id, "⏳ Download took too long. Please try again later.")
    except Exception as e:
        logger.error(f"Error sending file: {e}")
        await send_telegram_message(chat_id, "❌ Error sending file. Please try again later.")

async def send_quality_options(chat_id, url):
    keyboard = {
        "inline_keyboard": [
            [{"text": "240p", "callback_data": f"quality:240:{url}"}],
            [{"text": "360p", "callback_data": f"quality:360:{url}"}],
            [{"text": "480p", "callback_data": f"quality:480:{url}"}],
            [{"text": "720p", "callback_data": f"quality:720:{url}"}],
            [{"text": "1080p", "callback_data": f"quality:1080:{url}"}],
            [{"text": "Audio Only", "callback_data": f"quality:audio:{url}"}]
        ]
    }
    await send_telegram_message(chat_id, "📹 Select the quality:", keyboard)

def create_inline_keyboard(mp4_url=None, mp3_url=None, direct_link=None):
    keyboard = []
    if mp4_url:
        keyboard.append([{"text": "🎬 Watch Online", "url": mp4_url}])
    if mp3_url:
        keyboard.append([{"text": "🎵 Listen Online", "url": mp3_url}])
    if direct_link and not mp4_url:
        keyboard.append([{"text": "📥 Direct Download", "url": direct_link}])
    return {"inline_keyboard": keyboard}

async def process_url_with_quality(chat_id, url, quality):
    try:
        await send_telegram_message(chat_id, f"🔄 Processing your URL with quality {quality}...\n<code>{url}</code>")
        
        api_url = f"{BASE_DOMAIN}/?url={url}"
        if quality != "audio":
            api_url += f"&quality={quality}"
        else:
            api_url += "&sound"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    title = data.get("title", "Unknown Title")
                    thumbnail = data.get("thumbnail")
                    mp4_url = data.get("stream_mp4")
                    mp3_url = data.get("stream_mp3")
                    direct_link = data.get("link")
                    
                    response_text = f"""
✅ <b>Processing started!</b>

📝 <b>Title:</b> {title[:100]}{"..." if len(title) > 100 else ""}

🎬 <b>Video:</b> {"✅ Available" if mp4_url else "❌ Not available"}
🎵 <b>Audio:</b> {"✅ Available" if mp3_url else "❌ Not available"}

⚡ <b>Downloads are running in background!</b>
"""
                    if is_terabox_url(url):
                        if mp4_url:
                            response_text += "\n🎬 <b>Stream:</b> Available"
                        elif direct_link:
                            response_text += "\n📥 <b>Direct Download:</b> Available"
                    response_text += "\n\n👨‍💻 Dev: @SUN_GOD_LUFFYY"
                    
                    keyboard = create_inline_keyboard(mp4_url, mp3_url, direct_link if is_terabox_url(url) else None)
                    
                    if thumbnail:
                        await send_telegram_photo(chat_id, thumbnail, response_text, keyboard)
                    else:
                        await send_telegram_message(chat_id, response_text, keyboard)
                    
                    if data.get("file_name"):
                        file_path = os.path.join(DOWNLOAD_DIR, data["file_name"])
                        caption = "🎬 Your video is ready!" if mp4_url else "🎵 Your audio is ready!"
                        asyncio.create_task(send_file_when_ready(chat_id, file_path, caption))
                else:
                    await send_telegram_message(chat_id, f"❌ Failed to process URL: API returned status {response.status}")
    except Exception as e:
        logger.error(f"Error processing URL with quality: {e}")
        await send_telegram_message(chat_id, f"❌ Error: {str(e)}\n\n👨‍💻 Contact: @SUN_GOD_LUFFYY")

@app.post("/telegram_webhook")
async def telegram_webhook(request: Request):
    try:
        update = await request.json()
        
        if "callback_query" in update:
            callback_query = update["callback_query"]
            chat_id = callback_query["message"]["chat"]["id"]
            data = callback_query["data"]
            if data.startswith("quality:"):
                _, quality, url = data.split(":", 2)
                await process_url_with_quality(chat_id, url, quality)
            return JSONResponse({"ok": True})
        
        if "message" not in update:
            return JSONResponse({"ok": True})
        
        message = update["message"]
        chat_id = message["chat"]["id"]
        
        if message.get("text", "").startswith("/start"):
            first_name = message["from"].get("first_name", "User")
            welcome_text = f"""
🎉 Hi <b>{first_name}</b>! Welcome to our video downloader bot!

📱 Just send us any URL and we'll help you download it!

🔗 Supported platforms:
• YouTube • Instagram • Spotify • Terabox • And many more!

⚡ <b>Downloads are processed in background for maximum speed!</b>

👨‍💻 Developer: @SUN_GOD_LUFFYY
            """
            await send_telegram_message(chat_id, welcome_text)
            return JSONResponse({"ok": True})
        
        text = message.get("text", "")
        urls = extract_urls_from_text(text)
        
        if urls:
            for url in urls:
                await send_quality_options(chat_id, url)
        else:
            await send_telegram_message(
                chat_id,
                "📎 Please send a valid URL!\n\n✅ Supported: YouTube, Instagram (reels, posts, IGTV), Spotify, Terabox, and more!\n\n👨‍💻 Dev: @SUN_GOD_LUFFYY"
            )
        
        return JSONResponse({"ok": True})
    
    except Exception as e:
        logger.error(f"Telegram webhook error: {e}")
        return JSONResponse({"ok": True})

@app.get("/")
async def download_video(request: Request):
    asyncio.create_task(clean_old_files())
    
    url = request.query_params.get("url")
    sound = "sound" in request.query_params
    quality = next((q for q in ["240", "360", "480", "720", "1080"] if q in request.query_params), None)

    if not url:
        raise HTTPException(status_code=400, detail="URL parameter is required")

    if not (url.startswith("http://") or url.startswith("https://")):
        if url.startswith("www.") or any(domain in url for domain in ["youtube.com", "youtu.be", "instagram.com", "spotify.com", "terabox.com"]):
            url = "https://" + url
        else:
            raise HTTPException(status_code=400, detail="Invalid URL format")

    if is_instagram_url(url):
        url = clean_instagram_url(url)
    elif is_youtube_url(url):
        url = clean_youtube_url(url)

    logger.info(f"Processing URL: {url}")

    if is_spotify_url(url):
        if sound or quality:
            raise HTTPException(status_code=400, detail="Spotify URLs do not support &sound or &quality parameters")
        return await handle_spotify(url, request)
    elif is_terabox_url(url):
        if sound or quality:
            raise HTTPException(status_code=400, detail="Terabox URLs do not support &sound or &quality parameters")
        return await handle_terabox(url, request)
    elif is_instagram_url(url):
        return await handle_instagram(url, request, sound, quality)
    elif is_yt_dlp_supported(url):
        if not check_yt_dlp():
            raise HTTPException(status_code=500, detail="yt-dlp not available")
        return await handle_yt_dlp(url, request, sound, quality)
    else:
        raise HTTPException(status_code=400, detail="Unsupported URL type")

async def handle_yt_dlp(url: str, request: Request, sound: bool = False, quality: str = None):
    if is_youtube_video_id(url):
        url = f"https://www.youtube.com/watch?v={url}"
    if is_youtube_url(url):
        url = clean_youtube_url(url)
    
    metadata = get_yt_dlp_metadata(url)
    
    video_filename = audio_filename = None
    
    if not sound and not quality:
        video_filename = get_unique_filename(url, "1080", False)
        audio_filename = get_unique_filename(url, None, True)
        video_path = os.path.join(DOWNLOAD_DIR, video_filename)
        audio_path = os.path.join(DOWNLOAD_DIR, audio_filename)
        
        if video_path not in ACTIVE_DOWNLOADS:
            ACTIVE_DOWNLOADS.add(video_path)
            asyncio.create_task(background_yt_dlp_download(url, video_path, "video", "1080"))
        if audio_path not in ACTIVE_DOWNLOADS:
            ACTIVE_DOWNLOADS.add(audio_path)
            asyncio.create_task(background_yt_dlp_download(url, audio_path, "audio"))
    elif sound:
        audio_filename = get_unique_filename(url, None, True)
        audio_path = os.path.join(DOWNLOAD_DIR, audio_filename)
        if audio_path not in ACTIVE_DOWNLOADS:
            ACTIVE_DOWNLOADS.add(audio_path)
            asyncio.create_task(background_yt_dlp_download(url, audio_path, "audio"))
    else:
        video_filename = get_unique_filename(url, quality, False)
        video_path = os.path.join(DOWNLOAD_DIR, video_filename)
        if video_path not in ACTIVE_DOWNLOADS:
            ACTIVE_DOWNLOADS.add(video_path)
            asyncio.create_task(background_yt_dlp_download(url, video_path, "video", quality))

    base_url = str(request.base_url).rstrip('/')
    response = {
        "title": metadata["title"],
        "thumbnail": metadata["thumbnail"],
        "link": url,
        "stream_mp4": f"{base_url}/stream/{video_filename}" if video_filename else None,
        "stream_mp3": f"{base_url}/stream/{audio_filename}" if audio_filename else None,
        "file_name": video_filename or audio_filename
    }

    return JSONResponse(response)

async def background_yt_dlp_download(url, path, download_type, quality=None, retries=3, delay=1):
    async with DOWNLOAD_SEMAPHORE:
        try:
            DOWNLOAD_TASKS[path] = {"status": "downloading", "url": url, "retries": 0, "last_error": None}
            
            if os.path.exists(path):
                DOWNLOAD_TASKS[path] = {"status": "completed", "url": url, "retries": 0}
                logger.info(f"File already exists: {os.path.basename(path)}")
                return

            cookie_option = f"--cookies {COOKIES_FILE}" if validate_cookies_file() else ""
            
            if download_type == "video":
                quality_map = {
                    "240": "best[height<=240]",
                    "360": "best[height<=360]", 
                    "480": "best[height<=480]",
                    "720": "best[height<=720]",
                    "1080": "best[height<=1080]"
                }
                format_selector = quality_map.get(quality, "best")
                cmd = f'yt-dlp -f "{format_selector}" {cookie_option} --no-check-certificate -o "{path}" "{url}"'
            else:
                cmd = f'yt-dlp --extract-audio --audio-format mp3 --audio-quality 320K {cookie_option} --no-check-certificate -o "{path.replace(".mp3", ".%(ext)s")}" "{url}"'
            
            logger.info(f"Starting {download_type} download: {os.path.basename(path)}")
            
            def run_download():
                return subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=1800)
            
            for attempt in range(1, retries + 1):
                try:
                    process = await asyncio.get_event_loop().run_in_executor(executor, run_download)
                    
                    if process.returncode == 0:
                        DOWNLOAD_TASKS[path] = {"status": "completed", "url": url, "retries": attempt - 1}
                        logger.info(f"Completed {download_type} download: {os.path.basename(path)}")
                        return
                    else:
                        error_msg = process.stderr[:500]
                        DOWNLOAD_TASKS[path] = {"status": "downloading", "url": url, "retries": attempt, "last_error": error_msg}
                        logger.error(f"Download failed for {url}: {error_msg} (Attempt {attempt}/{retries})")
                        if attempt == retries:
                            DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "retries": attempt, "last_error": error_msg}
                            return
                except Exception as e:
                    error_msg = str(e)
                    DOWNLOAD_TASKS[path] = {"status": "downloading", "url": url, "retries": attempt, "last_error": error_msg}
                    logger.error(f"Download error for {url}: {e} (Attempt {attempt}/{retries})")
                    if attempt == retries:
                        DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "retries": attempt, "last_error": error_msg}
                        return
                await asyncio.sleep(delay)
            
        except Exception as e:
            DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "retries": retries, "last_error": str(e)}
            logger.error(f"Critical download error for {url}: {e}")
        finally:
            ACTIVE_DOWNLOADS.discard(path)

async def handle_terabox(url: str, request: Request):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://tb.hosters.club/?url={url}", timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    direct_link = data.get("direct_link")
                    if direct_link:
                        link_hash = hashlib.md5(direct_link.encode()).hexdigest()[:16]
                        filename = data.get("name", f"terabox_{link_hash}.mp4")
                        file_path = os.path.join(DOWNLOAD_DIR, filename)
                        
                        TERABOX_LINKS[link_hash] = {"link": direct_link, "name": filename}
                        
                        if file_path not in ACTIVE_DOWNLOADS:
                            ACTIVE_DOWNLOADS.add(file_path)
                            asyncio.create_task(background_download(direct_link, file_path))
                        
                        base_url = str(request.base_url).rstrip('/')
                        return JSONResponse({
                            "title": data.get("name", "Terabox Video"),
                            "thumbnail": data.get("thumbnail"),
                            "link": direct_link,
                            "stream_mp4": f"{base_url}/tb/{link_hash}",
                            "stream_mp3": None,
                            "file_name": filename
                        })
                    else:
                        raise HTTPException(status_code=500, detail="No direct link")
                else:
                    raise HTTPException(status_code=response.status, detail="Terabox API error")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Terabox error: {str(e)}")

async def handle_spotify(url: str, request: Request):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://sp.hosters.club/?url={url}", timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    if not data.get("error", True) and data.get("url"):
                        track_url = data["url"]
                        filename = get_unique_filename(track_url, None, True)
                        file_path = os.path.join(DOWNLOAD_DIR, filename)
                        track_hash = hashlib.md5(track_url.encode()).hexdigest()[:16]

                        SPOTIFY_DOWNLOAD_TASKS[track_hash] = {
                            "link": track_url, "name": filename, "file_path": file_path
                        }

                        if file_path not in ACTIVE_DOWNLOADS:
                            ACTIVE_DOWNLOADS.add(file_path)
                            asyncio.create_task(background_download(track_url, file_path))
                        
                        base_url = str(request.base_url).rstrip('/')
                        return JSONResponse({
                            "title": data.get("name", "Spotify Track"),
                            "thumbnail": None,
                            "link": track_url,
                            "stream_mp4": None,
                            "stream_mp3": f"{base_url}/spotify/{track_hash}",
                            "file_name": filename
                        })
                    else:
                        raise HTTPException(status_code=500, detail="Spotify API error")
                else:
                    raise HTTPException(status_code=response.status, detail="Spotify API error")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Spotify error: {str(e)}")

async def handle_instagram(url: str, request: Request, sound: bool = False, quality: str = None):
    info = await get_instagram_video_info(url)
    if not info or not info["video_url"]:
        error_msg = f"Invalid Instagram URL or no video found: {url}"
        logger.error(error_msg)
        raise HTTPException(status_code=400, detail=error_msg)

    output_filename = get_unique_filename(url, quality or "original", sound)
    output_path = os.path.join(DOWNLOAD_DIR, output_filename)

    if output_path not in ACTIVE_DOWNLOADS:
        ACTIVE_DOWNLOADS.add(output_path)
        asyncio.create_task(background_instagram_download(info["video_url"], output_path, sound, quality))

    base_url = str(request.base_url).rstrip('/')
    return JSONResponse({
        "title": info["title"],
        "thumbnail": info["thumbnail"],
        "link": info["video_url"],
        "stream_mp4": f"{base_url}/stream/{output_filename}" if not sound else None,
        "stream_mp3": f"{base_url}/stream/{output_filename}" if sound else None,
        "file_name": output_filename
    })

async def background_download(url, file_path, retries=3, delay=1):
    try:
        DOWNLOAD_TASKS[file_path] = {"status": "downloading", "url": url, "retries": 0, "last_error": None}
        for attempt in range(1, retries + 1):
            if await download_file(url, file_path, retries=1):
                DOWNLOAD_TASKS[file_path] = {"status": "completed", "url": url, "retries": attempt - 1}
                logger.info(f"Completed download: {os.path.basename(file_path)}")
                return
            else:
                error_msg = f"Failed to download {url} (Attempt {attempt}/{retries})"
                DOWNLOAD_TASKS[file_path] = {"status": "downloading", "url": url, "retries": attempt, "last_error": error_msg}
                logger.error(error_msg)
                if attempt == retries:
                    DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": url, "retries": attempt, "last_error": error_msg}
                    return
            await asyncio.sleep(delay)
    except Exception as e:
        error_msg = f"Critical download error: {str(e)}"
        DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": url, "retries": retries, "last_error": error_msg}
        logger.error(f"Download error for {url}: {e}")
    finally:
        ACTIVE_DOWNLOADS.discard(file_path)

async def background_instagram_download(video_url, output_path, sound, quality, retries=3, delay=1):
    temp_filename = f"temp_{int(time.time())}_{os.getpid()}.mp4"
    temp_path = os.path.join(DOWNLOAD_DIR, temp_filename)
    
    try:
        DOWNLOAD_TASKS[output_path] = {"status": "downloading", "url": video_url, "retries": 0, "last_error": None}
        
        for attempt in range(1, retries + 1):
            try:
                if await download_file(video_url, temp_path, retries=1):
                    if await process_instagram_video(temp_path, output_path, sound, quality):
                        DOWNLOAD_TASKS[output_path] = {"status": "completed", "url": video_url, "retries": attempt - 1}
                        logger.info(f"Completed Instagram download: {os.path.basename(output_path)}")
                        return
                    else:
                        error_msg = f"Instagram processing failed for {video_url} (Attempt {attempt}/{retries})"
                        DOWNLOAD_TASKS[output_path] = {"status": "downloading", "url": video_url, "retries": attempt, "last_error": error_msg}
                        logger.error(error_msg)
                        if attempt == retries:
                            DOWNLOAD_TASKS[output_path] = {"status": "failed", "url": video_url, "retries": attempt, "last_error": error_msg}
                            return
                else:
                    error_msg = f"Instagram download failed for {video_url} (Attempt {attempt}/{retries})"
                    DOWNLOAD_TASKS[output_path] = {"status": "downloading", "url": video_url, "retries": attempt, "last_error": error_msg}
                    logger.error(error_msg)
                    if attempt == retries:
                        DOWNLOAD_TASKS[output_path] = {"status": "failed", "url": video_url, "retries": attempt, "last_error": error_msg}
                        return
                await asyncio.sleep(delay)
            except Exception as e:
                error_msg = f"Instagram download error: {str(e)} (Attempt {attempt}/{retries})"
                DOWNLOAD_TASKS[output_path] = {"status": "downloading", "url": video_url, "retries": attempt, "last_error": error_msg}
                logger.error(error_msg)
                if attempt == retries:
                    DOWNLOAD_TASKS[output_path] = {"status": "failed", "url": video_url, "retries": attempt, "last_error": error_msg}
                    return
    except Exception as e:
        error_msg = f"Critical Instagram download error: {str(e)}"
        DOWNLOAD_TASKS[output_path] = {"status": "failed", "url": video_url, "retries": retries, "last_error": error_msg}
        logger.error(error_msg)
    finally:
        ACTIVE_DOWNLOADS.discard(output_path)
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except:
                pass

@app.get("/stream/{filename}")
async def stream_file(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    try:
        if os.path.exists(file_path):
            ext = filename.rsplit(".", 1)[-1].lower()
            content_type = {
                "mp4": "video/mp4", "mkv": "video/x-matroska", 
                "avi": "video/x-msvideo", "mp3": "audio/mpeg"
            }.get(ext, "application/octet-stream")
            return FileResponse(file_path, media_type=content_type)
        else:
            task = DOWNLOAD_TASKS.get(file_path, {})
            logger.warning(f"File not found: {filename}, Task status: {task.get('status', 'unknown')}, Error: {task.get('last_error', 'None')}")
            raise HTTPException(status_code=404, detail=f"File not yet available. Status: {task.get('status', 'unknown')}, Last error: {task.get('last_error', 'None')}")
    except Exception as e:
        logger.error(f"Stream error for {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Stream error: {str(e)}")

@app.get("/spotify/{track_hash}")
async def stream_spotify(track_hash: str):
    try:
        track_data = SPOTIFY_DOWNLOAD_TASKS.get(track_hash)
        if not track_data:
            logger.warning(f"Spotify track not found: {track_hash}")
            raise HTTPException(status_code=404, detail="Track not found")
        
        file_path = track_data["file_path"]
        if os.path.exists(file_path):
            return FileResponse(file_path, media_type="audio/mpeg")
        else:
            task = DOWNLOAD_TASKS.get(file_path, {})
            logger.warning(f"Spotify file not found: {file_path}, Task status: {task.get('status', 'unknown')}, Error: {task.get('last_error', 'None')}")
            raise HTTPException(status_code=404, detail=f"File not yet available. Status: {task.get('status', 'unknown')}, Last error: {task.get('last_error', 'None')}")
    except Exception as e:
        logger.error(f"Spotify stream error for {track_hash}: {e}")
        raise HTTPException(status_code=500, detail=f"Stream error: {str(e)}")

@app.get("/tb/{link_hash}")
async def stream_terabox(link_hash: str):
    try:
        link_data = TERABOX_LINKS.get(link_hash)
        if not link_data:
            logger.warning(f"Terabox link not found: {link_hash}")
            raise HTTPException(status_code=404, detail="Link not found")
        
        filename = link_data["name"]
        file_path = os.path.join(DOWNLOAD_DIR, filename)
        
        if os.path.exists(file_path):
            ext = filename.rsplit(".", 1)[-1].lower()
            content_type = {"mp4": "video/mp4", "mkv": "video/x-matroska", "avi": "video/x-msvideo"}.get(ext, "application/octet-stream")
            return FileResponse(file_path, media_type=content_type)
        else:
            task = DOWNLOAD_TASKS.get(file_path, {})
            logger.warning(f"Terabox file not found: {file_path}, Task status: {task.get('status', 'unknown')}, Error: {task.get('last_error', 'None')}")
            raise HTTPException(status_code=404, detail=f"File not yet available. Status: {task.get('status', 'unknown')}, Last error: {task.get('last_error', 'None')}")
    except Exception as e:
        logger.error(f"Terabox stream error for {link_hash}: {e}")
        raise HTTPException(status_code=500, detail=f"Stream error: {str(e)}")

@app.get("/status/{filename}")
async def check_status(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    try:
        if os.path.exists(file_path):
            return JSONResponse({"status": "completed", "retries": 0, "last_error": None})
        task = DOWNLOAD_TASKS.get(file_path)
        if task:
            return JSONResponse({
                "status": task["status"],
                "retries": task.get("retries", 0),
                "last_error": task.get("last_error", None),
                "url": task.get("url", None)
            })
        logger.warning(f"No download task found for {filename}")
        raise HTTPException(status_code=404, detail="No download task found")
    except Exception as e:
        logger.error(f"Status check error for {filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Status check error: {str(e)}")

@app.get("/yt")
async def download_youtube_by_id(id: str, request: Request):
    if not id:
        raise HTTPException(status_code=400, detail="ID parameter is required")
    url = f"https://www.youtube.com/watch?v={id}"
    return await handle_yt_dlp(url, request)

@app.get("/health")
async def health_check():
    return JSONResponse({
        "status": "healthy",
        "active_downloads": len(ACTIVE_DOWNLOADS),
        "total_tasks": len(DOWNLOAD_TASKS),
        "available_workers": MAX_WORKERS
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7777, workers=1)
