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
import re

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

executor = ThreadPoolExecutor(max_workers=100)

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
            file_age = now - min(mtime, ctime)
            logger.debug(f"Checking file {file_path}: age={file_age:.2f} seconds")
            if file_age > CACHE_DURATION:
                try:
                    os.remove(file_path)
                    logger.info(f"Removed old file: {file_path} (age: {file_age:.2f} seconds)")
                    if file_path in DOWNLOAD_TASKS:
                        del DOWNLOAD_TASKS[file_path]
                        logger.debug(f"Cleared expired task for {file_path}")
                except OSError as e:
                    logger.error(f"Failed to remove file {file_path}: {e}")

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
            logger.debug(f"Reusing filename for {url}: {os.path.basename(file_path)}")
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
        logger.warning(f"No cookies.txt found at {COOKIES_FILE}")
        return False
    try:
        with open(COOKIES_FILE, "r") as f:
            content = f.read().strip()
            if not content:
                logger.error(f"cookies.txt is empty")
                return False
            return True
    except IOError as e:
        logger.error(f"Failed to read cookies.txt: {e}")
        return False

# Generic metadata extraction using yt-dlp
@lru_cache(maxsize=1000)
def get_yt_dlp_metadata(url):
    if not check_yt_dlp():
        logger.error("Cannot fetch metadata: yt-dlp is not available")
        return {"title": "Unknown Title", "thumbnail": None}
    
    cookie_option = f"--cookies {COOKIES_FILE}" if validate_cookies_file() else ""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            cmd = f'yt-dlp --dump-json {cookie_option} --no-playlist --no-check-certificate --retries 3 "{url}"'
            logger.debug(f"Executing yt-dlp metadata command: {cmd}")
            process = subprocess.run(
                cmd, shell=True, text=True, capture_output=True, timeout=30
            )
            if process.returncode != 0:
                logger.error(f"yt-dlp metadata failed: {process.stderr}")
                if attempt == max_retries - 1:
                    return {"title": "Unknown Title", "thumbnail": None}
                time.sleep(2 ** (attempt + 1))
                continue
            data = json.loads(process.stdout)
            title = data.get("title", "Unknown Title")
            thumbnail = data.get("thumbnail") or next((t["url"] for t in data.get("thumbnails", []) if t.get("url")), None)
            logger.debug(f"Metadata for {url}: title={title}")
            return {"title": title, "thumbnail": thumbnail}
        except Exception as e:
            logger.error(f"Error fetching metadata for {url}: {e}")
            if attempt == max_retries - 1:
                return {"title": "Unknown Title", "thumbnail": None}
            time.sleep(2 ** (attempt + 1))

# File download utility
async def download_file(url, path):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300)) as session:
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    async with aiofiles.open(path, "wb") as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)
                    logger.info(f"Successfully downloaded file: {path}")
                    return True
                logger.error(f"Failed to download {url}: Status {response.status}")
                return False
        except Exception as e:
            logger.error(f"Error downloading file {url}: {e}")
            return False

# Enhanced Instagram video URL and title extraction
async def get_instagram_video_info(post_url):
    def sync_get_instagram_info():
        try:
            L = instaloader.Instaloader()
            shortcode = post_url.split("/")[-2] if post_url.endswith("/") else post_url.split("/")[-1]
            post = instaloader.Post.from_shortcode(L.context, shortcode)
            return {
                "video_url": post.video_url if post.is_video else None,
                "title": (post.caption or f"Instagram_{shortcode}")[:100],
                "thumbnail": post.url
            }
        except Exception as e:
            logger.error(f"Error fetching Instagram info: {e}")
            return None
    return await asyncio.get_event_loop().run_in_executor(executor, sync_get_instagram_info)

# Enhanced Instagram video processing with better MP3 support
async def process_instagram_video(temp_path, output_path, sound=False, quality=None):
    if not check_ffmpeg():
        logger.error("Cannot process Instagram video: ffmpeg is not available")
        raise HTTPException(status_code=500, detail="ffmpeg is not installed or not in PATH")
    try:
        if sound:
            cmd = f'ffmpeg -i "{temp_path}" -vn -acodec libmp3lame -ab 320k -ar 44100 -ac 2 "{output_path}" -y'
        else:
            if quality:
                resolution = {
                    "1080": "1920x1080",
                    "720": "1280x720", 
                    "480": "854x480",
                    "360": "640x360",
                    "240": "426x240"
                }[quality]
                cmd = f'ffmpeg -i "{temp_path}" -vcodec libx264 -s {resolution} -acodec aac -ab 128k -preset veryfast -crf 23 -pix_fmt yuv420p "{output_path}" -y'
            else:
                cmd = f'ffmpeg -i "{temp_path}" -vcodec libx264 -acodec aac -ab 128k -preset veryfast -crf 23 -pix_fmt yuv420p "{output_path}" -y'
        
        logger.info(f"Processing Instagram video with command: {cmd}")
        process = await asyncio.create_subprocess_shell(
            cmd, 
            stdout=asyncio.subprocess.PIPE, 
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            logger.error(f"ffmpeg failed: {stderr.decode()}")
            raise HTTPException(status_code=500, detail=f"ffmpeg processing failed: {stderr.decode()}")
        
        logger.info(f"Successfully processed Instagram video: {output_path}")
        return True
    except Exception as e:
        logger.error(f"Error processing Instagram video: {e}")
        return False

# Telegram Bot Functions
def extract_urls_from_text(text):
    """Extract URLs from text message"""
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    urls = re.findall(url_pattern, text)
    # Also check for URLs without protocol
    if not urls:
        # Check for common domains without protocol
        domain_pattern = r'(?:www\.)?(?:youtube\.com|youtu\.be|instagram\.com|spotify\.com|terabox\.com)[^\s]*'
        potential_urls = re.findall(domain_pattern, text, re.IGNORECASE)
        urls = [f"https://{url}" if not url.startswith(('http://', 'https://')) else url for url in potential_urls]
    return urls

async def send_telegram_message(chat_id, text, reply_markup=None):
    """Send message to Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=data) as response:
                result = await response.json()
                logger.info(f"Sent Telegram message to {chat_id}")
                return result
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")

async def send_telegram_photo(chat_id, photo_url, caption, reply_markup=None):
    """Send photo with caption to Telegram"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    data = {
        "chat_id": chat_id,
        "photo": photo_url,
        "caption": caption,
        "parse_mode": "HTML"
    }
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=data) as response:
                result = await response.json()
                logger.info(f"Sent Telegram photo to {chat_id}")
                return result
        except Exception as e:
            logger.error(f"Failed to send Telegram photo: {e}")
            # Fallback to text message
            await send_telegram_message(chat_id, caption, reply_markup)

def create_inline_keyboard(mp4_url=None, mp3_url=None, direct_link=None):
    """Create inline keyboard for Telegram"""
    keyboard = []
    if mp4_url:
        keyboard.append([{"text": "🎬 Watch Online", "url": mp4_url}])
    if mp3_url:
        keyboard.append([{"text": "🎵 Listen Online", "url": mp3_url}])
    if direct_link and not mp4_url:  # For Terabox direct links when stream not available
        keyboard.append([{"text": "📥 Direct Download", "url": direct_link}])
    return {"inline_keyboard": keyboard}

async def trigger_download(url):
    """Make a request to trigger actual download"""
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as session:
            async with session.get(url) as response:
                logger.info(f"Triggered download for {url}: {response.status}")
                return response.status == 200
    except Exception as e:
        logger.error(f"Failed to trigger download for {url}: {e}")
        return False

# Telegram Webhook Handler with ACTUAL download triggering
@app.post("/telegram_webhook")
async def telegram_webhook(request: Request):
    try:
        update = await request.json()
        logger.info(f"Received Telegram update: {update}")
        
        if "message" not in update:
            return JSONResponse({"ok": True})
        
        message = update["message"]
        chat_id = message["chat"]["id"]
        
        # Handle /start command
        if message.get("text", "").startswith("/start"):
            first_name = message["from"].get("first_name", "User")
            welcome_text = f"""
🎉 Hi <b>{first_name}</b>! Welcome to our video downloader bot!

📱 Just send us any URL and we'll help you download it!

🔗 Supported platforms:
• YouTube
• Instagram  
• Spotify
• Terabox
• And many more!

👨‍💻 Developer: @SUN_GOD_LUFFYY
            """
            await send_telegram_message(chat_id, welcome_text)
            return JSONResponse({"ok": True})
        
        # Handle URL messages
        text = message.get("text", "")
        urls = extract_urls_from_text(text)
        
        if urls:
            for url in urls:
                try:
                    # Send processing message
                    processing_msg = await send_telegram_message(
                        chat_id, 
                        f"🔄 Processing your URL...\n<code>{url}</code>"
                    )
                    
                    # Make request to our own API to get metadata and stream URLs
                    api_url = f"{BASE_DOMAIN}/?url={url}"
                    
                    async with aiohttp.ClientSession() as session:
                        async with session.get(api_url, timeout=aiohttp.ClientTimeout(total=60)) as response:
                            if response.status == 200:
                                data = await response.json()
                                
                                # Extract information
                                title = data.get("title", "Unknown Title")
                                thumbnail = data.get("thumbnail")
                                mp4_url = data.get("stream_mp4")
                                mp3_url = data.get("stream_mp3") 
                                direct_link = data.get("link")  # For Terabox direct links
                                
                                # ACTUALLY TRIGGER THE DOWNLOADS by making requests to stream URLs
                                download_tasks = []
                                if mp4_url:
                                    download_tasks.append(trigger_download(mp4_url))
                                if mp3_url:
                                    download_tasks.append(trigger_download(mp3_url))
                                
                                # Wait a bit for downloads to start
                                if download_tasks:
                                    await asyncio.gather(*download_tasks, return_exceptions=True)
                                    await asyncio.sleep(2)  # Give downloads time to start
                                
                                # Create response message with title and status
                                response_text = f"""
✅ <b>Ready to download!</b>

📝 <b>Title:</b> {title[:100]}{"..." if len(title) > 100 else ""}

🎬 <b>Video:</b> {"✅ Available" if mp4_url else "❌ Not available"}
🎵 <b>Audio:</b> {"✅ Available" if mp3_url else "❌ Not available"}
"""
                                
                                # Special handling for Terabox
                                if is_terabox_url(url):
                                    if mp4_url:
                                        response_text += "\n🎬 <b>Stream:</b> Available"
                                    elif direct_link:
                                        response_text += "\n📥 <b>Direct Download:</b> Available"
                                
                                response_text += "\n\n⏰ <b>Downloads started!</b> Click buttons below to access."
                                response_text += "\n\n👨‍💻 Dev: @SUN_GOD_LUFFYY"
                                
                                # Create inline keyboard
                                keyboard = create_inline_keyboard(mp4_url, mp3_url, direct_link if is_terabox_url(url) else None)
                                
                                # Send with thumbnail if available
                                if thumbnail:
                                    try:
                                        await send_telegram_photo(chat_id, thumbnail, response_text, keyboard)
                                    except:
                                        await send_telegram_message(chat_id, response_text, keyboard)
                                else:
                                    await send_telegram_message(chat_id, response_text, keyboard)
                                
                            else:
                                error_text = await response.text()
                                await send_telegram_message(
                                    chat_id,
                                    f"❌ Failed to process URL: {url}\n\n<b>Error:</b> {error_text[:100]}...\n\nPlease try again or contact @SUN_GOD_LUFFYY"
                                )
                except asyncio.TimeoutError:
                    await send_telegram_message(
                        chat_id,
                        f"⏰ Request timeout for URL: {url}\n\nThe server is processing many requests. Please try again.\n\n👨‍💻 Contact: @SUN_GOD_LUFFYY"
                    )
                except Exception as e:
                    logger.error(f"Error processing URL {url}: {e}")
                    await send_telegram_message(
                        chat_id,
                        f"❌ Error processing URL: {url}\n\n<b>Error:</b> {str(e)[:100]}...\n\n👨‍💻 Contact: @SUN_GOD_LUFFYY"
                    )
        else:
            await send_telegram_message(
                chat_id,
                "📎 Please send a valid URL!\n\n✅ <b>Supported:</b> YouTube, Instagram, Spotify, Terabox and more!\n\n📝 <b>Example:</b> https://youtube.com/watch?v=...\n\n👨‍💻 Dev: @SUN_GOD_LUFFYY"
            )
        
        return JSONResponse({"ok": True})
    
    except Exception as e:
        logger.error(f"Telegram webhook error: {e}")
        return JSONResponse({"ok": True})

# Main endpoint - MODIFIED TO START DOWNLOADS IMMEDIATELY
@app.get("/")
async def download_video(request: Request):
    await clean_old_files()
    url = request.query_params.get("url")
    sound = "sound" in request.query_params
    quality = next((q for q in ["240", "360", "480", "720", "1080"] if q in request.query_params), None)

    if not url:
        logger.error("No URL provided in request")
        raise HTTPException(status_code=400, detail="URL parameter is required")

    # Support both HTTP and HTTPS
    if not (url.startswith("http://") or url.startswith("https://")):
        if url.startswith("www.") or any(domain in url for domain in ["youtube.com", "instagram.com", "spotify.com"]):
            url = "https://" + url
        else:
            logger.error(f"Invalid URL: {url}")
            raise HTTPException(status_code=400, detail="Invalid URL. Please provide a valid HTTP/HTTPS URL")

    api_key = request.headers.get("X-API-Key")
    is_authenticated = api_key == API_KEY

    logger.info(f"Processing URL: {url}, sound: {sound}, quality: {quality}")

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

# MODIFIED yt-dlp handler to start downloads IMMEDIATELY
async def handle_yt_dlp(url: str, request: Request, sound: bool = False, quality: str = None, platform: str = "generic", is_authenticated: bool = False):
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

    # START DOWNLOADS IMMEDIATELY - NOT IN BACKGROUND
    if video_filename and not os.path.exists(video_path):
        DOWNLOAD_TASKS[video_path] = {"status": "downloading", "url": url}
        asyncio.create_task(immediate_yt_dlp_download(url, video_path, "video", quality="1080" if not quality else quality, cookie_option=cookie_option, common_options=common_options))
    if audio_filename and not os.path.exists(audio_path):
        DOWNLOAD_TASKS[audio_path] = {"status": "downloading", "url": url}
        asyncio.create_task(immediate_yt_dlp_download(url, audio_path, "audio", cookie_option=cookie_option, common_options=common_options))

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

# IMMEDIATE download function for yt-dlp (not background)
async def immediate_yt_dlp_download(url, path, download_type, quality=None, cookie_option="", common_options=""):
    try:
        logger.info(f"Starting IMMEDIATE {download_type} download for {url}")
        
        if os.path.exists(path):
            file_age = time.time() - min(os.path.getmtime(path), os.path.getctime(path))
            if file_age < CACHE_DURATION:
                logger.info(f"File {path} already exists and is fresh")
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
        
        logger.info(f"Executing IMMEDIATE yt-dlp: {cmd}")
        process = await asyncio.create_subprocess_shell(
            cmd, 
            stdout=asyncio.subprocess.PIPE, 
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            logger.error(f"yt-dlp {download_type} download failed: {stderr.decode()}")
            DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "error": stderr.decode()}
        else:
            logger.info(f"Completed IMMEDIATE {download_type} download: {path}")
            DOWNLOAD_TASKS[path] = {"status": "completed", "url": url}
            
    except Exception as e:
        logger.error(f"Immediate download error for {url}: {e}")
        DOWNLOAD_TASKS[path] = {"status": "failed", "url": url, "error": str(e)}

# Enhanced Terabox handler with IMMEDIATE downloads
@cached(ttl=300, cache=Cache.MEMORY)
async def handle_terabox(url: str, request: Request, is_authenticated: bool):
    api_url = f"https://tb.hosters.club/?url={url}"
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                logger.info(f"Fetching Terabox API (attempt {attempt + 1}): {api_url}")
                async with session.get(api_url) as response:
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
                            
                            # START IMMEDIATE DOWNLOAD
                            if not os.path.exists(file_path):
                                DOWNLOAD_TASKS[file_path] = {"status": "downloading", "url": direct_link}
                                asyncio.create_task(immediate_download(link_hash, direct_link, file_path))
                            
                            base_url = str(request.base_url).rstrip('/')
                            response_data = {
                                "title": data.get("name", "Terabox Video"),
                                "thumbnail": data.get("thumbnail"),
                                "link": direct_link,
                                "stream_mp4": f"{base_url}/tb/{link_hash}",
                                "stream_mp3": None,
                                "file_name": filename
                            }
                            
                            return JSONResponse(response_data)
                        else:
                            raise HTTPException(status_code=500, detail="No direct_link provided")
                    else:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        raise HTTPException(status_code=response.status, detail="Terabox API error")
        except Exception as e:
            if attempt == max_retries - 1:
                raise HTTPException(status_code=500, detail=f"Terabox API failed: {str(e)}")
            await asyncio.sleep(2 ** attempt)

# Enhanced Spotify handler with IMMEDIATE downloads
@cached(ttl=300, cache=Cache.MEMORY)
async def handle_spotify(url: str, request: Request, is_authenticated: bool):
    api_url = f"http://sp.hosters.club/?url={url}"
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(api_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if not data.get("error", True) and data.get("url"):
                            track_url = data["url"]
                            filename = get_unique_filename(track_url, None, True)
                            file_path = os.path.join(DOWNLOAD_DIR, filename)
                            track_hash = base64.urlsafe_b64encode(hashlib.md5(track_url.encode()).digest()).decode()[:24]

                            SPOTIFY_DOWNLOAD_TASKS[track_hash] = {
                                "link": track_url,
                                "name": filename,
                                "file_path": file_path
                            }

                            # START IMMEDIATE DOWNLOAD
                            if not os.path.exists(file_path):
                                DOWNLOAD_TASKS[file_path] = {"status": "downloading", "url": track_url}
                                asyncio.create_task(immediate_spotify_download(track_hash, track_url, file_path))

                            base_url = str(request.base_url).rstrip('/')
                            response_data = {
                                "title": data.get("name", "Spotify Track"),
                                "thumbnail": None,
                                "link": track_url,
                                "stream_mp4": None,
                                "stream_mp3": f"{base_url}/spotify/{track_hash}",
                                "file_name": filename
                            }
                            
                            return JSONResponse(response_data)
                        else:
                            raise HTTPException(status_code=500, detail="Spotify API error")
                    else:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 ** attempt)
                            continue
                        raise HTTPException(status_code=response.status, detail="Spotify API error")
        except Exception as e:
            if attempt == max_retries - 1:
                raise HTTPException(status_code=500, detail=f"Spotify API failed: {str(e)}")
            await asyncio.sleep(2 ** attempt)

# Instagram handler with IMMEDIATE downloads
async def handle_instagram(url: str, request: Request, sound: bool = False, quality: str = None, is_authenticated: bool = False):
    logger.info(f"Processing Instagram URL: {url}")
    info = await get_instagram_video_info(url)
    if not info or not info["video_url"]:
        raise HTTPException(status_code=400, detail="Invalid Instagram URL or no video found")

    output_filename = get_unique_filename(url, quality if quality else "original", sound)
    output_path = os.path.join(DOWNLOAD_DIR, output_filename)

    # START IMMEDIATE DOWNLOAD
    if not os.path.exists(output_path):
        DOWNLOAD_TASKS[output_path] = {"status": "downloading", "url": info["video_url"]}
        asyncio.create_task(immediate_instagram_download(info["video_url"], output_path, sound, quality))

    base_url = str(request.base_url).rstrip('/')
    response = {
        "title": info["title"],
        "thumbnail": info["thumbnail"],
        "link": info["video_url"],
        "stream_mp4": f"{base_url}/stream/{output_filename}" if not sound else None,
        "stream_mp3": f"{base_url}/stream/{output_filename}" if sound else None,
        "file_name": output_filename
    }
    return JSONResponse(response)

# IMMEDIATE download functions
async def immediate_download(link_hash, link, file_path):
    try:
        DOWNLOAD_TASKS[file_path] = {"status": "downloading", "url": link}
        logger.info(f"Starting IMMEDIATE download for: {link}")
        if await download_file(link, file_path):
            DOWNLOAD_TASKS[file_path] = {"status": "completed", "url": link}
            logger.info(f"IMMEDIATE download completed: {file_path}")
        else:
            DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": link}
    except Exception as e:
        DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": link, "error": str(e)}
        logger.error(f"IMMEDIATE download failed: {e}")

async def immediate_spotify_download(track_hash, track_url, file_path):
    try:
        DOWNLOAD_TASKS[file_path] = {"status": "downloading", "url": track_url}
        logger.info(f"Starting IMMEDIATE Spotify download for: {track_url}")
        if await download_file(track_url, file_path):
            DOWNLOAD_TASKS[file_path] = {"status": "completed", "url": track_url}
            logger.info(f"IMMEDIATE Spotify download completed: {file_path}")
        else:
            DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": track_url}
    except Exception as e:
        DOWNLOAD_TASKS[file_path] = {"status": "failed", "url": track_url, "error": str(e)}
        logger.error(f"IMMEDIATE Spotify download failed: {e}")

async def immediate_instagram_download(video_url, output_path, sound, quality):
    temp_filename = f"temp_instagram_{int(time.time())}.mp4"
    temp_path = os.path.join(DOWNLOAD_DIR, temp_filename)
    try:
        DOWNLOAD_TASKS[output_path] = {"status": "downloading", "url": video_url}
        logger.info(f"Starting IMMEDIATE Instagram download for: {video_url}")
        
        if not await download_file(video_url, temp_path):
            DOWNLOAD_TASKS[output_path] = {"status": "failed", "url": video_url}
            return
        
        if not await process_instagram_video(temp_path, output_path, sound, quality):
            DOWNLOAD_TASKS[output_path] = {"status": "failed", "url": video_url}
            return
            
        DOWNLOAD_TASKS[output_path] = {"status": "completed", "url": video_url}
        logger.info(f"IMMEDIATE Instagram download completed: {output_path}")
        
    except Exception as e:
        DOWNLOAD_TASKS[output_path] = {"status": "failed", "url": video_url, "error": str(e)}
        logger.error(f"IMMEDIATE Instagram download failed: {e}")
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass

# Streaming endpoints (keep existing)
@app.get("/stream/{filename}")
async def stream_file(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    if os.path.exists(file_path):
        file_age = time.time() - min(os.path.getmtime(file_path), os.path.getctime(file_path))
        if file_age > CACHE_DURATION:
            if file_path in DOWNLOAD_TASKS and DOWNLOAD_TASKS[file_path]["status"] != "completed":
                os.remove(file_path)
                raise HTTPException(status_code=404, detail="File expired")
        
        ext = filename.rsplit(".", 1)[-1].lower()
        content_type = {
            "mp4": "video/mp4",
            "mkv": "video/x-matroska", 
            "avi": "video/x-msvideo",
            "mp3": "audio/mpeg"
        }.get(ext, "application/octet-stream")
        
        return FileResponse(file_path, media_type=content_type)
    raise HTTPException(status_code=404, detail="File not yet available")

@app.get("/spotify/{track_hash}")
async def stream_spotify(track_hash: str):
    track_data = SPOTIFY_DOWNLOAD_TASKS.get(track_hash)
    if not track_data:
        raise HTTPException(status_code=404, detail="Track not found")
    
    file_path = track_data["file_path"]
    if os.path.exists(file_path):
        return FileResponse(file_path, media_type="audio/mpeg")
    raise HTTPException(status_code=404, detail="File not yet available")

@app.get("/tb/{link_hash}")
async def stream_terabox(link_hash: str):
    link_data = TERABOX_LINKS.get(link_hash)
    if not link_data:
        raise HTTPException(status_code=404, detail="Link not found")
    
    filename = link_data["name"]
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    
    if os.path.exists(file_path):
        ext = filename.rsplit(".", 1)[-1].lower()
        content_type = {
            "mp4": "video/mp4",
            "mkv": "video/x-matroska",
            "avi": "video/x-msvideo"
        }.get(ext, "application/octet-stream")
        return FileResponse(file_path, media_type=content_type)
    raise HTTPException(status_code=404, detail="File not yet available")

# Status endpoints
@app.get("/status/{filename}")
async def check_status(filename: str):
    file_path = os.path.join(DOWNLOAD_DIR, filename)
    if os.path.exists(file_path):
        return JSONResponse({"status": "completed"})
    task = DOWNLOAD_TASKS.get(file_path)
    if task:
        return JSONResponse({"status": task["status"], "error": task.get("error")})
    raise HTTPException(status_code=404, detail="No download task found")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7777)
