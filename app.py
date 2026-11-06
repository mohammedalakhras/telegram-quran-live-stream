import os
import shutil
import asyncio
import logging
import time
import concurrent.futures
import io
import json
from collections import deque
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import psutil
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from fastapi import FastAPI, HTTPException, Header, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse

from fastapi.templating import Jinja2Templates
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# ==================== إعداد السجلات الحية للوحة التحكم ====================
log_buffer = deque(maxlen=60)

class DequeLogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        log_buffer.append(msg)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("QuranStreamer-RTMP")
deque_handler = DequeLogHandler()
deque_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S"))
logger.addHandler(deque_handler)

# ==================== متغيرات البيئة ====================
RTMP_URL = os.getenv("RTMP_URL", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "@MohammedAlakhras")
FOLDER_URL_1 = os.getenv("FOLDER_URL_1", "")
FOLDER_URL_2 = os.getenv("FOLDER_URL_2", "")
FOLDER_URL_3 = os.getenv("FOLDER_URL_3", "")
GDRIVE_CREDENTIALS_JSON = os.getenv("GDRIVE_CREDENTIALS_JSON", "")
PORT = int(os.getenv("PORT", "10000"))
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "123456")

SURAH_NAMES = [
    "الفاتحة", "البقرة", "آل عمران", "النساء", "المائدة", "الأنعام", "الأعراف", "الأنفال", "التوبة", "يونس",
    "هود", "يوسف", "الرعد", "إبراهيم", "الحجر", "النحل", "الإسراء", "الكهف", "مريم", "طه",
    "الأنبياء", "الحج", "المؤمنون", "النور", "الفرقان", "الشعراء", "النمل", "القصص", "العنكبوت", "الروم",
    "لقمان", "السجدة", "الأحزاب", "سبأ", "فاطر", "يس", "الصافات", "ص", "الزمر", "غافر",
    "فصلت", "الشورى", "الزخرف", "الدخان", "الجاثية", "الأحقاف", "محمد", "الفتح", "الحجرات", "ق",
    "الذاريات", "الطور", "النجم", "القمر", "الرحمن", "الواقعة", "الحديد", "المجادلة", "الحشر", "الممتحنة",
    "الصف", "الجمعة", "المنافقون", "التغابن", "الطلاق", "التحريم", "الملك", "القلم", "الحاقة", "المعارج",
    "نوح", "الجن", "المزمل", "المدثر", "القيامة", "الإنسان", "المرسلات", "النبأ", "النازعات", "عبس",
    "التكوير", "الانفطار", "المطففين", "الانشقاق", "البروج", "الطارق", "الأعلى", "الغاشية", "الفجر", "البلد",
    "الشمس", "الليل", "الضحى", "الشرح", "التين", "العلق", "القدر", "البينة", "الزلزلة", "العاديات",
    "القارعة", "التكاثر", "العصر", "الهمزة", "الفيل", "قريش", "الماعون", "الكوثر", "الكافرون", "النصر",
    "المسد", "الإخلاص", "الفلق", "الناس"
]

# أوضاع جودة البث القابلة للتبديل من لوحة التحكم
QUALITY_PROFILES = {
    "ultra_saver": {
        "label": "توفير فائق (240p - 25MB/ساعة)",
        "scale": "scale=-2:240,fps=5",
        "bv": "55k", "maxrate": "75k", "bufsize": "150k", "g": "10",
        "ba": "24k", "ar": "22050"
    },
    "eco": {
        "label": "اقتصادي متوازن (360p - 55MB/ساعة)",
        "scale": "scale=-2:360,fps=10",
        "bv": "100k", "maxrate": "140k", "bufsize": "280k", "g": "20",
        "ba": "32k", "ar": "22050"
    },
    "standard": {
        "label": "جودة عالية (480p - 120MB/ساعة)",
        "scale": "scale=-2:480,fps=15",
        "bv": "220k", "maxrate": "300k", "bufsize": "600k", "g": "30",
        "ba": "64k", "ar": "44100"
    }
}

stream_state = {
    "status": "جاري التهيئة...",
    "is_streaming": False,
    "current_surah_index": 0,
    "current_surah_name": "لم يبدأ بعد",
    "next_surah_name": "الفاتحة",
    "current_file": "-",
    "duration_seconds": 0.0,
    "started_at_ts": 0.0,
    "completed_khatmas": 0,
    "server_start_ts": time.time(),
    "last_ping_time": "لم يتم استقبال طلبات بعد",
    "ping_count": 0,
    "last_error": None,
    "force_skip": False,
    "target_surah_index": None,
    "loop_current_surah": False,
    "quality_profile": "eco",
    "is_downloading": False,
    "downloading_folder": "-"
}

video_files = []
for i in range(1, 115):
    file_name = f"{str(i).zfill(3)}.mp4"
    if 1 <= i <= 50:
        video_files.append((i, f"vid/{file_name}"))
    elif 51 <= i <= 100:
        video_files.append((i, f"vid2/{file_name}"))
    else:
        video_files.append((i, f"vid3/{file_name}"))

# متغيرات لحساب سرعة نقل البيانات اللحظية (Bitrate/Network Speed)
net_io_start = psutil.net_io_counters()
last_net_check_ts = time.time()
last_bytes_sent = net_io_start.bytes_sent
last_bytes_recv = net_io_start.bytes_recv
current_upload_kbps = 0.0
current_download_kbps = 0.0


# ==================== دوال حساب المساحة والموارد ====================
def format_bytes(size_bytes: float) -> str:
    if size_bytes < 1024:
        return f"{size_bytes:.0f} B"
    elif size_bytes < 1024 ** 2:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 ** 3:
        return f"{size_bytes / (1024 ** 2):.2f} MB"
    else:
        return f"{size_bytes / (1024 ** 3):.2f} GB"

def get_storage_and_files_info():
    total_downloaded_bytes = 0
    downloaded_indices = []

    for num, path in video_files:
        if os.path.exists(path):
            fsize = os.path.getsize(path)
            if fsize > 0:
                total_downloaded_bytes += fsize
                downloaded_indices.append(num)

    disk = shutil.disk_usage(".")
    return {
        "downloaded_count": len(downloaded_indices),
        "downloaded_indices": downloaded_indices,
        "downloaded_size_bytes": total_downloaded_bytes,
        "downloaded_size_formatted": format_bytes(total_downloaded_bytes),
        "disk_total_formatted": format_bytes(disk.total),
        "disk_used_formatted": format_bytes(disk.used),
        "disk_free_formatted": format_bytes(disk.free),
        "disk_free_bytes": disk.free,
        "disk_percent": round((disk.used / disk.total) * 100, 1) if disk.total > 0 else 0.0
    }

def get_system_telemetry():
    global last_net_check_ts, last_bytes_sent, last_bytes_recv, current_upload_kbps, current_download_kbps
    now = time.time()
    dt = max(now - last_net_check_ts, 0.5)

    net_now = psutil.net_io_counters()
    if now - last_net_check_ts >= 1.0:
        up_diff = max(0, net_now.bytes_sent - last_bytes_sent)
        down_diff = max(0, net_now.bytes_recv - last_bytes_recv)
        current_upload_kbps = round((up_diff / 1024.0) / dt, 1)
        current_download_kbps = round((down_diff / 1024.0) / dt, 1)
        last_bytes_sent = net_now.bytes_sent
        last_bytes_recv = net_now.bytes_recv
        last_net_check_ts = now

    total_sent_session = max(0, net_now.bytes_sent - net_io_start.bytes_sent)
    total_recv_session = max(0, net_now.bytes_recv - net_io_start.bytes_recv)

    vm = psutil.virtual_memory()
    proc = psutil.Process(os.getpid())
    try:
        app_ram_bytes = proc.memory_info().rss
        for child in proc.children(recursive=True):
            app_ram_bytes += child.memory_info().rss
    except Exception:
        app_ram_bytes = proc.memory_info().rss

    cpu_pct = psutil.cpu_percent(interval=None)

    return {
        "cpu_percent": round(cpu_pct, 1),
        "ram_total_formatted": format_bytes(vm.total),
        "ram_used_formatted": format_bytes(vm.used),
        "ram_free_formatted": format_bytes(vm.available),
        "ram_percent": round(vm.percent, 1),
        "app_ram_formatted": format_bytes(app_ram_bytes),
        "upload_speed_kbps": current_upload_kbps,
        "download_speed_kbps": current_download_kbps,
        "total_sent_formatted": format_bytes(total_sent_session),
        "total_recv_formatted": format_bytes(total_recv_session),
        "total_bandwidth_used_formatted": format_bytes(total_sent_session + total_recv_session)
    }


# ==================== دوال التحميل والبث ====================
async def download_videos_from_drive(folder_id: str, output_dir: str, expected_count: int = 0):
    if not folder_id or not GDRIVE_CREDENTIALS_JSON:
        logger.warning(f"بيانات التحميل للمجلد {output_dir} غير مكتملة.")
        return

    os.makedirs(output_dir, exist_ok=True)

    existing_files = [
        f for f in os.listdir(output_dir)
        if f.endswith('.mp4') and os.path.getsize(os.path.join(output_dir, f)) > 0
    ]
    if len(existing_files) >= expected_count and expected_count > 0:
        logger.info(f"المجلد {output_dir} مكتمل مسبقاً ({len(existing_files)} ملف). تم تخطي التحميل.")
        return

    stream_state["is_downloading"] = True
    stream_state["downloading_folder"] = output_dir
    stream_state["status"] = f"جاري تحميل ملفات ({output_dir}) من Drive..."
    logger.info(f"بدأ تحميل الفيديوهات عبر Google API إلى {output_dir}...")

    def _download():
        try:
            creds_dict = json.loads(GDRIVE_CREDENTIALS_JSON)
            creds = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=['https://www.googleapis.com/auth/drive.readonly']
            )
            service = build('drive', 'v3', credentials=creds)

            query = f"'{folder_id}' in parents and mimeType='video/mp4' and trashed=false"
            results = service.files().list(
                q=query, pageSize=200, fields="nextPageToken, files(id, name)", orderBy="name"
            ).execute()
            items = results.get('files', [])

            for item in items:
                file_path = os.path.join(output_dir, item['name'])
                if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                    continue
                request = service.files().get_media(fileId=item['id'])
                fh = io.FileIO(file_path, mode='wb')
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
            return True, "نجاح"
        except Exception as e:
            return False, str(e)

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        ok, msg = await loop.run_in_executor(pool, _download)

    stream_state["is_downloading"] = False
    stream_state["downloading_folder"] = "-"
    if ok:
        logger.info(f"اكتمل تحميل المجلد {output_dir} بنجاح.")
    else:
        logger.error(f"فشل تحميل المجلد {output_dir}: {msg}")
        stream_state["last_error"] = f"خطأ Drive API: {msg}"


async def get_video_duration(filename: str) -> float:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", filename,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
        duration_str = stdout.decode().strip()
        if duration_str:
            return float(duration_str)
    except Exception as e:
        logger.error(f"خطأ في قراءة مدة الملف {filename}: {e}")
    return 0.0


async def start_video_stream():
    if not RTMP_URL:
        logger.critical("متغير RTMP_URL مفقود! لن يبدأ البث حتى تتم إضافته.")
        stream_state["status"] = "خطأ: متغير RTMP_URL غير موجود"
        return

    prefetch_task = None
    try:
        await download_videos_from_drive(FOLDER_URL_1, "vid", 50)

        while True:
            i = 0
            while i < len(video_files):
                if stream_state["target_surah_index"] is not None:
                    i = stream_state["target_surah_index"] - 1
                    stream_state["target_surah_index"] = None

                surah_num, video_path = video_files[i]
                surah_name = SURAH_NAMES[surah_num - 1]
                next_idx = (surah_num - 1) if stream_state["loop_current_surah"] else (surah_num % 114)
                next_surah_name = SURAH_NAMES[next_idx]
                stream_state["force_skip"] = False

                # التحميل المسبق الذكي دون حذف الملفات القديمة
                if surah_num == 45:
                    prefetch_task = asyncio.create_task(download_videos_from_drive(FOLDER_URL_2, "vid2", 50))
                elif surah_num == 51 and prefetch_task:
                    await prefetch_task
                elif surah_num == 90:
                    prefetch_task = asyncio.create_task(download_videos_from_drive(FOLDER_URL_3, "vid3", 14))
                elif surah_num == 101 and prefetch_task:
                    await prefetch_task

                is_played = False
                retry_count = 0

                while not is_played and not stream_state["force_skip"] and stream_state["target_surah_index"] is None:
                    duration = await get_video_duration(video_path) if os.path.exists(video_path) else 0.0

                    if duration == 0.0:
                        retry_count += 1
                        stream_state["status"] = f"جاري تحميل سورة {surah_name} (محاولة {retry_count})..."
                        if os.path.exists(video_path):
                            os.remove(video_path)

                        if 1 <= surah_num <= 50:
                            await download_videos_from_drive(FOLDER_URL_1, "vid", 50)
                        elif 51 <= surah_num <= 100:
                            await download_videos_from_drive(FOLDER_URL_2, "vid2", 50)
                        else:
                            await download_videos_from_drive(FOLDER_URL_3, "vid3", 14)

                        await asyncio.sleep(3)
                        continue

                    try:
                        profile_key = stream_state.get("quality_profile", "eco")
                        prof = QUALITY_PROFILES.get(profile_key, QUALITY_PROFILES["eco"])

                        current_file_size = os.path.getsize(video_path) if os.path.exists(video_path) else 0
                        stream_state.update({
                            "status": f"يبث مباشرة 🟢 ({prof['label'].split('(')[0].strip()})",
                            "is_streaming": True,
                            "current_surah_index": surah_num,
                            "current_surah_name": f"سورة {surah_name}",
                            "next_surah_name": f"سورة {next_surah_name}",
                            "current_file": f"{video_path} ({format_bytes(current_file_size)})",
                            "duration_seconds": round(duration, 1),
                            "started_at_ts": time.time(),
                            "last_error": None
                        })

                        logger.info(f"بدأ بث: سورة {surah_name} | الدقة: {profile_key} | المدة: {duration:.0f} ثانية")

                        ffmpeg_proc = await asyncio.create_subprocess_exec(
                            "ffmpeg", "-re", "-i", video_path,
                            "-c:v", "libx264",
                            "-vf", prof["scale"],
                            "-pix_fmt", "yuv420p",
                            "-b:v", prof["bv"],
                            "-maxrate", prof["maxrate"],
                            "-bufsize", prof["bufsize"],
                            "-preset", "ultrafast",
                            "-tune", "stillimage",
                            "-g", prof["g"],
                            "-c:a", "aac",
                            "-b:a", prof["ba"],
                            "-ac", "1",
                            "-ar", prof["ar"],
                            "-f", "flv", "-flvflags", "no_duration_filesize", RTMP_URL,
                            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
                        )

                        while ffmpeg_proc.returncode is None:
                            if stream_state["force_skip"] or stream_state["target_surah_index"] is not None:
                                ffmpeg_proc.terminate()
                                logger.info(f"تم إيقاف/تخطي سورة {surah_name} بأمر المشرف.")
                                break
                            await asyncio.sleep(1)

                        is_played = True
                    except Exception as e:
                        logger.error(f"خطأ أثناء بث {video_path}: {e}")
                        stream_state["last_error"] = str(e)
                        await asyncio.sleep(5)

                if stream_state["target_surah_index"] is None:
                    if not stream_state["loop_current_surah"]:
                        i += 1
                    else:
                        logger.info(f"إعادة تكرار سورة {surah_name} (وضع التكرار مفعل).")

            stream_state["completed_khatmas"] += 1
            logger.info(f"🎉 اكتملت الختمة رقم {stream_state['completed_khatmas']} بحمد الله!")
            await download_videos_from_drive(FOLDER_URL_1, "vid", 50)

    except asyncio.CancelledError:
        logger.info("تم إيقاف محرك البث.")
    except Exception as e:
        logger.critical(f"خطأ حرج في البث: {e}")
        stream_state["status"] = f"توقف بسبب خطأ: {e}"
        stream_state["is_streaming"] = False
        stream_state["last_error"] = str(e)


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    psutil.cpu_percent(interval=None)
    stream_task = asyncio.create_task(start_video_stream())
    yield
    stream_task.cancel()


web_app = FastAPI(title="لوحة البث القرآني الاحترافية (RTMP)", lifespan=lifespan)

def format_seconds(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hrs, rem = divmod(seconds, 3600)
    mins, secs = divmod(rem, 60)
    if hrs > 0:
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"

@web_app.api_route("/ping", methods=["GET", "HEAD"])
async def ping():
    stream_state["ping_count"] += 1
    stream_state["last_ping_time"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    return JSONResponse({"ok": True, "ping_count": stream_state["ping_count"]})

@web_app.get("/api/status")
async def api_status():
    now = time.time()
    elapsed = 0.0
    remaining = 0.0
    progress_pct = 0.0

    if stream_state["is_streaming"] and stream_state["duration_seconds"] > 0:
        elapsed = min(now - stream_state["started_at_ts"], stream_state["duration_seconds"])
        remaining = max(stream_state["duration_seconds"] - elapsed, 0.0)
        progress_pct = round((elapsed / stream_state["duration_seconds"]) * 100, 1)

    uptime_sec = now - stream_state["server_start_ts"]
    storage_info = get_storage_and_files_info()
    telemetry = get_system_telemetry()

    return {
        **stream_state,
        "channel_id": CHANNEL_ID,
        "elapsed_formatted": format_seconds(elapsed),
        "remaining_formatted": format_seconds(remaining),
        "duration_formatted": format_seconds(stream_state["duration_seconds"]),
        "progress_pct": progress_pct,
        "uptime_formatted": format_seconds(uptime_sec),
        "storage": storage_info,
        "telemetry": telemetry,
        "logs": list(log_buffer)[-35:],
        "surah_names": SURAH_NAMES
    }


# ==================== نقاط التحكم الإدارية ====================
def verify_admin(password: str):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="كلمة المرور غير صحيحة!")

@web_app.post("/api/skip")
async def api_skip(password: str = Header(...)):
    verify_admin(password)
    stream_state["force_skip"] = True
    return {"message": "⏭ تم إرسال أمر تخطي السورة الحالية"}

@web_app.post("/api/jump/{surah_index}")
async def api_jump(surah_index: int, password: str = Header(...)):
    verify_admin(password)
    if 1 <= surah_index <= 114:
        stream_state["target_surah_index"] = surah_index
        return {"message": f"🚀 جاري الانتقال الفوري إلى سورة {SURAH_NAMES[surah_index - 1]}"}
    raise HTTPException(status_code=400, detail="رقم السورة غير صالح")

@web_app.post("/api/loop")
async def api_toggle_loop(password: str = Header(...)):
    verify_admin(password)
    stream_state["loop_current_surah"] = not stream_state["loop_current_surah"]
    state_txt = "مفعّل 🔁" if stream_state["loop_current_surah"] else "متوقف ➡️"
    logger.info(f"تغيير وضع تكرار السورة الحالية إلى: {state_txt}")
    return {"message": f"وضع تكرار السورة الحالية الآن: {state_txt}", "loop": stream_state["loop_current_surah"]}

@web_app.post("/api/quality/{profile}")
async def api_set_quality(profile: str, password: str = Header(...)):
    verify_admin(password)
    if profile not in QUALITY_PROFILES:
        raise HTTPException(status_code=400, detail="وضع الجودة غير معروف")
    stream_state["quality_profile"] = profile
    # إعادة تشغيل السورة الحالية فوراً بالجودة الجديدة
    stream_state["target_surah_index"] = max(1, stream_state["current_surah_index"])
    logger.info(f"تم تغيير جودة البث إلى: {QUALITY_PROFILES[profile]['label']}")
    return {"message": f"✅ تم تطبيق الجودة: {QUALITY_PROFILES[profile]['label']}"}

@web_app.post("/api/prefetch/{part}")
async def api_manual_prefetch(part: int, password: str = Header(...)):
    verify_admin(password)
    if stream_state["is_downloading"]:
        return {"message": "⏳ هناك عملية تحميل جارية بالفعل في الخلفية!"}
    if part == 1:
        asyncio.create_task(download_videos_from_drive(FOLDER_URL_1, "vid", 50))
    elif part == 2:
        asyncio.create_task(download_videos_from_drive(FOLDER_URL_2, "vid2", 50))
    elif part == 3:
        asyncio.create_task(download_videos_from_drive(FOLDER_URL_3, "vid3", 14))
    else:
        raise HTTPException(status_code=400, detail="رقم الدفعة غير صحيح")
    return {"message": f"📥 بدأ فحص وتحميل الدفعة رقم {part} في الخلفية"}


# ==================== الواجهة الأمامية الاحترافية ====================
@web_app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    # html_content = """
 
    # """
    # return HTMLResponse(content=html_content)
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={"channel_id": CHANNEL_ID}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:web_app", host="0.0.0.0", port=PORT, log_level="info")