import os
import shutil
import asyncio
import logging
import time
import subprocess
import concurrent.futures
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import HTMLResponse, JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("QuranStreamer-RTMP")

# متغيرات البيئة الأساسية
RTMP_URL = os.getenv("RTMP_URL", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "قناتك")
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
    "target_surah_index": None
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

async def download_videos_from_drive(folder_id: str, output_dir: str, expected_count: int = 0):
    if not folder_id or not GDRIVE_CREDENTIALS_JSON: return
    os.makedirs(output_dir, exist_ok=True)

    # فحص ذكي: هل الملفات موجودة مسبقاً ومكتملة؟
    existing_files = [f for f in os.listdir(output_dir) if f.endswith('.mp4') and os.path.getsize(os.path.join(output_dir, f)) > 0]
    if len(existing_files) >= expected_count and expected_count > 0:
        logger.info(f"المجلد {output_dir} مكتمل مسبقاً. سيتم تخطي التحميل لتوفير الموارد.")
        return

    stream_state["status"] = f"جاري تحميل دفعة من جوجل درايف..."

    def _download():
        try:
            import json
            creds_dict = json.loads(GDRIVE_CREDENTIALS_JSON)
            creds = service_account.Credentials.from_service_account_info(
                creds_dict, scopes=['https://www.googleapis.com/auth/drive.readonly'])
            service = build('drive', 'v3', credentials=creds)

            query = f"'{folder_id}' in parents and mimeType='video/mp4' and trashed=false"
            results = service.files().list(q=query, pageSize=200, fields="nextPageToken, files(id, name)", orderBy="name").execute()
            items = results.get('files', [])

            for item in items:
                file_path = os.path.join(output_dir, item['name'])
                # ملاحظة: file_path لا يوجد إلا بعد اكتمال الضغط بنجاح (انظر أسفل)
                # لذلك وجوده هنا يعني أن الملف جاهز تماماً للبث ولا حاجة لإعادة أي شيء.
                if os.path.exists(file_path) and os.path.getsize(file_path) > 0: continue

                raw_path = file_path + ".download"   # اسم مؤقت أثناء التحميل الخام من Drive
                request = service.files().get_media(fileId=item['id'])
                fh = io.FileIO(raw_path, mode='wb')
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done: _, done = downloader.next_chunk()

                # === الضغط لمرة واحدة فقط (ليس أثناء البث المباشر) ===
                # هذه هي نفس إعدادات الفيديو/الصوت التي كانت تُطبَّق سابقاً داخل أمر
                # البث المباشر (360p / 10fps / 100k فيديو / 32k صوت)، لكن الآن تُطبَّق
                # هنا فقط عند التحميل، بحيث لا يحتاج البث المباشر لاحقاً لأي ترميز (encoding)
                # على الإطلاق. هذا لا يحتاج أن يعمل بالوقت الحقيقي (-re غير مستخدم هنا)
                # لذلك لا مشكلة إن استغرق وقتاً أطول بسبب ضعف معالج الخطة المجانية.
                tmp_out = file_path + ".tmp.mp4"
                try:
                    subprocess.run([
                        "ffmpeg", "-y", "-i", raw_path,
                        "-c:v", "libx264",
                        "-vf", "scale=-2:360,fps=10",
                        "-pix_fmt", "yuv420p",
                        "-b:v", "100k", "-maxrate", "100k", "-bufsize", "100k",
                        "-x264-params", "nal-hrd=cbr:force-cfr=1",  # معدل بت ثابت حقيقي (CBR) بلا أي قفزات
                        "-preset", "veryfast",
                        "-tune", "stillimage",
                        "-g", "20",
                        "-c:a", "aac", "-b:a", "32k", "-ac", "1", "-ar", "22050",
                        tmp_out,
                    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=3600, check=True)

                    if os.path.exists(tmp_out) and os.path.getsize(tmp_out) > 0:
                        os.replace(tmp_out, file_path)  # إعادة تسمية ذرية: الملف النهائي يظهر دفعة واحدة فقط عند اكتماله
                except Exception as enc_err:
                    logger.error(f"فشل ضغط {item['name']}: {enc_err}")
                    if os.path.exists(tmp_out): os.remove(tmp_out)
                finally:
                    if os.path.exists(raw_path): os.remove(raw_path)  # لا حاجة للاحتفاظ بالنسخة الخام الأكبر حجماً
            return True, "نجاح"
        except Exception as e: return False, str(e)

    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, _download)

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
        logger.error(f"خطأ في حساب مدة {filename}: {e}")
    return 0.0


async def start_video_stream():
    if not RTMP_URL:
        logger.critical("متغير RTMP_URL مفقود! لا يمكن بدء البث.")
        stream_state["status"] = "خطأ: رابط RTMP مفقود."
        return

    # تم إزالة كود نظام الحضور الذي كان يسبب المشكلة

    prefetch_task = None
    try:
        stream_state["status"] = "جاري الفحص والتحميل..."
        await download_videos_from_drive(FOLDER_URL_1, "vid", 50)

        while True:
            i = 0
            while i < len(video_files):
                if stream_state["target_surah_index"] is not None:
                    i = stream_state["target_surah_index"] - 1
                    stream_state["target_surah_index"] = None

                surah_num, video_path = video_files[i]
                surah_name = SURAH_NAMES[surah_num - 1]
                stream_state["force_skip"] = False

                # التحميل المسبق الذكي
                if surah_num == 45: prefetch_task = asyncio.create_task(download_videos_from_drive(FOLDER_URL_2, "vid2", 50))
                elif surah_num == 51 and prefetch_task: await prefetch_task
                elif surah_num == 90: prefetch_task = asyncio.create_task(download_videos_from_drive(FOLDER_URL_3, "vid3", 14))
                elif surah_num == 101 and prefetch_task: await prefetch_task

                is_played = False
                retry_count = 0
                
                while not is_played and not stream_state["force_skip"] and stream_state["target_surah_index"] is None:
                    duration = await get_video_duration(video_path) if os.path.exists(video_path) else 0.0
                    
                    if duration == 0.0:
                        retry_count += 1
                        stream_state["status"] = f"جاري تحميل سورة {surah_name} ({retry_count})..."
                        if os.path.exists(video_path): os.remove(video_path)
                        
                        if 1 <= surah_num <= 50: await download_videos_from_drive(FOLDER_URL_1, "vid", 50)
                        elif 51 <= surah_num <= 100: await download_videos_from_drive(FOLDER_URL_2, "vid2", 50)
                        else: await download_videos_from_drive(FOLDER_URL_3, "vid3", 14)
                        
                        await asyncio.sleep(3)
                        continue

                    try:
                        stream_state.update({
                            "status": "يبث مباشرة 🟢 (توفير عالي)", 
                            "is_streaming": True, 
                            "current_surah_index": surah_num,
                            "current_surah_name": f"سورة {surah_name}", 
                            "duration_seconds": round(duration, 1), 
                            "started_at_ts": time.time(),
                            "last_error": None
                        })

                        # الملف هنا مضغوط مسبقاً (360p/10fps/100k) أثناء التحميل من Drive،
                        # لذلك البث المباشر الآن "نسخ" فقط (remux) بلا أي ترميز (encoding).
                        # هذا يكاد لا يستهلك أي معالج، وهو ما يمنع التقطّع الناتج عن
                        # ضعف/تحديد المعالج (CPU throttling) في الخطط المجانية مثل Koyeb.
                        ffmpeg_proc = await asyncio.create_subprocess_exec(
                            "ffmpeg", "-re", "-i", video_path,
                            "-c", "copy",
                            "-f", "flv", "-flvflags", "no_duration_filesize", RTMP_URL,
                            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
                        )

                        while ffmpeg_proc.returncode is None:
                            if stream_state["force_skip"] or stream_state["target_surah_index"] is not None:
                                ffmpeg_proc.terminate()
                                break
                            await asyncio.sleep(1)

                        is_played = True
                    except Exception as e:
                        logger.error(f"خطأ بث {video_path}: {e}")
                        stream_state["last_error"] = str(e)
                        await asyncio.sleep(5)
                
                if stream_state["target_surah_index"] is None: i += 1

            stream_state["completed_khatmas"] += 1
            stream_state["status"] = "اكتملت الختمة، جاري الفحص لإعادتها..."
            
            await download_videos_from_drive(FOLDER_URL_1, "vid", 50)

    except asyncio.CancelledError:
        logger.info("تم إيقاف مهمة البث المباشر.")
    except Exception as e:
        logger.critical(f"خطأ حرج في محرك البث: {e}")
        stream_state["status"] = f"توقف بسبب خطأ: {e}"
        stream_state["is_streaming"] = False
        stream_state["last_error"] = str(e)

@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    stream_task = asyncio.create_task(start_video_stream())
    yield
    stream_task.cancel()

web_app = FastAPI(title="لوحة البث المباشر للقرآن الكريم (RTMP)", lifespan=lifespan)

def format_seconds(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hrs, rem = divmod(seconds, 3600)
    mins, secs = divmod(rem, 60)
    if hrs > 0:
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"

@web_app.get("/ping")
async def ping():
    stream_state["ping_count"] += 1
    stream_state["last_ping_time"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return JSONResponse({"ok": True})

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
    
    downloaded_files = [num for num, path in video_files if os.path.exists(path) and os.path.getsize(path) > 0]

    return {
        **stream_state,
        "channel_id": CHANNEL_ID,
        "elapsed_formatted": format_seconds(elapsed),
        "remaining_formatted": format_seconds(remaining),
        "duration_formatted": format_seconds(stream_state["duration_seconds"]),
        "progress_pct": progress_pct,
        "uptime_formatted": format_seconds(uptime_sec),
        "downloaded_files_count": len(downloaded_files),
        "surah_names": SURAH_NAMES
    }

def verify_admin(password: str):
    if password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="كلمة المرور غير صحيحة")

@web_app.post("/api/skip")
async def api_skip(password: str = Header(...)):
    verify_admin(password)
    stream_state["force_skip"] = True
    return {"message": "تم إرسال أمر تخطي السورة الحالية"}

@web_app.post("/api/jump/{surah_index}")
async def api_jump(surah_index: int, password: str = Header(...)):
    verify_admin(password)
    if 1 <= surah_index <= 114:
        stream_state["target_surah_index"] = surah_index
        return {"message": f"جاري الانتقال إلى سورة {SURAH_NAMES[surah_index - 1]}"}
    raise HTTPException(status_code=400, detail="رقم السورة غير صحيح")


@web_app.get("/", response_class=HTMLResponse)
async def dashboard():
    html_content = """
    <!DOCTYPE html>
    <html lang="ar" dir="rtl">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>لوحة البث التفاعلية (RTMP)</title>
        <style>
            :root { --bg: #0f172a; --card: #1e293b; --accent: #10b981; --accent-hover: #059669; --text: #f8fafc; --muted: #94a3b8; --danger: #ef4444; }
            * { box-sizing: border-box; font-family: 'Segoe UI', Tahoma, sans-serif; }
            body { margin: 0; padding: 24px; background-color: var(--bg); color: var(--text); display: flex; justify-content: center; align-items: flex-start; min-height: 100vh; }
            .container { width: 100%; max-width: 850px; background: var(--card); border-radius: 16px; padding: 28px; box-shadow: 0 20px 40px rgba(0,0,0,0.4); border: 1px solid rgba(255,255,255,0.08); }
            .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 16px; margin-bottom: 24px; }
            .title { font-size: 1.4rem; font-weight: 700; margin: 0; }
            .badge { background: rgba(16, 185, 129, 0.15); color: var(--accent); padding: 6px 14px; border-radius: 999px; font-weight: 600; font-size: 0.9rem;}
            .surah-box { text-align: center; background: rgba(15, 23, 42, 0.6); padding: 24px; border-radius: 12px; margin-bottom: 20px; border: 1px solid rgba(255,255,255,0.05); }
            .surah-label { color: var(--muted); font-size: 0.95rem; margin-bottom: 8px; }
            .surah-name { font-size: 2.2rem; font-weight: 800; color: var(--accent); margin: 4px 0 16px; }
            .progress-bar { width: 100%; height: 10px; background: #334155; border-radius: 999px; overflow: hidden; margin-bottom: 10px; }
            .progress-fill { height: 100%; background: linear-gradient(90deg, #059669, #10b981); width: 0%; transition: width 0.5s linear; }
            .time-row { display: flex; justify-content: space-between; color: var(--muted); font-size: 0.9rem; }
            .control-panel { background: rgba(15, 23, 42, 0.8); padding: 20px; border-radius: 12px; margin-bottom: 20px; border: 1px dashed rgba(255,255,255,0.2); }
            .control-title { font-size: 1.1rem; font-weight: 600; margin-top: 0; margin-bottom: 15px; display: flex; align-items: center; gap: 8px; color: #e2e8f0; }
            .control-row { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; margin-bottom: 15px; }
            input[type="password"] { background: #334155; border: 1px solid #475569; color: white; padding: 10px 14px; border-radius: 8px; outline: none; width: 180px; }
            select { background: #334155; border: 1px solid #475569; color: white; padding: 10px 14px; border-radius: 8px; outline: none; flex-grow: 1; cursor: pointer; }
            button { padding: 10px 18px; border: none; border-radius: 8px; font-weight: 600; cursor: pointer; transition: all 0.2s; display: flex; align-items: center; gap: 6px; }
            .btn-primary { background: var(--accent); color: white; }
            .btn-danger { background: rgba(239, 68, 68, 0.2); color: #fca5a5; border: 1px solid rgba(239, 68, 68, 0.4); }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 14px; }
            .stat-card { background: rgba(15, 23, 42, 0.45); padding: 14px 18px; border-radius: 10px; border: 1px solid rgba(255,255,255,0.05); }
            .stat-label { color: var(--muted); font-size: 0.85rem; margin-bottom: 6px; }
            .stat-value { font-size: 1.05rem; font-weight: 700; }
            .toast { position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%); background: #1e293b; color: white; padding: 12px 24px; border-radius: 8px; border: 1px solid #475569; display: none; z-index: 1000; font-weight: 600;}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1 class="title">📡 لوحة البث المباشر للقرآن الكريم</h1>
                <span class="badge" id="status-badge">جاري التحميل...</span>
            </div>
            <div class="surah-box">
                <div class="surah-label">السورة التي تُبث الآن عبر القناة (<span id="surah-num">0</span> / 114)</div>
                <div class="surah-name" id="surah-name">...</div>
                <div class="progress-bar"><div class="progress-fill" id="progress-fill"></div></div>
                <div class="time-row">
                    <span>المنقضي: <strong id="elapsed">00:00</strong></span>
                    <span>المدة الكلية: <strong id="duration">00:00</strong></span>
                    <span>المتبقي: <strong id="remaining">00:00</strong></span>
                </div>
            </div>
            <div class="control-panel">
                <h3 class="control-title">⚙️ أدوات التحكم بالبث المباشر (RTMP)</h3>
                <div class="control-row">
                    <input type="password" id="admin-pass" placeholder="كلمة مرور المشرف..." required>
                    <button class="btn-danger" onclick="skipSurah()">⏭ تخطي السورة</button>
                </div>
                <div class="control-row">
                    <select id="surah-selector">
                        <option value="" disabled selected>-- اختر سورة للقفز إليها وبثها الآن --</option>
                    </select>
                    <button class="btn-primary" onclick="jumpToSurah()">🚀 تشغيل فوراً</button>
                </div>
            </div>
            <div class="grid">
                <div class="stat-card"><div class="stat-label">الملفات الجاهزة محلياً</div><div class="stat-value"><span id="downloaded-count">0</span> / 114 ملف</div></div>
                <div class="stat-card"><div class="stat-label">القناة المستهدفة</div><div class="stat-value" id="channel-id">-</div></div>
                <div class="stat-card"><div class="stat-label">عدد الختمات المكتملة</div><div class="stat-value" id="khatmas">0</div></div>
                <div class="stat-card"><div class="stat-label">مدة تشغيل السيرفر</div><div class="stat-value" id="uptime">00:00</div></div>
            </div>
        </div>
        <div id="toast" class="toast"></div>
        <script>
            let surahsLoaded = false;
            function showToast(message, isError = false) {
                const toast = document.getElementById('toast');
                toast.textContent = message;
                toast.style.backgroundColor = isError ? '#ef4444' : '#10b981';
                toast.style.display = 'block';
                setTimeout(() => toast.style.display = 'none', 4000);
            }
            async function sendCommand(endpoint) {
                const pass = document.getElementById('admin-pass').value;
                if (!pass) { showToast('يرجى إدخال كلمة المرور أولاً!', true); return; }
                try {
                    const res = await fetch(endpoint, { method: 'POST', headers: { 'password': pass } });
                    const data = await res.json();
                    if (res.ok) { showToast(data.message); fetchStatus(); } 
                    else { showToast(data.detail || 'خطأ في العملية', true); }
                } catch (e) { showToast('فشل الاتصال بالخادم', true); }
            }
            function skipSurah() { sendCommand('/api/skip'); }
            function jumpToSurah() {
                const idx = document.getElementById('surah-selector').value;
                if (!idx) { showToast('يرجى اختيار سورة', true); return; }
                sendCommand(`/api/jump/${idx}`);
            }
            async function fetchStatus() {
                try {
                    const res = await fetch('/api/status');
                    const data = await res.json();
                    document.getElementById('status-badge').textContent = data.status;
                    document.getElementById('surah-num').textContent = data.current_surah_index;
                    document.getElementById('surah-name').textContent = data.current_surah_name;
                    document.getElementById('channel-id').textContent = data.channel_id;
                    document.getElementById('khatmas').textContent = data.completed_khatmas;
                    document.getElementById('uptime').textContent = data.uptime_formatted;
                    document.getElementById('elapsed').textContent = data.elapsed_formatted;
                    document.getElementById('duration').textContent = data.duration_formatted;
                    document.getElementById('remaining').textContent = data.remaining_formatted;
                    document.getElementById('downloaded-count').textContent = data.downloaded_files_count;
                    document.getElementById('progress-fill').style.width = data.progress_pct + '%';
                    if (!surahsLoaded && data.surah_names) {
                        const selector = document.getElementById('surah-selector');
                        data.surah_names.forEach((name, index) => {
                            const opt = document.createElement('option');
                            opt.value = index + 1;
                            opt.textContent = `${index + 1}. سورة ${name}`;
                            selector.appendChild(opt);
                        });
                        surahsLoaded = true;
                    }
                } catch (e) {}
            }
            fetchStatus(); setInterval(fetchStatus, 2000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:web_app", host="0.0.0.0", port=PORT, log_level="info")