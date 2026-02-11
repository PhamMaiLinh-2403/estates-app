from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from io import BytesIO
import uuid
import pandas as pd
import threading
import traceback
import socket 
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from commons.config import *
from commons.state_manager import PipelineStateManager
from database.database_manager import DatabaseManager

from main import * 

app = FastAPI()
templates = Jinja2Templates(directory="templates")
RESULT_STORE = {}

# Global State
scrape_state = {
    "running": False,
    "last_run": None,
    "message": "Idle"
}

scrape_lock = threading.Lock()
scheduler_lock_socket = None
scheduler = BackgroundScheduler(timezone="Asia/Ho_Chi_Minh") 


# 1. SCHEDULER AND RECORVERY DETECTION SETUP 

@app.on_event("startup")
def check_pipeline_recovery():
    """
    Runs on server start. 
    Detects if the pipeline was killed (suspended) and auto-resumes.
    """
    sm = PipelineStateManager()
    if sm.is_suspended():
        print("[System] Detected suspended pipeline state. Auto-rescheduling resume...")
        # Schedule a resume in 1 minute to allow server to fully boot
        run_time = datetime.now() + timedelta(minutes=1)
        scheduler.add_job(
            retry_pipeline_job,
            trigger=DateTrigger(run_date=run_time),
            id=f"auto_resume_{int(datetime.now().timestamp())}"
        )

def start_scheduler():
    # 1. TRY TO ACQUIRE LOCK
    if not acquire_scheduler_lock():
        print("[System] Scheduler Lock Failed: Another instance is already running the scheduler.")
        return

    # 2. IF WE GOT THE LOCK, START THE SCHEDULER
    print("[System] Scheduler Lock Acquired. Starting Background Scheduler...")
    
    # Check for recovery
    check_pipeline_recovery()
    
    # Add your jobs
    scheduler.add_job(
        weekly_pipeline_job,
        CronTrigger(day_of_week='wed', hour=15, minute=0),
        id="weekly_scrape"
    )   
    scheduler.start()

start_scheduler()

# 2. JOB WRAPPERS 

def weekly_pipeline_job():
    """
    CRON TRIGGER: Friday 21:00
    Intent: NEW RUN.
    Action: Cleanup old files, start fresh.
    """
    print("[Scheduler] Starting Weekly Run - Phase 1: URLs Collection...")
    run_phase_wrapper(resume=False, phase="urls")

def phase_2_job():
    """
    DELAYED TRIGGER: 30 mins after Phase 1
    Intent: START PHASE 2 (Details).
    Action: Resume (keep URL files), scrape details, clean.
    """
    print("[Scheduler] Starting Weekly Run - Phase 2: Details Collection...")
    # Trigger Phase 2 (Must be resume=True to read the URLs collected in Phase 1)
    run_phase_wrapper(resume=True, phase="details")

def retry_pipeline_job():
    """
    RETRY TRIGGER: Error or Auto-Recovery
    Action: Try to run everything remaining.
    """
    print("[Scheduler] Starting Retry/Resume Run (Full Recovery)...")
    run_phase_wrapper(resume=True, phase="full")

def run_phase_wrapper(resume, phase):
    """
    Generic wrapper to handle locking, execution, and chaining.
    """
    with scrape_lock:
        if scrape_state["running"]:
            print(f"[System] Pipeline busy. Cannot start phase: {phase}.")
            return
        
        scrape_state["running"] = True
        scrape_state["message"] = f"Running Phase: {phase}..."
        scrape_state["last_run"] = datetime.now().isoformat()

    try:
        # Call the updated function in main.py
        success, reason = run_pipeline_safe(resume=resume, target_phase=phase)
        
        if success:
            scrape_state["message"] = f"Phase {phase} Completed"
            
            if phase == "urls":
                # Calculate 30 minutes from now
                run_time = datetime.now() + timedelta(minutes=30)
                print(f"[System] Phase 1 done. Scheduling Phase 2 for {run_time.strftime('%H:%M:%S')} (30 min rest)")
                
                # Schedule the next job
                scheduler.add_job(
                    phase_2_job,
                    trigger=DateTrigger(run_date=run_time),
                    id=f"phase_2_{int(datetime.now().timestamp())}"
                )
            
            elif phase == "details" or phase == "full":
                # If we finished details, we are actually done-done.
                PipelineStateManager().reset()
                scrape_state["message"] = "Weekly Pipeline Fully Completed"

        else:
            # If failed, trigger the standard retry logic
            scrape_state["message"] = f"Suspended: {reason}"
            schedule_retry_if_needed()

    except Exception as e:
        scrape_state["message"] = f"Failed: {e}"
        traceback.print_exc()
        schedule_retry_if_needed()
    finally:
        scrape_state["running"] = False

def schedule_retry_if_needed():
    """Checks retry limits and schedules next attempt."""
    sm = PipelineStateManager()
    current_retries = sm.increment_retry()
    
    if current_retries > 3:
        print("[System] Max retries (3) reached. Manual intervention required.")
        scrape_state["message"] = "Failed (Max Retries Reached)"
        return

    # Retry in 1 hour
    run_time = datetime.now() + timedelta(hours=1)
    print(f"[System] Scheduling retry #{current_retries} for {run_time.strftime('%H:%M:%S')}")
    
    scheduler.add_job(
        retry_pipeline_job, 
        trigger=DateTrigger(run_date=run_time),
        id=f"retry_{int(datetime.now().timestamp())}",
        replace_existing=True
    )

def acquire_scheduler_lock():
    """
    Try to bind to a specific port. 
    If successful, return True (we are the main process).
    If failed (port in use), return False (we are a duplicate worker).
    """
    global scheduler_lock_socket
    try:
        scheduler_lock_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Bind to a high port on localhost
        scheduler_lock_socket.bind(("127.0.0.1", 49152)) 
        return True
    except socket.error:
        return False
    

# 3. API ROUTES 

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    if scrape_state["running"]:
        return RedirectResponse("/system-busy")
    return templates.TemplateResponse("home.html", {"request": request})

@app.get("/system-busy", response_class=HTMLResponse)
async def system_busy(request: Request):
    return templates.TemplateResponse("system_busy.html", {"request": request})

@app.get("/system-status")
def system_status():
    return scrape_state

@app.post("/submit", response_class=HTMLResponse)
async def submit(
    request: Request,
    web: str = Form(...),
    start_time: datetime = Form(...),
    end_time: datetime = Form(...),
    # request_name: str = Form(...)
):
    if scrape_state["running"]:
        return RedirectResponse(
            url="/system-busy",
            status_code=303 # Forces POST to GET
        )
    start_time_display = start_time.strftime("%d/%m/%Y")
    end_time_display = end_time.strftime("%d/%m/%Y")

    df = DatabaseManager.extract_data(start_time, end_time, web)
    if df is None:
        result_text = "Không tìm thấy thông tin"
    else:
        result_text = f'Thành công thu thập thông tin. Số lượng thông tin thu thập: {df.shape[0]}'
    job_id = str(uuid.uuid4())
    RESULT_STORE[job_id] = df

    return templates.TemplateResponse(
        "results.html",
        {
            "request": request,
            "web": web,
            "start_time": start_time_display,
            "end_time": end_time_display,
            # "request_name": request_name,
            "result_text": result_text,
            "job_id": job_id
            # "scraped_data": df
        }
    )

@app.get("/job/{job_id}")
def job_status(job_id: str):
    df = RESULT_STORE[job_id]

    if df.shape[0] == 0:
        return {"status": "failed"}

    return {
        "status": "done",
        "row_count": int(df.shape[0])
    }

@app.get("/download/{job_id}")
async def download(job_id: str):
    df = RESULT_STORE[job_id]
    if df.shape[0] == 0:
        return {"error": "File không tồn tại hoặc đã hết hạn"}
    
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="data")
    buffer.seek(0)

    del RESULT_STORE[job_id]

    return StreamingResponse(
        buffer, 
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={job_id}.xlsx"
        })