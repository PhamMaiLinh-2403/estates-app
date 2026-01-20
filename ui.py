from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from io import BytesIO
import uuid
import pandas as pd
import threading
import traceback
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from commons.config import *
from commons.state_manager import PipelineStateManager
from main import run_pipeline_safe #, run_pipeline

from Batdongsan.orchestrator import (
    process_batdongsan_data 
)

from Onehousing.orchestrator import (
    process_onehousing_data
)
import pytz
import sqlite3
# from main import run_scrape_urls,run_scrape_details, run_cleaning_pipeline

app = FastAPI()
templates = Jinja2Templates(directory="templates")
RESULT_STORE = {}

scrape_state = {
    "running": False,
    "last_run": None,
    "message": "Idle"
}

scrape_lock = threading.Lock()
scheduler = BackgroundScheduler(timezone="Asia/Ho_Chi_Minh")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    if scrape_state["running"]:
        scrape_state["message"] = "Scraping in progress"
        return RedirectResponse("/system-busy")
    
    return templates.TemplateResponse(
        "home.html", 
        {"request": request}
    )

@app.get("/system-busy", response_class=HTMLResponse)
async def system_busy(request: Request):
    return templates.TemplateResponse(
        "system_busy.html", 
        {"request": request}
    )

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

    df = extract_data(start_time, end_time, web)
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

# ------- Functions ------
def extract_data(start_date, end_date, web):
    # start_date = datetime.strptime(start_date, "%d/%m/%Y").strftime("%Y-%m-%d")
    # end_date = datetime.strptime(end_date, "%d/%m/%Y").strftime("%Y-%m-%d")

    with sqlite3.connect(DATABASE_DIR) as conn:
        cursor = conn.cursor()
        
        if web == 'Cả hai':
            sql_statement = """
                        SELECT *
                        FROM cleaned
                        WHERE
                        date(
                            substr("Thời điểm giao dịch/rao bán", 7, 4) || '-' ||
                            substr("Thời điểm giao dịch/rao bán", 4, 2) || '-' ||
                            substr("Thời điểm giao dịch/rao bán", 1, 2)
                        )
                        BETWEEN date(?) AND date(?)
                        """
            cursor.execute(
                sql_statement,
                (start_date, end_date))
        else:
            sql_statement = """
                            SELECT *
                            FROM cleaned
                            WHERE
                            date(
                                substr("Thời điểm giao dịch/rao bán", 7, 4) || '-' ||
                                substr("Thời điểm giao dịch/rao bán", 4, 2) || '-' ||
                                substr("Thời điểm giao dịch/rao bán", 1, 2)
                            )
                            BETWEEN date(?) AND date(?)
                            AND Web = (?)
                            """
        
            cursor.execute(
                sql_statement,
                (start_date, end_date, web)
            )

        rows = cursor.fetchall()

    dup = [
        'Tỉnh/Thành phố', 
        'Thành phố/Quận/Huyện/Thị xã', 
        'Xã/Phường/Thị trấn', 
        'Đường phố', 
        'Giá rao bán/giao dịch', 
        'Giá ước tính', 
        'Số tầng công trình', 
        'Tổng diện tích sàn', 
        'Đơn giá xây dựng', 
        'Chất lượng còn lại', 
        'Diện tích đất (m2)', 
        'Kích thước mặt tiền (m)', 
        'Kích thước chiều dài (m)', 
        'Số mặt tiền tiếp giáp', 
        'Hình dạng', 
        'Độ rộng ngõ/ngách nhỏ nhất (m)', 
        'Khoảng cách tới trục đường chính (m)', 
        'Mục đích sử dụng đất',
        'Web',
        'Đơn giá đất', 
        'Lợi thế kinh doanh', 
    ]

    return_df = pd.DataFrame(rows, columns=[column[0] for column in cursor.description])
    return_df.drop_duplicates(subset=dup, inplace=True)
    return return_df

def retry_pipeline_job():
    """Wrapper to call resume logic via Scheduler"""
    print("Executing Retry Job...")
    run_pipeline_wrapper(is_retry=True)

def weekly_pipeline_job():
    """Wrapper to call new run logic via Scheduler"""
    print("Executing Weekly Job...")
    run_pipeline_wrapper(is_retry=False)

def run_pipeline_wrapper(is_retry=False):
    with scrape_lock:
        if scrape_state["running"]:
            print("Pipeline already running. Skipping job.")
            return
        
        scrape_state["running"] = True
        scrape_state["message"] = "Scraping (Retry)" if is_retry else "Scraping (Weekly)"
        scrape_state["last_run"] = datetime.now().isoformat()

    try:
        success, reason = run_pipeline_safe(resume=is_retry)
        
        if success:
            scrape_state["message"] = "Completed successfully"
            # Reset retry count on success
            PipelineStateManager().reset()
        else:
            scrape_state["message"] = f"Suspended: {reason}"
            schedule_retry()

    except Exception as e:
        scrape_state["message"] = f"Failed: {e}"
        traceback.print_exc()
        schedule_retry()
    finally:
        scrape_state["running"] = False

def schedule_retry():
    """Schedules a retry in 1 hour if limits not exceeded."""
    sm = PipelineStateManager()
    current_retries = sm.increment_retry()
    
    if current_retries > 3: # Limit global retries
        print("Max global retries (3) reached. Giving up.")
        return

    run_time = datetime.now() + timedelta(hours=1)
    print(f"Scheduling retry #{current_retries} for {run_time.strftime('%H:%M:%S')}")
    
    scheduler.add_job(
        retry_pipeline_job,
        trigger=DateTrigger(run_date=run_time),
        id=f"retry_{int(datetime.now().timestamp())}",
        replace_existing=True
    )

# Start Scheduler
scheduler.add_job(
    weekly_pipeline_job,
    CronTrigger(day_of_week='fri', hour=21, minute=0),
    id="weekly_scrape",
    replace_existing=True
)

def weekly_pipeline():
    if scrape_state['running']:
        return
    
    scrape_state['running'] = True
    scrape_state["message"] = "Scraping started"

    try:
        run_pipeline()
        scrape_state['last_run'] = datetime.now().isoformat()
        scrape_state['message'] = "Pipeline completed successfully"
    except Exception as e:
        scrape_state["message"] = "Pipeline failed"
        print(f'Pipeline stopped with error: {e}')
        traceback.print_exc()
    finally:
        scrape_state["running"] = False

def test_schedule():
    scrape_state['running'] = True

    print("Cleaning data...")
    df_bds_clean = process_batdongsan_data()
    df_bds_clean['Web'] = 'Batdongsan'
    df_oh_clean = process_onehousing_data()
    df_oh_clean['Web'] = 'Onehousing'
    df_oh_clean['Thời điểm giao dịch/rao bán'] = datetime.now().strftime("%d/%m/%Y")
    
    df_cleaned = pd.concat([df_bds_clean, df_oh_clean], axis=0)
    print(f"Batdongsan original shape: {df_bds_clean.shape}")
    print(f"Onehousing original shape: {df_oh_clean.shape}")
    print(f'Final shape: {df_cleaned.shape}')
    print(f'Columns: {df_cleaned.columns}')
    
    scrape_state['running'] = False

scheduler = BackgroundScheduler(timezone="Asia/Ho_Chi_Minh")
scheduler.add_job(
    weekly_pipeline,
    CronTrigger(
        day_of_week='tue',
        hour=15,
        minute=11
    ),
    id="weekly_scrape",
    replace_existing=True
)
scheduler.start()