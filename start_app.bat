@echo off
TITLE Real Estate Data Server Setup & Run
CLS

:: --- Check Python version ---
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [LOI] Python chua duoc cai dat hoac chua them vao PATH.
    echo Vui long cai dat Python (tick vao 'Add Python to PATH') roi chay lai.
    PAUSE
    EXIT
)

:: --- Create and activate venv ---
IF NOT EXIST "venv" (
    echo [INFO] Phat hien lan chay dau tien. Dang tao moi truong ao...
    python -m venv venv
    
    echo [INFO] Dang cai dat thu vien (viec nay chi lam 1 lan)...
    call venv\Scripts\activate.bat
    
    :: Upgrade pip 
    python.exe -m pip install --upgrade pip
    
    :: Install packages from requirements.txt
    pip install -r requirements.txt
    
    :: Install drivers for SeleniumBase
    sbase install chromedriver
    
    echo [OK] Cai dat moi truong hoan tat!
) ELSE (
    echo [INFO] Da tim thay moi truong ao. Dang khoi dong...
    call venv\Scripts\activate.bat
)

:: --- Set up Ngrok (Only on the first run) ---
IF NOT EXIST ".ngrok_configured" (
    echo.
    echo ---------------------------------------------------
    echo CHUA CAU HINH NGROK.
    echo Vui long nhap Authtoken cua ban (Lay tai dashboard.ngrok.com)
    echo ---------------------------------------------------
    set /p NGROK_TOKEN="Paste Ngrok Authtoken va an Enter: "
    
    ngrok config add-authtoken %NGROK_TOKEN%
    
    :: Tao file danh dau da cau hinh de lan sau khong hoi lai
    type NUL > .ngrok_configured
    echo [OK] Da luu token Ngrok.
)

:: --- Start up Server ---
echo.
echo ===================================================
echo     DANG KHOI DONG HE THONG...
echo     1. Dang mo Ngrok Tunnel...
echo     2. Dang chay FastAPI Server...
echo ===================================================
echo.

start "Ngrok Tunnel" /MIN ngrok http 8000

uvicorn ui:app --host 0.0.0.0 --port 8000

pause