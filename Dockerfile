FROM python:3.10 

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Prevent interactive prompts during apt install
    DEBIAN_FRONTEND=noninteractive \
    # Add directory to path
    PYTHONPATH=/app

WORKDIR /app

# Install system dependencies 
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    xvfb \
    libxi6 \
    libgconf-2-4 \
    default-jdk \
    curl \
    --no-install-recommends

# Install Google Chrome stable 
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list' && \
    apt-get update && \
    apt-get install -y google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages 
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

RUN seleniumbase install chromedriver

COPY . .

# Create directories for volumes 
RUN mkdir -p /app/output && \
    mkdir -p "/app/Dữ liệu địa giới hành chính" && \
    mkdir -p "/app/Dữ liệu thông tin kỹ thuật tài sản"

# Expose FastAPI port
EXPOSE 8000

CMD ["xvfb-run", "--auto-servernum", "--server-args='-screen 0 1920x1080x24'", "uvicorn", "ui:app", "--host", "0.0.0.0", "--port", "8000"]