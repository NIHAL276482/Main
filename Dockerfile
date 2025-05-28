FROM python:3.10-slim

WORKDIR /app

# Install system dependencies via apt
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    libcurl4-openssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY cookies.txt .
COPY . .

# Create downloads directory
RUN mkdir -p /app/downloads

EXPOSE 7777

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "7777"]