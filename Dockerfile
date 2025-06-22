# Use Ubuntu 20.04 as base image
FROM ubuntu:22.04

# Install systemd and system dependencies
RUN apt-get update && apt-get install -y \
    systemd \
    python3-pip \
    ffmpeg \
    curl \
    libcurl4-openssl-dev \
    build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Ensure systemd is properly configured
RUN systemctl set-default multi-user.target

# Set working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application files
COPY cookies.txt .
COPY . .

# Create downloads directory
RUN mkdir -p downloads

# Expose ports 7777, 80, and 443 for TCP
EXPOSE 7777 80 443

# Start systemd and run the Python app in the foreground
CMD ["/bin/bash", "-c", "/lib/systemd/systemd & uvicorn main:app --host 0.0.0.0 --port 7777"]
