FROM python:3.11-slim

WORKDIR /app

# Build deps for cryptg (C MTProto AES) and friends; removed after install.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Telethon session files live here; mount a volume so they survive restarts.
RUN mkdir -p /app/session

# Run the auto-scaling manager: it keeps 1 UI bot + N forwarding workers alive,
# scales them to load, and adapts to runtime health.
CMD ["python", "manager.py"]
