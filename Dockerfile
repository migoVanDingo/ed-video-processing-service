# syntax=docker/dockerfile:1.4
FROM python:3.11.9-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    git \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# If you still need PLATFORM_COMMON_TOKEN for private git installs, keep this
ARG PLATFORM_COMMON_TOKEN
ENV PLATFORM_COMMON_TOKEN=${PLATFORM_COMMON_TOKEN}

# Copy the requirements file directly
COPY requirements.txt .

# Install dependencies from requirements.txt
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .
EXPOSE 5008
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5008"]
