FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY requirements.txt constraints.txt ./
RUN pip install --no-cache-dir -r requirements.txt && pip check

WORKDIR /app/backend
COPY news_futures ./news_futures
ENTRYPOINT ["python", "-m", "news_futures"]
