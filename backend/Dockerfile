FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY requirements-telegram.txt requirements-openrouter.txt requirements-openrouter.lock requirements-sandbox.txt requirements-sandbox.lock ./
RUN pip install --no-cache-dir -r requirements-telegram.txt -r requirements-openrouter.txt -r requirements-sandbox.txt && pip check

WORKDIR /app/backend
COPY news_futures ./news_futures
ENTRYPOINT ["python", "-m", "news_futures"]
