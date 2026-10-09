FROM python:3.11-slim-bookworm
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY . /app/observability
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "observability.metrics_exporter"]
