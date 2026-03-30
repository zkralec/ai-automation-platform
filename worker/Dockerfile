FROM python:3.12-slim

WORKDIR /app
ENV PYTHONPATH=/app/worker
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir -U pip

COPY worker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "worker/heartbeat_supervisor.py"]
