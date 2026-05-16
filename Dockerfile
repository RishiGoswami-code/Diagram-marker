FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# The entrypoint will be overridden in docker-compose.yml
CMD ["python", "main.py"]
