FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY hsl_producer.py .

CMD ["python", "hsl_producer.py"]