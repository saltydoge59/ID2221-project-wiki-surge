FROM python:3.11-slim
RUN pip install --no-cache-dir confluent-kafka==2.6.1 pyarrow==18.1.0 requests==2.32.3
WORKDIR /app
COPY producer.py .
CMD ["python", "-u", "producer.py"]
