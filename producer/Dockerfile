FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python-ng
COPY producer.py /app/producer.py
CMD ["python", "-u", "producer.py"]