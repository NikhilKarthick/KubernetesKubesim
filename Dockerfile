# Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY distributedsystems.py /app/

RUN pip install flask

EXPOSE 5001

CMD ["python", "distributedsystems.py"]
