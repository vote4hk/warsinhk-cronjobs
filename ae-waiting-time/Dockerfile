### 1. Get Linux
FROM rappdw/docker-java-python:openjdk1.8.0_171-python3.6.6
RUN mkdir -p app
WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt
ENV ENDPOINT=$ENDPOINT
ENV ADMIN_SECRET=$ADMIN_SECRET
CMD ["python", "/app/run.py"]
