FROM python:3.6.4

RUN mkdir -p app

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

CMD ["python", "/app/run.py"]
