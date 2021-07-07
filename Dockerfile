FROM python:3

WORKDIR /home

COPY requirements.txt .
RUN pip3 install -r requirements.txt --no-cache-dir
COPY . .

CMD ["python3", "batching.py"]
