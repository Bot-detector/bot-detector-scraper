FROM python:3.9-slim

WORKDIR /project
COPY ./requirements.txt /project/requirements.txt

# required to install github.com/TheRealNoob/python-logging-loki.git
RUN apt update && \
    apt install git curl -y && \
    apt clean autoclean && \
    apt autoremove --yes && \
    rm -rf /var/lib/{apt,dpkg,cache,log}

RUN pip install --no-cache-dir -r /project/requirements.txt

COPY . /project

CMD ["python", "src/scraper.py"]
# CMD [ "sleep", "1000" ]
