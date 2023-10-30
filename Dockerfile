FROM python:3.11-slim as base

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# set the working directory
WORKDIR /project

# install dependencies
COPY ./requirements.txt /project
RUN pip install --no-cache-dir -r requirements.txt

# copy the scripts to the folder
COPY ./src /project/src

# production image
FROM base as production
# Creates a non-root user with an explicit UID and adds permission to access the /project folder
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /project
USER appuser

CMD ["python", "src/main.py"]
