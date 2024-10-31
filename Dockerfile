# Base stage
FROM python:3.11-slim AS base

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

# Non-root user setup stage
FROM base AS user-setup
# Creates a non-root user with an explicit UID and adds permission to access the /project folder
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /project
USER appuser

# Production stage for highscore
FROM user-setup AS production-highscore
CMD ["python", "src/main_highscore.py"]

# Production stage for runemetrics
FROM user-setup AS production-runemetrics
CMD ["python", "src/main_runemetrics.py"]
