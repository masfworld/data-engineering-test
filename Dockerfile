# Use an official Python image as a base
FROM python:3.11-slim

# Set environment variables for Poetry and Java
ENV POETRY_VERSION=1.8.4
ENV PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

# Install necessary system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk-headless \
    curl \
    psutils \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN

RUN export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))"

# Install Poetry
RUN pip install "poetry==$POETRY_VERSION"

# Set the working directory
WORKDIR /app

# Copy project files into the container
COPY pyproject.toml poetry.lock ./
COPY . .

# Install dependencies using Poetry
RUN poetry install --no-root --no-dev

EXPOSE 4040

# Command to run the application
CMD ["poetry", "run", "python", "main.py"]
# CMD ["sleep", "1000000"]