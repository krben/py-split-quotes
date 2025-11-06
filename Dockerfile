
# Use the official Python image from the Docker Hub
FROM python:3.12.7-slim


ENV TZ=Europe/Stockholm
RUN apt-get update && apt-get install -y tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Set the working directory in the container
WORKDIR /app


# Copy the application code into the container
COPY app/ /app

# Copy the requirements file into the container
COPY requirements/requirements.txt /requirements/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /requirements/requirements.txt

# Expose ports 8000
# EXPOSE 8000

# Define environment variable
# Define environment variable
ENV PYTHONUNBUFFERED=1
ENV NAME=QUOTESSPLITTER

# Run the application
CMD ["python", "app.py"]