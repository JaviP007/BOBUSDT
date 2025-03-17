# Use an official lightweight Python image from Docker Hub
FROM python:3.9-slim

# Set a working directory in the container
WORKDIR /app

# Copy in requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python script into the container
COPY BOBUSDT.py .

# By default, when the container starts, run this command:
CMD ["python", "BOBUSDT.py"]
