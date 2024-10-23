# Use an official Python base image for Python 3.12.1
FROM python:3.12.1-slim

# Set environment variables to avoid prompts during installation
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file to the working directory
COPY requirements.txt /app/

# Install system dependencies, including Git
RUN apt-get update && apt-get install -y \
    build-essential \
    libgl1-mesa-glx \
    libglib2.0-0 \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt

# Copy the rest of the application code
COPY . /app/

# Expose port 8000 for Streamlit
EXPOSE 8000

# Command to run the Streamlit app on port 8000
CMD ["streamlit", "run", "login.py", "--server.port=8000", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]
