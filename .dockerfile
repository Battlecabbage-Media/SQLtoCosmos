# Use an official Python runtime as a parent image
FROM python:3.8-slim-buster

# Set the working directory in the container to /app
WORKDIR /app

# Add the current directory contents into the container at /app
ADD . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Set sql_to_cosmos.py as the entry point of the container, we can then pass arguments to the script if needed.
ENTRYPOINT ["python", "sql_to_cosmos.py"]