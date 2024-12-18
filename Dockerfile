# Use an official Python runtime as a parent image
FROM python:3.13.0-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
# Mount the working directory so that changes in the code are reflected in the container
COPY . /app


# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Make port 8050 available to the world outside this container
EXPOSE 8050

# Define environment variable
ENV NAME BI-Project

# Run app.py when the container launches
CMD ["python", "app.py"]
