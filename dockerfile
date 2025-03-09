# Use the latest version of Python
FROM python:latest

# Set the working directory inside the container to /app
WORKDIR /app

# Copy the requirements.txt file from the host to the current working directory in the container
COPY requirements.txt .

# Install the Python dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy all files from the host's current directory to the container's current directory
COPY . .

# Specify the command to run the Streamlit application
CMD ["streamlit", "run", "streamlit_app.py"]
