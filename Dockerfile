FROM apache/spark:4.1.0-preview4-scala2.13-java21-python3-r-ubuntu

USER root

# Install Python deps
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Copy only code, NOT raw data
WORKDIR /app
COPY scripts/ /app/scripts/


