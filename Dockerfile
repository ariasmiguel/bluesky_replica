FROM clickhouse

WORKDIR /home

# Install Python 3.9 and pip (3.9 is a stable version with good library support)
RUN apt-get update && apt-get install -y \
    python3.9 \
    python3.9-dev \
    python3-pip \
    jq \
    curl \
    gpg \
    pv \
    wget \
    unzip

# Make Python 3.9 the default
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1

# Upgrade pip
RUN python3 -m pip install --upgrade pip

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Install AWS CLI
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm awscliv2.zip \
    && rm -rf aws

# Copy your scripts
COPY ingest.py /home/ingest.py
COPY run_ingest.sh /home/run_ingest.sh
RUN chmod +x /home/run_ingest.sh

ENTRYPOINT ["./run_ingest.sh"]