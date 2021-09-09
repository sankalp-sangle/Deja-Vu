# Download base image ubuntu 20.04
FROM ubuntu:18.04

# LABEL about the custom image
LABEL maintainer="sankalpsangle98@gmail.com"
LABEL version="0.1"
LABEL description="This is custom Docker Image for \
portraying my thesis work"

# Disable Prompt During Packages Installation
ARG DEBIAN_FRONTEND=noninteractive

# Update Ubuntu Software repository
RUN apt update

# Create app repository
WORKDIR /app

# Copy files
COPY . .

# Install python and python3 dependencies

RUN yes | apt install python-pip

RUN yes | apt install python3-pip

RUN yes | pip install --upgrade setuptools

RUN yes | pip3 install --upgrade setuptools

RUN yes | pip install -r requirementsPython.txt

RUN yes | pip3 install -r requirementsPython3.txt

RUN chmod 777 setup.sh

ENTRYPOINT [ "/app/setup.sh" ]

# Expose Port for the Application 
EXPOSE 5000
