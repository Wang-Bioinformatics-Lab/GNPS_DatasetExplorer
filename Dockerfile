FROM continuumio/miniconda3:4.10.3 as cleanup

# Set the container name

# Stop and remove the container, delete the image
RUN docker stop gnpsdatasetchooser-dash || true \
	&& docker rm gnpsdatasetchooser-dash || true \
   	&& docker rmi gnps_datasetexplorer-gnpsdatasetchooser-dash

# Start building the new image
FROM continuumio/miniconda3:4.10.3
MAINTAINER Mingxun Wang "mwang87@gmail.com"

RUN apt-get update && apt-get install -y build-essential

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install git+https://github.com/Wang-Bioinformatics-Lab/GNPSDataPackage.git

COPY . /app
WORKDIR /app
