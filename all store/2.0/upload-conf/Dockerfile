FROM python:3.9.6-slim

RUN apt-get update && apt-get install -y libpq-dev gcc
RUN apt-get install -y procps

WORKDIR /upload-conf
RUN mkdir data
COPY Requirement.txt .
COPY ./src ./src
COPY ./conf ./conf

RUN pip install --upgrade pip
RUN pip install -r Requirement.txt

CMD ["python", "./src/upload-conf.py"]