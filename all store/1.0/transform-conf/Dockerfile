FROM python:3.9.6-slim

RUN apt-get update && apt-get install -y libpq-dev gcc
RUN apt-get install -y procps

WORKDIR /transform-conf
COPY ./conf ./conf
COPY Requirement.txt .
COPY ./src ./src

RUN pip install --upgrade pip
RUN pip install -r Requirement.txt

CMD ["python", "./src/python_transform.py"]