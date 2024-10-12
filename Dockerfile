FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y openjdk-17-jdk

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH

COPY ./kubegrapher ./kubegrapher

COPY requirements.txt .
COPY setup.py .

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python", "kubegrapher/run.py"]