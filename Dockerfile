FROM python:3.8.5-buster

USER root

# Create working directory
RUN mkdir /opt/app-root && chmod 755 /opt/app-root
WORKDIR /opt/app-root

# Install the requirements
COPY ./requirements.txt .

RUN pip install pip==20.1.1
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Set Python path
ENV PYTHONPATH=/opt/app-root

EXPOSE 8080

CMD [ "/bin/sh", "run.sh" ]
