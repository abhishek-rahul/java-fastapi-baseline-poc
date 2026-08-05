FROM maven:3.9.9-eclipse-temurin-17 AS java-build
WORKDIR /build/java-client
COPY java-client/pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY java-client/src ./src
RUN mvn -q -DskipTests clean package

FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        tini \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY python-fastapi/requirements.txt /app/python-fastapi/requirements.txt
RUN pip install --no-cache-dir -r /app/python-fastapi/requirements.txt

COPY python-fastapi/app /app/python-fastapi/app
COPY --from=java-build /build/java-client/target/java-client-1.0.0.jar /app/java-client.jar
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

ENTRYPOINT ["/usr/bin/tini", "--", "/app/docker-entrypoint.sh"]
CMD ["4"]
