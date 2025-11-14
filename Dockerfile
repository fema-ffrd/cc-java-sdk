FROM --platform=linux/amd64 038611608639.dkr.ecr.us-east-1.amazonaws.com/compute-plugins:java-plugin-base

ENV PATH="/gradle/bin:${PATH}"

ARG GRADLE_VERSION=8.14

RUN echo 'Acquire::http::Pipeline-Depth 0;\nAcquire::http::No-Cache true; \nAcquire::BrokenProxy true;\n' > /etc/apt/apt.conf.d/99fixbadproxy

RUN wget -O gradle.zip https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip &&\
    unzip gradle.zip &&\
    mv gradle-${GRADLE_VERSION} gradle 

