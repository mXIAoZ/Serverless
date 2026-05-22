FROM docker.m.daocloud.io/library/python:3.12-slim AS java-bootstrap-builder

RUN sed -i 's|http://deb.debian.org/debian|https://mirrors.tuna.tsinghua.edu.cn/debian|g; s|http://deb.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g' /etc/apt/sources.list.d/debian.sources
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jdk-headless \
    && rm -rf /var/lib/apt/lists/*
COPY runtime/bootstrap/java/JavaBootstrap.java /tmp/java-bootstrap/JavaBootstrap.java
RUN javac -d /tmp/java-bootstrap/classes /tmp/java-bootstrap/JavaBootstrap.java \
    && jar --create --file /tmp/java-bootstrap/java-bootstrap.jar -C /tmp/java-bootstrap/classes .

FROM docker.m.daocloud.io/library/python:3.12-slim

RUN sed -i 's|http://deb.debian.org/debian|https://mirrors.tuna.tsinghua.edu.cn/debian|g; s|http://deb.debian.org/debian-security|https://mirrors.tuna.tsinghua.edu.cn/debian-security|g' /etc/apt/sources.list.d/debian.sources
RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# 本地预编译的二进制（Linux arm64）
COPY bin/runtime-server-linux /usr/local/bin/runtime-server
COPY bin/runtime-agent-linux  /usr/local/bin/runtime-agent
COPY bin/go-bootstrap-linux   /runtime/bootstrap/go-bootstrap
COPY --from=java-bootstrap-builder /tmp/java-bootstrap/java-bootstrap.jar /runtime/bootstrap/java-bootstrap.jar
COPY runtime/entrypoint.sh    /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/runtime-server \
             /usr/local/bin/runtime-agent \
             /usr/local/bin/entrypoint.sh \
             /runtime/bootstrap/go-bootstrap

# bootstrap 脚本
COPY runtime/bootstrap/ /runtime/bootstrap/

# 函数代码挂载点（运行时通过 -v 挂载）
RUN mkdir -p /function

WORKDIR /function

ENV FUNCTION_HANDLER=handler.handler
ENV FUNCTION_RUNTIME=python3
ENV FUNCTION_DIR=/function

EXPOSE 9000 9001

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
