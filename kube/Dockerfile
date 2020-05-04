
FROM centos:centos8.1.1911

RUN bash -c "dnf install -y nc java-11 && dnf clean all"

COPY "./stargate-clone" "/stargate"
WORKDIR "/stargate"

CMD ["java", "-jar", "target/stargate-1.0-SNAPSHOT.jar"]
