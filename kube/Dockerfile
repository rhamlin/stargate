
FROM centos:centos8.1.1911

COPY "./stargate-clone" "/stargate"
WORKDIR "/stargate"

RUN dnf install -y nc java-11 && dnf clean all && cp ./target/classes/logback-prod.xml ./target/classes/logback.xml

CMD ["java", "-jar", "target/stargate-1.0-SNAPSHOT.jar"]
