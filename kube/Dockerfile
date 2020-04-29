
FROM centos:centos8.1.1911

RUN bash -c "dnf install -y nc java-11"


COPY "./stargate-clone" "/stargate"
WORKDIR "/stargate"

CMD ["java", "-cp", "target/appstax-lite-1.0-SNAPSHOT-jar-with-dependencies.jar", "stargate.service.package"]


