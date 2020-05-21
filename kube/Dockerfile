#   Copyright DataStax, Inc.
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
FROM openjdk:11-jdk-slim
ARG SG_VERSION
COPY ./target ./stargate
COPY ./src/main/webapp ./stargate/src/main/webapp
WORKDIR ./stargate
ENV SGV ${SG_VERSION}
RUN cp ./classes/logback-prod.xml ./classes/logback.xml

CMD ["sh", "-c", "java -jar ./stargate-${SGV}.jar"]
