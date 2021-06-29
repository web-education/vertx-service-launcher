FROM openjdk:8-jre
LABEL maintainer="Damien BOISSIN <damien.boissin@opendigitaleducation.com>"

ARG JAR_FILE

COPY target/${JAR_FILE} /opt/
RUN wget -O /usr/local/openjdk-8/lib/ext/bcprov-jdk15on-160.jar https://www.bouncycastle.org/download/bcprov-jdk15on-160.jar && echo "security.provider.10=org.bouncycastle.jce.provider.BouncyCastleProvider" >> /usr/local/openjdk-8/lib/security/java.security
RUN ln -s /opt/${JAR_FILE} /opt/vertx-service-launcher.jar && groupadd vertx && useradd -u 1000 -g 1000 -m vertx && mkdir /srv/springboard && mkdir /srv/storage && chown -R vertx:vertx /srv

USER vertx

WORKDIR /srv/springboard
EXPOSE 8090

CMD java -agentlib:jdwp=transport=dt_socket,address=5000,server=y,suspend=${DEBUG_SUSPEND:-n} -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap -jar /opt/vertx-service-launcher.jar -Dvertx.services.path=/srv/springboard/mods -Dvertx.disableFileCaching=true -conf /srv/springboard/conf/vertx.conf
