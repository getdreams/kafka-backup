FROM gradle:6.2.0-jdk11 as builder

# get kafka-backup
WORKDIR /usr/src
RUN git clone https://github.com/getdreams/kafka-backup.git
WORKDIR /usr/src/kafka-backup
RUN git checkout draft-more-logs
RUN git status

# compile /usr/src/kafka-backup/build/libs/kafka-backup.jar
RUN ./gradlew shadowJar

# The actual image:
FROM confluentinc/cp-kafka-connect
COPY --from=builder /usr/src/kafka-backup/build/libs/kafka-backup.jar /etc/kafka-connect/jars/
