FROM maven:3.9.3-eclipse-temurin-17

WORKDIR /app

COPY server/ /app

RUN mvn clean install

CMD ["mvn", "exec:java", "-Dexec.mainClass=com.gbcsrt.Main"]