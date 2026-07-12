FROM gradle:8.7-jdk21 AS build
WORKDIR /app
COPY settings.gradle build.gradle ./
COPY src ./src
RUN gradle --no-daemon installDist

FROM eclipse-temurin:21-jre
COPY --from=build /app/build/install/runtime /opt/runtime
ENTRYPOINT ["/opt/runtime/bin/runtime"]
