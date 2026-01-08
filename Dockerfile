FROM eclipse-temurin:21.0.4_7-jre-alpine
RUN apk upgrade --no-cache
WORKDIR /app
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
COPY --chown=appuser:appgroup build/libs/*.jar app.jar
USER appuser
EXPOSE 8080
ENTRYPOINT ["java", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-XX:+UseContainerSupport", \
    "-jar", "/app/app.jar"]
