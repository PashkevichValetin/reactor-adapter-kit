FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY build/libs/*.jar app.jar
# Копируем конфиг из ресурсов (если нужно внешний конфиг)
COPY src/main/resources/application.yaml /app/config/application.yaml
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
