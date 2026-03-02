FROM eclipse-temurin:21.0.4_7-jre-alpine

# Метки
LABEL maintainer="Valentin Pashkevich" \
      version="1.0.0" \
      description="Reactive Stock Market API with SSE"

# Обновление безопасности
RUN apk upgrade --no-cache && \
    apk add --no-cache curl

# Рабочая директория
WORKDIR /app

# Создаем непривилегированного пользователя
RUN addgroup -S appgroup && \
    adduser -S appuser -G appgroup

# Копируем jar с правильными правами
COPY --chown=appuser:appgroup build/libs/*.jar app.jar

# Переключаемся на непривилегированного пользователя
USER appuser

# Healthcheck
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8080/api/stocks/health || exit 1

# Порт
EXPOSE 8080

# Запуск с оптимизациями для контейнера
ENTRYPOINT ["java", \
    "-Djava.security.egd=file:/dev/./urandom", \
    "-XX:+UseContainerSupport", \
    "-XX:MaxRAMPercentage=75.0", \
    "-XX:+ExitOnOutOfMemoryError", \
    "-jar", "/app/app.jar"]