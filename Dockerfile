# syntax=docker/dockerfile:1.6
# 1단계: Gradle 로 실행 가능한 jar 를 만든다. 캐시 마운트로 의존성 내려받기를 재사용한다.
FROM gradle:8.5-jdk17 AS build
WORKDIR /home/gradle/project
COPY build.gradle ./
COPY gradle ./gradle
COPY src ./src
RUN --mount=type=cache,target=/home/gradle/.gradle,uid=1000,gid=1000 \
    gradle bootJar --no-daemon -q \
    && mkdir /app && cd /app && jar xf /home/gradle/project/build/libs/crypto-tracker.jar

# 2단계: JRE 만 있는 작은 이미지에서 실행한다. Flink 는 프로세스 안에서 돌아 JDK 가 필요 없다.
# fat jar 를 그대로 java -jar 로 띄우면 Flink 의 로컬 JobMaster 가 시스템 클래스로더로 클래스를 찾다가
# 중첩 jar 안의 ExecutionConfig 를 못 찾는다. 그래서 jar 를 풀어 일반 클래스패스로 실행한다.
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/BOOT-INF/lib ./lib
COPY --from=build /app/BOOT-INF/classes ./classes
EXPOSE 8082
ENTRYPOINT ["java", "-cp", "classes:lib/*", "com.crypto.flink.CryptoFlinkApplication"]
