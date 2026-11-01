# TaskForge Orchestrator Service

Distributed workflow execution engine and DAG orchestrator for the TaskForge platform.

## Features
- **DAG Orchestration:** Evaluates workflow dependency graphs, resolves task ready-states, and schedules child tasks on upstream completion.
- **Transactional Outbox & Inbox:** Idempotent event processing and reliable Kafka message dispatching.
- **Kafka Event Consumer:** Processes execution requests and worker task completions/failures.
- **State Management:** Tracks workflow execution and task execution states in PostgreSQL.

## Tech Stack
- Java 21 & Spring Boot 3.3.4
- Spring Data JPA & PostgreSQL
- Spring Kafka
- Flyway Database Migrations
- Lombok

## Getting Started

### Prerequisites
- JDK 21
- PostgreSQL
- Apache Kafka

### Running Locally
```bash
./mvnw spring-boot:run
```

Or build the runnable jar:
```bash
./mvnw clean package
java -jar target/taskforge-orchestrator-service-0.1.0-SNAPSHOT.jar
```

### Docker
```bash
docker build -t taskforge-orchestrator-service .
docker run -p 8081:8081 taskforge-orchestrator-service
```
