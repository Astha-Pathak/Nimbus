# Distributed File Management System - Project Instructions

## Project Goal

Build an interview-ready, self-contained Distributed File Management System
demo that demonstrates:

- gRPC and Protocol Buffers
- distributed service communication
- asynchronous task scheduling
- concurrent worker execution
- task persistence
- retry and fault tolerance
- worker heartbeat and failure detection
- local file storage
- Docker-based local execution

The project must remain simple enough to run entirely on a local machine.

## Technology Stack

- Java 21
- Maven
- Spring Boot
- gRPC
- Protocol Buffers
- H2 or SQLite
- Local filesystem
- Docker
- Docker Compose

## Services

The project will contain:

1. proto
2. common
3. gateway
4. scheduler
5. worker

### Gateway

Responsible for:

- UploadFile
- DownloadFile
- SubmitTask
- GetTaskStatus

The Gateway communicates with the Scheduler through gRPC.

The Gateway must NOT directly access the Scheduler database.

### Scheduler

Responsible for:

- accepting tasks
- task persistence
- task queue
- task lifecycle
- worker registration
- worker scheduling
- retry and exponential backoff
- worker heartbeat monitoring
- worker failure detection
- task reassignment
- restart recovery

### Worker

Responsible for:

- registering with Scheduler
- sending heartbeats
- receiving tasks
- concurrent task execution
- task processors
- reporting task results

## Architecture

Client
    |
    | gRPC
    v
Gateway
    |
    | gRPC
    v
Scheduler
    |
    | gRPC
    +------------+------------+
    |            |            |
    v            v            v
Worker 1     Worker 2     Worker 3

Scheduler owns task and worker persistence.

Gateway owns file upload/download handling.

Workers process files stored in the local filesystem.

## Task Lifecycle

QUEUED -> RUNNING -> COMPLETED

QUEUED -> RUNNING -> FAILED

FAILED -> QUEUED when retry is allowed.

## Scheduling

Start with Round-Robin scheduling.

Define a SchedulingStrategy interface so that additional scheduling
strategies can be added later.

## Worker Execution

Use a configurable bounded Java thread pool.

Initial task processors:

- CHECKSUM
- COMPRESSION

Do not implement arbitrary code execution.

## Persistence

Use H2 or SQLite for the MVP.

Persist:

- task metadata
- task status
- worker metadata
- retry information
- timestamps
- task results/errors where appropriate

File contents must remain in the local filesystem, not in the database.

## Fault Tolerance

The MVP must eventually support:

- retry
- exponential backoff
- worker heartbeat
- worker failure detection
- task reassignment
- scheduler restart recovery

## Project Constraints

- No cloud services
- No paid services
- No external infrastructure
- No web UI
- No Kubernetes in the MVP
- No unnecessary frameworks
- Everything must be runnable locally

## Development Rules

Build incrementally.

Do NOT generate the entire application at once.

Implement one milestone at a time.

Do not add functionality that has not been requested for the current milestone.

Prefer simple, understandable implementations over excessive abstraction.

Every important feature should have tests.

Do not claim a feature is implemented unless it is actually implemented and tested.

Keep service responsibilities clearly separated.

Use meaningful package names and class names.

Use Java 26 features only where they improve clarity.

The code should be understandable by a software engineer reviewing it
during an interview.