Distributed File Management — Maven multi-module skeleton

This repo contains a Maven multi-module project skeleton for the Distributed File Management System.

Modules:
- proto — Protobuf & gRPC codegen (no .proto files yet)
- common — shared utilities
- gateway — Spring Boot app (depends on common + proto)
- scheduler — Spring Boot app (depends on common + proto)
- worker — Spring Boot app (depends on common + proto)

Build:

```bash
mvn -T1C clean test
```
