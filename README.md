# Kova

A Redis-compatible key-value server implemented in Java 21. Kova speaks the RESP (Redis Serialization Protocol) wire protocol, making it a drop-in target for any standard Redis client.

---

## Features

- **RESP protocol** — full implementation of the Redis Serialization Protocol (inline arrays, bulk strings, integers, simple strings, errors)
- **Core command set** — `PING`, `GET`, `SET`, `DEL`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `TTL`
- **TTL and lazy expiration** — keys expire on access rather than on a background timer
- **Append-Only File (AOF) persistence** — write commands are enqueued and flushed to disk asynchronously via a dedicated writer thread; the file is replayed on startup to restore state
- **Virtual thread concurrency** — each client connection runs on a virtual thread (Java 21), keeping the server lean under concurrent load
- **Structured logging** — SLF4J with Logback; separate console and rolling file appenders

---

## Architecture

```
Client (redis-cli, Jedis, etc.)
        |
        | TCP / RESP
        v
  ClientHandler            -- one virtual thread per connection
        |
        v
  CommandHandler           -- parses and dispatches commands
        |
        +---> StorageEngine     -- ConcurrentHashMap with TTL entries
        |
        +---> AofWriter         -- async queue -> FileChannel (DSYNC)
```

On startup, `KVServer` checks for an existing AOF file and replays it through `AofLoader` before accepting connections. After replay, a fresh `AofWriter` is opened in append mode and registered with the command handler.

---

## Getting Started

**Requirements**

- Java 21
- Maven 3.8+

**Build**

```bash
mvn compile
```

The build runs Spotless formatting checks (Google Java Format, AOSP style) as part of the compile phase.

**Run**

```bash
mvn exec:java -Dexec.mainClass="org.nur.server.KVServer"
```

The server listens on port `6379` by default. Connect with any Redis client:

```bash
redis-cli ping
# PONG

redis-cli set foo bar
# OK

redis-cli get foo
# "bar"

redis-cli expire foo 30
# (integer) 1

redis-cli ttl foo
# (integer) 29
```

**AOF file**

Kova writes to `aof.log` in the working directory. Delete the file to start with a clean slate. The file is replayed automatically on the next startup.

---

## Project Structure

```
src/main/java/org/nur/
  commands/       Command record and CommandHandler dispatch logic
  exception/      Typed exceptions (arity, protocol, AOF, server)
  persistence/    AofWriter (async flush) and AofLoader (replay)
  protocol/       RespParser, RespSerializer, RespWriter, RespValue types
  server/         KVServer (main), ClientHandler (per-connection loop)
  storage/        StorageEngine (ConcurrentHashMap + TTL)
```

---

## Dependencies

| Library | Version | Purpose |
|---|---|---|
| SLF4J | 2.0.17 | Logging facade |
| Logback Classic | 1.5.32 | Logging implementation |
| Spotless (build) | 2.43.0 | Code formatting enforcement |

---

## License

MIT
