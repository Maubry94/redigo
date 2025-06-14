# Redigo

A mini Redis implementation in Go with advanced features.

## Features

- **Key-Value Store**: Basic SET/GET/DELETE operations
- **TTL Support**: Automatic key expiration
- **AOF (Append Only File)**: Command logging for durability
- **Snapshots**: Point-in-time database backups
- **Reverse Indexing**: Fast searching by value and key patterns
- **Concurrent Access**: Thread-safe operations with mutex protection

## Available Commands

### Basic Operations
- `SET key value [ttl]` - Store a key-value pair with optional TTL
- `GET key` - Retrieve a value by key
- `DELETE key` - Remove a key-value pair
- `TTL key` - Get remaining time to live for a key
- `EXPIRE key seconds` - Set expiration time for a key

### Search Operations (Reverse Indexing)
- `SEARCHVALUE value` - Find all keys with the specified value
- `SEARCHPREFIX prefix` - Find all keys starting with prefix
- `SEARCHSUFFIX suffix` - Find all keys ending with suffix
- `SEARCHCONTAINS substring` - Find all keys containing substring

### Persistence
- `SAVE` - Create immediate snapshot
- `BGSAVE` - Create background snapshot

## Architecture

The project implements Redis-like functionality with:

- **In-memory storage** with concurrent access protection
- **Reverse indexes** for efficient value-based and pattern searches
- **AOF logging** for command durability
- **Periodic snapshots** for data persistence
- **Automatic expiration** for TTL management

## Usage

1. Start the server: `go run cmd/redigo/main.go`
2. Connect via TCP on port 6380 (configurable)
3. Send commands as plain text

Example:
```
SET user:1 "john" 3600
GET user:1
SEARCHVALUE "john"
SEARCHPREFIX "user:"
```
