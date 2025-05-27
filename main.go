package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"redigo/core/envs"
	"redigo/core/redigo"
)

// handleConnection processes incoming client connections and handles their commands
// Each connection is handled in its own goroutine for concurrency
func handleConnection(conn net.Conn, store *redigo.RedigoDB) {
    defer conn.Close()
    buf := make([]byte, 1024)

    for {
        n, err := conn.Read(buf)
        if err != nil {
            return // Client disconnected or connection error
        }

        cmd := strings.TrimSpace(string(buf[:n]))
        parts := strings.Split(cmd, " ")

        if len(parts) < 1 {
            conn.Write([]byte("Invalid command!\n"))
            continue
        }

        // Process different commands
        switch parts[0] {
        case "SET":
            // SET <key> <value> [<ttl>] - Stores a value with optional TTL in seconds
            if len(parts) < 3 || len(parts) > 4 {
                conn.Write([]byte("Usage: SET key value [ttl]\n"))
                continue
            }
            
            var ttl int64 = 0
            if len(parts) == 4 {
                // Parse TTL parameter if provided
                ttlVal, err := strconv.ParseInt(parts[3], 10, 64)
                if err != nil {
                    conn.Write([]byte(fmt.Sprintf("Invalid TTL value: %v\n", err)))
                    continue
                }
                ttl = ttlVal
            }
            
            err := store.Set(parts[1], redigo.RedigoString(parts[2]), ttl)
            if err != nil {
                conn.Write([]byte(fmt.Sprintf("Error: %v\n", err)))
            } else {
                conn.Write([]byte("OK\n"))
            }
            
        case "GET":
            // GET <key> - Retrieves the value associated with the specified key
            if len(parts) != 2 {
                conn.Write([]byte("Missing key\n"))
                continue
            }
            value, ok := store.Get(parts[1])
            if ok {
                conn.Write([]byte(fmt.Sprintf("%v\n", value)))
            } else {
                conn.Write([]byte("nil\n"))
            }
            
        case "TTL":
            // TTL <key> - Returns the remaining time to live for a key with TTL
            if len(parts) != 2 {
                conn.Write([]byte("Usage: TTL key\n"))
                continue
            }
            ttl, exists := store.GetTTL(parts[1])
            if !exists {
                conn.Write([]byte("-2\n")) // Key doesn't exist
            } else if ttl == 0 {
                conn.Write([]byte("-1\n")) // Key exists but has no TTL
            } else {
                conn.Write([]byte(fmt.Sprintf("%d\n", ttl)))
            }
            
        case "EXPIRE":
            // EXPIRE <key> <seconds> - Set a timeout on key
            if len(parts) != 3 {
                conn.Write([]byte("Usage: EXPIRE key seconds\n"))
                continue
            }
            seconds, err := strconv.ParseInt(parts[2], 10, 64)
            if err != nil {
                conn.Write([]byte(fmt.Sprintf("Invalid seconds value: %v\n", err)))
                continue
            }
            
            success := store.SetExpiry(parts[1], seconds)
            if success {
                conn.Write([]byte("1\n")) // Success
            } else {
                conn.Write([]byte("0\n")) // Key doesn't exist
            }
            
        case "SAVE":
            // SAVE - Forces the creation of a snapshot and AOF compaction
            err := store.ForceSave()
            if err != nil {
                conn.Write([]byte(fmt.Sprintf("Error saving database: %v\n", err)))
            } else {
                conn.Write([]byte("Database saved successfully\n"))
            }
            
        case "BGSAVE":
            // BGSAVE - Creates a snapshot in a background process
            go func() {
                if err := store.CreateSnapshot(); err != nil {
                    fmt.Printf("Error creating snapshot: %v\n", err)
                } else {
                    fmt.Println("Background snapshot completed successfully")
                }
            }()
            conn.Write([]byte("Background saving started\n"))
            
        default:
            conn.Write([]byte(fmt.Sprintf("Unknown command '%v'.\n", parts[0])))
        }
    }
}

// main initializes and runs the Redigo server
func main() {
    envs.LoadEnv()

    // Get port from environment variable or use default :6379
    config := envs.Gets()
    port := config.RedigoPort
    

    // Initialize the database with hybrid persistence (AOF + snapshots)
    db, err := redigo.InitRedigo()
    if err != nil {
        fmt.Printf("Error initializing database: %v\n", err)
        return
    }
    defer db.CloseAOF() // Ensure proper cleanup on shutdown

    fmt.Printf("Redigo server started on :%s\n", port)
    
    // Start TCP server
    ln, err := net.Listen("tcp", ":"+port)
    if err != nil {
        panic(err)
    }
    defer ln.Close()

    // Accept and handle incoming connections
    for {
        conn, err := ln.Accept()
        if err != nil {
            continue
        }
        go handleConnection(conn, db) // Handle each client in a separate goroutine
    }
}