package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"redigo/envs"
	"redigo/internal/redigo"
	"redigo/internal/redigo/types"
)

// Command constants
const (
    SET_COMMAND    = "SET"                   // Store key-value pair
    GET_COMMAND    = "GET"                   // Retrieve value by key
    DELETE_COMMAND = "DELETE"                // Remove key-value pair
    TTL_COMMAND    = "TTL"                   // Get time-to-live for a key
    EXPIRE_COMMAND = "EXPIRE"                // Set expiration time for a key
    SAVE_COMMAND   = "SAVE"                  // Force save database to disk
    BGSAVE_COMMAND = "BGSAVE"                // Background save database to disk
    SEARCH_VALUE_COMMAND = "SEARCHVALUE"     // Find keys by their values
    SEARCH_PREFIX_COMMAND = "SEARCHPREFIX"   // Find keys starting with prefix
    SEARCH_SUFFIX_COMMAND = "SEARCHSUFFIX"   // Find keys ending with suffix
    SEARCH_CONTAINS_COMMAND = "SEARCHCONTAINS" // Find keys containing substring
)

// Sends a message to the client connection or prints to stdout if no connection
func writeResponse(conn net.Conn, msg string) {
    if conn != nil {
        conn.Write([]byte(msg))
    } else {
        fmt.Print(msg)
    }
}

// Handles error output, sending to client or stderr appropriately
func writeError(conn net.Conn, err error) {
    if err != nil {
        if conn != nil {
            conn.Write([]byte(fmt.Sprintf("Error: %v\n", err)))
        } else {
            fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        }
    }
}

// Converts string TTL value to int64
func parseTTL(arg string) (int64, error) {
    return strconv.ParseInt(arg, 10, 64)
}

// Processes client commands for a single TCP connection
// This function runs in a goroutine for each client connection
func HandleConnection(connection net.Conn, store *redigo.RedigoDB) {
    defer connection.Close() // Ensure connection is closed when function exits
    buffer := make([]byte, 1024) // Buffer to read client commands

    // Infinite loop to handle multiple commands from the same client
    for {
        // Read command from client
        n, err := connection.Read(buffer)
        if err != nil {
            return // Client disconnected or error occurred
        }

        // Parse command and arguments
        command := strings.TrimSpace(string(buffer[:n]))
        args := strings.Fields(command)
        if len(args) == 0 {
            writeResponse(connection, "Invalid command!\n")
            continue
        }

        // Handle different commands using a switch statement
        switch strings.ToUpper(args[0]) {
            case SET_COMMAND:
                // SET command: SET key value [ttl]
                if len(args) < 3 || len(args) > 4 {
                    writeResponse(connection, "Usage: SET {key} {value} [ttl]\n")
                    return
                }
                var ttl int64
                if len(args) == 4 {
                    // Parse custom TTL if provided
                    var err error
                    ttl, err = parseTTL(args[3])
                    if err != nil {
                        writeResponse(connection, fmt.Sprintf("Invalid TTL value: %v\n", err))
                        return
                    }
                } else {
                    // Use default TTL from configuration
                    config := envs.Gets()
                    ttl = config.DefaultTTL
                }
                err := store.Set(args[1], types.RedigoString(args[2]), ttl)
                if err != nil {
                    writeError(connection, err)
                } else {
                    writeResponse(connection, "OK\n")
                }
                
            case GET_COMMAND:
                // GET command: GET key
                if len(args) != 2 {
                    writeResponse(connection, "Usage: GET key\n")
                    return
                }
                value, err := store.Get(args[1])
                if err != nil {
                    writeError(connection, err)
                } else {
                    writeResponse(connection, fmt.Sprintf("%v\n", value))
                }
                
            case DELETE_COMMAND:
                // DELETE command: DELETE key
                if len(args) != 2 {
                    writeResponse(connection, "Usage: DELETE {key}\n")
                    return
                }
                success := store.Delete(args[1])
                if success {
                    writeResponse(connection, "1\n") // Return 1 if key existed and was deleted
                } else {
                    writeResponse(connection, "0\n") // Return 0 if key didn't exist
                }
                
            case TTL_COMMAND:
                // TTL command: TTL key - returns remaining time to live
                if len(args) != 2 {
                    writeResponse(connection, "Usage: TTL {key}\n")
                    return
                }
                ttl, exists := store.GetTtl(args[1])
                switch {
                case !exists:
                    writeResponse(connection, "-2\n") // Key doesn't exist
                case ttl == 0:
                    writeResponse(connection, "-1\n") // Key exists but has no expiration
                default:
                    writeResponse(connection, fmt.Sprintf("%d\n", ttl)) // Return TTL in seconds
                }
                
            case EXPIRE_COMMAND:
                // EXPIRE command: EXPIRE key seconds - set expiration time
                if len(args) != 3 {
                    writeResponse(connection, "Usage: EXPIRE {key} seconds\n")
                    return
                }
                seconds, err := parseTTL(args[2])
                if err != nil {
                    writeResponse(connection, fmt.Sprintf("Invalid seconds value: %v\n", err))
                    return
                }
                success := store.SetExpiry(args[1], seconds)
                if success {
                    writeResponse(connection, "1\n") // Expiration set successfully
                } else {
                    writeResponse(connection, "0\n") // Key doesn't exist
                }
                
            case SAVE_COMMAND:
                // SAVE command: Force synchronous save to disk
                err := store.ForceSave()
                if err != nil {
                    writeError(connection, fmt.Errorf("failed to save database: %v", err))
                } else {
                    writeResponse(connection, "Database saved successfully\n")
                }
                
            case BGSAVE_COMMAND:
                // BGSAVE command: Start background save process
                go func() {
                    if err := store.UpdateSnapshot(); err != nil {
                        writeError(connection, fmt.Errorf("background snapshot failed: %v", err))
                    } else {
                        writeResponse(connection, "Background saving completed successfully\n")
                    }
                }()
                writeResponse(connection, "Background saving started\n")
                
            case SEARCH_VALUE_COMMAND:
                // SEARCHVALUE command: Find keys that have the specified value
                if len(args) != 2 {
                    writeResponse(connection, "Usage: SEARCHVALUE {value}\n")
                    return
                }
                keys := store.SearchByValue(args[1])
                if len(keys) == 0 {
                    writeResponse(connection, "No keys found\n")
                } else {
                    writeResponse(connection, fmt.Sprintf("Found keys: %v\n", keys))
                }
                
            case SEARCH_PREFIX_COMMAND:
                // SEARCHPREFIX command: Find keys starting with the specified prefix
                if len(args) != 2 {
                    writeResponse(connection, "Usage: SEARCHPREFIX {prefix}\n")
                    return
                }
                keys := store.SearchByKeyPrefix(args[1])
                if len(keys) == 0 {
                    writeResponse(connection, "No keys found\n")
                } else {
                    writeResponse(connection, fmt.Sprintf("Found keys: %v\n", keys))
                }
                
            case SEARCH_SUFFIX_COMMAND:
                // SEARCHSUFFIX command: Find keys ending with the specified suffix
                if len(args) != 2 {
                    writeResponse(connection, "Usage: SEARCHSUFFIX {suffix}\n")
                    return
                }
                keys := store.SearchByKeySuffix(args[1])
                if len(keys) == 0 {
                    writeResponse(connection, "No keys found\n")
                } else {
                    writeResponse(connection, fmt.Sprintf("Found keys: %v\n", keys))
                }
                
            case SEARCH_CONTAINS_COMMAND:
                // SEARCHCONTAINS command: Find keys containing the specified substring
                if len(args) != 2 {
                    writeResponse(connection, "Usage: SEARCHCONTAINS {substring}\n")
                    return
                }
                keys := store.SearchByKeyContains(args[1])
                if len(keys) == 0 {
                    writeResponse(connection, "No keys found\n")
                } else {
                    writeResponse(connection, fmt.Sprintf("Found keys: %v\n", keys))
                }
                
            default:
                // Unknown command
                writeResponse(connection, fmt.Sprintf("Unknown command '%v'.\n", args[0]))
        }
    }
}

// Entry point of the Redigo server
func main() {
    // Load environment configuration
    envs.LoadEnv()
    config := envs.Gets()
    port := config.RedigoPort
    
    // Initialize the Redigo database
    database, err := redigo.InitializeRedigo()
    if err != nil {
        writeError(nil, fmt.Errorf("failed to initialize Redigo database: %v", err))
        return
    }

    // Ensure AOF is properly closed on exit
    defer database.CloseAof()
    writeResponse(nil, fmt.Sprintf("Redigo server started on port %s\n", port))
    
    // Start TCP listener on the configured port
    ln, err := net.Listen("tcp", ":"+port)
    if err != nil {
        panic(err)
    }
    defer ln.Close()

    // Main server loop - accept and handle client connections
    for {
        conn, err := ln.Accept()
        if err != nil {
            continue // Skip failed connections
        }
        // Handle each client connection in a separate goroutine for concurrency
        go HandleConnection(conn, database)
    }
}
