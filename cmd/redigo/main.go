package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"redigo/envs"
	"redigo/internal/redigo"
	"redigo/internal/redigo/types"
)

const (
    SET_COMMAND    = "SET"
    GET_COMMAND    = "GET"
    TTL_COMMAND    = "TTL"
    EXPIRE_COMMAND = "EXPIRE"
    SAVE_COMMAND   = "SAVE"
    BGSAVE_COMMAND = "BGSAVE"
)

func writeResponse(conn net.Conn, msg string) {
    conn.Write([]byte(msg))
}

func writeError(conn net.Conn, err error) {
    if err != nil {
        conn.Write([]byte(fmt.Sprintf("Error: %v\n", err)))
    }
}

func parseTTL(arg string) (int64, error) {
    return strconv.ParseInt(arg, 10, 64)
}

func HandleConnection(connection net.Conn, store *redigo.RedigoDB) {
    defer connection.Close()
    buffer := make([]byte, 1024)

    for {
        n, err := connection.Read(buffer)
        if err != nil {
            return
        }

        command := strings.TrimSpace(string(buffer[:n]))
        args := strings.Fields(command)
        if len(args) == 0 {
            writeResponse(connection, "Invalid command!\n")
            continue
        }

        switch strings.ToUpper(args[0]) {
            case SET_COMMAND:
                if len(args) < 3 || len(args) > 4 {
                    writeResponse(connection, "Usage: SET {key} {value} [ttl]\n")
                    return
                }
                var ttl int64
                if len(args) == 4 {
                    var err error
                    ttl, err = parseTTL(args[3])
                    if err != nil {
                        writeResponse(connection, fmt.Sprintf("Invalid TTL value: %v\n", err))
                        return
                    }
                }
                err := store.Set(args[1], types.RedigoString(args[2]), ttl)
                if err != nil {
                    writeError(connection, err)
                } else {
                    writeResponse(connection, "OK\n")
                }
            case GET_COMMAND:
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
            case TTL_COMMAND:
                if len(args) != 2 {
                    writeResponse(connection, "Usage: TTL {key}\n")
                    return
                }
                ttl, exists := store.GetTtl(args[1])
                switch {
                case !exists:
                    writeResponse(connection, "-2\n")
                case ttl == 0:
                    writeResponse(connection, "-1\n")
                default:
                    writeResponse(connection, fmt.Sprintf("%d\n", ttl))
                }
            case EXPIRE_COMMAND:
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
                    writeResponse(connection, "1\n")
                } else {
                    writeResponse(connection, "0\n")
                }
            case SAVE_COMMAND:
                err := store.ForceSave()
                if err != nil {
                    writeError(connection, fmt.Errorf("failed to save database: %v", err))
                } else {
                    writeResponse(connection, "Database saved successfully\n")
                }
            case BGSAVE_COMMAND:
                go func() {
                    if err := store.UpdateSnapshot(); err != nil {
                        writeError(connection, fmt.Errorf("background snapshot failed: %v", err))
                    } else {
                        writeResponse(connection, "Background saving completed successfully\n")
                    }
                }()
                writeResponse(connection, "Background saving started\n")
            default:
                writeResponse(connection, fmt.Sprintf("Unknown command '%v'.\n", args[0]))
        }
    }
}

func main() {
    envs.LoadEnv()

    config := envs.Gets()
    port := config.RedigoPort
    

    database, err := redigo.InitializeRedigo()
    if err != nil {
        writeError(nil, fmt.Errorf("failed to initialize Redigo database: %v", err))
        return
    }

    defer database.CloseAof()
    writeResponse(nil, fmt.Sprintf("Redigo server started on port %s\n", port))
    
    ln, err := net.Listen("tcp", ":"+port)
    if err != nil {
        panic(err)
    }
    
    defer ln.Close()

    for {
        conn, err := ln.Accept()
        if err != nil {
            continue
        }
        go HandleConnection(conn, database)
    }
}