package main

import (
	"fmt"
	"net"
	"redigo/core/redigo"
	"strings"
)

func handleConnection(conn net.Conn, store *redigo.RedigoDB) {
    defer conn.Close()
    buf := make([]byte, 1024)

    for {
        n, err := conn.Read(buf)
        if err != nil {
            return
        }

        cmd := strings.TrimSpace(string(buf[:n]))
        parts := strings.Split(cmd, " ")

        if len(parts) < 1 {
            conn.Write([]byte("Invalid command!\n"))
            continue
        }

        switch parts[0] {
        case "SET":
            if len(parts) != 3 {
                conn.Write([]byte("Missing key value!\n"))
                continue
            }
            err := store.Set(parts[1], redigo.RedigoString(parts[2]))
            if err != nil {
                conn.Write([]byte(fmt.Sprintf("Error: %v\n", err)))
            } else {
                conn.Write([]byte("OK\n"))
            }
        case "GET":
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
        case "SAVE":
            err := store.ForceSave()
            if err != nil {
                conn.Write([]byte(fmt.Sprintf("Error saving database: %v\n", err)))
            } else {
                conn.Write([]byte("Database saved successfully\n"))
            }
        case "BGSAVE":
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

func main() {
    db, err := redigo.InitRedigo()
    if err != nil {
        fmt.Printf("Error initializing database: %v\n", err)
        return
    }
    defer db.CloseAOF()

    fmt.Println("Redigo server started on :6379 (hybrid persistence: AOF + snapshots)")

    ln, err := net.Listen("tcp", ":6379")
    if err != nil {
        panic(err)
    }
    defer ln.Close()

    for {
        conn, err := ln.Accept()
        if err != nil {
            continue
        }
        go handleConnection(conn, db)
    }
}