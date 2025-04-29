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
			store.Set(parts[1], redigo.RedigoString(parts[2]))
			conn.Write([]byte("OK\n"))
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
		default:
			conn.Write([]byte(fmt.Sprintf("Unknown command '%v'.\n", parts[0])))
		}
	}
}

func main() {
	db := redigo.InitRedigo()
	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer ln.Close()

	fmt.Println("Redigo server started on :6379")

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn, db)
	}
}
