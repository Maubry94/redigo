package main

import (
	"fmt"
	"net"
	"strings"

	"redigo/envs"
	"redigo/internal/redigo"
	"redigo/pkg/utils"
)

type ClientResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Error   error  `json:"error,omitempty"`
}

func NewSuccessResponse(message string) ClientResponse {
	return ClientResponse{
		Success: true,
		Message: message,
		Error:   nil,
	}
}

func NewErrorResponse(err error) ClientResponse {
	return ClientResponse{
		Success: false,
		Message: fmt.Sprintf("Error: %v", err),
		Error:   err,
	}
}

func NewUsageErrorResponse(usage string) ClientResponse {
	return ClientResponse{
		Success: false,
		Message: usage,
		Error:   fmt.Errorf("invalid usage"),
	}
}

func (response ClientResponse) ToString() string {
	return response.Message + "\n"
}

const (
	SET_COMMAND             = "SET"            // Store key-value pair
	GET_COMMAND             = "GET"            // Retrieve value by key
	DELETE_COMMAND          = "DELETE"         // Remove key-value pair
	TTL_COMMAND             = "TTL"            // Get time-to-live for a key
	EXPIRE_COMMAND          = "EXPIRE"         // Set expiration time for a key
	SAVE_COMMAND            = "SAVE"           // Force save database to disk
	BGSAVE_COMMAND          = "BGSAVE"         // Background save database to disk
	SEARCH_VALUE_COMMAND    = "SEARCHVALUE"    // Find keys by their values
	SEARCH_PREFIX_COMMAND   = "SEARCHPREFIX"   // Find keys starting with prefix
	SEARCH_SUFFIX_COMMAND   = "SEARCHSUFFIX"   // Find keys ending with suffix
	SEARCH_CONTAINS_COMMAND = "SEARCHCONTAINS" // Find keys containing substring
)

func writeResponse(conn net.Conn, response ClientResponse) {
	stringResponse := response.ToString()
	if conn != nil {
		conn.Write([]byte(stringResponse))
	} else {
		fmt.Print(stringResponse)
	}
}

func HandleConnection(connection net.Conn, store *redigo.RedigoDB) {
	defer connection.Close()
	buffer := make([]byte, 1024)

	for {
		numberOfBytesRead, err := connection.Read(buffer)
		if err != nil {
			return
		}

		rawCommand := strings.TrimSpace(string(buffer[:numberOfBytesRead]))
		cookedCommand := strings.Fields(rawCommand)

		response := HandleCommand(cookedCommand, store)

		writeResponse(connection, response)
	}
}

func handleSetCommand(arguments []string, store *redigo.RedigoDB) ClientResponse {
	numberOfArguments := len(arguments)
	if numberOfArguments < 3 || numberOfArguments > 4 {
		return NewUsageErrorResponse("Usage: SET {key} {value} [ttl]")
	}

	var ttl int64
	if numberOfArguments == 4 {
		var err error
		if ttl, err = utils.FromStringToInt64(arguments[3]); err != nil {
			return NewErrorResponse(fmt.Errorf("invalid TTL value: %v", err))
		}
	} else {
		config := envs.Gets()
		ttl = config.DefaultTTL
	}

	if err := store.Set(arguments[1], arguments[2], ttl); err != nil {
		return NewErrorResponse(fmt.Errorf("failed to set value: %v", err))
	}
	return NewSuccessResponse("OK")
}

func handleGetCommand(arguments []string, store *redigo.RedigoDB) ClientResponse {
	if len(arguments) != 2 {
		return NewUsageErrorResponse("Usage: GET {key}")
	}

	value, err := store.Get(arguments[1])
	if err != nil {
		return NewErrorResponse(fmt.Errorf("failed to get value: %v", err))
	}
	return NewSuccessResponse(fmt.Sprintf("%v", value))
}

func handleDeleteCommand(arguments []string, store *redigo.RedigoDB) ClientResponse {
	if len(arguments) != 2 {
		return NewUsageErrorResponse("Usage: DELETE {key}")
	}

	if hasDeleted := store.Delete(arguments[1]); hasDeleted {
		return NewSuccessResponse("1")
	}
	return NewSuccessResponse("0")
}

func handleTtlCommand(arguments []string, store *redigo.RedigoDB) ClientResponse {
	if len(arguments) != 2 {
		return NewUsageErrorResponse("Usage: TTL {key}")
	}

	requestedKey := arguments[1]
	ttl, exists := store.GetTtl(requestedKey)
	if !exists {
		return NewSuccessResponse(fmt.Sprintf("Key : %v doesn't exists.", requestedKey))
	} else if ttl == 0 {
		return NewSuccessResponse(fmt.Sprintf("No expiration for : %v.", requestedKey))
	}
	return NewSuccessResponse(fmt.Sprintf("%d", ttl))
}

func handleExpireCommand(arguments []string, store *redigo.RedigoDB) ClientResponse {
	if len(arguments) != 3 {
		return NewUsageErrorResponse("Usage: EXPIRE {key} seconds")
	}

	requestedKey := arguments[1]
	seconds, err := utils.FromStringToInt64(arguments[2])
	if err != nil {
		return NewErrorResponse(fmt.Errorf("invalid seconds value: %v", err))
	}

	if success := store.SetExpiry(requestedKey, seconds); success {
		return NewSuccessResponse("OK")
	}
	return NewSuccessResponse(fmt.Sprintf("Key : %v doesn't exists.", requestedKey))
}

func handleSaveCommand(store *redigo.RedigoDB) ClientResponse {
	if err := store.ForceSave(); err != nil {
		return NewErrorResponse(fmt.Errorf("failed to save database: %v", err))
	}
	return NewSuccessResponse("Database saved successfully")
}

func handleBgsaveCommand(store *redigo.RedigoDB) ClientResponse {
	go func() {
		if err := store.UpdateSnapshot(); err != nil {
			fmt.Printf("Background snapshot failed: %v\n", err)
		} else {
			fmt.Println("Background saving completed successfully")
		}
	}()
	return NewSuccessResponse("Background saving started")
}

func handleSearchValueCommand(arguments []string, store *redigo.RedigoDB) ClientResponse {
	if len(arguments) != 2 {
		return NewUsageErrorResponse("Usage: SEARCHVALUE {value}")
	}

	keys := store.SearchByValue(arguments[1])
	if keys == nil {
		return NewSuccessResponse("No keys found")
	}
	return NewSuccessResponse(fmt.Sprintf("Found keys: %v", keys))
}

func handleSearchPrefixCommand(arguments []string, store *redigo.RedigoDB) ClientResponse {
	if len(arguments) != 2 {
		return NewUsageErrorResponse("Usage: SEARCHPREFIX {prefix}")
	}

	keys := store.SearchByKeyPrefix(arguments[1])
	if keys == nil {
		return NewSuccessResponse("No keys found")
	}
	return NewSuccessResponse(fmt.Sprintf("Found keys: %v", keys))
}

func handleSearchSuffixCommand(arguments []string, store *redigo.RedigoDB) ClientResponse {
	if len(arguments) != 2 {
		return NewUsageErrorResponse("Usage: SEARCHSUFFIX {suffix}")
	}

	keys := store.SearchByKeySuffix(arguments[1])
	if keys == nil {
		return NewSuccessResponse("No keys found")
	}
	return NewSuccessResponse(fmt.Sprintf("Found keys: %v", keys))
}

func handleSearchContainsCommand(arguments []string, store *redigo.RedigoDB) ClientResponse {
	if len(arguments) != 2 {
		return NewUsageErrorResponse("Usage: SEARCHCONTAINS {substring}")
	}

	keys := store.SearchByKeyContains(arguments[1])
	if len(keys) == 0 {
		return NewSuccessResponse("No keys found")
	}
	return NewSuccessResponse(fmt.Sprintf("Found keys: %v", keys))
}

func HandleCommand(arguments []string, store *redigo.RedigoDB) ClientResponse {
	if len(arguments) == 0 {
		return NewUsageErrorResponse("Invalid command!")
	}

	requestedCommand := strings.ToUpper(arguments[0])

	switch requestedCommand {
	case SET_COMMAND:
		return handleSetCommand(arguments, store)
	case GET_COMMAND:
		return handleGetCommand(arguments, store)
	case DELETE_COMMAND:
		return handleDeleteCommand(arguments, store)
	case TTL_COMMAND:
		return handleTtlCommand(arguments, store)
	case EXPIRE_COMMAND:
		return handleExpireCommand(arguments, store)
	case SAVE_COMMAND:
		return handleSaveCommand(store)
	case BGSAVE_COMMAND:
		return handleBgsaveCommand(store)
	case SEARCH_VALUE_COMMAND:
		return handleSearchValueCommand(arguments, store)
	case SEARCH_PREFIX_COMMAND:
		return handleSearchPrefixCommand(arguments, store)
	case SEARCH_SUFFIX_COMMAND:
		return handleSearchSuffixCommand(arguments, store)
	case SEARCH_CONTAINS_COMMAND:
		return handleSearchContainsCommand(arguments, store)
	default:
		return NewErrorResponse(fmt.Errorf("unknown command '%v'", arguments[0]))
	}
}

func main() {
	envs.LoadEnv()
	config := envs.Gets()
	port := config.RedigoPort

	database, err := redigo.InitializeRedigo()
	if err != nil {
		writeResponse(
			nil,
			NewErrorResponse(fmt.Errorf("failed to initialize Redigo database: %v", err)),
		)
		return
	}

	defer database.CloseAof()
	writeResponse(
		nil,
		NewSuccessResponse(fmt.Sprintf("Redigo server started on port %s\n", port)),
	)

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go HandleConnection(conn, database)
	}
}
