package envs

import (
	"fmt"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Envs struct {
	// Defines the TCP port on which the server will listen for connections
	RedigoPort string `env:"REDIGO_PORT" envDefault:"6379"`

	// Determines how often database snapshots are saved to disk
	SnapshotSaveInterval time.Duration `env:"SNAPSHOT_SAVE_INTERVAL" envDefault:"5m"`

	// Controls how frequently write buffers are flushed to disk
	FlushBufferInterval time.Duration `env:"FLUSH_BUFFER_INTERVAL" envDefault:"10m"`

	// Sets how often the server checks for and removes expired keys
	DataExpirationInterval time.Duration `env:"DATA_EXPIRATION_INTERVAL" envDefault:"1m"`

	// Default time-to-live for keys when no TTL is specified
	// 0 = no expiration by default
	DefaultTTL int64 `env:"DEFAULT_TTL" envDefault:"0"`

	// Specifies the root directory for data storage
	RedigoRootDirPath string `env:"REDIGO_ROOT_DIR_PATH" envDefault:""`
}

// Attempts to load environment variables from a .env file
// If the file doesn't exist, it prints a warning but continues with default values
func LoadEnv() {
	err := godotenv.Load()
	if err != nil {
		fmt.Printf("Warning: .env file not found, using default values\n")
	}
}

// Gets parses environment variables into the Envs struct and returns the configuration
// It exits the program if there's an error parsing the environment variables
func Gets() Envs {
	var envs Envs

	// Parse environment variables into the struct using the env tags
	if err := env.Parse(&envs); err != nil {
		fmt.Printf("Error parsing env variables: %v\n", err)
		os.Exit(1)
	}

	return envs
}
