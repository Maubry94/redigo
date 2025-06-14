package envs

import (
	"fmt"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Envs struct {
    RedigoPort            string        `env:"REDIGO_PORT" envDefault:"6379"`
    SnapshotSaveInterval  time.Duration `env:"SNAPSHOT_SAVE_INTERVAL" envDefault:"5m"`
    FlushBufferInterval   time.Duration `env:"FLUSH_BUFFER_INTERVAL" envDefault:"10m"`
    DataExpirationInterval time.Duration `env:"DATA_EXPIRATION_INTERVAL" envDefault:"1m"` 
    DefaultTTL            int64         `env:"DEFAULT_TTL" envDefault:"0"`  // 0 = pas d'expiration par défaut
    RedigoRootDirPath     string        `env:"REDIGO_ROOT_DIR_PATH" envDefault:""`
}

func LoadEnv() {
    err := godotenv.Load()
    if err != nil {
        fmt.Printf("Warning: .env file not found, using default values\n")
    }
}

func Gets() Envs {
    var envs Envs

    if err := env.Parse(&envs); err != nil {
        fmt.Printf("Error parsing env variables: %v\n", err)
        os.Exit(1)
    }

    return envs
}