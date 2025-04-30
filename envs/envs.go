package envs

import (
	"fmt"
	"os"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

type Envs struct {
    AutosaveInterval      time.Duration `env:"AUTOSAVE_INTERVAL" envDefault:"5m"`
    AofCompactionInterval time.Duration `env:"AOF_COMPACTION_INTERVAL" envDefault:"10m"`  
    MaxSnapshots          int           `env:"MAX_SNAPSHOTS" envDefault:"5"`
    DataDirectory         string        `env:"DATA_DIRECTORY"`
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