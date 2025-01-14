package config

import (
	"os"

	"whatsapp/internal/domain"
)

// Config holds all configuration for the application
type Config struct {
	Client domain.ClientConfig
}

// Load loads configuration from environment variables
func Load() *Config {
	return &Config{
		Client: domain.ClientConfig{
			DBPath:   getEnv("DB_PATH", "file:whatsapp.db?_pragma=foreign_keys(1)"),
			LogLevel: getEnv("LOG_LEVEL", "DEBUG"),
		},
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
