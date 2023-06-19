package chunkserver

import "github.com/kelseyhightower/envconfig"

type Config struct {
	Server struct {
		Host string `envconfig:"SERVER_HOST"`
		Port int    `envconfig:"SERVER_PORT"`
	}
	Master struct {
		Addr string `envconfig:"MASTER_ADDR"`
	}
	Chunks struct {
		Path string `envconfig:"CHUNK_PATH"`
	}
}

func GetConfig() (*Config, error) {
	var cfg Config
	err := envconfig.Process("", &cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
