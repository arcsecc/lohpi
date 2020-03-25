package main

import (
	"fmt"
	"github.com/spf13/viper"
)

func main() {
	if err := readConfig(); err != nil {
		panic(err)
	}

	fmt.Printf("kake\n")
}

func readConfig() error {
	viper.SetConfigName("firestore_config")
	viper.AddConfigPath("/var/tmp")
	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")

	err := viper.ReadInConfig()
	if err != nil {
		return err
	}

	// Behavior variables
	viper.SetDefault("num_subjects", 2)
	viper.SetDefault("num_studies", 10)
	viper.SetDefault("data_users", 1)
	viper.SetDefault("files_per_study", 2)
	viper.SetDefault("file_size", 256)
	viper.SetDefault("fuse_mount", "/home/thomas/go/src/firestore")
	viper.SetDefault("set_files", true)
	viper.SafeWriteConfig()
	return nil
}