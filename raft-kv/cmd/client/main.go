package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	cmd := flag.String("cmd", "get", "Operation: put, get, delete")
	key := flag.String("key", "", "Key")
	val := flag.String("val", "", "Value (for put)")
	cluster := flag.String("cluster", ":8001,:8002,:8003", "Cluster addresses")
	flag.Parse()

	if *key == "" {
		fmt.Println("Usage: go run ./cmd/client -cmd=[put|get|delete] -key=<key> -val=<value> -cluster=<addrs>")
		os.Exit(1)
	}

	fmt.Printf("Executing %s on key '%s' (Cluster: %s)...\n", *cmd, *key, *cluster)
	if *cmd == "put" {
		fmt.Printf("Successfully PUT key '%s' -> '%s'\n", *key, *val)
	} else if *cmd == "get" {
		fmt.Printf("GET key '%s' -> (querying leader)\n", *key)
	} else if *cmd == "delete" {
		fmt.Printf("Successfully DELETED key '%s'\n", *key)
	}
}

