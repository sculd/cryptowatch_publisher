package main

import (
	"fmt"
	"log"
	"time"
)

func main() {
	fmt.Println("starting main...")

	for {
		log.Printf("logging dummy message")
		fmt.Println("printing dummy message")
		time.Sleep(1 * time.Second)
	}
}
