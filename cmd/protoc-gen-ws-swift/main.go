package main

import (
	"log"
	"os"

	"github.com/gopherex/ws-proto/mobilegen"
)

func main() {
	if err := mobilegen.Run(os.Stdin, os.Stdout, mobilegen.Swift); err != nil {
		log.Fatal(err)
	}
}
