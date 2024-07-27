package main

import (
	"context"
	"log"

	"github.com/qynonyq/ton_dev_go_hw2/internal/app"
	"github.com/qynonyq/ton_dev_go_hw2/internal/scanner"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if _, err := app.InitApp(); err != nil {
		return err
	}
	ctx := context.Background()

	sc, err := scanner.NewScanner(ctx)
	if err != nil {
		return err
	}
	sc.Listen(ctx)

	return nil
}
