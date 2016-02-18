package main

import (
	"github.com/codegangsta/cli"
	"os"
	"github.com/scalaxy/scalaxy/server"
	"log"
)

func main() {
	app := cli.NewApp()
	app.Name = "scalaxyd"
	app.Usage = "distributed column based newsql database"
	app.Action = actionRun
	app.Run(os.Args)
}

func actionRun(ctx *cli.Context) {
	srv := server.NewServer()
	log.Printf("run server")
	if err := srv.Run(); err != nil {
		log.Fatalf("run err: %s", err)
	}
}