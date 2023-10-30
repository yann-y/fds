package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
	"github.com/yann-y/fds/internal/iam/auth"
	"github.com/yann-y/fds/internal/utils"
)

var startCmd = &cli.Command{
	Name:  "daemon",
	Usage: "Start a file dag storage process",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Usage: "set server listen",
			Value: ":9000",
		},
		&cli.StringFlag{
			Name:    "data-dir",
			Aliases: []string{"data"},
			Usage:   "directory to store data in",
			Value:   "./store-data",
		},
		&cli.StringFlag{
			Name:        "pool-addr",
			DefaultText: "/ip4/127.0.0.1/tcp/5001",
			Usage:       "set the ipfs http address you want connect",
			Value:       "/ip4/127.0.0.1/tcp/5001",
		},
		&cli.StringFlag{
			Name:    "root-user",
			Usage:   "set root file dag root user",
			EnvVars: []string{EnvRootUser},
			Value:   auth.DefaultAccessKey,
		},
		&cli.StringFlag{
			Name:    "root-password",
			Usage:   "set root file dag root password",
			EnvVars: []string{EnvRootPassword},
			Value:   auth.DefaultSecretKey,
		},
	},
	Action: func(cctx *cli.Context) error {
		startServer(cctx)
		return nil
	},
}

func main() {
	utils.SetupLogLevels()
	local := []*cli.Command{
		startCmd,
	}
	app := &cli.App{
		Name:                 "fds",
		Usage:                "fds",
		Version:              "1.0.1",
		EnableBashCompletion: true,
		Commands:             local,
	}
	app.Setup()
	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error: ", err)
	}
}
