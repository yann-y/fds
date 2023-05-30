package main

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"github.com/yann-y/fds/internal/iam/auth"
	"github.com/yann-y/fds/internal/utils"
	"os"
)

var startCmd = &cli.Command{
	Name:  "daemon",
	Usage: "Start a filedag storage process",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Usage: "set server listen",
			Value: ":9985",
		},
		&cli.StringFlag{
			Name:  "datadir",
			Usage: "directory to store data in",
			Value: "./store-data",
		},
		&cli.StringFlag{
			Name:  "pool-addr",
			Usage: "set the pool rpc address you want connect",
		},
		&cli.StringFlag{
			Name:    "root-user",
			Usage:   "set root filedag root user",
			EnvVars: []string{EnvRootUser},
			Value:   auth.DefaultAccessKey,
		},
		&cli.StringFlag{
			Name:    "root-password",
			Usage:   "set root filedag root password",
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
		Name:                 "filedag-storage",
		Usage:                "filedag-storage",
		Version:              "0.0.11",
		EnableBashCompletion: true,
		Commands:             local,
	}
	app.Setup()
	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error: ", err)
	}
}
