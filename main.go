package main

import (
	"os"

	"github.com/alecthomas/kong"
	"github.com/buildkite/buildkite-custom-scheduler/internal/commands"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var cli struct {
	Server commands.ServerCmd `cmd:"" help:"Start the API server"`
	Worker commands.WorkerCmd `cmd:"" help:"Start a worker"`
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	ctx := kong.Parse(&cli,
		kong.Name("buildkite-custom-scheduler"),
		kong.Description("A custom Buildkite scheduler using the Stacks API"),
		kong.UsageOnError(),
	)

	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}
