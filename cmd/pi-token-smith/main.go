//go:build linux

// Pi Token Smith captures observable model billing evidence from Pi.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Jcaltamar/pi-token-smith/internal/cli"
	"github.com/Jcaltamar/pi-token-smith/internal/daemon"
)

func main() {
	paths, err := daemon.DefaultRuntimePaths()
	if err != nil {
		fmt.Fprintln(os.Stderr, "resolve runtime paths:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := cli.Run(ctx, os.Args[1:], cli.DefaultDependencies(paths)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
