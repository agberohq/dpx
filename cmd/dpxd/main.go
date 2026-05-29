// Command dpxd is the DPX node daemon and operations CLI.
//
// v0.1 scope: backup subcommand only.
// Full HTTP API and membership management deferred to v0.2.
//
// dpxd is not required when DPX is used as an embedded library.
// Teller embeds DPX directly via dpx.Open().
//
// Usage:
//
//	dpxd backup --data /data/dpx --dest /backups/$(date +%Y%m%d)
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/agberohq/dpx"
	"github.com/agberohq/dpx/engine/pebble"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "backup":
		runBackup(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: dpxd <command> [flags]")
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  backup   --data <dir> --dest <dir>")
}

func runBackup(args []string) {
	fs := flag.NewFlagSet("backup", flag.ExitOnError)
	dataDir := fs.String("data", "", "Pebble data directory (required)")
	destDir := fs.String("dest", "", "backup destination directory (required)")
	fs.Parse(args)

	if *dataDir == "" || *destDir == "" {
		fs.Usage()
		os.Exit(1)
	}

	n, err := dpx.Open(dpx.Config{
		Engine: pebble.New(*dataDir),
	})
	if err != nil {
		log.Fatalf("open: %v", err)
	}
	defer n.Close()

	if err := n.Backup(context.Background(), *destDir); err != nil {
		log.Fatalf("backup: %v", err)
	}
	fmt.Printf("backup written to %s\n", *destDir)
}
