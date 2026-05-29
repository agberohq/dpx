package tests

import (
	"context"
	"os"
	"testing"

	"github.com/agberohq/dpx"
	"github.com/agberohq/dpx/engine/memory"
	dpxraft "github.com/agberohq/dpx/raft"
)

func TestBackup(t *testing.T) {
	eng := memory.New()
	cfg := dpx.Config{
		Engine: eng,
	}

	node, err := dpx.Open(cfg, dpxraft.Open)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer node.Close()

	ctx := context.Background()
	err = node.RunInTx(ctx, func(tx dpx.KVTx) error {
		return tx.Set(ctx, []byte("key"), []byte("val"))
	})
	if err != nil {
		t.Fatalf("Set: %v", err)
	}

	dir := t.TempDir()
	if err := node.Backup(ctx, dir); err != nil {
		t.Fatalf("Backup: %v", err)
	}

	// Verify backup created
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("backup dir stat: %v", err)
	}
}

func TestBackupOnClosedNode(t *testing.T) {
	eng := memory.New()
	cfg := dpx.Config{
		Engine: eng,
	}

	node, err := dpx.Open(cfg, dpxraft.Open)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	node.Close()

	err = node.Backup(context.Background(), t.TempDir())
	if err != dpx.ErrStoreClosed {
		t.Errorf("got %v, want ErrStoreClosed", err)
	}
}
