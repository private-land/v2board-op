package sshconfig

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestApplyAndWrite_ReplacesHostNameLineOnly(t *testing.T) {
	original := `# top comment
Host alpha
    HostName 1.1.1.1
    User ubuntu

Host beta
    HostName 2.2.2.2
    Port 22
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	cfg, err := Parse(path)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	updates := []Update{
		{Alias: "alpha", OldIP: "1.1.1.1", NewIP: "9.9.9.9", Line: cfg.Blocks[0].HostNameLine},
	}
	backup, err := cfg.ApplyAndWrite(path, updates)
	if err != nil {
		t.Fatalf("ApplyAndWrite: %v", err)
	}

	got, _ := os.ReadFile(path)
	want := `# top comment
Host alpha
    HostName 9.9.9.9
    User ubuntu

Host beta
    HostName 2.2.2.2
    Port 22
`
	if string(got) != want {
		t.Errorf("file content mismatch.\nGOT:\n%s\nWANT:\n%s", got, want)
	}

	if !strings.HasPrefix(filepath.Base(backup), "config.bak.") {
		t.Errorf("backup name=%q want config.bak.<timestamp>", filepath.Base(backup))
	}
	if _, err := os.Stat(backup); err != nil {
		t.Errorf("backup file missing: %v", err)
	}
	bk, _ := os.ReadFile(backup)
	if string(bk) != original {
		t.Errorf("backup should equal original")
	}
}

func TestApplyAndWrite_PreservesIndent(t *testing.T) {
	original := "Host alpha\n\tHostName 1.1.1.1\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, _ := Parse(path)
	updates := []Update{{Alias: "alpha", NewIP: "9.9.9.9", Line: cfg.Blocks[0].HostNameLine}}
	if _, err := cfg.ApplyAndWrite(path, updates); err != nil {
		t.Fatalf("ApplyAndWrite: %v", err)
	}
	got, _ := os.ReadFile(path)
	want := "Host alpha\n\tHostName 9.9.9.9\n"
	if string(got) != want {
		t.Errorf("indent not preserved.\nGOT:%q\nWANT:%q", got, want)
	}
}

func TestApplyAndWrite_PreservesMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte("Host a\n    HostName 1.1.1.1\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, _ := Parse(path)
	updates := []Update{{Alias: "a", NewIP: "9.9.9.9", Line: cfg.Blocks[0].HostNameLine}}
	if _, err := cfg.ApplyAndWrite(path, updates); err != nil {
		t.Fatal(err)
	}

	info, _ := os.Stat(path)
	if info.Mode().Perm() != fs.FileMode(0o600) {
		t.Errorf("mode=%v want 0600", info.Mode().Perm())
	}
}

func TestApplyAndWrite_NoUpdatesIsNoOp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	original := "Host a\n    HostName 1.1.1.1\n"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, _ := Parse(path)
	backup, err := cfg.ApplyAndWrite(path, nil)
	if err != nil {
		t.Fatalf("ApplyAndWrite nil updates: %v", err)
	}
	if backup != "" {
		t.Errorf("no updates should produce no backup, got %q", backup)
	}
	got, _ := os.ReadFile(path)
	if string(got) != original {
		t.Errorf("file should be untouched")
	}
}
