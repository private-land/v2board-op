package sshconfig

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// HostBlock represents one top-level "Host <alias>" block in a config file.
//
// HostNameLine is -1 when the block contains no HostName directive (such
// blocks are intentionally never updated by Diff/Apply).
type HostBlock struct {
	Alias        string
	HostNameLine int
	HostNameIP   string
	Skip         bool
	SkipReason   string
}

// Config holds the original file split into lines plus parsed Host blocks.
// Lines are kept verbatim so rewrites can replace single lines without
// reformatting the rest of the file.
type Config struct {
	Lines  []string
	Blocks []*HostBlock
}

// Parse reads an SSH config file from disk and returns a Config.
func Parse(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	cfg := &Config{Lines: lines}
	cfg.Blocks = parseBlocks(lines)
	return cfg, nil
}

func parseBlocks(lines []string) []*HostBlock {
	var blocks []*HostBlock
	var current *HostBlock

	for i, raw := range lines {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		// Normalize keyword=value → keyword value (per ssh_config(5)).
		trimmed = strings.Replace(trimmed, "=", " ", 1)
		fields := strings.Fields(trimmed)
		if len(fields) < 2 {
			continue
		}
		keyword := strings.ToLower(fields[0])

		switch keyword {
		case "host":
			if current != nil {
				blocks = append(blocks, current)
			}
			current = newHostBlock(fields[1:])
		case "match":
			// Match block ends the current Host block. We do not produce
			// a HostBlock for Match (per design: skipped entirely).
			if current != nil {
				blocks = append(blocks, current)
				current = nil
			}
		case "hostname":
			// Only the first HostName in a block wins (matches OpenSSH).
			if current != nil && !current.Skip && current.HostNameLine == -1 {
				current.HostNameLine = i
				current.HostNameIP = fields[1]
			}
		}
	}
	if current != nil {
		blocks = append(blocks, current)
	}
	return blocks
}

func newHostBlock(aliases []string) *HostBlock {
	b := &HostBlock{HostNameLine: -1}
	if len(aliases) > 1 {
		b.Skip = true
		b.SkipReason = "multiple aliases on one Host line"
		return b
	}
	alias := aliases[0]
	if strings.ContainsAny(alias, "*?") {
		b.Skip = true
		b.SkipReason = "wildcard alias"
		return b
	}
	b.Alias = alias
	return b
}
