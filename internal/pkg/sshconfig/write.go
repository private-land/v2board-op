package sshconfig

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

var hostNameLineRE = regexp.MustCompile(`^(\s*)([Hh][Oo][Ss][Tt][Nn][Aa][Mm][Ee])(\s+)(\S+)(.*)$`)

// ApplyAndWrite rewrites the SSH config at path with the given updates.
//
// Behavior:
//   - If updates is empty, the file is left untouched and "" is returned.
//   - Otherwise: a timestamped backup is created next to the original, then
//     the new content is written atomically (temp file + rename), preserving
//     the original file mode.
//
// Returns the backup file path on success.
func (c *Config) ApplyAndWrite(path string, updates []Update) (string, error) {
	if len(updates) == 0 {
		return "", nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("stat %s: %w", path, err)
	}

	newLines := make([]string, len(c.Lines))
	copy(newLines, c.Lines)
	for _, u := range updates {
		if u.Line < 0 || u.Line >= len(newLines) {
			return "", fmt.Errorf("update for %q has invalid line %d", u.Alias, u.Line)
		}
		rewritten, ok := rewriteHostNameLine(newLines[u.Line], u.NewIP)
		if !ok {
			return "", fmt.Errorf("line %d for %q is not a HostName line: %q",
				u.Line, u.Alias, newLines[u.Line])
		}
		newLines[u.Line] = rewritten
	}

	backup := fmt.Sprintf("%s.bak.%s", path, time.Now().Format("20060102-150405"))
	if err := copyFile(path, backup, info.Mode().Perm()); err != nil {
		return "", fmt.Errorf("create backup: %w", err)
	}

	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		_ = os.Remove(backup)
		return "", fmt.Errorf("open temp: %w", err)
	}
	if _, err := io.WriteString(out, strings.Join(newLines, "\n")); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		_ = os.Remove(backup)
		return "", fmt.Errorf("write temp: %w", err)
	}
	// Append a trailing newline when the file is non-empty.
	// SSH config files conventionally end in '\n'; the bufio.Scanner used by
	// Parse drops the final newline, so we re-add one here. This may add a
	// trailing '\n' to a file that originally lacked one — acceptable since
	// SSH config does not assign meaning to trailing whitespace.
	if len(c.Lines) > 0 {
		if _, err := io.WriteString(out, "\n"); err != nil {
			_ = out.Close()
			_ = os.Remove(tmp)
			_ = os.Remove(backup)
			return "", err
		}
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		_ = os.Remove(backup)
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("rename: %w", err)
	}
	return backup, nil
}

func rewriteHostNameLine(line, newIP string) (string, bool) {
	m := hostNameLineRE.FindStringSubmatch(line)
	if m == nil {
		return "", false
	}
	return m[1] + m[2] + m[3] + newIP + m[5], true
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
