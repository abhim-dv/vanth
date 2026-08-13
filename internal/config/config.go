// Package config resolves Vanth's canonical state root and runtime limits.
//
// Home resolution must match src/vanth/paths.py: VANTH_HOME is canonical,
// AGENT_BG_HOME is a supported alias, both must agree when both are set, and
// the result is an absolute, symlink-resolved path.
package config

import (
	"fmt"
	"os"
	"path/filepath"
)

// Version is stamped at build time via -ldflags. It mirrors the Python
// package version so daemon discovery metadata stays consistent.
var Version = "1.0.0"

// DefaultHome returns the state root used when no home is configured.
func DefaultHome() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home: %w", err)
	}
	return filepath.Join(home, ".vanth"), nil
}

// CanonicalHome resolves the one state root shared by daemon, client, runner,
// and monitor. It mirrors src/vanth/paths.canonical_home.
func CanonicalHome() (string, error) {
	vant := os.Getenv("VANTH_HOME")
	agent := os.Getenv("AGENT_BG_HOME")
	if vant != "" && agent != "" {
		vantPath, err := filepath.Abs(vant)
		if err != nil {
			return "", fmt.Errorf("resolve VANTH_HOME: %w", err)
		}
		agentPath, err := filepath.Abs(agent)
		if err != nil {
			return "", fmt.Errorf("resolve AGENT_BG_HOME: %w", err)
		}
		if vantPath != agentPath {
			return "", fmt.Errorf("VANTH_HOME and AGENT_BG_HOME refer to different state directories")
		}
		return filepath.Clean(vantPath), nil
	}
	configured := vant
	if configured == "" {
		configured = agent
	}
	if configured == "" {
		return DefaultHome()
	}
	abs, err := filepath.Abs(configured)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", homeVarName(configured, vant, agent), err)
	}
	return filepath.Clean(abs), nil
}

func homeVarName(configured, vant, agent string) string {
	if vant != "" {
		return "VANTH_HOME"
	}
	if agent != "" {
		return "AGENT_BG_HOME"
	}
	return "home"
}
