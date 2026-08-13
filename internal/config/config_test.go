package config

import (
	"os"
	"path/filepath"
	"testing"
)

func setHomeEnv(t *testing.T, vant, agent string) {
	t.Helper()
	for _, name := range []string{"VANTH_HOME", "AGENT_BG_HOME"} {
		t.Setenv(name, "")
	}
	if vant != "" {
		t.Setenv("VANTH_HOME", vant)
	}
	if agent != "" {
		t.Setenv("AGENT_BG_HOME", agent)
	}
}

func TestCanonicalHomeVanthTakesPrecedence(t *testing.T) {
	setHomeEnv(t, `C:\state\vanth`, "")
	got, err := CanonicalHome()
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Clean(`C:\state\vanth`) {
		t.Fatalf("got %q", got)
	}
}

func TestCanonicalHomeAgentAlias(t *testing.T) {
	setHomeEnv(t, "", `C:\state\agent`)
	got, err := CanonicalHome()
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Clean(`C:\state\agent`) {
		t.Fatalf("got %q", got)
	}
}

func TestCanonicalHomeConflictingAliasesRejected(t *testing.T) {
	setHomeEnv(t, `C:\a`, `C:\b`)
	if _, err := CanonicalHome(); err == nil {
		t.Fatal("expected conflict error")
	}
}

func TestCanonicalHomeMatchingAliasesOK(t *testing.T) {
	setHomeEnv(t, `C:\same`, `C:\same`)
	got, err := CanonicalHome()
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Clean(`C:\same`) {
		t.Fatalf("got %q", got)
	}
}

func TestCanonicalHomeDefaultsToUserVanth(t *testing.T) {
	setHomeEnv(t, "", "")
	if os.Getenv("VANTH_HOME") != "" || os.Getenv("AGENT_BG_HOME") != "" {
		t.Fatal("test environment must clear both home vars")
	}
	got, err := CanonicalHome()
	if err != nil {
		t.Fatal(err)
	}
	want, _ := DefaultHome()
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}
