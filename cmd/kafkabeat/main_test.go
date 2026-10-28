package main

import (
	"runtime/debug"
	"testing"
)

func TestModuleVersion(t *testing.T) {
	build := func(v string) *debug.BuildInfo {
		return &debug.BuildInfo{Main: debug.Module{Path: "github.com/gnocent/kafkabeat", Version: v}}
	}
	for _, c := range []struct {
		injected string
		info     *debug.BuildInfo
		ok       bool
		want     string
	}{
		{"v0.1.0", build("v0.2.0"), true, "v0.1.0"}, // -ldflags l'emporte
		{"dev", build("v0.1.1"), true, "v0.1.1"},    // go install
		{"dev", build("(devel)"), true, "dev"},
		{"dev", build(""), true, "dev"},
		{"dev", nil, false, "dev"},
	} {
		if got := moduleVersion(c.injected, c.info, c.ok); got != c.want {
			t.Errorf("moduleVersion(%q, %v) = %q, want %q", c.injected, c.info, got, c.want)
		}
	}
}
