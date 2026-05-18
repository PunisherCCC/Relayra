package proxy

import (
	"testing"
	"time"
)

func TestScoreReliabilitySample(t *testing.T) {
	score := scoreReliabilitySample(WebSocketSampleResult{
		HandshakeOK:   true,
		AliveDuration: 15 * time.Second,
		ProbesSent:    3,
		ProbesAcked:   2,
	}, 30*time.Second)

	if score != 33 {
		t.Fatalf("scoreReliabilitySample() = %d, want 33", score)
	}
}

func TestGradeReliability(t *testing.T) {
	tests := []struct {
		score int
		want  string
	}{
		{95, "excellent"},
		{75, "usable"},
		{50, "fragile"},
		{20, "poor"},
	}

	for _, tt := range tests {
		if got := gradeReliability(tt.score); got != tt.want {
			t.Fatalf("gradeReliability(%d) = %q, want %q", tt.score, got, tt.want)
		}
	}
}
