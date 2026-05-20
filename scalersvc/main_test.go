package main

import "testing"

func TestDesiredReplicasScalesUpOnHighUtilization(t *testing.T) {
	s := &scaler{
		pol: policy{
			TargetConcurrency: 1,
			ScaleUpUtilPct:    80,
			ScaleUpP99Ms:      500,
			ScaleUpErrPct:     10,
			ScaleDownUtilPct:  20,
		},
		maxReplicas: 5,
	}

	got := s.desiredReplicas(1, 1, 0, 0, 0)
	if got != 2 {
		t.Fatalf("desiredReplicas = %d, want 2", got)
	}
}

func TestDesiredReplicasClampsAtMaxReplicas(t *testing.T) {
	s := &scaler{
		pol: policy{
			TargetConcurrency: 1,
			ScaleUpUtilPct:    80,
			ScaleUpP99Ms:      500,
			ScaleUpErrPct:     10,
			ScaleDownUtilPct:  20,
		},
		maxReplicas: 2,
	}

	got := s.desiredReplicas(2, 2, 3, 800, 0)
	if got != 2 {
		t.Fatalf("desiredReplicas = %d, want 2", got)
	}
}

func TestDesiredReplicasScalesUpOnP99(t *testing.T) {
	s := &scaler{
		pol:         policy{TargetConcurrency: 1, ScaleUpUtilPct: 80, ScaleUpP99Ms: 500, ScaleUpErrPct: 10, ScaleDownUtilPct: 20},
		maxReplicas: 5,
	}
	got := s.desiredReplicas(1, 0, 0, 800, 0)
	if got != 2 {
		t.Fatalf("desiredReplicas = %d, want 2", got)
	}
}

func TestDesiredReplicasDoesNotScaleDownWhenUtilizationIsHigh(t *testing.T) {
	s := &scaler{
		pol:         policy{TargetConcurrency: 1, ScaleUpUtilPct: 95, ScaleUpP99Ms: 500, ScaleUpErrPct: 10, ScaleDownUtilPct: 20},
		maxReplicas: 5,
	}
	got := s.desiredReplicas(2, 1, 0, 0, 0)
	if got != 2 {
		t.Fatalf("desiredReplicas = %d, want 2", got)
	}
}
