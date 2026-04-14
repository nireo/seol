package seol

import (
	"testing"
)

func TestLeveledCompactionPlannerSelectsOverlappingL0AndL1Ranges(t *testing.T) {
	dir := t.TempDir()
	manifest, err := openManifest(dir)
	if err != nil {
		t.Fatalf("openManifest: %v", err)
	}
	tables := []TableMeta{
		{Filename: "400.sst", Level: 0, Smallest: []byte("a"), Largest: []byte("c"), CreatedAt: 400},
		{Filename: "300.sst", Level: 0, Smallest: []byte("m"), Largest: []byte("p"), CreatedAt: 300},
		{Filename: "200.sst", Level: 0, Smallest: []byte("x"), Largest: []byte("z"), CreatedAt: 200},
		{Filename: "100.sst", Level: 1, Smallest: []byte("a"), Largest: []byte("f"), CreatedAt: 100},
		{Filename: "101.sst", Level: 1, Smallest: []byte("m"), Largest: []byte("s"), CreatedAt: 101},
		{Filename: "102.sst", Level: 1, Smallest: []byte("x"), Largest: []byte("zz"), CreatedAt: 102},
	}
	if err := manifest.ReplaceTables(tables); err != nil {
		t.Fatalf("ReplaceTables: %v", err)
	}

	planner := leveledCompactionPlanner{}
	plan, err := planner.Plan(dir, Options{})
	if err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if plan == nil {
		t.Fatalf("Plan returned nil")
	}

	if got, want := len(plan.sourceTables), 1; got != want {
		t.Fatalf("source table count: got %d, want %d", got, want)
	}
	if got, want := plan.sourceTables[0].Filename, "200.sst"; got != want {
		t.Fatalf("selected source table: got %q, want %q", got, want)
	}
	if got, want := len(plan.targetTables), 1; got != want {
		t.Fatalf("target table count: got %d, want %d", got, want)
	}
	if got, want := plan.targetTables[0].Filename, "102.sst"; got != want {
		t.Fatalf("selected target table: got %q, want %q", got, want)
	}
	if plan.sourceLevel != 0 || plan.targetLevel != 1 {
		t.Fatalf("plan levels: got %d->%d, want 0->1", plan.sourceLevel, plan.targetLevel)
	}
}
