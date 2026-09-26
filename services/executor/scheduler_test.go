package main

import "testing"

// pcrSteps is the default workflow, as the scheduler sees it after
// fill_sample_plate has completed.
func pcrSteps() []Step {
	st := func(name, device string, deps ...string) Step {
		return Step{ID: name, Name: name, DeviceID: device, Status: StepPending, DependsOn: deps}
	}
	steps := []Step{
		st("combine", "liquid-handler-1", "incubate_samples", "warm_reagent_plate", "fill_buffer_plate"),
		st("fill_buffer_plate", "liquid-handler-1", "fill_sample_plate"),
		st("fill_reagent_plate", "liquid-handler-1", "fill_sample_plate"),
		st("fill_sample_plate", "liquid-handler-1"),
		st("incubate_samples", "incubator-1", "fill_sample_plate"),
		st("read_plate", "plate-reader-1", "combine"),
		st("warm_reagent_plate", "incubator-1", "fill_reagent_plate"),
	}
	steps[3].Status = StepCompleted
	return steps
}

func statusOf(steps []Step) map[string]string {
	m := map[string]string{}
	for _, s := range steps {
		m[s.Name] = s.Status
	}
	return m
}

func TestChainLengths(t *testing.T) {
	got := chainLengths(pcrSteps())
	want := map[string]int{
		"fill_sample_plate":  5,
		"fill_reagent_plate": 4,
		"warm_reagent_plate": 3,
		"incubate_samples":   3,
		"fill_buffer_plate":  3,
		"combine":            2,
		"read_plate":         1,
	}
	for name, n := range want {
		if got[name] != n {
			t.Errorf("chain length of %s = %d, want %d", name, got[name], n)
		}
	}
}

// When the reagent and buffer fills both become ready, the reagent fill must
// win the liquid handler: it has the warm-up still to come behind it.
func TestReadyByPriorityPrefersLongerChain(t *testing.T) {
	steps := pcrSteps()
	ready := readyByPriority(steps, statusOf(steps))

	var names []string
	for _, s := range ready {
		names = append(names, s.Name)
	}
	if len(names) != 3 {
		t.Fatalf("ready = %v, want the two fills and incubate_samples", names)
	}
	if names[0] != "fill_reagent_plate" {
		t.Errorf("ready = %v, want fill_reagent_plate first", names)
	}
}

func TestReadyWaitsForAllDependencies(t *testing.T) {
	steps := pcrSteps()
	for i := range steps {
		switch steps[i].Name {
		case "incubate_samples", "warm_reagent_plate", "fill_reagent_plate":
			steps[i].Status = StepCompleted
		}
	}
	// fill_buffer_plate is still pending, so combine must not be ready.
	for _, s := range readyByPriority(steps, statusOf(steps)) {
		if s.Name == "combine" {
			t.Fatalf("combine is ready while fill_buffer_plate is pending")
		}
	}
}
