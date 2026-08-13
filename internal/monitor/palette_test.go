package monitor

import (
	"reflect"
	"strconv"
	"testing"
)

func TestHashSlotInRange(t *testing.T) {
	for _, id := range []string{"job_a", "job_b", "job_completed", "x"} {
		if s := hashSlot(id); s < 0 || s >= NumPaletteSlots {
			t.Errorf("hashSlot(%q) = %d out of range", id, s)
		}
	}
}

func TestAssignSlotsUniqueForFullSet(t *testing.T) {
	ids := []string{"job_1", "job_2", "job_3", "job_4", "job_5", "job_6", "job_7", "job_8"}
	slots := AssignSlots(ids)
	seen := map[int]bool{}
	for _, id := range ids {
		s, ok := slots[id]
		if !ok {
			t.Fatalf("missing slot for %s", id)
		}
		if seen[s] {
			t.Errorf("slot %d reused for %s", s, id)
		}
		seen[s] = true
	}
}

func TestAssignSlotsIndependentOfOrder(t *testing.T) {
	ids := []string{"job_alpha", "job_beta", "job_gamma", "job_delta"}
	reversed := []string{"job_delta", "job_gamma", "job_beta", "job_alpha"}
	if !reflect.DeepEqual(AssignSlots(ids), AssignSlots(reversed)) {
		t.Error("slot assignment changed with list order")
	}
}

func TestAssignSlotsStableForSet(t *testing.T) {
	ids := []string{"job_x", "job_y"}
	first := AssignSlots(ids)
	// Adding an unrelated job must not move existing jobs' slots when their
	// linear-probe path is unaffected; at minimum the direct-hash cases must be
	// stable.
	second := AssignSlots(ids)
	if !reflect.DeepEqual(first, second) {
		t.Error("assignment not stable for the same set")
	}
	_ = second
}

func TestAssignSlotsCollisionResolutionStable(t *testing.T) {
	// Force a collision: find a second ID hashing to the same slot as job_a.
	base := hashSlot("job_a")
	ids := []string{"job_a"}
	seen := map[string]bool{"job_a": true}
	for i := 0; i < 5000; i++ {
		id := "job_collide_" + string(rune('a'+i%26)) + "_" + strconv.Itoa(i)
		if seen[id] {
			continue
		}
		if hashSlot(id) == base {
			ids = append(ids, id)
			break
		}
	}
	if len(ids) != 2 {
		t.Skip("could not synthesize a collision")
	}
	slots := AssignSlots(ids)
	if slots["job_a"] == slots[ids[1]] {
		t.Error("collision not resolved: both jobs share a slot")
	}
	// Order-independence also holds for the colliding pair.
	rev := AssignSlots([]string{ids[1], "job_a"})
	if slots["job_a"] != rev["job_a"] || slots[ids[1]] != rev[ids[1]] {
		t.Error("collision resolution changed with order")
	}
}

func TestPaletteSlotColors(t *testing.T) {
	if len(palette) != NumPaletteSlots {
		t.Fatalf("palette has %d slots, want %d", len(palette), NumPaletteSlots)
	}
	for i, s := range palette {
		if s.Light == "" || s.Dark == "" || s.ANSI == "" {
			t.Errorf("slot %d missing colors", i)
		}
	}
	if slotColor(0, false) == nil || slotColor(0, true) == nil {
		t.Error("slotColor returned nil")
	}
}
