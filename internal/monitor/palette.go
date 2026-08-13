package monitor

import (
	"hash/fnv"
	"image/color"
	"sort"

	"charm.land/lipgloss/v2"
)

// PaletteSlot is one of the eight stable categorical color slots from plan
// section 13.6, with matching light/dark hex values and an ANSI family label.
type PaletteSlot struct {
	Slot  int
	Light string
	Dark  string
	ANSI  string
}

// palette is the fixed eight-slot job color table.
var palette = [...]PaletteSlot{
	{Slot: 0, Light: "#2a78d6", Dark: "#3987e5", ANSI: "blue"},
	{Slot: 1, Light: "#1baf7a", Dark: "#199e70", ANSI: "cyan"},
	{Slot: 2, Light: "#eda100", Dark: "#c98500", ANSI: "yellow"},
	{Slot: 3, Light: "#008300", Dark: "#008300", ANSI: "green"},
	{Slot: 4, Light: "#4a3aa7", Dark: "#9085e9", ANSI: "bright magenta/violet"},
	{Slot: 5, Light: "#e34948", Dark: "#e66767", ANSI: "red"},
	{Slot: 6, Light: "#e87ba4", Dark: "#d55181", ANSI: "magenta"},
	{Slot: 7, Light: "#eb6834", Dark: "#d95926", ANSI: "bright yellow/orange"},
}

// NumPaletteSlots is the number of available categorical slots.
const NumPaletteSlots = 8

// PaletteSlotByIndex returns the slot at index i (clamped).
func PaletteSlotByIndex(i int) PaletteSlot {
	if i < 0 {
		i = 0
	}
	if i >= len(palette) {
		i = len(palette) - 1
	}
	return palette[i]
}

// hashSlot returns a deterministic 0-7 slot for a job ID via FNV-1a.
func hashSlot(jobID string) int {
	h := fnv.New32a()
	h.Write([]byte(jobID))
	return int(h.Sum32() % NumPaletteSlots)
}

// AssignSlots assigns each job a stable slot. The assignment sorts a copy of
// the job IDs and linear-probes on collision, so results depend only on the
// visible set and never on list order. With more jobs than slots, the first
// (sorted) NumPaletteSlots jobs receive unique slots and the remainder fall
// back to their raw hash slot (they are folded away by the eight-overlay cap).
func AssignSlots(jobIDs []string) map[string]int {
	sorted := make([]string, len(jobIDs))
	copy(sorted, jobIDs)
	sort.Strings(sorted)
	result := make(map[string]int, len(sorted))
	used := [NumPaletteSlots]bool{}
	filled := 0
	for _, id := range sorted {
		slot := hashSlot(id)
		if filled < NumPaletteSlots {
			for used[slot] {
				slot = (slot + 1) % NumPaletteSlots
			}
			used[slot] = true
			filled++
		}
		result[id] = slot
	}
	return result
}

// slotColor returns the light or dark hex color for a slot.
func slotColor(slot int, dark bool) color.Color {
	s := PaletteSlotByIndex(slot)
	if dark {
		return lipgloss.Color(s.Dark)
	}
	return lipgloss.Color(s.Light)
}
