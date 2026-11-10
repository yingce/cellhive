package compaction

import "time"

// Celld's non-snapshot compaction schedule. Level 9 is the complete snapshot
// level and is intentionally excluded from this table.
type LevelSchedule struct {
	Level    int
	Interval time.Duration
}

var CelldLevels = [...]LevelSchedule{
	{Level: 0, Interval: 0},
	{Level: 1, Interval: 30 * time.Second},
	{Level: 2, Interval: 5 * time.Minute},
	{Level: 3, Interval: time.Hour},
}

const CelldSnapshotLevel = 9

func CelldSchedule(level int) (LevelSchedule, bool) {
	if level < 0 || level >= len(CelldLevels) {
		return LevelSchedule{}, false
	}
	return CelldLevels[level], true
}

// NextAt returns the next UTC interval boundary, matching celld's
// CompactionLevel::next_compaction_at rather than time since process startup.
func (s LevelSchedule) NextAt(now time.Time) time.Time {
	if s.Interval <= 0 {
		return now
	}
	return now.UTC().Truncate(s.Interval).Add(s.Interval)
}
