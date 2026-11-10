//go:build race

package tiercache_test

// raceEnabled: the race detector makes sync.Pool drop items on purpose, so
// allocation budgets cannot be measured under it.
const raceEnabled = true
