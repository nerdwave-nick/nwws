//go:build race

package nwws

// raceEnabled reports a build with the race detector, under which
// sync.Pool drops some of the buffers put into it on purpose.
const raceEnabled = true
