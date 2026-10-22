package sessions

import (
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// One file had by two agents is in allFiles twice, and the read published
// both jobs: the second replaced the first's channel in parsing, so
// publishing either of them closed what the map said by then — a close of
// nil, which took the app down on every start (#2932, #3255). One job per path
// keeps every channel published exactly once.
func TestRepeatedPathInOneRefresh(t *testing.T) {
	inZone(t, 0)
	claude, _ := setup(t)
	moreSessions(t, claude, 8)
	if n := len(List(0)); n < 8 {
		t.Fatalf("listed %d sessions", n)
	}
	growSessions(t, claude, 8)

	mu.Lock()
	files := allFiles()
	mu.Unlock()
	var doubled []file
	for _, f := range files {
		doubled = append(doubled, f, f) // every file twice, as allFiles may return one
	}
	if len(doubled) == 0 {
		t.Skip("no session files to read twice")
	}

	// The panic that failed this: close of nil in publish. It takes the
	// process down, so the read is run here and the test survives the fix.
	done := make(chan struct{})
	go func() {
		defer close(done)
		mu.Lock()
		defer mu.Unlock()
		loadCache()
		defer useDBs()()
		refresh(doubled, doubled)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("a refresh with every path twice never finished")
	}

	// Nothing left behind: a channel still open in parsing would have a
	// waitParsing of the next read blocked on a parse never published.
	mu.Lock()
	left := len(parsing)
	mu.Unlock()
	if left != 0 {
		t.Fatalf("%d files left marked as being read", left)
	}
}

// A refresh that took the channel of an earlier one already publishing is
// not that one's to close: each job closes the channel it made, so a second
// read of the same files under the start's warm-up cannot end on a close of
// nil or of a channel already closed.
func TestConcurrentRefreshesPublishTheirOwn(t *testing.T) {
	inZone(t, 0)
	claude, _ := setup(t)
	moreSessions(t, claude, 12)
	if n := len(List(0)); n < 12 {
		t.Fatalf("listed %d sessions", n)
	}
	growSessions(t, claude, 12)

	hold, started := make(chan struct{}), make(chan struct{})
	var once sync.Once
	parseHook = func() {
		once.Do(func() { close(started) })
		<-hold
	}
	release := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(func() { release(); parseHook = nil })

	warm := make(chan struct{})
	go func() { defer close(warm); statsAt(0, statsNow) }()
	<-started

	// A second read of the same changed files while the first is under way:
	// the warm-up reads statsFor(0) and statsFor(30) one after another, and
	// a page asks while those are still going.
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			statsAt(30, statsNow)
		}()
	}
	second := make(chan struct{})
	go func() { defer close(second); wg.Wait() }()

	time.Sleep(200 * time.Millisecond)
	release()
	select {
	case <-warm:
	case <-time.After(60 * time.Second):
		t.Fatal("the first read never finished")
	}
	select {
	case <-second:
	case <-time.After(60 * time.Second):
		t.Fatal("a read alongside another never finished")
	}

	mu.Lock()
	left := len(parsing)
	paths := make([]string, 0, left)
	for p := range parsing {
		paths = append(paths, filepath.Base(p))
	}
	mu.Unlock()
	if left != 0 {
		t.Fatalf("%d files left marked as being read: %v", left, paths)
	}
}
