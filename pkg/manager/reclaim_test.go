package manager

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestCollectLinkedFolders(t *testing.T) {
	mount := t.TempDir()
	lib := t.TempDir()

	for _, p := range []string{
		"__all__/Show.S01.1080p/Show.S01E01.mkv",
		"__all__/Movie.2020.1080p/Movie.2020.1080p.mkv",
	} {
		full := filepath.Join(mount, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mustLink := func(target, link string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
	}
	mustLink(filepath.Join(mount, "__all__/Show.S01.1080p/Show.S01E01.mkv"), filepath.Join(lib, "Show/Season 01/Show - S01E01.mkv"))
	mustLink(filepath.Join(mount, "__all__/Movie.2020.1080p/Movie.2020.1080p.mkv"), filepath.Join(lib, "Movie (2020)/Movie (2020).mkv"))
	// A broken link still protects its folder: the entry may be mid-repair.
	mustLink(filepath.Join(mount, "__all__/Gone.Release/gone.mkv"), filepath.Join(lib, "Gone/gone.mkv"))
	if err := os.WriteFile(filepath.Join(lib, "Movie (2020)/poster.jpg"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	linked, n, links, err := collectLinkedFolders(context.Background(), []string{lib}, mount)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("symlinks = %d, want 3", n)
	}
	for _, want := range []string{"Show.S01.1080p", "Movie.2020.1080p", "Gone.Release"} {
		if _, ok := linked[want]; !ok {
			t.Errorf("folder %q not collected", want)
		}
	}
	if len(linked) != 3 {
		t.Errorf("linked = %v, want 3 folders", linked)
	}
	if len(links) != 3 {
		t.Errorf("links into the mount = %d, want 3", len(links))
	}
}

func TestCollectLinkedFoldersFailsClosed(t *testing.T) {
	ctx := context.Background()
	if _, _, _, err := collectLinkedFolders(ctx, nil, ""); err == nil {
		t.Error("no paths: want error")
	}
	if _, _, _, err := collectLinkedFolders(ctx, []string{filepath.Join(t.TempDir(), "missing")}, ""); err == nil {
		t.Error("missing path: want error")
	}
	// An empty or unmounted library must never make everything look unreferenced.
	if _, _, _, err := collectLinkedFolders(ctx, []string{t.TempDir()}, ""); err == nil {
		t.Error("path without symlinks: want error")
	}
}

func TestClassifyReclaim(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	config.Reset()
	t.Cleanup(config.Reset)

	old := time.Now().Add(-72 * time.Hour)
	cutoff := time.Now().Add(-24 * time.Hour)
	recent := time.Now().Add(-time.Hour)

	base := func() *storage.Entry {
		return &storage.Entry{
			InfoHash:   "abc",
			Name:       "Movie.2020.1080p",
			Category:   "radarr",
			IsComplete: true,
			AddedOn:    old,
			CreatedAt:  old,
		}
	}
	folderOf := func(e *storage.Entry) string { return e.GetFolder() }

	tests := []struct {
		name   string
		mutate func(e *storage.Entry)
		linked bool
		want   reclaimVerdict
	}{
		{"unreferenced and old", nil, false, reclaimRemove},
		{"referenced", nil, true, reclaimKeepReferenced},
		{"incomplete", func(e *storage.Entry) { e.IsComplete = false }, false, reclaimKeepNotReady},
		{"downloading", func(e *storage.Entry) { e.IsDownloading = true }, false, reclaimKeepNotReady},
		{"bad", func(e *storage.Entry) { e.Bad = true }, false, reclaimKeepNotReady},
		{"recently completed", func(e *storage.Entry) { e.CompletedAt = &recent }, false, reclaimKeepYoung},
		{"recently imported", func(e *storage.Entry) { e.ImportedAt = &recent }, false, reclaimKeepYoung},
		{"strm import", func(e *storage.Entry) { e.Action = config.DownloadActionStrm }, false, reclaimKeepAction},
		{"download import", func(e *storage.Entry) { e.Action = config.DownloadActionDownload }, false, reclaimKeepAction},
		{"symlink import", func(e *storage.Entry) { e.Action = config.DownloadActionSymlink }, false, reclaimRemove},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := base()
			if tt.mutate != nil {
				tt.mutate(e)
			}
			linked := map[string]struct{}{}
			if tt.linked {
				linked[folderOf(e)] = struct{}{}
			}
			_, got := classifyReclaim(e, linked, cutoff)
			if got != tt.want {
				t.Errorf("verdict = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestDeadLinkSetAdd(t *testing.T) {
	base := t.TempDir()
	mount := filepath.Join(base, "mount")
	entry := func(root, name string) string { return filepath.Join(root, "__all__", name) }

	d := &deadLinkSet{
		known:     map[string]struct{}{"live": {}},
		mountRoot: mount,
		byArr:     map[string]*deadArrFiles{},
	}
	a := &arr.Arr{Name: "arr"}
	f := []arr.ContentFile{{Path: filepath.Join(base, "library", "ep.mkv"), TargetPath: "ep.mkv"}}

	d.add(a, entry(mount, "live"), "live", f)                              // entry exists
	d.add(a, entry(filepath.Join(base, "elsewhere"), "gone"), "gone", f)   // outside the mount
	d.add(a, entry(filepath.Join(base, "mount-other"), "gone"), "gone", f) // prefix lookalike
	if len(d.byArr) != 0 {
		t.Fatalf("recorded %v, want nothing", d.byArr)
	}

	d.add(a, entry(mount, "gone"), "gone", f)
	g := d.byArr["arr"]
	if g == nil || len(g.files) != 1 || g.links[0].Target != filepath.Join(entry(mount, "gone"), "ep.mkv") {
		t.Fatalf("dead link not recorded correctly: %+v", g)
	}

	empty := &deadLinkSet{known: map[string]struct{}{}, mountRoot: mount, byArr: map[string]*deadArrFiles{}}
	empty.add(a, entry(mount, "gone"), "gone", f)
	if len(empty.byArr) != 0 {
		t.Fatal("an empty store must never yield dead links")
	}
	var nilSet *deadLinkSet
	nilSet.add(a, entry(mount, "gone"), "gone", f) // must not panic
}

func TestOrphanCandidates(t *testing.T) {
	links := []libLink{{path: "a", folder: "live"}, {path: "b", folder: "gone"}}
	got := orphanCandidates(links, map[string]struct{}{"live": {}})
	if len(got) != 1 || got[0].path != "b" {
		t.Fatalf("got %+v, want only the link to the missing entry", got)
	}
	if got := orphanCandidates(links, map[string]struct{}{}); got != nil {
		t.Fatal("an empty store must never yield orphans")
	}
}

func TestMediaOwner(t *testing.T) {
	base := t.TempDir()
	show := filepath.Join(base, "tv", "Show (2020)")
	movie := filepath.Join(base, "movies", "Movie (2020)")
	folders := map[string]int{show: 7, movie: 9}

	if id, ok := mediaOwner(filepath.Join(show, "Season 02", "Show - S02E03.mkv"), folders); !ok || id != 7 {
		t.Errorf("episode owner = %d, %v; want 7", id, ok)
	}
	if id, ok := mediaOwner(filepath.Join(movie, "Movie (2020).mkv"), folders); !ok || id != 9 {
		t.Errorf("movie owner = %d, %v; want 9", id, ok)
	}
	if _, ok := mediaOwner(filepath.Join(base, "tv", "Other", "x.mkv"), folders); ok {
		t.Error("unknown folder must have no owner")
	}
}

func TestSeasonOf(t *testing.T) {
	base := t.TempDir()
	cases := []struct {
		path string
		want int
		ok   bool
	}{
		{filepath.Join(base, "Show", "Season 04", "x.mkv"), 4, true},
		{filepath.Join(base, "Show", "Season.12", "x.mkv"), 12, true},
		{filepath.Join(base, "Show", "Show.S03E10.1080p.mkv"), 3, true},
		{filepath.Join(base, "Movie (2020)", "Movie (2020).mkv"), 0, false},
	}
	for _, c := range cases {
		if got, ok := seasonOf(c.path); got != c.want || ok != c.ok {
			t.Errorf("seasonOf(%s) = %d, %v; want %d, %v", filepath.Base(c.path), got, ok, c.want, c.ok)
		}
	}
}

func TestListableArr(t *testing.T) {
	for _, c := range []struct {
		typ  arr.Type
		want bool
	}{{arr.Sonarr, true}, {arr.Radarr, true}, {arr.Lidarr, false}, {arr.Readarr, false}, {arr.Others, false}} {
		if got := listableArr(&arr.Arr{Type: c.typ}); got != c.want {
			t.Errorf("listableArr(%s) = %v, want %v", c.typ, got, c.want)
		}
	}
	if listableArr(nil) {
		t.Error("nil Arr must not be listable")
	}
}

func TestRecordArrFiles(t *testing.T) {
	base := t.TempDir()
	d := &deadLinkSet{arrPaths: map[string]struct{}{}}
	ep := filepath.Join(base, "Show", "Season 01", "ep.mkv")
	d.recordArrFiles([]arr.ContentFile{{Path: ep}, {Path: ""}})
	if _, ok := d.arrPaths[ep]; !ok || len(d.arrPaths) != 1 {
		t.Fatalf("arrPaths = %v, want only %s", d.arrPaths, ep)
	}
	var nilSet *deadLinkSet
	nilSet.recordArrFiles([]arr.ContentFile{{Path: ep}}) // must not panic
}

func TestMayHoldLibrary(t *testing.T) {
	for _, c := range []struct {
		typ  arr.Type
		want bool
	}{{arr.Sonarr, true}, {arr.Radarr, true}, {arr.Others, true}, {arr.Lidarr, false}, {arr.Readarr, false}} {
		if got := mayHoldLibrary(&arr.Arr{Type: c.typ}); got != c.want {
			t.Errorf("mayHoldLibrary(%s) = %v, want %v", c.typ, got, c.want)
		}
	}
}

func TestAlreadyGone(t *testing.T) {
	for _, c := range []struct {
		msg  string
		want bool
	}{
		{"realdebrid API error: Status: 404", true},
		{"torrent not found", false}, // only a real HTTP 404 counts
		{"realdebrid API error: Status: 503", false},
		{"context deadline exceeded", false},
	} {
		if got := alreadyGone(errors.New(c.msg)); got != c.want {
			t.Errorf("alreadyGone(%q) = %v, want %v", c.msg, got, c.want)
		}
	}
}
