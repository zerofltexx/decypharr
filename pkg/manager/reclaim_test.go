package manager

import (
	"context"
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

	linked, n, err := collectLinkedFolders(context.Background(), []string{lib})
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
}

func TestCollectLinkedFoldersFailsClosed(t *testing.T) {
	ctx := context.Background()
	if _, _, err := collectLinkedFolders(ctx, nil); err == nil {
		t.Error("no paths: want error")
	}
	if _, _, err := collectLinkedFolders(ctx, []string{filepath.Join(t.TempDir(), "missing")}); err == nil {
		t.Error("missing path: want error")
	}
	// An empty or unmounted library must never make everything look unreferenced.
	if _, _, err := collectLinkedFolders(ctx, []string{t.TempDir()}); err == nil {
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
	d := &deadLinkSet{
		known:     map[string]struct{}{"Live.Entry": {}},
		mountRoot: "/mnt/remote/realdebrid",
		byArr:     map[string]*deadArrFiles{},
	}
	a := &arr.Arr{Name: "sonarr"}
	f := []arr.ContentFile{{Path: "/media/tv/Show/S01E01.mkv", TargetPath: "S01E01.mkv"}}

	d.add(a, "/mnt/remote/realdebrid/__all__/Live.Entry", "Live.Entry", f)  // entry exists
	d.add(a, "/mnt/local/other/Gone.Entry", "Gone.Entry", f)                // outside the mount
	d.add(a, "/mnt/remote/realdebridx/__all__/Gone.Entry", "Gone.Entry", f) // prefix lookalike
	if len(d.byArr) != 0 {
		t.Fatalf("recorded %v, want nothing", d.byArr)
	}
	d.add(a, "/mnt/remote/realdebrid/__all__/Gone.Entry", "Gone.Entry", f)
	g := d.byArr["sonarr"]
	if g == nil || len(g.files) != 1 || g.links[0].Target != "/mnt/remote/realdebrid/__all__/Gone.Entry/S01E01.mkv" {
		t.Fatalf("dead link not recorded correctly: %+v", g)
	}

	empty := &deadLinkSet{known: map[string]struct{}{}, mountRoot: "/mnt/remote/realdebrid", byArr: map[string]*deadArrFiles{}}
	empty.add(a, "/mnt/remote/realdebrid/__all__/Gone.Entry", "Gone.Entry", f)
	if len(empty.byArr) != 0 {
		t.Fatal("an empty store must never yield dead links")
	}
	var nilSet *deadLinkSet
	nilSet.add(a, "/x", "x", f) // must not panic
}
