package manager

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// Reclaim removes debrid entries that no library symlink points at any more.
//
// When an Arr upgrades or deletes a file it removes the symlink, but the entry
// it pointed into stays on the debrid account forever. The pass walks the
// configured library folders, collects the entry folder every symlink points
// into, and treats any in-scope entry whose folder nothing links to as
// reclaimable.
//
// It is deliberately conservative:
//   - only entries whose category is an Arr name are considered, so torrents
//     added to the debrid account by hand are never touched. Entries outside
//     that scope (no category because the account sync recorded them first,
//     or a renamed Arr's old category) are adopted once the library is seen
//     linking to them: they get the ReclaimTag and are in scope from then on.
//     An out-of-scope entry the library never links to is never adopted;
//   - entries still in the download queue, still downloading, not complete,
//     younger than MinAge, or imported with a non-symlink action are skipped;
//   - one link into an entry's folder protects the whole entry;
//   - a missing, unreadable or symlink-free library path aborts the pass;
//   - more than MaxPerRun reclaimable entries aborts the pass;
//   - with Delete off (the default) nothing is removed, only reported.

// ReclaimTag marks an out-of-scope entry the library has been seen linking
// to, which makes it eligible for reclaim once those links are gone.
const ReclaimTag = "reclaim:linked"

// ReclaimItem is one entry the pass found unreferenced.
type ReclaimItem struct {
	InfoHash    string    `json:"info_hash"`
	Name        string    `json:"name"`
	Folder      string    `json:"folder"`
	Category    string    `json:"category"`
	Size        int64     `json:"size"`
	CompletedAt time.Time `json:"completed_at"`
	Error       string    `json:"error,omitempty"`
}

// ReclaimReport is the outcome of one reclaim pass.
type ReclaimReport struct {
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	DryRun       bool      `json:"dry_run"`
	Aborted      string    `json:"aborted,omitempty"`
	LibraryPaths []string  `json:"library_paths"`
	Categories   []string  `json:"categories"`

	Symlinks      int `json:"symlinks"`       // symlinks found under the library paths
	LinkedFolders int `json:"linked_folders"` // distinct entry folders they point into

	Adopted           int `json:"adopted"`    // out-of-scope linked entries tagged this pass
	Considered        int `json:"considered"` // in-scope entries examined
	Referenced        int `json:"referenced"`
	SkippedQueued     int `json:"skipped_queued"`
	SkippedIncomplete int `json:"skipped_incomplete"`
	SkippedYoung      int `json:"skipped_young"`
	SkippedAction     int `json:"skipped_action"`

	// Dead links: Arr files whose symlink points into the mount at an entry
	// that no longer exists (found by the sweep's Arr scan). They are deleted
	// in the Arr and re-acquired, through the same path as broken-file repair.
	DeadLinks         []DeadLink `json:"dead_links"`
	DeadLinksAborted  string     `json:"dead_links_aborted,omitempty"`
	DeadLinksRepaired int        `json:"dead_links_repaired"`
	DeadLinksFailed   int        `json:"dead_links_failed"`

	Reclaimable      []ReclaimItem  `json:"reclaimable"`
	ReclaimableBytes int64          `json:"reclaimable_bytes"`
	ByCategory       map[string]int `json:"by_category"`
	Deleted          int            `json:"deleted"`
	Failed           int            `json:"failed"`
}

// ErrReclaimRunning is returned when a reclaim pass is already in progress.
var ErrReclaimRunning = errors.New("reclaim already running")

type reclaimState struct {
	running sync.Mutex
	mu      sync.RWMutex
	last    *ReclaimReport
	dead    *deadLinkSet // from the most recent Arr-source sweep
}

// DeadLink is one Arr file whose symlink target entry is gone.
type DeadLink struct {
	Arr    string `json:"arr"`
	Path   string `json:"path"`
	Target string `json:"target"`
}

// deadLinkSet collects dead links during a sweep's Arr enumeration.
type deadLinkSet struct {
	mu         sync.Mutex
	known      map[string]struct{} // entry folder names in the store
	mountRoot  string
	incomplete bool // an Arr listing failed: the set may be partial
	collected  time.Time
	byArr      map[string]*deadArrFiles
}

type deadArrFiles struct {
	arr   *arr.Arr
	files []arr.ContentFile
	links []DeadLink
}

// deadLinkMaxAge bounds how stale a sweep's dead-link set may be when a
// manual reclaim run acts on it.
const deadLinkMaxAge = 6 * time.Hour

func newDeadLinkSet(known map[string]struct{}) *deadLinkSet {
	return &deadLinkSet{
		known:     known,
		mountRoot: filepath.Clean(config.Get().Mount.MountPath),
		collected: time.Now(),
		byArr:     make(map[string]*deadArrFiles),
	}
}

func (d *deadLinkSet) markIncomplete() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.incomplete = true
	d.mu.Unlock()
}

// add records files whose resolved entry folder (entryPath) has no entry.
// Targets outside the mount are ignored: they are not ours to judge.
func (d *deadLinkSet) add(a *arr.Arr, entryPath, name string, files []arr.ContentFile) {
	if d == nil || len(d.known) == 0 || d.mountRoot == "" || d.mountRoot == "." {
		return
	}
	if _, ok := d.known[name]; ok {
		return // the entry exists; the lookup failed for another reason
	}
	if !isUnder(entryPath, d.mountRoot) {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	g, ok := d.byArr[a.Name]
	if !ok {
		g = &deadArrFiles{arr: a}
		d.byArr[a.Name] = g
	}
	for _, f := range files {
		g.files = append(g.files, f)
		g.links = append(g.links, DeadLink{Arr: a.Name, Path: f.Path, Target: filepath.Join(entryPath, f.TargetPath)})
	}
}

func isUnder(path, root string) bool {
	path = filepath.Clean(path)
	return path == root || strings.HasPrefix(path, root+string(filepath.Separator))
}

func (r *Repair) setDeadLinks(d *deadLinkSet) {
	r.reclaim.mu.Lock()
	r.reclaim.dead = d
	r.reclaim.mu.Unlock()
}

// LastReclaimReport returns the most recent reclaim report, or nil.
func (r *Repair) LastReclaimReport() *ReclaimReport {
	r.reclaim.mu.RLock()
	defer r.reclaim.mu.RUnlock()
	return r.reclaim.last
}

// RunReclaim runs one reclaim pass with the current config. dryRun forces a
// report-only pass even when deletion is enabled in config.
func (r *Repair) RunReclaim(ctx context.Context, dryRun bool) (*ReclaimReport, error) {
	if !r.reclaim.running.TryLock() {
		return nil, ErrReclaimRunning
	}
	defer r.reclaim.running.Unlock()

	cfg := r.cfg().Reclaim
	dry := dryRun || !cfg.Delete
	report := r.runReclaim(ctx, cfg, r.reclaimCategories(cfg), dry)
	r.repairDeadLinks(ctx, cfg, report, dry)

	r.reclaim.mu.Lock()
	r.reclaim.last = report
	r.reclaim.mu.Unlock()
	return report, nil
}

// reclaimAfterSweep is called once a scheduled sweep has completed.
func (r *Repair) reclaimAfterSweep(ctx context.Context) {
	if !r.cfg().Reclaim.Enabled {
		return
	}
	if _, err := r.RunReclaim(ctx, false); err != nil {
		r.logger.Warn().Err(err).Msg("Reclaim: skipped")
	}
}

func (r *Repair) reclaimCategories(cfg config.ReclaimConfig) []string {
	var names []string
	if len(cfg.Categories) > 0 {
		names = cfg.Categories
	} else {
		for _, a := range r.manager.arr.GetAll() {
			if a != nil {
				names = append(names, a.Name)
			}
		}
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

func (r *Repair) runReclaim(ctx context.Context, cfg config.ReclaimConfig, categories []string, dryRun bool) *ReclaimReport {
	log := r.logger.With().Str("pass", "reclaim").Logger()
	report := &ReclaimReport{
		StartedAt:    time.Now(),
		DryRun:       dryRun,
		LibraryPaths: cfg.LibraryPaths,
		Categories:   categories,
		ByCategory:   make(map[string]int),
	}
	defer func() { report.FinishedAt = time.Now() }()

	abort := func(format string, args ...any) *ReclaimReport {
		report.Aborted = fmt.Sprintf(format, args...)
		log.Warn().Str("reason", report.Aborted).Msg("Reclaim: aborted, nothing deleted")
		return report
	}

	if len(categories) == 0 {
		return abort("no categories in scope")
	}
	minAge, err := time.ParseDuration(cfg.MinAge)
	if err != nil || minAge < 0 {
		return abort("invalid min_age %q", cfg.MinAge)
	}

	linked, symlinks, err := collectLinkedFolders(ctx, cfg.LibraryPaths)
	if err != nil {
		return abort("%v", err)
	}
	report.Symlinks = symlinks
	report.LinkedFolders = len(linked)

	scope := make(map[string]struct{}, len(categories))
	for _, c := range categories {
		scope[c] = struct{}{}
	}
	cutoff := time.Now().Add(-minAge)

	var adopt []string
	err = r.manager.storage.ForEach(func(e *storage.Entry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, ok := scope[e.Category]; !ok && !slices.Contains(e.Tags, ReclaimTag) {
			if _, linkedNow := linked[e.GetFolder()]; linkedNow {
				adopt = append(adopt, e.InfoHash)
			}
			return nil
		}
		report.Considered++
		item, verdict := classifyReclaim(e, linked, cutoff)
		if verdict == reclaimKeepReferenced {
			report.Referenced++
			return nil
		}
		if verdict == reclaimKeepNotReady {
			report.SkippedIncomplete++
			return nil
		}
		if verdict == reclaimKeepYoung {
			report.SkippedYoung++
			return nil
		}
		if verdict == reclaimKeepAction {
			report.SkippedAction++
			return nil
		}
		if _, err := r.manager.storage.GetQueued(e.InfoHash); err == nil {
			report.SkippedQueued++
			return nil
		}
		report.Reclaimable = append(report.Reclaimable, item)
		report.ReclaimableBytes += item.Size
		report.ByCategory[item.Category]++
		return nil
	})
	if err != nil {
		return abort("listing entries: %v", err)
	}
	// Adoption only records what the library links to now, so it runs in dry
	// runs too: an entry unlinked before deletion is switched on must still be
	// recognised as library content.
	report.Adopted = r.adoptLinked(adopt)

	sort.Slice(report.Reclaimable, func(i, j int) bool {
		return report.Reclaimable[i].CompletedAt.Before(report.Reclaimable[j].CompletedAt)
	})

	log.Info().
		Bool("dry_run", dryRun).
		Int("symlinks", report.Symlinks).
		Int("linked_folders", report.LinkedFolders).
		Int("adopted", report.Adopted).
		Int("considered", report.Considered).
		Int("referenced", report.Referenced).
		Int("reclaimable", len(report.Reclaimable)).
		Int64("reclaimable_bytes", report.ReclaimableBytes).
		Msg("Reclaim: scan complete")

	if len(report.Reclaimable) > cfg.MaxPerRun {
		return abort("%d reclaimable entries exceeds max_per_run %d; review a dry run, then raise max_per_run",
			len(report.Reclaimable), cfg.MaxPerRun)
	}
	if dryRun {
		for _, it := range report.Reclaimable {
			log.Info().Str("name", it.Name).Str("category", it.Category).Msg("Reclaim: would remove")
		}
		return report
	}

	for i := range report.Reclaimable {
		if ctx.Err() != nil {
			break
		}
		it := &report.Reclaimable[i]
		if err := r.reclaimEntry(it.InfoHash); err != nil {
			it.Error = err.Error()
			report.Failed++
			log.Warn().Err(err).Str("name", it.Name).Msg("Reclaim: remove failed")
			continue
		}
		report.Deleted++
		log.Info().Str("name", it.Name).Str("category", it.Category).Msg("Reclaim: removed")
	}
	if report.Deleted > 0 {
		r.manager.RefreshEntries(true)
	}
	return report
}

// repairDeadLinks acts on the dead links the last sweep found: each Arr's
// file records are deleted and the media re-acquired. A partial Arr listing,
// a stale set, or more than MaxPerRun links skips the step.
func (r *Repair) repairDeadLinks(ctx context.Context, cfg config.ReclaimConfig, report *ReclaimReport, dryRun bool) {
	r.reclaim.mu.Lock()
	d := r.reclaim.dead
	if !dryRun {
		r.reclaim.dead = nil // act on a set at most once
	}
	r.reclaim.mu.Unlock()
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	report.DeadLinks = []DeadLink{}
	names := make([]string, 0, len(d.byArr))
	for name, g := range d.byArr {
		names = append(names, name)
		report.DeadLinks = append(report.DeadLinks, g.links...)
	}
	sort.Strings(names)
	log := r.logger.With().Str("pass", "reclaim").Logger()

	switch {
	case d.incomplete:
		report.DeadLinksAborted = "an Arr listing failed during the sweep"
	case time.Since(d.collected) > deadLinkMaxAge:
		report.DeadLinksAborted = "dead-link set is older than " + deadLinkMaxAge.String() + "; wait for the next sweep"
	case len(report.DeadLinks) > cfg.MaxPerRun:
		report.DeadLinksAborted = fmt.Sprintf("%d dead links exceeds max_per_run %d", len(report.DeadLinks), cfg.MaxPerRun)
	}
	if report.DeadLinksAborted != "" {
		log.Warn().Str("reason", report.DeadLinksAborted).Int("dead_links", len(report.DeadLinks)).Msg("Reclaim: dead links skipped")
		return
	}
	if dryRun {
		for _, l := range report.DeadLinks {
			log.Info().Str("arr", l.Arr).Str("path", l.Path).Msg("Reclaim: would re-acquire dead link")
		}
		return
	}
	for _, name := range names {
		if ctx.Err() != nil {
			return
		}
		g := d.byArr[name]
		files := r.stillDead(g)
		if len(files) == 0 {
			continue
		}
		if err := r.reacquireArrFiles(ctx, g.arr, files); err != nil {
			report.DeadLinksFailed += len(files)
			continue
		}
		report.DeadLinksRepaired += len(files)
		log.Info().Str("arr", name).Int("files", len(files)).Msg("Reclaim: dead links re-acquired")
	}
}

// stillDead re-checks each file just before acting: the symlink must still
// point at the same target, and that target's entry must still be missing.
func (r *Repair) stillDead(g *deadArrFiles) []arr.ContentFile {
	out := make([]arr.ContentFile, 0, len(g.files))
	for i, f := range g.files {
		target := readSymlinkTarget(f.Path)
		if target == "" || filepath.Clean(target) != filepath.Clean(g.links[i].Target) {
			continue
		}
		if item, err := r.manager.GetEntryItem(filepath.Base(filepath.Dir(target))); err == nil && item != nil {
			continue
		}
		out = append(out, f)
	}
	return out
}

// adoptLinked tags the given out-of-scope entries with ReclaimTag. Writes
// happen after the store walk, never during it.
func (r *Repair) adoptLinked(hashes []string) int {
	n := 0
	for _, h := range hashes {
		e, err := r.manager.GetEntry(h)
		if err != nil || e == nil || slices.Contains(e.Tags, ReclaimTag) {
			continue
		}
		e.Tags = append(e.Tags, ReclaimTag)
		if err := r.manager.storage.AddOrUpdate(e); err != nil {
			r.logger.Warn().Err(err).Str("name", e.Name).Msg("Reclaim: adopt failed")
			continue
		}
		n++
	}
	return n
}

// reclaimEntry removes one entry from the debrid provider(s) and the store.
// Unlike Manager.DeleteEntry it does not refresh the mount per entry; the
// caller refreshes once after the batch.
func (r *Repair) reclaimEntry(infoHash string) error {
	// Re-read: the entry may have been re-queued or deleted since the scan.
	if _, err := r.manager.storage.GetQueued(infoHash); err == nil {
		return errors.New("entry was re-queued")
	}
	e, err := r.manager.GetEntry(infoHash)
	if err != nil {
		return err
	}
	r.manager.RemoveTorrentPlacements(e)
	return r.manager.storage.Delete(infoHash)
}

type reclaimVerdict int

const (
	reclaimRemove reclaimVerdict = iota
	reclaimKeepReferenced
	reclaimKeepNotReady
	reclaimKeepYoung
	reclaimKeepAction
)

// classifyReclaim decides whether an in-scope entry may be reclaimed. The
// queue check is done separately because it needs storage.
func classifyReclaim(e *storage.Entry, linked map[string]struct{}, cutoff time.Time) (ReclaimItem, reclaimVerdict) {
	folder := e.GetFolder()
	item := ReclaimItem{
		InfoHash: e.InfoHash,
		Name:     e.Name,
		Folder:   folder,
		Category: e.Category,
		Size:     e.Bytes,
	}
	if item.Size == 0 {
		item.Size = e.Size
	}
	if folder == "" {
		return item, reclaimKeepNotReady
	}
	if _, ok := linked[folder]; ok {
		return item, reclaimKeepReferenced
	}
	if !e.IsComplete || e.IsDownloading || e.Bad {
		return item, reclaimKeepNotReady
	}
	switch e.Action {
	case "", config.DownloadActionSymlink:
	default:
		// download/strm/none imports leave no symlink to find.
		return item, reclaimKeepAction
	}
	item.CompletedAt = lastActivity(e)
	if item.CompletedAt.After(cutoff) {
		return item, reclaimKeepYoung
	}
	return item, reclaimRemove
}

func lastActivity(e *storage.Entry) time.Time {
	t := e.AddedOn
	if e.CreatedAt.After(t) {
		t = e.CreatedAt
	}
	if e.CompletedAt != nil && e.CompletedAt.After(t) {
		t = *e.CompletedAt
	}
	if e.ImportedAt != nil && e.ImportedAt.After(t) {
		t = *e.ImportedAt
	}
	return t
}

// collectLinkedFolders walks the library paths and returns the set of entry
// folder names that symlinks point into (the base name of each target's parent
// directory), plus the number of symlinks seen. It fails if a path is missing
// or unreadable, or if no symlinks are found at all, since either would make
// every entry look unreferenced.
func collectLinkedFolders(ctx context.Context, roots []string) (map[string]struct{}, int, error) {
	if len(roots) == 0 {
		return nil, 0, errors.New("no library_paths configured")
	}
	linked := make(map[string]struct{})
	total := 0
	for _, root := range roots {
		root = filepath.Clean(root)
		info, err := os.Stat(root)
		if err != nil {
			return nil, 0, fmt.Errorf("library path %s: %w", root, err)
		}
		if !info.IsDir() {
			return nil, 0, fmt.Errorf("library path %s is not a directory", root)
		}
		n := 0
		err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if d.Type()&fs.ModeSymlink == 0 {
				return nil
			}
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(path), target)
			}
			folder := filepath.Base(filepath.Dir(filepath.Clean(target)))
			if folder != "." && folder != string(filepath.Separator) {
				linked[folder] = struct{}{}
			}
			n++
			return nil
		})
		if err != nil {
			return nil, 0, fmt.Errorf("walking %s: %w", root, err)
		}
		if n == 0 {
			return nil, 0, fmt.Errorf("library path %s contains no symlinks", root)
		}
		total += n
	}
	return linked, total, nil
}
