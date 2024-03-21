package io

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"

	"github.com/benthosdev/benthos/v4/internal/codec/interop"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	fileInputFieldPaths          = "paths"
	fileInputFieldDeleteOnFinish = "delete_on_finish"
	fileInputFieldFollow         = "follow"
)

func fileInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Local").
		Summary(`Consumes data from files on disk, emitting messages according to a chosen codec.`).
		Description(`
### Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- path
- mod_time_unix
- mod_time (RFC3339)
`+"```"+`

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Example(
			"Read a Bunch of CSVs",
			"If we wished to consume a directory of CSV files as structured documents we can use a glob pattern and the `csv` scanner:",
			`
input:
  file:
    paths: [ ./data/*.csv ]
    scanner:
      csv: {}
`,
		).
		Fields(
			service.NewStringListField(fileInputFieldPaths).
				Description("A list of paths to consume sequentially. Glob patterns are supported, including super globs (double star)."),
		).
		Fields(interop.OldReaderCodecFields("lines")...).
		Fields(
			service.NewBoolField(fileInputFieldDeleteOnFinish).
				Description("Whether to delete input files from the disk once they are fully consumed.").
				Advanced().
				Default(false),
		).Fields(
		service.NewBoolField(fileInputFieldFollow).
			Description("Whether to follow the directories matched by the provided globs").
			Advanced().
			Default(false),
	)
}

func init() {
	err := service.RegisterBatchInput("file", fileInputSpec(),
		func(pConf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
			r, err := fileConsumerFromParsed(pConf, res)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatched(r), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type scannerInfo struct {
	nm         *service.Resources
	scanner    interop.FallbackReaderStream
	path       string
	modTimeUTC time.Time
	file       fs.File
	mut        sync.Mutex
}

func (t *scannerInfo) Read(p []byte) (n int, err error) {
	t.mut.Lock()
	defer t.mut.Unlock()
	return t.file.Read(p)
}

func (t *scannerInfo) Close() error {
	t.mut.Lock()
	defer t.mut.Unlock()
	return t.file.Close()
}

func (t *scannerInfo) ReOpen() error {
	t.mut.Lock()
	defer t.mut.Unlock()
	err := t.file.Close()
	if err != nil {
		return err
	}
	file, err := t.nm.FS().Open(t.path)
	if err != nil {
		return err
	}
	t.file = file
	return nil
}

type fileConsumer struct {
	log *service.Logger
	nm  *service.Resources

	follow  bool
	watcher *fsnotify.Watcher

	pathsGlobs []string
	filesQueue []string
	files      map[string]*scannerInfo

	scannerCtor interop.FallbackReaderCodec

	filesMut sync.Mutex

	delete bool
}

func fileConsumerFromParsed(conf *service.ParsedConfig, nm *service.Resources) (*fileConsumer, error) {
	paths, err := conf.FieldStringList(fileInputFieldPaths)
	if err != nil {
		return nil, err
	}

	deleteOnFinish, err := conf.FieldBool(fileInputFieldDeleteOnFinish)
	if err != nil {
		return nil, err
	}
	ctor, err := interop.OldReaderCodecFromParsed(conf)
	if err != nil {
		return nil, err
	}

	follow, err := conf.FieldBool(fileInputFieldFollow)
	if err != nil {
		return nil, err
	}

	return &fileConsumer{
		nm:          nm,
		log:         nm.Logger(),
		scannerCtor: ctor,
		pathsGlobs:  paths,
		files:       make(map[string]*scannerInfo),
		delete:      deleteOnFinish,
		follow:      follow,
	}, nil
}

const waitFor = 10 * time.Millisecond

type backoff struct {
	timer *time.Timer
	count int
}

func (f *fileConsumer) addFile(fpath string) error {
	file, err := f.nm.FS().Open(fpath)
	if err != nil {
		return err
	}

	var modTimeUTC time.Time
	if fInfo, err := file.Stat(); err == nil {
		modTimeUTC = fInfo.ModTime().UTC()
	} else {
		f.log.Errorf("Failed to read metadata from file '%v'", fpath)
	}

	tail := scannerInfo{
		nm:         f.nm,
		modTimeUTC: modTimeUTC,
		file:       file,
	}

	details := scanner.SourceDetails{
		Name: fpath,
	}

	scanner, err := f.scannerCtor.Create(&tail, func(ctx context.Context, err error) error {
		return nil
	}, details)
	if err != nil {
		file.Close()
		return err
	}

	tail.scanner = scanner

	f.filesMut.Lock()
	defer f.filesMut.Unlock()
	f.files[fpath] = &tail
	f.filesQueue = append(f.filesQueue, fpath)
	if f.follow {
		f.watcher.Add(path.Dir(fpath))
	}
	return nil
}

func (f *fileConsumer) notifyLoop(w *fsnotify.Watcher) error {
	timers := make(map[string]*backoff)
	for {
		select {
		// Read from Errors.
		case err, ok := <-w.Errors:
			if !ok { // Channel was closed (i.e. Watcher.Close() was called).
				return err
			}
			fmt.Printf("ERROR: %s", err)
		// Read from Events.
		case e, ok := <-w.Events:
			if !ok { // Channel was closed (i.e. Watcher.Close() was called).
				return errors.New("channel was closed")
			}
			if e.Name != "" {
				switch {
				case e.Op.Has(fsnotify.Create):
					for _, patternPath := range f.pathsGlobs {
						matched, err := path.Match(patternPath, e.Name)
						if err != nil {
							return err
						}
						if matched {
							tail, ok := f.files[e.Name]
							if !ok {
								f.addFile(e.Name)
							} else {
								tail.ReOpen()
							}
						}
					}
				case e.Op.Has(fsnotify.Write):
					_, ok := f.files[e.Name]
					if ok {
						timer, ok := timers[e.Name]
						if !ok {
							timers[e.Name].timer = time.AfterFunc(waitFor, func() {
								f.filesQueue = append(f.filesQueue, e.Name)
								delete(timers, e.Name)
							})
						} else if timer.count > 100 {
							timer.timer.Stop()
							f.filesQueue = append(f.filesQueue, e.Name)
							delete(timers, e.Name)
						} else {
							timer.timer.Reset(waitFor)
							timer.count++
						}
					}
					// note we're ignoring chmod, rename and remove events here
				}
			}
		}
	}
}

func (f *fileConsumer) Connect(ctx context.Context) error {
	expandedPaths, err := filepath.Globs(f.nm.FS(), f.pathsGlobs)
	if err != nil {
		return err
	}
	if f.follow {
		w, err := fsnotify.NewWatcher()
		if err != nil {
			return err
		}
		f.watcher = w
		go f.notifyLoop(f.watcher)
	}
	for _, path := range expandedPaths {
		f.addFile(path)
	}
	return nil
}

func (f *fileConsumer) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
			for i, path := range f.filesQueue {
				tail := f.files[path]
				parts, codecAckFn, err := tail.scanner.NextBatch(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) ||
						errors.Is(err, context.DeadlineExceeded) {
						err = component.ErrTimeout
					}
					if err != component.ErrTimeout {
						f.filesMut.Lock()
						// remove the failed file from the queue for now
						// it will get re-added once is changed again
						f.filesQueue = append(f.filesQueue[:i], f.filesQueue[i+1:]...)
						f.filesMut.Unlock()
					} else if errors.Is(err, io.EOF) && f.follow {
						// continue loop to try next file
						continue
					}
					return nil, nil, err
				} else {
					f.filesMut.Lock()
					// remove the successful file from the queue
					f.filesQueue = append(f.filesQueue[:i], f.filesQueue[i+1:]...)
					f.filesMut.Unlock()
				}
				for _, part := range parts {
					part.MetaSetMut("path", path)
					part.MetaSetMut("mod_time_unix", tail.modTimeUTC.Unix())
					part.MetaSetMut("mod_time", tail.modTimeUTC.Format(time.RFC3339))
				}

				if len(parts) == 0 {
					_ = codecAckFn(ctx, nil)
					return nil, nil, component.ErrTimeout
				}

				return parts, func(rctx context.Context, res error) error {
					return codecAckFn(rctx, res)
				}, nil
			}
		}
		if !f.follow {
			return nil, nil, component.ErrTypeClosed
		}
	}
}

func (f *fileConsumer) Close(ctx context.Context) (err error) {
	f.filesMut.Lock()
	defer f.filesMut.Unlock()
	if f.follow {
		err = f.watcher.Close()
	}
	if err != nil {
		return err
	}
	for _, tail := range f.files {
		err = tail.scanner.Close(ctx)
		if err != nil {
			return err
		}
	}
	return
}
