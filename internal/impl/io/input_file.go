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
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	fileInputFieldPaths          = "paths"
	fileInputFieldDeleteOnFinish = "delete_on_finish"
	fileInputFieldFollow         = "follow"
	fileInputFieldFollowEnabled  = "enabled"
	fileInputFieldCache          = "cache"
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
				Description("Whether to delete input files from the disk once they are fully consumed. (Incompatible with follow)").
				Advanced().
				Default(false),
			service.NewObjectField(fileInputFieldFollow,
				service.NewBoolField(fileInputFieldFollowEnabled).
					Description("Whether file following is enabled.").
					Default(false),
				service.NewStringField(fileInputFieldCache).
					Description("A [cache resource](/docs/components/caches/about) for saving file positions as the files are processed as they are followed - commonly the file cache.").
					Default(""),
			).Description("An experimental mode whereby the input will watch the target paths for new files and consume them.").
				Version("3.42.0"),
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

type fileMessage struct {
	parts  service.MessageBatch
	ackFcn service.AckFunc
	err    error
}

type fileConsumer struct {
	log *service.Logger
	nm  *service.Resources

	cacheName string

	follow  bool
	watcher *fsnotify.Watcher

	pathsGlobs []string
	messages   chan fileMessage
	errors     chan error

	scannerCtor interop.FallbackReaderCodec

	delete bool

	wg sync.WaitGroup

	shutSig *shutdown.Signaller
}

func fileConsumerFromParsed(conf *service.ParsedConfig, nm *service.Resources) (f *fileConsumer, err error) {
	f = &fileConsumer{
		nm:       nm,
		log:      nm.Logger(),
		messages: make(chan fileMessage),
		errors:   make(chan error),
		shutSig:  shutdown.NewSignaller(),
	}

	f.pathsGlobs, err = conf.FieldStringList(fileInputFieldPaths)
	if err != nil {
		return nil, err
	}

	f.delete, err = conf.FieldBool(fileInputFieldDeleteOnFinish)
	if err != nil {
		return
	}
	f.scannerCtor, err = interop.OldReaderCodecFromParsed(conf)
	if err != nil {
		return
	}

	wConf := conf.Namespace(fileInputFieldFollow)
	if f.follow, _ = wConf.FieldBool(fileInputFieldFollowEnabled); f.follow {
		if f.cacheName, err = wConf.FieldString(fileInputFieldCache); err != nil {
			return
		}
		if !nm.HasCache(f.cacheName) {
			return nil, fmt.Errorf("cache resource '%v' was not found", f.cacheName)
		}
	}

	if f.follow && f.delete {
		return nil, fmt.Errorf("Follow and delete options are incompatible")
	}

	return
}

type tailInfo struct {
	nm         *service.Resources
	scanner    interop.FallbackReaderStream
	path       string
	modTimeUTC time.Time
	file       fs.File
	follow     bool
	hasUpdates chan bool
	backoff    *backoff
	mut        sync.Mutex
}

type backoff struct {
	timer *time.Timer
	count int
}

func (t *tailInfo) Read(p []byte) (n int, err error) {
	n, err = t.file.Read(p)
	for t.follow && errors.Is(err, io.EOF) {
		if n == 0 {
			_, ok := <-t.hasUpdates
			if ok {
				n, err = t.file.Read(p)
			} else {
				err = io.EOF
				return
			}
		} else {
			err = nil
			return
		}
	}
	return
}

func (t *tailInfo) Close() error {
	close(t.hasUpdates)
	return t.file.Close()
}

func (t *tailInfo) ReOpen() error {
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

const waitFor = 10 * time.Millisecond

func (f *fileConsumer) addFile(ctx context.Context, fpath string) (tail *tailInfo, err error) {
	file, err := f.nm.FS().Open(fpath)
	if err != nil {
		return
	}

	var modTimeUTC time.Time
	if fInfo, err := file.Stat(); err == nil {
		modTimeUTC = fInfo.ModTime().UTC()
	} else {
		f.log.Errorf("Failed to read metadata from file '%v'", fpath)
	}

	tail = &tailInfo{
		nm:         f.nm,
		modTimeUTC: modTimeUTC,
		file:       file,
		follow:     f.follow,
		hasUpdates: make(chan bool),
	}

	details := scanner.SourceDetails{
		Name: fpath,
	}

	scanner, err := f.scannerCtor.Create(tail, func(ctx context.Context, err error) error {
		if err == nil && !f.follow && f.delete {
			return f.nm.FS().Remove(fpath)
		}
		return nil
	}, details)

	if err != nil {
		file.Close()
		return
	}

	tail.scanner = scanner

	if f.follow {
		f.watcher.Add(path.Dir(fpath))
	}
	go f.fileLoop(ctx, tail)
	return
}

func (f *fileConsumer) fileLoop(ctx context.Context, tail *tailInfo) {
	f.wg.Add(1)
	defer f.wg.Done()
	path := tail.path
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var err error
			tail.mut.Lock()
			parts, codecAckFn, err := tail.scanner.NextBatch(ctx)
			tail.mut.Unlock()
			if err != nil {
				if errors.Is(err, context.Canceled) ||
					errors.Is(err, context.DeadlineExceeded) {
					err = component.ErrTimeout
				}
				if errors.Is(err, io.EOF) {
					return
				}
				f.messages <- fileMessage{
					parts:  nil,
					ackFcn: nil,
					err:    err,
				}
				continue
			}
			for _, part := range parts {
				part.MetaSetMut("path", path)
				part.MetaSetMut("mod_time_unix", tail.modTimeUTC.Unix())
				part.MetaSetMut("mod_time", tail.modTimeUTC.Format(time.RFC3339))
			}
			if len(parts) == 0 {
				_ = codecAckFn(ctx, nil)
				err = component.ErrTimeout
				f.messages <- fileMessage{
					parts:  nil,
					ackFcn: nil,
					err:    err,
				}
				continue
			}
			f.messages <- fileMessage{
				parts: parts,
				ackFcn: func(rctx context.Context, res error) error {
					return codecAckFn(rctx, res)
				},
				err: err,
			}
		}
	}
}

func (f *fileConsumer) notifyLoop(w *fsnotify.Watcher, paths []string, dirs []string) {
	files := make(map[string]*tailInfo)
	ctx, done := f.shutSig.CloseAtLeisureCtx(context.Background())
	defer func() {
		done()
		_ = f.watcher.Close()
		for _, tail := range files {
			tail.scanner.Close(ctx)
			if tail.backoff != nil {
				tail.backoff.timer.Stop()
			}
		}
		f.wg.Wait()
		close(f.errors)
		close(f.messages)
		f.shutSig.ShutdownComplete()
	}()

	for _, path := range paths {
		files[path], _ = f.addFile(ctx, path)
	}

	for _, dir := range dirs {
		w.Add(dir)
	}

	for {
		select {
		case <-f.shutSig.CloseAtLeisureChan():
			return
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			f.errors <- err
		case e, ok := <-w.Events:
			if !ok {
				return
			}
			if e.Name != "" {
				switch {
				case e.Op.Has(fsnotify.Create):
					for _, patternPath := range f.pathsGlobs {
						matched, _ := path.Match(patternPath, e.Name)
						if matched {
							_, ok := files[e.Name]
							if !ok {
								f.addFile(ctx, e.Name)
							}
						}
					}
				case e.Op.Has(fsnotify.Write):
					trail, ok := files[e.Name]
					if ok {
						if trail.backoff == nil {
							trail.backoff = &backoff{
								timer: time.AfterFunc(waitFor, func() {
									trail.hasUpdates <- true
									trail.backoff.count = 0
								}),
							}
						} else if trail.backoff.count > 100 {
							trail.backoff.timer.Stop()
							trail.hasUpdates <- true
							trail.backoff.count = 0
						} else {
							trail.backoff.timer.Reset(waitFor)
							trail.backoff.count++
						}
					}
					// note we're ignoring chmod, rename and remove events here
				}
			}
		}
	}
}

func (f *fileConsumer) Connect(ctx context.Context) error {
	matchedFiles, err := filepath.Globs(f.nm.FS(), f.pathsGlobs)
	if err != nil {
		return err
	}
	if f.follow {
		var dirGlobs []string
		for _, glob := range f.pathsGlobs {
			dirGlobs = append(dirGlobs, path.Dir(glob))
		}
		matchedDirs, err := filepath.Globs(f.nm.FS(), dirGlobs)
		if err != nil {
			return err
		}
		w, err := fsnotify.NewWatcher()
		if err != nil {
			return err
		}
		f.watcher = w
		go f.notifyLoop(f.watcher, matchedFiles, matchedDirs)
	}
	return nil
}

func (f *fileConsumer) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case err, ok := <-f.errors:
			if !ok {
				return nil, nil, component.ErrTypeClosed
			}
			return nil, nil, err
		case msg, ok := <-f.messages:
			if !ok {
				return nil, nil, component.ErrTypeClosed
			}
			return msg.parts, msg.ackFcn, msg.err
		}
	}
}

func (f *fileConsumer) Close(ctx context.Context) (err error) {
	f.shutSig.CloseAtLeisure()
	select {
	case <-f.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return
}
