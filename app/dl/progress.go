package dl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"github.com/go-faster/errors"

	"github.com/iyear/tdl/core/downloader"
	"github.com/iyear/tdl/core/util/fsutil"
)

type progress struct {
	mu   sync.Mutex // guards stdout so concurrent log lines don't interleave
	opts Options

	it *iter
}

func newProgress(it *iter, opts Options) *progress {
	return &progress{
		opts: opts,
		it:   it,
	}
}

// logf prints a single log line to stdout, prefixed with a timestamp.
func (p *progress) logf(format string, args ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Printf("%s %s\n", time.Now().Format("2006-01-02 15:04:05"), fmt.Sprintf(format, args...))
}

func (p *progress) OnAdd(elem downloader.Elem) {
	p.logf("start: %s", p.elemString(elem))
}

func (p *progress) OnDownload(_ downloader.Elem, _ downloader.ProgressState) {
	// no progress bar, nothing to do per chunk
}

func (p *progress) OnDone(elem downloader.Elem, err error) {
	e := elem.(*iterElem)

	if cerr := e.to.Close(); cerr != nil {
		p.fail(elem, errors.Wrap(cerr, "close file"))
		return
	}

	if err != nil {
		if !errors.Is(err, context.Canceled) { // don't report user cancel
			p.fail(elem, errors.Wrap(err, "progress"))
		}
		_ = os.Remove(e.to.Name()) // just try to remove temp file, ignore error
		return
	}

	p.it.Finish(e.logicalPos)

	if err := p.donePost(e); err != nil {
		p.fail(elem, errors.Wrap(err, "post file"))
		return
	}

	p.logf("done: %s", p.elemString(elem))
}

func (p *progress) donePost(elem *iterElem) error {
	newfile := strings.TrimSuffix(filepath.Base(elem.to.Name()), tempExt)

	if p.opts.RewriteExt {
		mime, err := mimetype.DetectFile(elem.to.Name())
		if err != nil {
			return errors.Wrap(err, "detect mime")
		}
		ext := mime.Extension()
		if ext != "" && (filepath.Ext(newfile) != ext) {
			newfile = fsutil.GetNameWithoutExt(newfile) + ext
		}
	}

	newpath := filepath.Join(filepath.Dir(elem.to.Name()), newfile)
	if err := os.Rename(elem.to.Name(), newpath); err != nil {
		return errors.Wrap(err, "rename file")
	}

	// Set file modification time to message date if available
	if elem.file.Date > 0 {
		fileTime := time.Unix(elem.file.Date, 0)
		if err := os.Chtimes(newpath, fileTime, fileTime); err != nil {
			return errors.Wrap(err, "set file time")
		}
	}

	return nil
}

func (p *progress) fail(elem downloader.Elem, err error) {
	p.logf("failed: %s error: %s", p.elemString(elem), err.Error())
}

func (p *progress) elemString(elem downloader.Elem) string {
	e := elem.(*iterElem)
	return fmt.Sprintf("%s(%d):%d -> %s",
		e.from.VisibleName(),
		e.from.ID(),
		e.fromMsg.ID,
		strings.TrimSuffix(e.to.Name(), tempExt))
}
