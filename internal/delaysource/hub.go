package delaysource

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"

	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/stream"
	"github.com/bluenviron/mediamtx/internal/unit"
)

type PathManager interface {
	AddReader(req defs.PathAddReaderReq) (*defs.PathAddReaderRes, error)
}

type Hub struct {
	sourcePath string

	parentCtx   context.Context
	ctx         context.Context
	ctxCancel   context.CancelFunc
	pathManager PathManager
	parent      logger.Writer

	mutex    sync.RWMutex
	refCount int
	maxDelay time.Duration
	nextSeq  uint64
	items    []BufferedUnit
	desc     *description.Session
	readyErr error
	ready    chan struct{}
	done     chan struct{}

	reader    *stream.Reader
	sourceRes *defs.PathAddReaderRes
}

var (
	hubsMutex sync.Mutex
	hubs      = make(map[string]*Hub)
)

func acquireHub(
	parentCtx context.Context,
	sourcePath string,
	delay time.Duration,
	pathManager PathManager,
	parent logger.Writer,
) *Hub {
	hubsMutex.Lock()
	defer hubsMutex.Unlock()

	h, ok := hubs[sourcePath]
	if !ok {
		ctx, ctxCancel := context.WithCancel(parentCtx)

		h = &Hub{
			sourcePath:  sourcePath,
			parentCtx:   parentCtx,
			ctx:         ctx,
			ctxCancel:   ctxCancel,
			pathManager: pathManager,
			parent:      parent,
			maxDelay:    delay + 5*time.Second,
			ready:       make(chan struct{}),
			done:        make(chan struct{}),
		}

		hubs[sourcePath] = h
		go h.run()
	}

	h.refCount++

	h.mutex.Lock()
	if delay+5*time.Second > h.maxDelay {
		h.maxDelay = delay + 5*time.Second
	}
	h.mutex.Unlock()

	return h
}

func releaseHub(h *Hub) {
	hubsMutex.Lock()
	defer hubsMutex.Unlock()

	h.refCount--
	if h.refCount > 0 {
		return
	}

	delete(hubs, h.sourcePath)
	h.ctxCancel()
	<-h.done
}

func (h *Hub) Log(level logger.Level, format string, args ...any) {
	h.parent.Log(level, "[delay hub "+h.sourcePath+"] "+format, args...)
}

func (h *Hub) Close() {
	h.ctxCancel()
}

func (h *Hub) APIReaderDescribe() *defs.APIPathReader {
	return &defs.APIPathReader{
		Type: defs.APIPathReaderTypeHidden,
		ID:   h.sourcePath,
	}
}

func (h *Hub) WaitReady(ctx context.Context) (*description.Session, error) {
	select {
	case <-h.ready:
		h.mutex.RLock()
		defer h.mutex.RUnlock()

		if h.readyErr != nil {
			return nil, h.readyErr
		}

		return h.desc, nil

	case <-ctx.Done():
		return nil, fmt.Errorf("terminated")
	}
}

func (h *Hub) Snapshot() []BufferedUnit {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	out := make([]BufferedUnit, len(h.items))
	copy(out, h.items)
	return out
}

func (h *Hub) run() {
	defer close(h.done)

	err := h.runInner()

	h.mutex.Lock()
	if h.desc == nil && h.readyErr == nil {
		h.readyErr = err
		close(h.ready)
	}
	h.mutex.Unlock()
}

func (h *Hub) runInner() error {
	res, err := h.pathManager.AddReader(defs.PathAddReaderReq{
		Author: h,
		AccessRequest: defs.PathAccessRequest{
			Name:     h.sourcePath,
			SkipAuth: true,
		},
		Res: make(chan defs.PathAddReaderRes),
	})
	if err != nil {
		return err
	}
	if res.Err != nil {
		return res.Err
	}

	h.sourceRes = res
	sourceStream := res.Stream
	if sourceStream == nil {
		return fmt.Errorf("source path '%s' has no stream", h.sourcePath)
	}

	h.reader = &stream.Reader{
		SkipOutboundBytes: true,
		Parent:            h,
	}

	for _, medi := range sourceStream.Desc.Medias {
		for _, forma := range medi.Formats {
			medi := medi
			forma := forma

			h.reader.OnData(medi, forma, func(u *unit.Unit) error {
				h.push(BufferedUnit{
					ReceivedAt: time.Now(),
					Media:      medi,
					Format:     forma,
					Unit:       cloneUnit(u),
					KeyFrame:   isKeyFrame(medi, forma, u),
				})
				return nil
			})
		}
	}

	sourceStream.AddReader(h.reader)

	h.mutex.Lock()
	h.desc = sourceStream.Desc
	close(h.ready)
	h.mutex.Unlock()

	defer func() {
		sourceStream.RemoveReader(h.reader)

		if h.sourceRes != nil && h.sourceRes.Path != nil {
			h.sourceRes.Path.RemoveReader(defs.PathRemoveReaderReq{
				Author: h,
			})
		}
	}()

	select {
	case err := <-h.reader.Error():
		return err

	case <-h.ctx.Done():
		return fmt.Errorf("terminated")
	}
}

func (h *Hub) push(item BufferedUnit) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.nextSeq++
	item.Seq = h.nextSeq

	h.items = append(h.items, item)

	cutoff := time.Now().Add(-h.maxDelay)

	first := 0
	for first < len(h.items) && h.items[first].ReceivedAt.Before(cutoff) {
		first++
	}

	if first > 0 {
		copy(h.items, h.items[first:])
		h.items = h.items[:len(h.items)-first]
	}
}
