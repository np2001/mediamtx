package delaysource

import (
	"context"
	"fmt"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/description"

	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/stream"
)

type Parent interface {
	logger.Writer

	StaticSourceHandlerSetReady(context.Context, defs.PathSourceStaticSetReadyReq)
	StaticSourceHandlerSetNotReady(context.Context, defs.PathSourceStaticSetNotReadyReq)
}

type Source struct {
	ParentCtx   context.Context
	PathName    string
	SourcePath  string
	Delay       time.Duration
	PathManager PathManager
	Parent      Parent

	ctx       context.Context
	ctxCancel context.CancelFunc
	done      chan struct{}

	hub       *Hub
	subStream *stream.SubStream
}

func (s *Source) Initialize() {
	s.ctx, s.ctxCancel = context.WithCancel(s.ParentCtx)
	s.done = make(chan struct{})
}

func (s *Source) Start() {
	go s.run()
}

func (s *Source) Close() {
	if s.ctxCancel != nil {
		s.ctxCancel()
	}

	if s.done != nil {
		<-s.done
	}
}

func (s *Source) Log(level logger.Level, format string, args ...any) {
	s.Parent.Log(level, "[delay source "+s.PathName+" <- "+s.SourcePath+"] "+format, args...)
}

func (s *Source) APISourceDescribe() *defs.APIPathSource {
	return &defs.APIPathSource{
		Type: defs.APIPathSourceTypeDelaySource,
		ID:   s.SourcePath,
	}
}

func (s *Source) run() {
	defer close(s.done)

	err := s.runInner()
	if err != nil && s.ctx.Err() == nil {
		s.Log(logger.Error, "%v", err)
	}

	if s.subStream != nil {
		res := make(chan struct{})

		s.Parent.StaticSourceHandlerSetNotReady(s.ctx, defs.PathSourceStaticSetNotReadyReq{
			Res: res,
		})

		<-res
	}

	if s.hub != nil {
		releaseHub(s.hub)
	}
}

func (s *Source) runInner() error {
	s.hub = acquireHub(
		s.ParentCtx,
		s.SourcePath,
		s.Delay,
		s.PathManager,
		s.Parent,
	)

	desc, err := s.hub.WaitReady(s.ctx)
	if err != nil {
		return err
	}

	err = s.setReady(desc)
	if err != nil {
		return err
	}

	return s.writerLoop()
}

func (s *Source) setReady(desc *description.Session) error {
	res := make(chan defs.PathSourceStaticSetReadyRes)

	s.Parent.StaticSourceHandlerSetReady(s.ctx, defs.PathSourceStaticSetReadyReq{
		Desc:          desc,
		UseRTPPackets: true,
		ReplaceNTP:    false,
		Res:           res,
	})

	select {
	case r := <-res:
		if r.Err != nil {
			return r.Err
		}

		s.subStream = r.SubStream
		return nil

	case <-s.ctx.Done():
		return fmt.Errorf("terminated")
	}
}

func (s *Source) writerLoop() error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	cursor := &Cursor{}

	for {
		select {
		case <-ticker.C:
			target := time.Now().Add(-s.Delay)
			items := s.hub.Snapshot()
			ready := cursor.ReadyItems(items, target)

			for i := range ready {
				item := ready[i]
				if item.Unit == nil {
					continue
				}

				s.subStream.WriteUnit(item.Media, item.Format, item.Unit)
			}

		case <-s.ctx.Done():
			return fmt.Errorf("terminated")
		}
	}
}
