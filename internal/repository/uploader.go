package repository

import (
	"context"
	"io"
	"sync"
)

// uploadPool bounds both concurrency and queued disk usage. submit blocks when
// all workers are occupied; the scanner can prepare just one additional pack.
type uploadPool struct {
	ctx    context.Context
	cancel context.CancelFunc
	slots  chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	err    error
}

func newUploadPool(ctx context.Context, n int) *uploadPool {
	ctx, cancel := context.WithCancel(ctx)
	return &uploadPool{ctx: ctx, cancel: cancel, slots: make(chan struct{}, n)}
}

func (p *uploadPool) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	return p.ctx.Err()
}

func (p *uploadPool) submit(job func(context.Context) error) error {
	if err := p.Err(); err != nil {
		return err
	}
	select {
	case p.slots <- struct{}{}:
	case <-p.ctx.Done():
		return p.Err()
	}
	if err := p.Err(); err != nil {
		<-p.slots
		return err
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer func() { <-p.slots }()
		if err := job(p.ctx); err != nil {
			p.mu.Lock()
			if p.err == nil {
				p.err = err
			}
			p.mu.Unlock()
			p.cancel()
		}
	}()
	return nil
}

func (p *uploadPool) wait() error { p.wg.Wait(); return p.Err() }
func (p *uploadPool) close()      { p.cancel(); p.wg.Wait() }

type synchronizedWriter struct {
	mu  sync.Mutex
	out io.Writer
}

func (w *synchronizedWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.out.Write(b)
}
