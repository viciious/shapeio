package shapeio

import (
	"context"
	"io"
	"time"

	"golang.org/x/time/rate"
)

type Reader struct {
	r       io.ReadCloser
	limiter *rate.Limiter
	ctx     context.Context
	limit   int
	firstOp bool
}

type Writer struct {
	w       io.Writer
	limiter *rate.Limiter
	ctx     context.Context
	limit   int
	firstOp bool
}

// NewReader returns a reader that implements io.Reader with rate limiting.
func NewReader(r io.Reader) *Reader {
	return &Reader{
		r:   io.NopCloser(r),
		ctx: context.Background(),
	}
}

// NewReaderWithContext returns a reader that implements io.Reader with rate limiting.
func NewReaderWithContext(r io.Reader, ctx context.Context) *Reader {
	return &Reader{
		r:   io.NopCloser(r),
		ctx: ctx,
	}
}

// NewReadCloser returns a reader that implements io.ReadCloser with rate limiting.
func NewReadCloser(r io.ReadCloser) *Reader {
	return &Reader{
		r:   r,
		ctx: context.Background(),
	}
}

// NewReadCloserWithContext returns a reader that implements io.ReadCloser with rate limiting.
func NewReadCloserWithContext(r io.ReadCloser, ctx context.Context) *Reader {
	return &Reader{
		r:   r,
		ctx: ctx,
	}
}

// NewWriter returns a writer that implements io.Writer with rate limiting.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:   w,
		ctx: context.Background(),
	}
}

// NewWriterWithContext returns a writer that implements io.Writer with rate limiting.
func NewWriterWithContext(w io.Writer, ctx context.Context) *Writer {
	return &Writer{
		w:   w,
		ctx: ctx,
	}
}

// SetRateLimit sets rate limit (bytes/sec) to the reader.
func (s *Reader) SetRateLimit(bytesPerSec float64) {
	s.limit = int(bytesPerSec)
	s.limiter = rate.NewLimiter(rate.Limit(bytesPerSec), s.limit)
	s.firstOp = true
}

// Read reads bytes into p.
func (s *Reader) Read(p []byte) (int, error) {
	if s.limiter == nil {
		return s.r.Read(p)
	}

	if s.firstOp {
		s.firstOp = false
		s.limiter.AllowN(time.Now(), s.limit) // spend initial burst
	}

	for i := 0; i < len(p); {
		rem := len(p) - i
		limit := s.limit
		if limit > rem {
			limit = rem
		}

		n, err := s.r.Read(p[i : i+limit])
		if err != nil {
			return i + n, err
		}
		if err := s.limiter.WaitN(s.ctx, n); err != nil {
			return i + n, err
		}
		i += limit
	}
	return len(p), nil
}

// Read closes the reader
func (s *Reader) Close() error {
	return s.r.Close()
}

// SetRateLimit sets rate limit (bytes/sec) to the writer.
func (s *Writer) SetRateLimit(bytesPerSec float64) {
	s.limit = int(bytesPerSec)
	s.limiter = rate.NewLimiter(rate.Limit(bytesPerSec), s.limit)
	s.firstOp = true
}

// Write writes bytes from p.
func (s *Writer) Write(p []byte) (int, error) {
	if s.limiter == nil {
		return s.w.Write(p)
	}

	if s.firstOp {
		s.firstOp = false
		s.limiter.AllowN(time.Now(), s.limit) // spend initial burst
	}

	for i := 0; i < len(p); {
		rem := len(p) - i
		limit := s.limit
		if limit > rem {
			limit = rem
		}

		n, err := s.w.Write(p[i : i+limit])
		if err != nil {
			return i + n, err
		}
		if err := s.limiter.WaitN(s.ctx, n); err != nil {
			return i + n, err
		}
		i += limit
	}
	return len(p), nil
}
