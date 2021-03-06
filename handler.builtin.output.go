package gocassos

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

func (h *HttpOutputHandler) pretty_chunk_status() string {
	pretty_chunk := make([]uint8, h.total_chunks)
	for idx, value := range h.chunk_status {
		pretty_chunk[idx] = uint8('_')
		switch {
		case int64(idx) <= h.waiting_for_chunk:
			pretty_chunk[idx] = uint8('D')
		case int64(idx) > h.waiting_for_chunk && value:
			pretty_chunk[idx] = uint8('w')
		}
	}
	return string(pretty_chunk)
}

func (h *HttpOutputHandler) streamer() {
	defer h.in_progress.Done()

	h.waiting_for_chunk = 0

	for {
		chunk, open := <-h.pipeline
		if !open {
			NVM.Printf("[%s] HTTP_STREAN: Pipeline to the %s streamer closed on chunk %d/%d- aborting", h.o.ClientId, h.o.FullName(), h.waiting_for_chunk, h.total_chunks)
			return
		}
		h.chunk_status[chunk] = true
		if NVM.Enabled() {
			NVM.Printf("[%s] HTTP_STREAM: Chunk status for %s/%d: [%s]", h.o.ClientId, h.o.id, chunk, h.pretty_chunk_status())
		}
		for check_chunk := h.waiting_for_chunk; check_chunk < h.total_chunks; check_chunk++ {
			if h.chunk_status[check_chunk] == true {
				NVM.Printf("[%s] HTTP_STREAM: Sending %s chunk %d/%d", h.o.ClientId, h.o.id, check_chunk, h.total_chunks)
				h.waiting_for_chunk = check_chunk + 1
				h.t.Seek(check_chunk*h.o.ChunkSize, 0)
				_, err := io.CopyN(h.w, h.t, h.o.ChunkSize)
				if err != nil {
					if err == io.EOF && h.waiting_for_chunk == h.total_chunks {
						// streamed last chunk
						BTW.Printf("[%s] HTTP_STREAM: Finished streaming for %s (took %0.3fs) %s", h.o.ClientId, h.o.id, time.Since(h.o.fetch_start).Seconds(), err)
						return
					}

					FUUU.Printf("[%s] HTTP_STREAM: Error streaming %s %d/%d - %s", h.o.ClientId, h.o.id, check_chunk, h.total_chunks, err)
					h.o.failure = err
					h.failure = err
					return
				}
			} else {
				// no next-in-line chunks ; wait for next piece
				NVM.Printf("[%s] HTTP_STREAM: Waiting more pieces for %s", h.o.ClientId, h.o.id)
				break
			}
		}
	}

}

func (h *HttpOutputHandler) stream_output(payload *[]uint8, offset *int64) (int, error) {
	if h == nil {
		return 0, ErrNullReference
	}
	l, err := h.t.WriteAt(*payload, *offset)
	if err != nil {
		h.o.failure = err
		h.failure = err
	} else {
		chunk := h.mark_chunk_ready(offset)
		h.pipeline <- chunk
	}
	return l, err
}

func (h *HttpOutputHandler) mark_chunk_ready(offset *int64) int64 {
	chunk := int64((*offset) / h.o.ChunkSize)
	h.chunk_status[chunk] = true
	return chunk
}

func (h *HttpOutputHandler) buffered_output(payload *[]uint8, offset *int64) (int, error) {
	if h == nil {
		return 0, ErrNullReference
	}
	l, err := h.t.WriteAt(*payload, *offset)
	h.mark_chunk_ready(offset)
	return l, err
}

func (h *HttpOutputHandler) send_output_headers() {
	// Do not touch content-type in case it was already set
	if h.w.Header().Get("content-type") == "" {
		h.w.Header().Set("Content-Type", "application/octet-stream")
	}
	h.w.Header().Add("ETag", fmt.Sprintf("%s", h.o.Nodetag))
	h.w.Header().Add("Last-Modified", time.Unix(h.o.Updated, 0).Format(time.RFC1123))
	h.w.Header().Add("Content-Length", fmt.Sprintf("%d", h.o.ObjectSize))
}

// Check if we received all chunks
func (h *HttpOutputHandler) check_chunk_status() bool {
	if h.waiting_for_chunk+1 != h.total_chunks {
		return false
	}
	for idx, chunk_status := range h.chunk_status {
		if chunk_status != true {
			h.o.failure = fmt.Errorf("Missing chunk %d/%d", idx, h.total_chunks)
			h.failure = h.o.failure

			BTW.Printf("[%s] HTTP_OUTPUT: Missing chunk %d on %s - aborting", h.o.ClientId, idx, h.o.id)
			return false
		}
	}
	return true
}

// Signals transfer end; should trigger output flush for batch transfers
func (h *HttpOutputHandler) Close() error {
	if h == nil {
		return ErrNullReference
	}
	// Protect Close() so you can both evaluate the error and use it as a deferred cleanup
	h.mu.Lock()
	if h.already_closed {
		h.mu.Unlock() // unlock ASAP so other routines can also Close() in case of errors
		return errors.New("Already closed")
	}
	h.already_closed = true
	h.mu.Unlock()

	defer h.o.cfg.in_progress.Done()
	defer h.t.Close()

	switch h.mode {
	case StreamMode:
		NVM.Printf("[%s] HTTP_STREAM: Received all chunks and waiting end of streaming for %s", h.o.ClientId, h.o.FullName())
		close(h.pipeline)
		h.in_progress.Wait()
		h.check_chunk_status()
		if h.failure != nil {
			FUUU.Printf("[%s] HTTP_STREAM: Aborting streaming of %s due to failure (took %0.3fs)", h.o.ClientId, h.o.id, time.Since(h.o.fetch_start).Seconds())
			return errors.New("Failed to retrieve from the backend")
		}

	case BatchMode:
		h.check_chunk_status()

		if h.failure != nil {
			FUUU.Printf("[%s] HTTP_BUFFER: Returning NotFound due to failure for %s (took %0.3fs)", h.o.ClientId, h.o.id, time.Since(h.o.fetch_start).Seconds())
			http.NotFound(h.w, h.r)
			return errors.New("Failed to retrieve from the backend")
		}
		FYI.Printf("[%s] HTTP_BUFFER: Sending %s to client (took %0.3fs)", h.o.ClientId, h.o.id, time.Since(h.o.fetch_start).Seconds())
		h.send_output_headers()
		h.t.Seek(0, 0)
		io.Copy(h.w, h.t)
		return nil
	default:
		return errors.New("Unknown transfer mode")
	}
	return nil
}

func (h *HttpOutputHandler) WriteAt(payload []byte, offset int64) (int, error) {
	switch h.mode {
	case StreamMode:
		return h.stream_output(&payload, &offset)
	case BatchMode:
		return h.buffered_output(&payload, &offset)
	}
	return 0, errors.New("Unknown transfer mode")
}

func (o *Object) NewHttpOutputHandler(w http.ResponseWriter, r *http.Request, mode int) error {
	if mode != StreamMode && mode != BatchMode {
		return errors.New("Unknown transfer mode")
	}

	t, err := ioutil.TempFile("", "gocassos_temp_")
	if err != nil {
		return err
	}
	os.Remove(t.Name())

	h := new(HttpOutputHandler)
	h.mode = mode
	h.w = w
	h.r = r
	h.t = t
	h.o = o
	h.already_closed = false
	h.total_chunks = o.NumChunks
	// inline_payload support
	if h.total_chunks == 0 {
		h.total_chunks = 1
	}
	h.chunk_status = make([]bool, h.total_chunks)
	o.cfg.in_progress.Add(1)

	if mode == StreamMode {
		h.in_progress.Add(1)
		h.pipeline = make(chan int64, o.NumChunks)
		h.send_output_headers()
		go h.streamer()
	}
	o.OutputHandler = h
	return nil
}
