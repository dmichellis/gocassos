package gocassos

import "io"

func (h *HttpInputHandler) RequestBodyInput(chunk, chunk_size int64) (*int64, *[]uint8, error) {
	if h == nil {
		return nil, nil, ErrNullReference
	}
	payload := make([]byte, chunk_size)
	size_, err := io.ReadAtLeast(h.r.Body, payload, int(chunk_size))
	size := int64(size_)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return &size, &payload, io.EOF
		}
		FUUU.Printf("[%s] INPUT_HTTP: Failed to read chunk %d (%d size) for %s (%s)", h.o.ClientId, chunk, chunk_size, h.o.id, err)
		return nil, nil, err
	}
	return &size, &payload, nil
}
