package gocassos

import (
	"errors"
	"fmt"
	"time"
)

// TODO
func (o *Object) Fetch() error {
	if o == nil {
		return ErrNullReference
	}
	if o.OutputHandler == nil {
		return errors.New("Invalid output handler")
	}

	o.cfg.in_progress.Add(1)
	defer o.cfg.in_progress.Done()
	defer func() {
		BTW.Printf("[%s] FETCH_%s: %s (lookup:%0.4fs fetch:%0.4fs)", o.ClientId, o.Status(), o.id, o.LookupTime.Seconds(), o.FetchTime.Seconds())
	}()
	defer o.OutputHandler.Close()

	if o.cfg.ConcurrentGetsPerObj > 0 {
		o.fetcher_control = make(chan struct{}, o.cfg.ConcurrentGetsPerObj)
	}

	o.fetch_start = time.Now()
	defer func() {
		o.FetchTime = time.Since(o.fetch_start)
	}()

	if o.cfg.ConcurrentGetsPerObj > 0 {
		o.fetcher_control = make(chan struct{}, o.cfg.ConcurrentGetsPerObj)
	}

	var chunk int64
	for chunk = 0; chunk < o.NumChunks && o.failure == nil; chunk++ {
		// Limit # of in flight requests per object fetch
		if o.fetcher_control != nil {
			o.fetcher_control <- struct{}{}
		}
		o.in_progress.Add(1)
		go o.fetch_chunk(chunk)
	}
	o.in_progress.Wait()

	if o.failure != nil {
		return o.failure
	}

	if close_err := o.OutputHandler.Close(); close_err != nil {
		return close_err
	}

	return nil
}

// TODO
func (o *Object) fetch_chunk(chunk int64) {
	defer o.in_progress.Done()
	defer func() {
		if o.fetcher_control != nil {
			<-o.fetcher_control
		}
	}()

	var payload []uint8
	var err error
	// NVM.Printf("fetching %d", chunk)
	for index, cons := range o.cfg.read_consistency {
		if err = o.cfg.Conn.Query(`SELECT payload FROM object_chunks WHERE objectname = ? AND updated = ? AND nodetag = ? AND chunk_num = ?`, o.Objectname, o.Updated, o.Nodetag, chunk).Consistency(cons).Scan(&payload); err != nil {
			WTF.Printf("[%s] FETCH: %s consistency failed to retrive chunk %d/%d for %s (%s) (will retry with a different consistency)", o.ClientId, o.cfg.read_consistency_str[index], chunk, o.NumChunks, o.id, err)
		} else {
			if index > 0 {
				FYI.Printf("[%s] FETCH: %s consistency SUCCEEDED to retrive chunk %d/%d for %s", o.ClientId, o.cfg.read_consistency_str[index], chunk, o.NumChunks, o.id)
			}
			break
		}
	}
	if err != nil {
		FUUU.Printf("[%s] FETCH: Failed to retrive chunk %d/%d for %s (%s) - aborting", o.ClientId, chunk, o.NumChunks, o.id, err)
		o.failure = fmt.Errorf("Failed to retrieve chunk %d/%d", chunk, o.NumChunks)
		return
	} else {
		if _, err_out := o.OutputHandler.WriteAt(payload, chunk*o.ChunkSize); err_out != nil {
			o.failure = err_out
			FUUU.Printf("[%s] FETCH: Output handler returned error '%s' for chunk %d/%d on %s - aborting", o.ClientId, err_out, chunk, o.NumChunks, o.id)
		}
	}
	if o.finished_chunks != nil {
		o.finished_chunks <- chunk
	}
}
