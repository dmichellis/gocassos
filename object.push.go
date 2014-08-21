package gocassos

import (
	"io"
	"path/filepath"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gocql/gocql"
)

func (c *ObjectStorage) PreparePush(client_identifier, objectname string) (*Object, error) {
	if c == nil {
		return nil, ErrNullReference
	}

	if !c.AllowUpdates {
		if o, _ := c.lookup(&client_identifier, &objectname, true); o != nil {
			FYI.Printf("[%s] PUSH: Refusing to update object %s (%0.3fs lookup)", client_identifier, objectname, o.LookupTime.Seconds())
			return nil, ErrRefused
		}
	}
	o := new(Object)
	o.ClientId = client_identifier
	o.Objectname = objectname
	o.cfg = c
	o.ChunkSize = c.ChunkSize
	o.Updated = int64(time.Now().Unix())
	o.Nodetag, _ = gocql.RandomUUID()
	o.set_id()
	return o, nil
}

func (o *Object) Push() error {
	if o == nil {
		return ErrNullReference
	}
	o.cfg.in_progress.Add(1)
	defer o.cfg.in_progress.Done()
	defer o.InputHandler.Close()
	defer func() {
		BTW.Printf("[%s] PUSH_%s: %s (lookup:%0.4fs push:%0.4fs)", o.ClientId, o.Status(), o.id, o.LookupTime.Seconds(), o.PushTime.Seconds())
	}()

	o.push_start = time.Now()
	defer func() {
		o.PushTime = time.Since(o.push_start)
	}()

	if ttl := o.Ttl(); ttl != 0 {
		BTW.Printf("[%s] PUSH: Starting for %s (expires in %ds)", o.ClientId, o.id, ttl)
	} else {
		BTW.Printf("[%s] PUSH: Starting for %s", o.ClientId, o.id)
	}

	if o.cfg.ConcurrentPutsPerObj > 0 {
		o.pusher_control = make(chan struct{}, o.cfg.ConcurrentPutsPerObj)
	}

	for {
		if o.pusher_control != nil {
			o.pusher_control <- struct{}{}
		}
		payload_ := make([]byte, o.ChunkSize)
		len, read_err := io.ReadAtLeast(o.InputHandler, payload_, int(o.ChunkSize))
		payload := payload_[:len]

		if read_err != nil && read_err != io.EOF && read_err != io.ErrUnexpectedEOF {
			FUUU.Printf("[%s] OBJECT_PUSH: Failed to read chunk %d on %s (%s) - aborting", o.ClientId, o.NumChunks, o.id, read_err)
			o.Remove()
			close(o.pusher_control)
			return nil
		}
		o.in_progress.Add(1)
		go o.push_chunk(o.NumChunks, &payload)

		o.NumChunks++
		o.ObjectSize += int64(len)
		if read_err == io.EOF || read_err == io.ErrUnexpectedEOF {
			BTW.Printf("[%s] OBJECT_PUSH: Finished reading chunks for %s", o.ClientId, o.id)
			break
		}
	}

	o.in_progress.Wait()
	return o.push_metadata()
}

func (o *Object) push_metadata() error {
	if o.failure {
		FUUU.Printf("[%s] OBJECT_PUSH: Aborting push for %s", o.ClientId, o.id)
		o.Remove()
		return ErrChunksFailed
	}

	var err error

	for idx, cons := range o.cfg.write_consistency {
		if err = o.cfg.Conn.Query(`INSERT INTO objects (objectname, updated, nodetag, num_chunks, object_size, chunk_size, path) VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL ?`, o.Objectname, o.Updated, o.Nodetag, o.NumChunks, o.ObjectSize, o.ChunkSize, filepath.Dir(o.Objectname), o.Ttl()).Consistency(cons).Exec(); err != nil {
			WTF.Printf("[%s] OBJECT_PUSH: Failed pushing metadata for %s with consistency %s (%s) - will retry with a different consistency", o.ClientId, o.id, o.cfg.write_consistency_str[idx], err)
		} else {
			if idx > 0 {
				FYI.Printf("[%s] OBJECT_PUSH: Succeeded pushing %s with consistency %s", o.ClientId, o.id, o.cfg.write_consistency_str[idx])
			}
			break
		}
	}
	if err != nil {
		FUUU.Printf("[%s] OBJECT_PUSH: write failed %s (%s) - aborting", o.ClientId, o.id, err)
		o.failure = true
		o.Remove()
		return err
	}
	o.CleanupDupes()
	FYI.Printf("[%s] OBJECT_PUSH: Pushed %s to the backend (%d chunks, %s bytes)", o.ClientId, o.id, o.NumChunks, humanize.Comma(o.ObjectSize))
	return nil
}

func (o *Object) push_chunk(chunk int64, payload *[]byte) {
	defer func() {
		if o.pusher_control != nil {
			<-o.pusher_control
		}
	}()
	defer o.in_progress.Done()

	var err error
	for idx, cons := range o.cfg.write_consistency {
		if err = o.cfg.Conn.Query(`INSERT INTO object_chunks (objectname, updated, nodetag, chunk_num, payload) VALUES (?, ?, ?, ?, ?) USING TTL ?`, o.Objectname, o.Updated, o.Nodetag, chunk, payload, o.Ttl()).Consistency(cons).Exec(); err != nil {
			WTF.Printf("[%s] OBJECT_PUSH: Failed pushing chunk %d on %s with consistency %s (%s) - will retry with a different consistency", o.ClientId, chunk, o.id, o.cfg.write_consistency_str[idx], err)
		} else {
			if idx > 0 {
				FYI.Printf("[%s] OBJECT_PUSH: Succeeded for chunk %d on %s with consistency %s", o.ClientId, chunk, o.id, o.cfg.write_consistency_str[idx])
			}
			break
		}
	}
	if err != nil {
		FUUU.Printf("[%s] OBJECT_PUSH: write failed for chunk %d on %s (%s) - aborting", o.ClientId, chunk, o.id, err)
		o.failure = true
	}
	return
}
