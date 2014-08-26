package gocassos

import (
	"fmt"
	"time"
)

func (c *ObjectStorage) Lookup(client_identifier, objectname string) (*Object, error) {
	return c.lookup(&client_identifier, &objectname, false)
}

func (c *ObjectStorage) lookup(client_identifier, objectname *string, internal bool) (*Object, error) {
	if c.Conn == nil {
		return nil, ErrCassandraNotConnected
	}

	o := new(Object)
	o.ClientId = *client_identifier
	o.cfg = c
	o.lookup_start = time.Now()
	defer func() {
		o.LookupTime = time.Since(o.lookup_start)
	}()

	if !internal {
		BTW.Printf("[%s] LOOKUP: Looking for object '%s'", o.ClientId, *objectname)
	}

	var err error
	var ttl int64
	for index, cons := range c.read_consistency {
		if err = c.Conn.Query(`SELECT objectname, updated, nodetag, num_chunks, chunk_size, object_size, TTL(num_chunks), metadata FROM objects WHERE objectname = ? ORDER BY updated DESC LIMIT 1`, *objectname).Consistency(cons).Scan(&o.Objectname, &o.Updated, &o.Nodetag, &o.NumChunks, &o.ChunkSize, &o.ObjectSize, &ttl, &o.Metadata); err != nil {
			if !internal {
				WTF.Printf("[%s] LOOKUP: Consistency '%s' returned '%s' for %s", o.ClientId, c.read_consistency_str[index], err, *objectname)
			}
		} else {
			if index > 0 {
				if !internal {
					FYI.Printf("[%s] LOOKUP: Consistency '%s' SUCCEEDED for %s", o.ClientId, c.read_consistency_str[index], *objectname)
				}
			}
			// set up assorted internal stuff here
			o.failure = false
			o.set_id()
			if ttl != 0 {
				o.Expiration = time.Unix(time.Now().Unix()+ttl, 0)
			}
			return o, nil
		}
	}
	return nil, ErrNotFound
}

func (o *Object) set_id() {
	o.id = fmt.Sprintf("['%s' %d %s]", o.Objectname, o.Updated, o.Nodetag)
}
