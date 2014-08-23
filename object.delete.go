package gocassos

import (
	"bytes"
	"time"

	"github.com/gocql/gocql"
)

func (o *Object) Remove() error {
	if !o.cfg.AllowUpdates {
		FYI.Printf("[%s] PUSH: Refusing to delete object %s (%0.3fs lookup)", o.ClientId, o.id, o.LookupTime.Seconds())
		return ErrRefused
	}
	o.cfg.in_progress.Add(1)
	NVM.Printf("REMOVE: Removing %s", o.id)
	go o.async_remove()
	return nil
}

func (o *Object) async_remove() {
	defer o.cfg.in_progress.Done()

	if err := o.cfg.Conn.Query(`DELETE FROM objects WHERE objectname = ? AND updated = ? AND nodetag = ?`, o.Objectname, o.Updated, o.Nodetag).Consistency(gocql.One).Exec(); err != nil {
		WTF.Printf("REMOVE: Failure removing %s - %s", o.id, err)
		return
	}
	//NVM.Printf("REMOVE: Removing chunks for %s", o.id)
	for chunk := int64(0); chunk < o.NumChunks; chunk++ {
		//		NVM.Printf("REMOVE: Removing chunk %d/%d for %s", chunk, o.NumChunks, o.id)
		if err := o.cfg.Conn.Query(`DELETE FROM object_chunks WHERE objectname = ? AND updated = ? AND nodetag = ? AND chunk_num = ? `, o.Objectname, o.Updated, o.Nodetag, chunk).Consistency(gocql.One).Exec(); err != nil {
			WTF.Printf("REMOVE: Error removing chunk %d/%d on %s - %s", chunk, o.NumChunks, o.id, err)
			break
		}
	}
	NVM.Printf("REMOVE: Done removing chunks for %s", o.id)
	return
}

func (o *Object) CleanupDupes() {
	o.cfg.in_progress.Add(1)
	go o.async_cleanup_dupes()
	return
}

func (o *Object) async_cleanup_dupes() {
	defer o.cfg.in_progress.Done()

	time.Sleep(time.Duration(o.cfg.ScrubGraceTime) * time.Second)
	BTW.Printf("SCRUB: Cleaning up duplicates for %s", o.Objectname)
	iter := o.cfg.Conn.Query(`SELECT objectname, updated, nodetag, num_chunks FROM objects WHERE objectname = ? `, o.Objectname).Consistency(gocql.One).Iter()

	var latest, obj, tmp *Object
	latest = o
	var objectname string
	var updated, num_chunks int64
	var nodetag gocql.UUID
	for iter.Scan(&objectname, &updated, &nodetag, &num_chunks) {
		obj = new(Object)
		obj.cfg = o.cfg
		obj.Objectname = objectname
		obj.Updated = updated
		obj.Nodetag = nodetag
		obj.NumChunks = num_chunks
		obj.set_id()
		switch {
		case obj.Updated > latest.Updated: // newer timestamp
			tmp = latest
			latest = obj
			obj = tmp
		case obj.Updated == latest.Updated: //same timestamp; compare nodetags
			switch bytes.Compare(obj.Nodetag.Bytes(), latest.Nodetag.Bytes()) {
			case 0: // same object
				obj = nil
			case 1: // obj nodetag greater than latest
				tmp = latest
				latest = obj
				obj = tmp
			}
		}
		// made this far, obj must be removed
		if obj != nil {
			BTW.Printf("SCRUB: Dropping duplicated object %s", obj.id)
			obj.Remove()
		}
	}
	latest.set_id()
	if obj != nil {
		BTW.Printf("SCRUB: Keeping object %s", latest.id)
	}
	return
}
