gocassos
========

GO Cassandra Simple Object Storage

gocassos is a library for simple chunked object storage using Cassandra as a backend.

It supports pluggable input and output handlers so you can couple it with virtually everything, from a LHARD (an euphemism of mine for *L*ame *H*alf-*A*rsed *R*EST *D*aemon) to a complex protocol buffer implementation.


How to use
----------
```
package main

import (
	"os"
	"time"

	"github.com/dmichellis/gocassos"
	. "github.com/dmichellis/gocassos/logging"
    /* To use gocassos logging with the following loglevels:
    Kewl & Boring name
    FUUU | FATAL
    WTF  | ERROR
    FYI  | INFO
    BTW  | DEBUG
    NVM  | TRACE
    */
	"github.com/gocql/gocql"
)

func main() {
	c := new(gocassos.ObjectStorage)
	c.Init()

	cluster := gocql.NewCluster("localhost")
	cluster.Keyspace = "images"
	cluster.Consistency = gocql.One
	cluster.NumConns = 3
	cluster.RetryPolicy.NumRetries = 3
	cluster.Timeout = 3 * time.Second

	var err error
	if c.Conn, err = cluster.CreateSession(); err != nil {
		FUUU.Fatalf("Error connecting to cassandra: %s", err)
	}

	c.AllowUpdates = true
	c.ScrubGraceTime = 0
	gocassos.LogLevel = FYI.Level()
	o, err := c.Lookup("local cli", "/some/thing.dat")
	if err != nil {
		FUUU.Printf("Lookup failed for borf %s", ee)
	}
    FYI.Printf("Time to live for this object: %d", o.Ttl())

    o, err = c.PreparePush("local cli", "/other/thing.txt")
	if err != nil {
		FUUU.Fatalf("Failed: %s", err)
	}
	o.ChunkSize = 100000
    o.Expiration = time.Unix( time.Now().Unix() + 86400, 0 )
	f, err_f := os.Open("/local/file.txt")
	if err_f != nil {
		FUUU.Printf("Failed: %s", err_f)
		return
	}
	o.InputHandler = f

	o.Push()
	FYI.Printf("Took %0.4fs", o.PushTime.Seconds())

    // let us now fetch our file
	out, err_out := os.Create("out.test")
	if err_out != nil {
		FUUU.Fatalf("create: %s", err_out)
	}
	if o, err := c.Lookup("local cli", "/other/thing.txt"); err == nil {
		o.OutputHandler = out
		gocassos.LogLevel = NVM.Level()
		FYI.Printf("Fetching")
		o.Fetch()
		FYI.Printf("Fetch took %0.3fs", o.FetchTime.Seconds())
	}
	c.Wait()
}
```
