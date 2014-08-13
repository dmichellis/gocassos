package gocassos

import (
	"errors"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

type HttpInputHandler struct {
	w           http.ResponseWriter
	r           *http.Request
	o           *Object
	failure     bool
	in_progress sync.WaitGroup
}

type HttpOutputHandler struct {
	HttpInputHandler

	t        *os.File
	mode     int
	pipeline chan int64

	waiting_for_chunk int64
	chunk_status      []bool
}

type ReadAndWriteAt interface {
	ReadAt([]byte, int64) (int, error)
	WriteAt([]byte, int64) (int, error)
	Close() error
}

type InputHandler interface {
	Read([]byte) (int, error)
	Close() error
}

type OutputHandler interface {
	WriteAt([]byte, int64) (int, error)
	Close() error
}

type Object struct {
	ClientId string

	Objectname string
	Updated    int64
	Nodetag    gocql.UUID
	id         string

	NumChunks  int64
	ChunkSize  int64
	ObjectSize int64

	pusher_expiration_queries []string

	finished_chunks chan int64
	fetcher_control chan struct{}
	pusher_control  chan struct{}

	failure     bool
	in_progress sync.WaitGroup

	lookup_start time.Time
	LookupTime   time.Duration
	fetch_start  time.Time
	FetchTime    time.Duration
	push_start   time.Time
	PushTime     time.Duration

	OutputHandler OutputHandler
	InputHandler  InputHandler

	cfg *ObjectStorage
}

var consistencies = map[string]gocql.Consistency{
	"localquorum": gocql.LocalQuorum,
	"one":         gocql.One,
	"quorum":      gocql.Quorum,
	"any":         gocql.Any,
	"all":         gocql.All,
}

type ObjectStorage struct {
	Conn           *gocql.Session
	ScrubGraceTime int
	AllowUpdates   bool
	PopulatePaths  bool
	ChunkSize      int64

	write_consistency_str []string
	write_consistency     []gocql.Consistency
	read_consistency_str  []string
	read_consistency      []gocql.Consistency

	ConcurrentGetsPerObj int
	ConcurrentPutsPerObj int

	in_progress sync.WaitGroup
}

type Logger struct {
	prefix string
	level  int
}

const (
	StreamMode = iota
	BatchMode
	HeadMode
)

var ErrNullReference = errors.New("Null object reference")
var ErrRefused = errors.New("Update refused")
var ErrNotFound = errors.New("Not Found")
var ErrChunksFailed = errors.New("Failed to push chunks")
var ErrCassandraNotConnected = errors.New("Cassandra not connected")
