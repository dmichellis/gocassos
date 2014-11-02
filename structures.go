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
	failure     error
	in_progress sync.WaitGroup
}

type HttpOutputHandler struct {
	HttpInputHandler

	t        *os.File
	mode     int
	pipeline chan int64

	total_chunks int64

	already_closed bool
	mu             sync.Mutex

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
	Expiration time.Time
	Metadata   map[string]string

	finished_chunks chan int64
	fetcher_control chan struct{}
	pusher_control  chan struct{}

	tmp_payload *[]byte

	// doesn't warrant a mutex; it's meant just to signal any concurrent
	// routines that they shouldn't bother any more
	failure     error
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
	"localone":    gocql.LocalOne,
	"one":         gocql.One,
	"quorum":      gocql.Quorum,
	"any":         gocql.Any,
	"all":         gocql.All,
	"eachquorum":  gocql.EachQuorum,
}

type ObjectStorage struct {
	Conn           *gocql.Session
	ScrubGraceTime int
	AllowUpdates   bool
	PopulatePaths  bool
	ChunkSize      int64

	InlinePayloadMax int

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

const inlinePayloadMarker = -1
const (
	StreamMode = iota
	BatchMode
	HeadMode
)

var (
	TransferModes = map[string]int{
		"stream": StreamMode,
		"batch":  BatchMode,
		"head":   HeadMode,
	}

	TransferModeCodes = map[int]string{
		StreamMode: "stream",
		BatchMode:  "batch",
		HeadMode:   "head",
	}

	ErrNullReference         = errors.New("Null object reference")
	ErrRefused               = errors.New("Update refused")
	ErrNotFound              = errors.New("Not Found")
	ErrChunksFailed          = errors.New("Failed to push chunks")
	ErrCassandraNotConnected = errors.New("Cassandra not connected")
)
