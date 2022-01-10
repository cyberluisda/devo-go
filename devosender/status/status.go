package status

import (
	"fmt"
	"io/ioutil"
	"sync"

	"github.com/xujiajun/nutsdb"
)

// Builder is the abstrastraction of any Status Builder implementation
type Builder interface {
	Build() (Status, error)
}

// NutsDBStatusBuilder represents the Builder for NutsDBstatus
type NutsDBStatusBuilder struct {
	dbOpts                             nutsdb.Options
	eventsTTLSeconds                   uint32
	bufferSize                         uint
	filesToConsolidateDb               int
	recreateDbClientAfterConsolidation bool
}

const (
	// DefaultEventsTTLSeconds is the default value for EventsTTLSeconds used by NewSNutsDBStatusBuilder
	DefaultEventsTTLSeconds uint32 = 3600
	// DefaultBufferSize is the default size of the total events buffer manged by Status
	// implementation
	DefaultBufferSize uint = 5000000
	// DefaultFilesToConsolidateDb is the default value used when NutsDBStatusBuilder.FilesToConsolidateDb
	// is not called or not set
	DefaultFilesToConsolidateDb = 5
	// DefaultRecreateDbClientAfterConsolidation is the default value used when
	// NutsDBStatusBuilder.RecreateDbClientAfterConsolidation is not called
	DefaultRecreateDbClientAfterConsolidation = true
)

// NewSNutsDBStatusBuilder is the factory method for instantiate *NutsDBStatusBuilder with
// default values
func NewSNutsDBStatusBuilder() *NutsDBStatusBuilder {
	return &NutsDBStatusBuilder{
		nutsdb.DefaultOptions,
		DefaultEventsTTLSeconds,
		DefaultBufferSize,
		DefaultFilesToConsolidateDb,
		DefaultRecreateDbClientAfterConsolidation,
	}
}

// EventsTTLSeconds sets the EventsTTLSeconds builder value
func (nsb *NutsDBStatusBuilder) EventsTTLSeconds(ttl uint32) *NutsDBStatusBuilder {
	nsb.eventsTTLSeconds = ttl
	return nsb
}

// NutsDBOptions sets the nutsdb.Options builder value
func (nsb *NutsDBStatusBuilder) NutsDBOptions(opts nutsdb.Options) *NutsDBStatusBuilder {
	nsb.dbOpts = opts
	return nsb
}

// DbPath sets the Dir property in nutsdb.Options builder value
func (nsb *NutsDBStatusBuilder) DbPath(path string) *NutsDBStatusBuilder {
	nsb.dbOpts.Dir = path
	return nsb
}

// DbSegmentSize sets the SegmentSize (Maximum size for each status persisted file)
// property in nutsdb.Options builder value:
func (nsb *NutsDBStatusBuilder) DbSegmentSize(size int64) *NutsDBStatusBuilder {
	nsb.dbOpts.SegmentSize = size
	return nsb
}

// DbEntryIdxMode sets the EntryIdxMode (See https://pkg.go.dev/github.com/xujiajun/nutsdb@v0.6.0#section-readme)
// property in nutsdb.Options builder value
func (nsb *NutsDBStatusBuilder) DbEntryIdxMode(mode nutsdb.EntryIdxMode) *NutsDBStatusBuilder {
	nsb.dbOpts.EntryIdxMode = mode
	return nsb
}

// DbRWMode sets the RWMode and StartFileLoadingMode (Database read-write mode,
// see https://pkg.go.dev/github.com/xujiajun/nutsdb@v0.6.0#section-readme) properties
// in nutsdb.Options builder value
func (nsb *NutsDBStatusBuilder) DbRWMode(mode nutsdb.RWMode) *NutsDBStatusBuilder {
	nsb.dbOpts.RWMode = mode
	nsb.dbOpts.StartFileLoadingMode = mode
	return nsb
}

// BufferSize sets the BufferSize builder value
func (nsb *NutsDBStatusBuilder) BufferSize(size uint) *NutsDBStatusBuilder {
	nsb.bufferSize = size
	return nsb
}

// FilesToConsolidateDb sets the minimum number of files threshold to run consolidate
// nutsdb files (Merge) when HouseKeeping func is called.
// files value will be internally assigned only if it is greater than 1
func (nsb *NutsDBStatusBuilder) FilesToConsolidateDb(files int) *NutsDBStatusBuilder {
	if files >= 2 {
		nsb.filesToConsolidateDb = files
	}
	return nsb
}

// RecreateDbClientAfterConsolidation enable the close and open nutsdb client when a
// consolidation operation (Merge) is done during HouseKeeping call. This feature
// prevents some memory leaks related with internal nutsdb objects management.
func (nsb *NutsDBStatusBuilder) RecreateDbClientAfterConsolidation(b bool) *NutsDBStatusBuilder {
	nsb.recreateDbClientAfterConsolidation = b
	return nsb
}

// Build builds NutsDBStatus instance based on Builder config.
// NutsDBStatus.Initialize is called just after build connection
func (nsb *NutsDBStatusBuilder) Build() (*NutsDBStatus, error) {
	if nsb.dbOpts.Dir == "" {
		return nil, fmt.Errorf("Empty Dir in nutsdb.Opts")
	}

	// Open nutsdb
	db, err := nutsdb.Open(nsb.dbOpts)
	if err != nil {
		return nil, fmt.Errorf("While open nutsdb: %w", err)
	}

	r := &NutsDBStatus{
		db:                                 db,
		dbOpts:                             nsb.dbOpts,
		eventTTL:                           nsb.eventsTTLSeconds,
		bufferSize:                         nsb.bufferSize,
		filesToConsolidateDb:               nsb.filesToConsolidateDb,
		recreateDbClientAfterConsolidation: nsb.recreateDbClientAfterConsolidation,
	}

	err = r.Initialize()
	if err != nil {
		return r, fmt.Errorf("While initialize NutsDBStatus: %w", err)
	}
	return r, nil
}

// Status is the engine abstranction to save and retrieve EventRecord instances
// from status implementation.
type Status interface {
	New(er *EventRecord) error
	Update(oldID, newID string) error
	Get(ID string) (*EventRecord, int, error)
	FinishRecord(ID string) error
	AllIDs() ([]string, error)
	FindAll() ([]*EventRecord, error)
	Stats() Stats
	HouseKeeping() error
	Close() error
}

// NutsDBStatus is the nutsdb implementation of Status interface
type NutsDBStatus struct {
	db                                 *nutsdb.DB
	dbOpts                             nutsdb.Options
	dbMtx                              sync.Mutex
	initialized                        bool
	eventTTL                           uint32
	bufferSize                         uint
	filesToConsolidateDb               int
	recreateDbClientAfterConsolidation bool
}


// NumberOfFiles return the number of files, really non directories elements, in
// a directory path without entering in sub-directories
func NumberOfFiles(path string) int {
	files, _ := ioutil.ReadDir(path)
	r := 0
	for _, f := range files {
		if !f.IsDir() {
			r++
		}
	}
	return r
}

