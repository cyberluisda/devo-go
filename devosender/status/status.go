package status

import (
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

