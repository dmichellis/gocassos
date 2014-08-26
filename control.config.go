package gocassos

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

func (c *ObjectStorage) SetConsistencies(read []string, write []string) error {
	var err error
	if c.read_consistency, err = parse_consistency(&read); err != nil {
		return err
	}
	if c.write_consistency, err = parse_consistency(&write); err != nil {
		return err
	}
	c.read_consistency_str = read
	c.write_consistency_str = write
	NVM.Printf("CONFIG: Setting consistencies read: %s write: %s", read, write)
	return nil
}

func (c *ObjectStorage) Init() {
	c.ScrubGraceTime = 10
	c.ConcurrentGetsPerObj = 0
	c.ConcurrentPutsPerObj = 0
	c.ChunkSize = 1000
	c.SetConsistencies([]string{"one"}, []string{"one"})
	return
}

func parse_consistency(names *[]string) ([]gocql.Consistency, error) {
	tmp := make([]gocql.Consistency, len(*names))
	for i, cons := range *names {
		cons = strings.ToLower(cons)
		if _, ok := consistencies[cons]; ok {
			tmp[i] = consistencies[cons]
		} else {
			FUUU.Printf("CONFIG: Unknown consistency: %s", cons)
			return nil, fmt.Errorf("Unknown consistency '%s'", cons)
		}
	}
	return tmp, nil
}

func (c *ObjectStorage) Wait() {
	if c != nil {
		c.in_progress.Wait()
	}
}
