package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unsafe"
)

// columnStorePage implements the Page interface for pages of columnStoreFiles.
type columnStorePage struct {
	Dirty        bool
	pageNumber   int32
	colNumber    int32
	numSlots     int32
	numUsedSlots int32
	desc         *TupleDesc
	colFile      *ColumnFile
	tuples       [](*Tuple)
}

func (c *columnStorePage) getNumSlots() int {
	return int(c.numSlots)
}

// creates a new columnStorePage for a specific column in a ColumnFile
// It calculates the number of slots based on the column's type and initializes the page
func newColumnPage(desc *TupleDesc, colNumber int, pageNumber int, f *ColumnFile) *columnStorePage {
	field := desc.Fields[colNumber]
	var tupleSize int32
	switch field.Ftype {
	case IntType:
		tupleSize = int32(unsafe.Sizeof(int64(0)))
	case StringType:
		tupleSize = int32(unsafe.Sizeof(byte(0))) * int32(StringLength)
	default:
		errors.New("unsupported")
	}

	const headerSize = 8
	numSlots := ((int32)(PageSize) - headerSize) / tupleSize

	return &columnStorePage{
		Dirty:        false,
		pageNumber:   int32(pageNumber),
		colNumber:    int32(colNumber),
		numSlots:     numSlots,
		numUsedSlots: 0,
		desc:         &TupleDesc{Fields: []FieldType{field}},
		colFile:      f,
		tuples:       make([]*Tuple, numSlots),
	}
}

// insertTuple inserts a tuple into the first available slot in the columnStorePage.
// If the page is full, it returns an error
func (c *columnStorePage) insertTuple(t *Tuple) (recordID, error) {
	if c.numUsedSlots >= c.numSlots {
		return nil, errors.New("page is full")
	}

	toInsert, _ := t.project(c.desc.Fields)

	for i, tup := range c.tuples {
		if tup == nil {
			c.tuples[i] = toInsert
			c.numUsedSlots += 1
			c.Dirty = true
			return i, nil
		}
	}

	return nil, errors.New("no available slot found")
}

// deleteTuple removes the tuple at the specified recordID from the columnStorePage
// If the recordID is invalid or the tuple does not exist, it returns an error
func (c *columnStorePage) deleteTuple(rid recordID) error {
	index, ok := rid.(int)
	if !ok {
		return errors.New("invalid recordID")
	}

	if index < 0 || index >= int(c.numSlots) || c.tuples[index] == nil {
		return errors.New("tuple to delete does not exist in page")
	}

	c.tuples[index] = nil
	c.numUsedSlots -= 1
	c.Dirty = true
	return nil
}

func (c *columnStorePage) isDirty() bool {
	return c.Dirty
}

func (c *columnStorePage) setDirty(tid TransactionID, Dirty bool) {
	c.Dirty = Dirty
}

func (c *columnStorePage) getFile() DBFile {
	return c.colFile
}

// toBuffer serializes the columnStorePage into a buffer:
// writes the number of slots, the number of used slots, and all non-nil tuples into the buffer

func (c *columnStorePage) toBuffer() (*bytes.Buffer, error) {

	buf := new(bytes.Buffer)
	writeToBuffer := func(data interface{}) error {
		return binary.Write(buf, binary.LittleEndian, data)
	}

	if err := writeToBuffer(c.numSlots); err != nil {
		return nil, err
	}
	if err := writeToBuffer(c.numUsedSlots); err != nil {
		return nil, err
	}

	for _, tup := range c.tuples {
		if tup != nil {
			if err := tup.writeTo(buf); err != nil {
				return nil, err
			}
		}
	}

	return buf, nil
}

// initializes a columnStorePage from the given buffer.
func (c *columnStorePage) initFromBuffer(buf *bytes.Buffer) error {
	var numUsedSlots int32

	// read numSlots
	if err := binary.Read(buf, binary.LittleEndian, &c.numSlots); err != nil {
		return err
	}

	// read numUsedSlots
	if err := binary.Read(buf, binary.LittleEndian, &numUsedSlots); err != nil {
		return err
	}

	c.tuples = make([]*Tuple, c.numSlots)

	// populate tuples
	for i := 0; i < int(numUsedSlots); i++ {
		tup, err := readTupleFrom(buf, c.desc)
		if err != nil {
			return err
		}
		_, err = c.insertTuple(tup)
		if err != nil {
			return err
		}
	}

	return nil
}

// tupleIter returns returns the next non-nil tuple and nil when all tuples are exhausted
func (c *columnStorePage) tupleIter() func() (*Tuple, error) {
	index := 0

	return func() (*Tuple, error) {
		for index < int(c.numSlots) {
			tup := c.tuples[index]
			index += 1
			if tup != nil {
				return tup, nil
			}
		}
		return nil, nil
	}
}
