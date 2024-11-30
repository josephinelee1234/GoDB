package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	Dirty        bool
	pageNumber   int
	numSlots     int32
	numUsedSlots int32
	desc         *TupleDesc
	file         *HeapFile
	tuples       []*Tuple
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	perTupleSize := int32(0)
	for _, curr_field := range desc.Fields {
		if curr_field.Ftype == IntType {
			perTupleSize += 8
		} else if curr_field.Ftype == StringType {
			perTupleSize += int32(StringLength)
		} else {
			return nil, errors.New("invalid")
		}
	}
	page := &heapPage{
		pageNumber:   pageNo,
		numSlots:     int32(PageSize-8) / perTupleSize,
		numUsedSlots: 0,
		desc:         desc,
		file:         f,
	}
	page.tuples = make([]*Tuple, page.numSlots)
	return page, nil
}

func (h *heapPage) getNumSlots() int {
	return int(h.numSlots)
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	for slot, tup := range h.tuples {
		if tup == nil {
			h.numUsedSlots += 1
			t.Rid = fmt.Sprintf("%d-%d", h.pageNumber, slot)
			h.tuples[slot] = &Tuple{
				Desc:   *h.desc,
				Fields: t.Fields,
				Rid:    t.Rid,
			}
			h.Dirty = true
			return t.Rid, nil
		}
	}
	return "", errors.New("no available slots for tuple insertion")
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	str, ok := rid.(string)
	if !ok {
		return errors.New("invalid record ID type")
	}

	strSlice := strings.Split(str, "-")
	if len(strSlice) != 2 {
		return errors.New("invalid record ID format")
	}

	slot, err := strconv.Atoi(strSlice[1])
	if err != nil {
		return errors.New("invalid slot number")
	}

	if slot < 0 || slot >= len(h.tuples) || h.tuples[slot] == nil {
		return errors.New("invalid slot or tuple does not exist")
	}

	h.tuples[slot] = nil
	h.numUsedSlots -= 1
	h.Dirty = true
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.Dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	h.Dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	return p.file
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	if err := writeBinary(buf, h.numSlots); err != nil {
		return nil, err
	}
	if err := writeBinary(buf, h.numUsedSlots); err != nil {
		return nil, err
	}

	for _, tuple := range h.tuples {
		if tuple == nil {
			continue
		}
		if err := tuple.writeTo(buf); err != nil {
			return nil, err
		}
	}
	if err := padBuffer(buf, PageSize); err != nil {
		return nil, err
	}
	return buf, nil
}

func writeBinary(buf *bytes.Buffer, data interface{}) error {
	return binary.Write(buf, binary.LittleEndian, data)
}

func padBuffer(buf *bytes.Buffer, targetSize int) error {
	if buf.Len() < targetSize {
		padding := make([]byte, targetSize-buf.Len())
		_, err := buf.Write(padding)
		return err
	}
	return nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	err := binary.Read(buf, binary.LittleEndian, &h.numSlots)
	if err != nil {
		return err
	}
	err = binary.Read(buf, binary.LittleEndian, &h.numUsedSlots)
	if err != nil {
		return err
	}
	h.tuples = make([]*Tuple, h.numSlots)
	for i := 0; i < int(h.numUsedSlots); i++ {
		tuple, err := readTupleFrom(buf, h.desc)
		if err != nil {
			break
		}

		tuple.Rid = fmt.Sprintf("%d-%d", h.pageNumber, i)
		tuple.Desc = *h.desc
		h.tuples[i] = tuple
	}
	return err
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	i := 0
	return func() (res *Tuple, err error) {
		if p.numUsedSlots == 0 {
			return nil, nil
		}
		for {
			if i >= len(p.tuples) {
				return nil, nil
			}
			res = p.tuples[i]
			i += 1
			if res == nil {
				continue
			}
			return
		}
	}
}
