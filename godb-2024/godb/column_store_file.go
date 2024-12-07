package godb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"

	"bufio"
	"strconv"
	"strings"
)

type columnStoreFile struct {
	filenames       map[int]string
	td              TupleDesc
	bufPool         *BufferPool
	pagesEachColumn int
	colAmount       int
	CFLock          sync.Mutex
}

// initializes a new columnStoreFile
func NewcolumnStoreFile(fromFiles map[int]string, td TupleDesc, bp *BufferPool) (*columnStoreFile, error) {
	if len(td.Fields) != len(fromFiles) {
		return nil, errors.New("number of files and columns do not match")
	}

	colFile := &columnStoreFile{
		td:              td,
		filenames:       fromFiles,
		bufPool:         bp,
		colAmount:       len(td.Fields),
		pagesEachColumn: 0,
	}

	for _, filename := range fromFiles {
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
		}

		fi, err := file.Stat()
		file.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to get file info for %s: %w", filename, err)
		}

		colFile.pagesEachColumn = ((int(fi.Size()) + PageSize - 1) / PageSize) / colFile.colAmount

		break
	}

	return colFile, nil
}

func (f *columnStoreFile) NumPages() int {
	return f.pagesEachColumn * f.colAmount
}

// largely the same as LoadFromCSV from heap_file.go
func (f *columnStoreFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		f.insertTuple(&newT, tid)

		bp.CommitTransaction(tid)
	}
	return nil
}

func (f *columnStoreFile) readPage(pageNumber int) (Page, error) {
	column := pageNumber % f.colAmount
	filename, ok := f.filenames[column]
	if !ok {
		return nil, fmt.Errorf("file for column %d not found", column)
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	offset := int64(PageSize * (pageNumber / f.colAmount))
	if _, err := file.Seek(offset, 0); err != nil {
		return nil, err
	}

	data := make([]byte, PageSize)
	if _, err := file.Read(data); err != nil {
		return nil, err
	}

	cp := newColumnPage(&f.td, column, pageNumber, f)
	if err := cp.initFromBuffer(bytes.NewBuffer(data)); err != nil {
		return nil, err
	}
	cp.Dirty = false

	return cp, nil
}

// insert tuple
func (f *columnStoreFile) insertTuple(t *Tuple, tid TransactionID) error {
	j := 0

	// try inserting the tuple into existing pages
	for i := 0; i < f.pagesEachColumn; i++ {
		inserted, err := f.tryInsertIntoPage(t, tid, i*f.colAmount+j)
		if err != nil {
			return err
		}
		if inserted {
			return nil
		}
	}

	// if insertion fails, create new pages and insert
	return f.createNewPagesAndInsert(t, tid, j)
}

// helper function to attempt insertion into an existing page
func (f *columnStoreFile) tryInsertIntoPage(t *Tuple, tid TransactionID, pageNumber int) (bool, error) {
	page, err := f.bufPool.GetPage(f, pageNumber, tid, WritePerm)
	if err != nil {
		return false, err
	}

	cp := page.(*columnStorePage)
	slot, err := cp.insertTuple(t)
	if err != nil {
		return false, nil
	}
	t.Rid = RecordID{pageNo: pageNumber, slotNo: slot.(int)}

	for i := 1; i < f.colAmount; i++ {
		if err := f.insertIntoColumnPage(t, tid, pageNumber+i); err != nil {
			return false, err
		}
	}

	return true, nil
}

// helper to insert into a specific column page
func (f *columnStoreFile) insertIntoColumnPage(t *Tuple, tid TransactionID, pageNumber int) error {
	page, err := f.bufPool.GetPage(f, pageNumber, tid, WritePerm)
	if err != nil {
		return err
	}

	cp := page.(*columnStorePage)
	_, err = cp.insertTuple(t)
	return err
}

// helper to create new pages and insert the tuple
func (f *columnStoreFile) createNewPagesAndInsert(t *Tuple, tid TransactionID, colIdx int) error {
	f.CFLock.Lock()
	defer f.CFLock.Unlock()

	// create and append new pages for each column
	for k := 0; k < f.colAmount; k++ {
		newPageNumber := f.pagesEachColumn*f.colAmount + k
		newPage := newColumnPage(&f.td, k, newPageNumber, f)
		f.flushPage(newPage)

		if k == colIdx { // insert the tuple into the primary column first
			bufPoolPage, err := f.bufPool.GetPage(f, newPageNumber, tid, WritePerm)
			if err != nil {
				return err
			}

			slot, err := bufPoolPage.(*columnStorePage).insertTuple(t)
			if err != nil {
				return err
			}

			t.Rid = RecordID{pageNo: newPageNumber, slotNo: slot.(int)}
		} else { // insert into other columns
			if err := f.insertIntoColumnPage(t, tid, newPageNumber); err != nil {
				return err
			}
		}
	}

	f.pagesEachColumn += 1
	return nil
}

// deleteTuple removes a tuple from all column pages in the columnStoreFile.
// It uses the tuple's RecordID (rid) to determine the starting page number
// and iterates over all columns to delete the tuple
func (f *columnStoreFile) deleteTuple(t *Tuple, tid TransactionID) error {
	rid := t.Rid.(RecordID)
	for i := 0; i < f.colAmount; i++ {
		pageNumber := rid.pageNo + i
		if err := f.deleteFromColumnPage(pageNumber, rid.slotNo, tid); err != nil {
			return err
		}
	}

	return nil
}

// helper function for deleteTuple; removes a tuple from a specific column page by its slot number
func (f *columnStoreFile) deleteFromColumnPage(pageNumber int, slotNo int, tid TransactionID) error {
	page, err := f.bufPool.GetPage(f, pageNumber, tid, WritePerm)
	if err != nil {
		return err
	}

	cp := page.(*columnStorePage)
	return cp.deleteTuple(slotNo)
}

func (f *columnStoreFile) flushPage(page Page) error {
	// convert the page to a columnStorePage and serialize it to a buffer
	cp := page.(*columnStorePage)
	buf, err := cp.toBuffer()
	if err != nil {
		return err
	}

	// get the file and offset for the page
	column := int(cp.colNumber)
	pageNumber := int(cp.pageNumber)
	slotInColumn := pageNumber / f.colAmount
	filename, ok := f.filenames[column]
	if !ok {
		return fmt.Errorf("file for column %d not found", column)
	}

	offset := int64(PageSize * slotInColumn)

	if err := writeBufferToFile(filename, offset, buf.Bytes()); err != nil {
		return err
	}

	page.setDirty(0, false)
	return nil
}

// helper, writes the given buffer to a file at the specified offset
func writeBufferToFile(filename string, offset int64, data []byte) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Seek(offset, 0); err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	return nil
}

func (f *columnStoreFile) Descriptor() *TupleDesc {
	return &f.td
}

func (f *columnStoreFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	columns := make([]int, f.colAmount)
	for i := 0; i < f.colAmount; i++ {
		columns[i] = i
	}
	return f.IteratorCol(columns, tid)
}

func (f *columnStoreFile) IteratorCol(columns []int, tid TransactionID) (func() (*Tuple, error), error) {
	pageInColumn := 0
	pages := make([]*columnStorePage, len(columns))
	iters := make([]func() (*Tuple, error), len(columns))
	if err := f.initColumnPagesAndIterators(columns, pages, iters, pageInColumn, tid); err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {
		for {
			tuples := make([]*Tuple, len(columns))
			for i := 0; i < len(columns); i++ {
				t, _ := iters[i]()
				tuples[i] = t
			}

			// if the first column runs out of tuples, move to the next page
			if tuples[0] == nil {
				pageInColumn += 1
				if pageInColumn >= f.pagesEachColumn {
					return nil, nil
				}
				if err := f.initColumnPagesAndIterators(columns, pages, iters, pageInColumn, tid); err != nil {
					return nil, err
				}
				continue
			}

			// combine tuples across columns into a single tuple
			var combined *Tuple
			for _, tup := range tuples {
				combined = joinTuples(combined, tup)
			}
			return combined, nil
		}
	}, nil
}

// helper, initializes pages and iterators for the specified columns
func (f *columnStoreFile) initColumnPagesAndIterators(columns []int, pages []*columnStorePage, iters []func() (*Tuple, error), pageInColumn int, tid TransactionID) error {
	for index, col := range columns {
		pageNumber := pageInColumn*f.colAmount + col
		p, err := f.bufPool.GetPage(f, pageNumber, tid, ReadPerm)
		if err != nil {
			return err
		}
		pages[index] = p.(*columnStorePage)
		iters[index] = pages[index].tupleIter()
	}
	return nil
}

// internal strucuture to use as key for a column store page
type columnHash struct {
	filename   string
	pageNumber int
}

func (f *columnStoreFile) pageKey(pgNo int) any {
	filename, ok := f.filenames[pgNo%f.colAmount]
	if !ok {
		panic(fmt.Sprintf("no file for column %d", pgNo%f.colAmount))
	}
	return columnHash{
		filename:   filename,
		pageNumber: pgNo,
	}
}
