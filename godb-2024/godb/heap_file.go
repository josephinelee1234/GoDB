package godb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

type RecordID struct {
	pageNo int
	slotNo int
}

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	backingFile    string
	tupleDesc      *TupleDesc
	bufPool        *BufferPool
	pagesNum       int
	availablePages []bool
	HFLock         sync.Mutex
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	heapFile := &HeapFile{
		backingFile:    fromFile,
		tupleDesc:      td,
		bufPool:        bp,
		availablePages: make([]bool, 0),
	}

	heapFile.pagesNum = heapFile.NumPages()
	for i := 0; i < heapFile.pagesNum; i++ {
		heapFile.availablePages = append(heapFile.availablePages, true)
	}

	return heapFile, nil
}

// Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	return f.backingFile
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	fileInfo, err := os.Stat(f.backingFile)
	if err != nil {
		return 0
	}
	size := fileInfo.Size()
	num_pages := int(size / int64(PageSize))
	remainder := size % int64(PageSize)
	if remainder != 0 {
		num_pages += 1
	}
	return num_pages
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
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

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		//bp.FlushAllPages()

		bp.CommitTransaction(tid)

	}
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {
	data := make([]byte, PageSize)
	new_buf := new(bytes.Buffer)
	offset := int64(pageNo * PageSize)
	file, err := os.OpenFile(f.backingFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to page: %w", err)
	}

	if _, err := file.Read(data); err != nil {
		return nil, fmt.Errorf("failed to read data from page: %w", err)
	}

	if err := binary.Write(new_buf, binary.LittleEndian, data); err != nil {
		return nil, fmt.Errorf("failed to write binary data: %w", err)
	}

	heap_page := &heapPage{
		pageNumber: pageNo,
		desc:       f.tupleDesc,
		file:       f,
	}
	if err := heap_page.initFromBuffer(new_buf); err != nil {
		return nil, fmt.Errorf("failed to initialize heap page: %w", err)
	}

	return heap_page, nil
}

// Add the tuple to the HeapFile. This method should search through pages in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {

	var validPage *heapPage

	if len(t.Fields) == len(t.Desc.Fields) {
		for pageNo, idle := range f.availablePages {
			if idle { // Check if the page is idle
				buf_page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
				if err != nil {
					return err
				}

				tmpPage := buf_page.(*heapPage)
				if tmpPage.numUsedSlots < tmpPage.numSlots {
					validPage = tmpPage
					break
				} else {
					f.availablePages[pageNo] = false // Mark the page as no longer idle
				}
			}
		}
		if validPage == nil {

			if err := f.createNewPage(t); err != nil {
				return err
			}
			return nil
		}
		if _, err := validPage.insertTuple(t); err != nil {
			return err
		}

		validPage.setDirty(tid, true)
		return nil

	}
	return errors.New("invalid")

}
func (f *HeapFile) createNewPage(t *Tuple) error {
	f.HFLock.Lock()
	defer f.HFLock.Unlock()
	newPage, err := newHeapPage(f.tupleDesc, f.pagesNum, f)
	if err != nil {
		return err
	}

	if _, err := newPage.insertTuple(t); err != nil {
		return err
	}

	if err := f.flushPage(newPage); err != nil {
		return err
	}

	if len(f.bufPool.Pages) < f.bufPool.NumPages {
		f.bufPool.Pages[f.pageKey(f.pagesNum)] = newPage
	}
	f.availablePages = append(f.availablePages, true)
	f.pagesNum += 1
	return nil
}

// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {

	if t.Rid == nil {
		return nil
	}

	rid, ok := t.Rid.(string)
	if !ok {
		return errors.New("invalid record ID type")
	}

	// Split the record ID string to extract page number and slot
	strSlice := strings.Split(rid, "-")
	if len(strSlice) != 2 {
		return errors.New("invalid record ID format")
	}

	// Convert the page number
	pageNumber, err := strconv.Atoi(strSlice[0])
	if err != nil {
		return errors.New("invalid page number")
	}
	// Convert the slot number (if needed for further processing)
	_, err = strconv.Atoi(strSlice[1])
	if err != nil {
		return errors.New("invalid slot number")
	}

	// Fetch the page from the buffer pool
	tmpPage, err := f.bufPool.GetPage(f, pageNumber, tid, WritePerm)
	if err != nil {
		return err
	}

	// Type assert to *heapPage
	page, ok := tmpPage.(*heapPage)
	if !ok {
		return errors.New("invalid page type")
	}

	// Delete the tuple using its RID
	if t.Rid != nil {
		if err := page.deleteTuple(t.Rid); err != nil {
			return err
		}
	}

	//f.availablePages[pageNumber] = false

	return nil
}

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
func (f *HeapFile) flushPage(p Page) error {
	page, ok := p.(*heapPage)
	if !ok {
		return errors.New("invalid page type")
	}

	file, err := os.OpenFile(f.backingFile, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer func() error {
		if closeErr := file.Close(); closeErr != nil {
			err = closeErr
			return err
		}
		return nil
	}()

	if _, err := file.Seek(int64(page.pageNumber*PageSize), io.SeekStart); err != nil {
		return err
	}

	buf, err := page.toBuffer()
	if err != nil {
		return err
	}

	if _, err := buf.WriteTo(file); err != nil {
		return err
	}

	page.Dirty = false
	return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.tupleDesc
}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
// Make sure to set the returned tuple's TupleDescriptor to the TupleDescriptor of
// the HeapFile. This allows it to correctly capture the table qualifier.
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iterIndex := 0
	tupleMap := make(map[int]func() (*Tuple, error))
	return func() (tuple *Tuple, err error) {
		for iterIndex < f.pagesNum {
			tmpPage, err := f.bufPool.GetPage(f, iterIndex, tid, ReadPerm)
			if err != nil {
				return nil, err
			}

			page := tmpPage.(*heapPage)
			if tupleMap[iterIndex] == nil {
				tupleMap[iterIndex] = page.tupleIter()
			}

			tuple, err = tupleMap[iterIndex]()
			if err != nil {
				return nil, err
			}

			if tuple != nil {
				tuple.Desc = *f.tupleDesc
				return tuple, nil
			}
			iterIndex++
		}

		return nil, nil
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	return heapHash{
		FileName: f.backingFile,
		PageNo:   pgNo,
	}
}
