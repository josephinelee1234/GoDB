package godb

// provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

import (
	"errors"
	"sync"
	"time"
)

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	Pages                   map[any]Page
	NumPages                int
	poolLock                sync.Mutex
	transactionDependencies map[TransactionID](map[TransactionID]struct{})
	readPermissionLocks     map[TransactionID](map[any]struct{})
	writePermissionLocks    map[TransactionID](map[any]struct{})
	currentTransactions     map[TransactionID]struct{}
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (buf *BufferPool, err error) {
	pages := make(map[any]Page)
	buf = &BufferPool{
		NumPages:                numPages,
		Pages:                   pages,
		transactionDependencies: make(map[TransactionID](map[TransactionID]struct{})),
		readPermissionLocks:     make(map[TransactionID](map[any]struct{})),
		writePermissionLocks:    make(map[TransactionID](map[any]struct{})),
		currentTransactions:     make(map[TransactionID]struct{}),
	}
	return
}

func (bp *BufferPool) hasCycle() bool {
	current_iteration_visited := make(map[TransactionID]bool)
	visited := make(map[TransactionID]bool)

	var dfs func(tid TransactionID) bool
	dfs = func(tid TransactionID) bool {
		current_iteration_visited[tid] = true
		visited[tid] = true

		for next := range bp.transactionDependencies[tid] {
			if !visited[next] {
				if dfs(next) {
					return true
				}
			} else if current_iteration_visited[next] {
				return true
			}
		}

		current_iteration_visited[tid] = false
		return false
	}

	// Perform DFS for each unvisited transaction
	for tid := range bp.currentTransactions {
		if !visited[tid] && dfs(tid) {
			return true
		}
	}
	return false
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() {
	for _, page := range bp.Pages {
		if !page.isDirty() {
			continue
		}
		err := page.getFile().flushPage(page)
		if err != nil {
			return
		}
		page.setDirty(NewTID(), false)
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	bp.poolLock.Lock()
	defer bp.poolLock.Unlock()

	// Check if transaction is active
	if _, exists := bp.currentTransactions[tid]; !exists {
		return
	}

	// Roll back any pages modified by this transaction
	bp.rollbackTransactionPages(tid)

	// Clean up transaction-related records and locks
	bp.removeTransactionLocks(tid)

	for _, dependencies := range bp.transactionDependencies {
		delete(dependencies, tid)
	}
	time.Sleep(1 * time.Millisecond) //giving other transactions a chance to complete

}

func (bp *BufferPool) rollbackTransactionPages(tid TransactionID) {
	for pageKey := range bp.writePermissionLocks[tid] {
		if page, found := bp.Pages[pageKey]; found && page.isDirty() {
			delete(bp.Pages, pageKey)
			bp.NumPages--
		}
	}
}

func (bp *BufferPool) removeTransactionLocks(tid TransactionID) {

	delete(bp.writePermissionLocks, tid)
	delete(bp.transactionDependencies, tid)
	delete(bp.currentTransactions, tid)
	delete(bp.readPermissionLocks, tid)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
	bp.poolLock.Lock()
	defer bp.poolLock.Unlock()

	for pageKey, _ := range bp.writePermissionLocks[tid] {
		page, found := bp.Pages[pageKey]
		if found {
			if page.isDirty() {
				f := page.getFile()
				f.flushPage(page)
			}
		}
	}

	bp.removeTransactionLocks(tid)
	for _, dependencies := range bp.transactionDependencies {
		delete(dependencies, tid)
	}
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	bp.poolLock.Lock()
	defer bp.poolLock.Unlock()

	bp.transactionDependencies[tid] = make(map[TransactionID]struct{})
	bp.readPermissionLocks[tid] = make(map[any]struct{})
	bp.writePermissionLocks[tid] = make(map[any]struct{})
	bp.currentTransactions[tid] = struct{}{}

	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNumber int, tid TransactionID, perm RWPerm) (Page, error) {
	key := file.pageKey(pageNumber)
	bp.poolLock.Lock()
	if _, alive := bp.currentTransactions[tid]; !alive {
		bp.poolLock.Unlock()
		return nil, errors.New("invalid transaction")
	}
	bp.poolLock.Unlock()

	for {
		bp.poolLock.Lock()
		if bp.checkConflictingLocks(tid, key, perm) {
			if bp.hasCycle() {
				bp.poolLock.Unlock()
				bp.AbortTransaction(tid)
				time.Sleep(5 * time.Millisecond) //avoid immediate re-locking
				return nil, errors.New("transaction aborted; there is a cycle")
			}
			// wait and retry if there's a conflict
			bp.poolLock.Unlock()
			time.Sleep(5 * time.Millisecond)
		} else {
			break // no conflicts, safe to acquire lock
		}
	}

	defer bp.poolLock.Unlock()

	// acquire the requested lock
	if perm == ReadPerm {
		bp.readPermissionLocks[tid][key] = struct{}{}
	} else if perm == WritePerm {
		bp.writePermissionLocks[tid][key] = struct{}{}
	}
	if specific_page, a := bp.Pages[key]; a {
		return specific_page, nil
	}
	if len(bp.Pages) >= bp.NumPages {
		err := bp.evictPage()
		if err != nil {
			return nil, err
		}
	}
	specific_page, err := file.readPage(pageNumber)
	if err != nil {
		return nil, err
	}
	bp.Pages[key] = specific_page
	return specific_page, nil
}

func (bp *BufferPool) evictPage() error {
	for key_from_map, specific_page := range bp.Pages {
		if !specific_page.isDirty() {
			delete(bp.Pages, key_from_map)
			return nil
		}
	}
	return GoDBError{BufferPoolFullError, "buffer pool all dirty"}
}

func (bp *BufferPool) checkConflictingLocks(tid TransactionID, key any, perm any) bool {
	conflict := false
	for otherTID := range bp.currentTransactions {
		if otherTID == tid {
			continue
		}

		// check for conflicting write or read locks based on permission type
		if perm == ReadPerm {
			conflict = bp.addDependencyIfLocked(otherTID, tid, key, bp.writePermissionLocks)
		} else if perm == WritePerm {
			conflict = bp.addDependencyIfLocked(otherTID, tid, key, bp.readPermissionLocks) ||
				bp.addDependencyIfLocked(otherTID, tid, key, bp.writePermissionLocks)
		}
		if conflict {
			break
		}
	}
	return conflict
}

func (bp *BufferPool) addDependencyIfLocked(otherTID, tid TransactionID, key any, locks map[TransactionID]map[any]struct{}) bool {
	if _, locked := locks[otherTID][key]; locked {
		bp.transactionDependencies[tid][otherTID] = struct{}{}
		return true
	}
	return false
}
