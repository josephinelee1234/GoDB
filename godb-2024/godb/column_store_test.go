package godb

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func makeColumnFileTestVars() (TupleDesc, Tuple, Tuple, *ColumnFile, *BufferPool, TransactionID) {
	os.Remove("coltest_name.dat")
	os.Remove("coltest_age.dat")
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}

	var t1 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"josie"},
			IntField{20},
		}}

	var t2 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"annie"},
			IntField{17},
		}}

	bp, err := NewBufferPool(25)

	cf, err := NewColumnFile(map[int]string{0: "coltest_name.dat", 1: "coltest_age.dat"}, td, bp)
	if err != nil {
		errors.New("error making test variables")
	}

	tid := NewTID()
	bp.BeginTransaction(tid)

	return td, t1, t2, cf, bp, tid

}

func TestColumnFileCreateAndInsert(t *testing.T) {
	_, t1, t2, cf, _, tid := makeColumnFileTestVars()
	err := cf.insertTuple(&t1, tid)
	cf.insertTuple(&t2, tid)
	iter, err := cf.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 2 {
		t.Errorf("ColumnFile iterator expected 2 tuples, got %d", i)
	}
}

func TestColumnFileDelete(t *testing.T) {
	_, t1, t2, cf, _, tid := makeColumnFileTestVars()
	err := cf.insertTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = cf.insertTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	err = cf.deleteTuple(&t1, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, err := cf.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	t3, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if t3 == nil {
		t.Fatalf("ColumnFile iterator expected 1 tuple")
	}

	err = cf.deleteTuple(&t2, tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	iter, err = cf.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}

	t3, err = iter()
	if err != nil {
		t.Fatalf(err.Error())
	}

	if t3 != nil {
		t.Fatalf("ColumnFile iterator expected 0 tuple")
	}
}

func TestColumnFilePageKey(t *testing.T) {
	td, t1, _, cf, bp, tid := makeColumnFileTestVars()

	os.Remove("coltest_age.dat")
	cf2, err := NewHeapFile(TestingFile2, &td, bp)
	if err != nil {
		t.Fatalf(err.Error())
	}

	for cf.NumPages() < 2 {
		err = cf.insertTuple(&t1, tid)
		if err != nil {
			t.Fatalf(err.Error())
		}

		err = cf2.insertTuple(&t1, tid)
		if err != nil {
			t.Fatalf(err.Error())
		}

		if cf.NumPages() == 0 {
			t.Fatalf("Heap file should have at least one page after insertion.")
		}

		bp.FlushAllPages()
	}

	for i := 0; i < cf.NumPages(); i++ {
		if cf.pageKey(i) != cf.pageKey(i) {
			t.Fatalf("Expected equal pageKey")
		}
		if cf.pageKey(i) == cf.pageKey((i+1)%cf.NumPages()) {
			t.Fatalf("Expected non-equal pageKey for different pages")
		}
		if cf.pageKey(i) == cf2.pageKey(i) {
			t.Fatalf("Expected non-equal pageKey for different heapfiles")
		}
	}
}

func TestColumnFileSize(t *testing.T) {
	_, t1, _, cf, bp, _ := makeColumnFileTestVars()

	tid := NewTID()
	bp.BeginTransaction(tid)
	cf.insertTuple(&t1, tid)
	page, err := bp.GetPage(cf, 0, tid, ReadPerm)
	if err != nil {
		t.Fatalf("unexpected error, getPage, %s", err.Error())
	}
	cf.flushPage(page)
	info, err := os.Stat(TestingFile)
	if err != nil {
		t.Fatalf("unexpected error, stat, %s", err.Error())
	}
	if info.Size() != int64(PageSize) {
		t.Fatalf("column file page is not %d bytes;  NOTE:  This error may be OK, but many implementations that don't write full pages break.", PageSize)
	}
}

func TestColumnFileDirtyBit(t *testing.T) {
	_, t1, _, cf, bp, _ := makeColumnFileTestVars()

	tid := NewTID()
	bp.BeginTransaction(tid)
	cf.insertTuple(&t1, tid)
	cf.insertTuple(&t1, tid)
	page, _ := bp.GetPage(cf, 0, tid, ReadPerm)
	if !page.isDirty() {
		t.Fatalf("Expected page to be dirty")
	}
}

func TestColumnPageInsert(t *testing.T) {
	var expectedSlots_name = ((PageSize - 8) / (StringLength))
	var expectedSlots_age = ((PageSize - 8) / 8)
	td, t1, t2, cf, _, _ := makeColumnFileTestVars()
	page_name := newColumnPage(&td, 0, 0, cf)
	page_age := newColumnPage(&td, 1, 0, cf)

	if page_name.getNumSlots() != expectedSlots_name {
		t.Fatalf("Incorrect number of slots, expected %d, got %d", expectedSlots_name, page_name.getNumSlots())
	}
	if page_age.getNumSlots() != expectedSlots_age {
		t.Fatalf("Incorrect number of slots, expected %d, got %d", expectedSlots_age, page_age.getNumSlots())
	}

	page_name.insertTuple(&t1)
	page_name.insertTuple(&t2)

	iter := page_name.tupleIter()
	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		cnt++
	}
	if cnt != 2 {
		t.Errorf("Expected 2 tuples in iterator, got %d", cnt)
	}

}

func TestColumnPageDelete(t *testing.T) {
	td, t1, t2, cf, _, _ := makeColumnFileTestVars()
	pgName := newColumnPage(&td, 0, 0, cf)

	pgName.insertTuple(&t1)
	rid, _ := pgName.insertTuple(&t2)

	pgName.deleteTuple(rid)
	iter := pgName.tupleIter()
	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		cnt++
	}
	if cnt != 1 {
		t.Errorf("Expected 2 tuples in iterator, got %d", cnt)
	}

}

func TestColumnPageInsertTuple(t *testing.T) {
	td, t1, _, cf, _, _ := makeColumnFileTestVars()
	page := newColumnPage(&td, 0, 0, cf)
	free := page.getNumSlots()

	for i := 0; i < free; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"josie"},
				IntField{int64(i)},
			},
		}
		page.insertTuple(&addition)

		iter := page.tupleIter()
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		cnt, found := 0, false
		for {

			tup, _ := iter()
			fields := []FieldType{td.Fields[0]}
			additionProjected, _ := addition.project(fields)
			found = found || additionProjected.equals(tup)
			if tup == nil {
				break
			}

			cnt += 1
		}
		if cnt != i+1 {
			t.Errorf("Expected %d tuple in interator, got %d", i+1, cnt)
		}
		if !found {
			t.Errorf("Expected inserted tuple to be FOUND, got NOT FOUND")
		}
	}
	_, err := page.insertTuple(&t1)

	if err == nil {
		t.Errorf("Expected error due to full page")
	}
}

func TestColumnPageDeleteTuple(t *testing.T) {
	td, _, _, cf, _, _ := makeColumnFileTestVars()
	page := newColumnPage(&td, 0, 0, cf)
	free := page.getNumSlots()

	list := make([]recordID, free)
	for i := 0; i < free; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"josie"},
				IntField{int64(i)},
			},
		}
		list[i], _ = page.insertTuple(&addition)
	}
	if len(list) == 0 {
		t.Fatalf("Rid list is empty.")
	}

	for _, rid := range list {
		err := page.deleteTuple(rid)
		if err != nil {
			t.Errorf("Found error %s", err.Error())
		}
	}

	err := page.deleteTuple(list[0])
	if err == nil {
		t.Errorf("page should be empty; expected error")
	}
}

func TestColumnPageDirty(t *testing.T) {
	td, _, _, hf, _, _ := makeColumnFileTestVars()
	page := newColumnPage(&td, 0, 0, hf)

	page.setDirty(0, true)
	if !page.isDirty() {
		t.Errorf("page should be dirty")
	}
	page.setDirty(0, true)
	if !page.isDirty() {
		t.Errorf("page should be dirty")
	}
	page.setDirty(-1, false)
	if page.isDirty() {
		t.Errorf("page should be not dirty")
	}
}

func TestColumnPageSerialization(t *testing.T) {

	td, _, _, cf, _, _ := makeColumnFileTestVars()
	page := newColumnPage(&td, 0, 0, cf)
	free := page.getNumSlots()

	for i := 0; i < free-1; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"josie"},
				IntField{int64(i)},
			},
		}
		page.insertTuple(&addition)
	}

	buf, _ := page.toBuffer()
	page2 := newColumnPage(&td, 0, 0, cf)
	err := page2.initFromBuffer(buf)
	if err != nil {
		t.Fatalf("Error loading heap page from buffer.")
	}

	iter, iter2 := page.tupleIter(), page2.tupleIter()
	if iter == nil {
		t.Fatalf("iter was nil.")
	}
	if iter2 == nil {
		t.Fatalf("iter2 was nil.")
	}

	findEqCount := func(t0 *Tuple, iter3 func() (*Tuple, error)) int {
		cnt := 0
		for tup, _ := iter3(); tup != nil; tup, _ = iter3() {
			if t0.equals(tup) {
				cnt += 1
			}
		}
		return cnt
	}

	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		if findEqCount(tup, page.tupleIter()) != findEqCount(tup, page2.tupleIter()) {
			t.Errorf("Serialization / deserialization doesn't result in identical heap page.")
		}
	}
}

func TestIntFilterCol(t *testing.T) {
	_, t1, t2, cf, _, tid := makeColumnFileTestVars()
	cf.insertTuple(&t1, tid)
	cf.insertTuple(&t2, tid)
	var f FieldType = FieldType{"age", "", IntType}
	filt, err := NewFilter(&ConstExpr{IntField{17}, IntType}, OpGt, &FieldExpr{f}, cf)
	if err != nil {
		t.Errorf(err.Error())
	}
	iter, err := filt.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		cnt++
	}
	if cnt != 1 {
		t.Errorf("unexpected number of results")
	}
}

func TestStringFilterCol(t *testing.T) {
	_, t1, t2, cf, _, tid := makeColumnFileTestVars()
	cf.insertTuple(&t1, tid)
	cf.insertTuple(&t2, tid)
	var f FieldType = FieldType{"name", "", StringType}
	filt, err := NewFilter(&ConstExpr{StringField{"josie"}, StringType}, OpEq, &FieldExpr{f}, cf)
	if err != nil {
		t.Errorf(err.Error())
	}
	iter, err := filt.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}

	cnt := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		cnt++
	}
	if cnt != 1 {
		t.Errorf("unexpected number of results")
	}
}

func TestJoinCol(t *testing.T) {
	td, t1, t2, cf, bp, tid := makeColumnFileTestVars()
	cf.insertTuple(&t1, tid)
	cf.insertTuple(&t2, tid)
	cf.insertTuple(&t2, tid)

	os.Remove(JoinTestFile)
	os.Remove("JoinTestFile2.dat")
	cf2, err := NewColumnFile(map[int]string{0: JoinTestFile, 1: "JoinTestFile2.dat"}, td, bp)
	if err != nil {
		t.Errorf("unexpected error initializing column file")
		return
	}
	cf2.insertTuple(&t1, tid)
	cf2.insertTuple(&t2, tid)
	cf2.insertTuple(&t2, tid)

	outT1 := joinTuples(&t1, &t1)
	outT2 := joinTuples(&t2, &t2)

	leftField := FieldExpr{td.Fields[1]}
	join, err := NewJoin(cf, &leftField, cf2, &leftField, 100)
	if err != nil {
		t.Errorf("unexpected error initializing join")
		return
	}
	iter, err := join.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	cnt := 0
	cntOut1 := 0
	cntOut2 := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		if t.equals(outT1) {
			cntOut1++
		} else if t.equals(outT2) {
			cntOut2++
		}
		cnt++
	}
	if cnt != 5 {
		t.Errorf("unexpected number of join results (%d, expected 5)", cnt)
	}
	if cntOut1 != 1 {
		t.Errorf("unexpected number of t1 results (%d, expected 1)", cntOut1)
	}
	if cntOut2 != 4 {
		t.Errorf("unexpected number of t2 results (%d, expected 4)", cntOut2)
	}

}

func TestProjectCol(t *testing.T) {
	_, t1, t2, cf, _, tid := makeColumnFileTestVars()
	cf.insertTuple(&t1, tid)
	cf.insertTuple(&t2, tid)
	var outNames []string = make([]string, 1)
	outNames[0] = "outf"
	fieldExpr := FieldExpr{t1.Desc.Fields[0]}
	proj, _ := NewProjectOp([]Expr{&fieldExpr}, outNames, false, cf)
	if proj == nil {
		t.Fatalf("project was nil")
	}
	iter, _ := proj.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	tup, err := iter()
	if err != nil {
		t.Fatalf(err.Error())
	}
	if len(tup.Fields) != 1 || tup.Desc.Fields[0].Fname != "outf" {
		t.Errorf("invalid output tuple")
	}

}

func TestLoadCSVPerformance50(t *testing.T) {
	td := TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
		{Fname: "id", Ftype: IntType},
		{Fname: "salary", Ftype: IntType},
		{Fname: "bonus", Ftype: IntType},
		{Fname: "address", Ftype: StringType},
		{Fname: "phone", Ftype: StringType},
		{Fname: "email", Ftype: StringType},
		{Fname: "ig_handle", Ftype: StringType},
		{Fname: "has_pets", Ftype: StringType},
		{Fname: "no_siblings", Ftype: StringType},
		{Fname: "spouse_name", Ftype: StringType},
		{Fname: "child_name", Ftype: StringType},
		{Fname: "has_allergies", Ftype: StringType},
		{Fname: "likes_cats", Ftype: StringType},
	}}

	file, err := os.Open("performance_test_50.csv")
	if err != nil {
		t.Fatalf("Failed to open CSV file: %s", err)
	}
	defer file.Close()

	columnBufferPool, err := NewBufferPool(200)
	if err != nil {
		t.Fatalf("Failed to create column buffer pool: %s", err)
	}

	colFiles := make(map[int]string, 15)
	for i := 0; i < 15; i++ {
		colFiles[i] = fmt.Sprintf("%dperformancetest.dat", i)
	}

	defer func() {
		for _, file := range colFiles {
			os.Remove(file)
		}
	}()

	colFile, err := NewColumnFile(colFiles, td, columnBufferPool)
	if err != nil {
		t.Fatalf("Failed to create column file: %s", err)
	}

	tidColumn := NewTID()
	columnBufferPool.BeginTransaction(tidColumn)

	if err := colFile.LoadFromCSV(file, true, ",", false); err != nil {
		t.Fatalf("Failed to load CSV into column file: %s", err)
	}

	startColumn := time.Now()
	columnIter, _ := colFile.IteratorCol([]int{5}, tidColumn)
	columnTupleCount := 0
	for {
		tuple, _ := columnIter()
		if tuple == nil {
			break
		}
		columnTupleCount++
	}
	columnElapsed := time.Since(startColumn).Microseconds()

	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("Failed to reset file pointer: %s", err)
	}

	heapBufferPool, err := NewBufferPool(200)
	if err != nil {
		t.Fatalf("Failed to create heap buffer pool: %s", err)
	}

	heapFileName := "heap_performance_test.dat"
	defer os.Remove(heapFileName)

	heapFile, err := NewHeapFile(heapFileName, &td, heapBufferPool)
	if err != nil {
		t.Fatalf("Failed to create heap file: %s", err)
	}

	tidHeap := NewTID()
	heapBufferPool.BeginTransaction(tidHeap)

	if err := heapFile.LoadFromCSV(file, true, ",", false); err != nil {
		t.Fatalf("Failed to load CSV into heap file: %s", err)
	}

	startHeap := time.Now()
	heapIter, _ := heapFile.Iterator(tidHeap)
	heapTupleCount := 0
	for {
		tuple, _ := heapIter()
		if tuple == nil {
			break
		}
		heapTupleCount++
	}
	heapElapsed := time.Since(startHeap).Microseconds()

	fmt.Printf("New test! 50 rows\n")
	fmt.Printf("Column store iteration took %d microseconds (%d tuples)\n", columnElapsed, columnTupleCount)
	fmt.Printf("Heap file iteration took %d microseconds (%d tuples)\n", heapElapsed, heapTupleCount)
}

func TestLoadCSVPerformance500(t *testing.T) {
	td := TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
		{Fname: "id", Ftype: IntType},
		{Fname: "salary", Ftype: IntType},
		{Fname: "bonus", Ftype: IntType},
		{Fname: "address", Ftype: StringType},
		{Fname: "phone", Ftype: StringType},
		{Fname: "email", Ftype: StringType},
		{Fname: "ig_handle", Ftype: StringType},
		{Fname: "has_pets", Ftype: StringType},
		{Fname: "no_siblings", Ftype: StringType},
		{Fname: "spouse_name", Ftype: StringType},
		{Fname: "child_name", Ftype: StringType},
		{Fname: "has_allergies", Ftype: StringType},
		{Fname: "likes_cats", Ftype: StringType},
	}}

	file, err := os.Open("performance_test_500.csv")
	if err != nil {
		t.Fatalf("Failed to open CSV file: %s", err)
	}
	defer file.Close()

	columnBufferPool, err := NewBufferPool(200)
	if err != nil {
		t.Fatalf("Failed to create column buffer pool: %s", err)
	}

	colFiles := make(map[int]string, 15)
	for i := 0; i < 15; i++ {
		colFiles[i] = fmt.Sprintf("%dperformancetest.dat", i)
	}

	defer func() {
		for _, file := range colFiles {
			os.Remove(file)
		}
	}()

	colFile, err := NewColumnFile(colFiles, td, columnBufferPool)
	if err != nil {
		t.Fatalf("Failed to create column file: %s", err)
	}
	tidColumn := NewTID()
	columnBufferPool.BeginTransaction(tidColumn)

	if err := colFile.LoadFromCSV(file, true, ",", false); err != nil {
		t.Fatalf("Failed to load CSV into column file: %s", err)
	}

	startColumn := time.Now()
	columnIter, _ := colFile.IteratorCol([]int{5}, tidColumn)
	columnTupleCount := 0
	for {
		tuple, _ := columnIter()
		if tuple == nil {
			break
		}
		columnTupleCount++
	}
	columnElapsed := time.Since(startColumn).Microseconds()

	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("Failed to reset file pointer: %s", err)
	}

	heapBufferPool, err := NewBufferPool(200)
	if err != nil {
		t.Fatalf("Failed to create heap buffer pool: %s", err)
	}

	heapFileName := "heap_performance_test.dat"
	defer os.Remove(heapFileName)

	heapFile, err := NewHeapFile(heapFileName, &td, heapBufferPool)
	if err != nil {
		t.Fatalf("Failed to create heap file: %s", err)
	}

	tidHeap := NewTID()
	heapBufferPool.BeginTransaction(tidHeap)

	if err := heapFile.LoadFromCSV(file, true, ",", false); err != nil {
		t.Fatalf("Failed to load CSV into heap file: %s", err)
	}

	startHeap := time.Now()
	heapIter, _ := heapFile.Iterator(tidHeap)
	heapTupleCount := 0
	for {
		tuple, _ := heapIter()
		if tuple == nil {
			break
		}
		heapTupleCount++
	}
	heapElapsed := time.Since(startHeap).Microseconds()

	fmt.Printf("New test! 500 rows\n")
	fmt.Printf("Column store iteration took %d microseconds (%d tuples)\n", columnElapsed, columnTupleCount)
	fmt.Printf("Heap file iteration took %d microseconds (%d tuples)\n", heapElapsed, heapTupleCount)
}

func TestLoadCSVPerformance2000(t *testing.T) {
	td := TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
		{Fname: "id", Ftype: IntType},
		{Fname: "salary", Ftype: IntType},
		{Fname: "bonus", Ftype: IntType},
		{Fname: "address", Ftype: StringType},
		{Fname: "phone", Ftype: StringType},
		{Fname: "email", Ftype: StringType},
		{Fname: "ig_handle", Ftype: StringType},
		{Fname: "has_pets", Ftype: StringType},
		{Fname: "no_siblings", Ftype: StringType},
		{Fname: "spouse_name", Ftype: StringType},
		{Fname: "child_name", Ftype: StringType},
		{Fname: "has_allergies", Ftype: StringType},
		{Fname: "likes_cats", Ftype: StringType},
	}}

	file, err := os.Open("performance_test_2000.csv")
	if err != nil {
		t.Fatalf("Failed to open CSV file: %s", err)
	}
	defer file.Close()

	columnBufferPool, err := NewBufferPool(200)
	if err != nil {
		t.Fatalf("Failed to create column buffer pool: %s", err)
	}

	colFiles := make(map[int]string, 15)
	for i := 0; i < 15; i++ {
		colFiles[i] = fmt.Sprintf("%dperformancetest.dat", i)
	}

	defer func() {
		for _, file := range colFiles {
			os.Remove(file)
		}
	}()

	colFile, err := NewColumnFile(colFiles, td, columnBufferPool)
	if err != nil {
		t.Fatalf("Failed to create column file: %s", err)
	}

	tidColumn := NewTID()
	columnBufferPool.BeginTransaction(tidColumn)

	if err := colFile.LoadFromCSV(file, true, ",", false); err != nil {
		t.Fatalf("Failed to load CSV into column file: %s", err)
	}

	startColumn := time.Now()
	columnIter, _ := colFile.IteratorCol([]int{5}, tidColumn)
	columnTupleCount := 0
	for {
		tuple, _ := columnIter()
		if tuple == nil {
			break
		}
		columnTupleCount++
	}
	columnElapsed := time.Since(startColumn).Microseconds()

	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("Failed to reset file pointer: %s", err)
	}

	heapBufferPool, err := NewBufferPool(200)
	if err != nil {
		t.Fatalf("Failed to create heap buffer pool: %s", err)
	}

	heapFileName := "heap_performance_test.dat"
	defer os.Remove(heapFileName)

	heapFile, err := NewHeapFile(heapFileName, &td, heapBufferPool)
	if err != nil {
		t.Fatalf("Failed to create heap file: %s", err)
	}

	tidHeap := NewTID()
	heapBufferPool.BeginTransaction(tidHeap)

	if err := heapFile.LoadFromCSV(file, true, ",", false); err != nil {
		t.Fatalf("Failed to load CSV into heap file: %s", err)
	}

	startHeap := time.Now()
	heapIter, _ := heapFile.Iterator(tidHeap)
	heapTupleCount := 0
	for {
		tuple, _ := heapIter()
		if tuple == nil {
			break
		}
		heapTupleCount++
	}
	heapElapsed := time.Since(startHeap).Microseconds()

	fmt.Printf("New test! 2000 rows\n")
	fmt.Printf("Column store iteration took %d microseconds (%d tuples)\n", columnElapsed, columnTupleCount)
	fmt.Printf("Heap file iteration took %d microseconds (%d tuples)\n", heapElapsed, heapTupleCount)
}

func TestLoadCSVPerformance10000(t *testing.T) {
	td := TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
		{Fname: "id", Ftype: IntType},
		{Fname: "salary", Ftype: IntType},
		{Fname: "bonus", Ftype: IntType},
		{Fname: "address", Ftype: StringType},
		{Fname: "phone", Ftype: StringType},
		{Fname: "email", Ftype: StringType},
		{Fname: "ig_handle", Ftype: StringType},
		{Fname: "has_pets", Ftype: StringType},
		{Fname: "no_siblings", Ftype: StringType},
		{Fname: "spouse_name", Ftype: StringType},
		{Fname: "child_name", Ftype: StringType},
		{Fname: "has_allergies", Ftype: StringType},
		{Fname: "likes_cats", Ftype: StringType},
	}}

	file, err := os.Open("performance_test_10000.csv")
	if err != nil {
		t.Fatalf("Failed to open CSV file: %s", err)
	}
	defer file.Close()

	columnBufferPool, err := NewBufferPool(200)
	if err != nil {
		t.Fatalf("Failed to create column buffer pool: %s", err)
	}

	colFiles := make(map[int]string, 15)
	for i := 0; i < 15; i++ {
		colFiles[i] = fmt.Sprintf("%dperformancetest.dat", i)
	}

	defer func() {
		for _, file := range colFiles {
			os.Remove(file)
		}
	}()

	colFile, err := NewColumnFile(colFiles, td, columnBufferPool)
	if err != nil {
		t.Fatalf("Failed to create column file: %s", err)
	}

	tidColumn := NewTID()
	columnBufferPool.BeginTransaction(tidColumn)

	if err := colFile.LoadFromCSV(file, true, ",", false); err != nil {
		t.Fatalf("Failed to load CSV into column file: %s", err)
	}

	startColumn := time.Now()
	columnIter, _ := colFile.IteratorCol([]int{5}, tidColumn)
	columnTupleCount := 0
	for {
		tuple, _ := columnIter()
		if tuple == nil {
			break
		}
		columnTupleCount++
	}
	columnElapsed := time.Since(startColumn).Microseconds()

	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("Failed to reset file pointer: %s", err)
	}

	heapBufferPool, err := NewBufferPool(200)
	if err != nil {
		t.Fatalf("Failed to create heap buffer pool: %s", err)
	}

	heapFileName := "heap_performance_test.dat"
	defer os.Remove(heapFileName)

	heapFile, err := NewHeapFile(heapFileName, &td, heapBufferPool)
	if err != nil {
		t.Fatalf("Failed to create heap file: %s", err)
	}

	tidHeap := NewTID()
	heapBufferPool.BeginTransaction(tidHeap)

	if err := heapFile.LoadFromCSV(file, true, ",", false); err != nil {
		t.Fatalf("Failed to load CSV into heap file: %s", err)
	}

	startHeap := time.Now()
	heapIter, _ := heapFile.Iterator(tidHeap)
	heapTupleCount := 0
	for {
		tuple, _ := heapIter()
		if tuple == nil {
			break
		}
		heapTupleCount++
	}
	heapElapsed := time.Since(startHeap).Microseconds()

	fmt.Printf("New test! 10,000 rows\n")
	fmt.Printf("Column store iteration took %d microseconds (%d tuples)\n", columnElapsed, columnTupleCount)
	fmt.Printf("Heap file iteration took %d microseconds (%d tuples)\n", heapElapsed, heapTupleCount)
}

func TestLoadCSVPerformance20000(t *testing.T) {
	td := TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
		{Fname: "id", Ftype: IntType},
		{Fname: "salary", Ftype: IntType},
		{Fname: "bonus", Ftype: IntType},
		{Fname: "address", Ftype: StringType},
		{Fname: "phone", Ftype: StringType},
		{Fname: "email", Ftype: StringType},
		{Fname: "ig_handle", Ftype: StringType},
		{Fname: "has_pets", Ftype: StringType},
		{Fname: "no_siblings", Ftype: StringType},
		{Fname: "spouse_name", Ftype: StringType},
		{Fname: "child_name", Ftype: StringType},
		{Fname: "has_allergies", Ftype: StringType},
		{Fname: "likes_cats", Ftype: StringType},
	}}

	file, err := os.Open("performance_test_20000.csv")
	if err != nil {
		t.Fatalf("Failed to open CSV file: %s", err)
	}
	defer file.Close()

	columnBufferPool, err := NewBufferPool(200)
	if err != nil {
		t.Fatalf("Failed to create column buffer pool: %s", err)
	}

	colFiles := make(map[int]string, 15)
	for i := 0; i < 15; i++ {
		colFiles[i] = fmt.Sprintf("%dperformancetest.dat", i)
	}

	defer func() {
		for _, file := range colFiles {
			os.Remove(file)
		}
	}()

	colFile, err := NewColumnFile(colFiles, td, columnBufferPool)
	if err != nil {
		t.Fatalf("Failed to create column file: %s", err)
	}

	tidColumn := NewTID()
	columnBufferPool.BeginTransaction(tidColumn)

	if err := colFile.LoadFromCSV(file, true, ",", false); err != nil {
		t.Fatalf("Failed to load CSV into column file: %s", err)
	}

	startColumn := time.Now()
	columnIter, _ := colFile.IteratorCol([]int{5}, tidColumn)
	columnTupleCount := 0
	for {
		tuple, _ := columnIter()
		if tuple == nil {
			break
		}
		columnTupleCount++
	}
	columnElapsed := time.Since(startColumn).Microseconds()

	if _, err := file.Seek(0, 0); err != nil {
		t.Fatalf("Failed to reset file pointer: %s", err)
	}

	heapBufferPool, err := NewBufferPool(200)
	if err != nil {
		t.Fatalf("Failed to create heap buffer pool: %s", err)
	}

	heapFileName := "heap_performance_test.dat"
	defer os.Remove(heapFileName)

	heapFile, err := NewHeapFile(heapFileName, &td, heapBufferPool)
	if err != nil {
		t.Fatalf("Failed to create heap file: %s", err)
	}

	tidHeap := NewTID()
	heapBufferPool.BeginTransaction(tidHeap)

	if err := heapFile.LoadFromCSV(file, true, ",", false); err != nil {
		t.Fatalf("Failed to load CSV into heap file: %s", err)
	}

	startHeap := time.Now()
	heapIter, _ := heapFile.Iterator(tidHeap)
	heapTupleCount := 0
	for {
		tuple, _ := heapIter()
		if tuple == nil {
			break
		}
		heapTupleCount++
	}
	heapElapsed := time.Since(startHeap).Microseconds()

	fmt.Printf("New test! 20,000 rows\n")
	fmt.Printf("Column store iteration took %d microseconds (%d tuples)\n", columnElapsed, columnTupleCount)
	fmt.Printf("Heap file iteration took %d microseconds (%d tuples)\n", heapElapsed, heapTupleCount)
}
