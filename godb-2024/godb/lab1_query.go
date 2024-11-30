package godb

import (
	"os"
)

/*
computeFieldSum should (1) load the csv file named fileName into a heap file
(see [HeapFile.LoadFromCSV]), (2) compute the sum of the integer field named
sumField string and, (3) return its value as an int.

The supplied csv file is comma delimited and has a header.

If the file doesn't exist, can't be opened, the field doesn't exist, or the
field is not an integer, you should return an error.

Note that when you create a HeapFile, you will need to supply a file name;
you can supply a non-existant file, in which case it will be created.
However, subsequent invocations of this method will result in tuples being
reinserted into this file unless you delete (e.g., with [os.Remove] it before
calling NewHeapFile.

Note that you should NOT pass fileName into NewHeapFile -- fileName is a CSV
file that you should call LoadFromCSV on.
*/
func computeFieldSum(bp *BufferPool, fileName string, td TupleDesc, sumField string) (int, error) {

	sum := 0
	os.Remove("test")
	heap_file, err := NewHeapFile("test", &td, bp)
	if err != nil {
		return 0, err
	}
	index, err := findFieldInTd(FieldType{Fname: sumField}, &td)
	if err != nil {
		return 0, err
	}
	file, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}
	err = heap_file.LoadFromCSV(file, true, ",", false)
	if err != nil {
		return 0, err
	}
	tid := NewTID()
	bp.BeginTransaction(tid)
	iterator, err := heap_file.Iterator(tid)
	if err != nil {
		return 0, err
	}
	for {
		t, err := iterator()
		if err != nil || t == nil {
			bp.CommitTransaction(tid)
			return sum, err
		}
		val_to_add, a := t.Fields[index].(IntField)
		if !a {
			bp.CommitTransaction(tid)
			return sum, err
		}
		sum += int(val_to_add.Value)
	}
}
