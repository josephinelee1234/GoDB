package godb

type DeleteOp struct {
	deleteFile DBFile
	child      Operator
	res        *TupleDesc
}

// Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{
		deleteFile: deleteFile,
		child:      child,
		res: &TupleDesc{[]FieldType{{
			Fname: "count",
			Ftype: IntType,
		}}},
	}
}

// The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (i *DeleteOp) Descriptor() *TupleDesc {
	return i.res
}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	count := int64(0)
	child_iter, err := dop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	for {
		t, err := child_iter()
		if err != nil {
			return nil, err
		}
		if t == nil {
			break
		}

		err = dop.deleteFile.deleteTuple(t, tid)
		if err != nil {
			return nil, err
		}
		count++
	}

	return func() (*Tuple, error) {
		return &Tuple{
			Desc:   *dop.Descriptor(),
			Fields: []DBValue{IntField{count}},
		}, nil
	}, nil
}
