package godb

type InsertOp struct {
	insertFile DBFile
	child      Operator
	res        *TupleDesc
}

// Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{
		insertFile: insertFile,
		child:      child,
		res: &TupleDesc{[]FieldType{{
			Fname: "count",
			Ftype: IntType,
		}}},
	}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	return i.res
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {

	child_iter, err := iop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	counter := int64(0)

	return func() (*Tuple, error) {
		for {
			t, err := child_iter()
			if err != nil {
				return nil, err
			}
			if t == nil {
				break
			}

			err = iop.insertFile.insertTuple(t, tid)
			if err != nil {
				return nil, err
			}
			counter += 1
		}

		return &Tuple{
			Desc:   *iop.Descriptor(),
			Fields: []DBValue{IntField{counter}},
		}, nil
	}, nil
}
