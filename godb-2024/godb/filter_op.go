package godb

type Filter struct {
	op    BoolOp
	left  Expr
	right Expr
	child Operator
}

// Construct a filter operator on ints.
func NewFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter, error) {
	return &Filter{op, field, constExpr, child}, nil
}

// Return a TupleDescriptor for this filter op.
func (f *Filter) Descriptor() *TupleDesc {
	return f.child.Descriptor()
}

// Filter operator implementation. This function should iterate over the results
// of the child iterator and return a tuple if it satisfies the predicate.
//
// HINT: you can use [types.evalPred] to compare two values.
func (f *Filter) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	child_iter, _ := f.child.Iterator(tid)

	return func() (*Tuple, error) {
		for {
			tuple, _ := child_iter()
			if tuple == nil {
				return nil, nil
			}

			leftVal, err := f.left.EvalExpr(tuple)
			if err != nil {
				return nil, err
			}

			rightVal, err := f.right.EvalExpr(tuple)
			if err != nil {
				return nil, err
			}

			if leftVal.EvalPred(rightVal, f.op) {
				return tuple, nil
			}
		}
	}, nil
}
