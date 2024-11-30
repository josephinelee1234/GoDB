package godb

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{
		child:     child,
		limitTups: lim,
	}
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	count := 0
	expr, _ := l.limitTups.EvalExpr(nil)
	child_iter, _ := l.child.Iterator(tid)

	return func() (*Tuple, error) {
		for {
			tuple, err := child_iter()
			if err != nil {
				return nil, err
			}
			if tuple == nil || count >= int(expr.(IntField).Value) {
				return nil, nil
			}
			count += 1
			return tuple, nil
		}
	}, nil
}
