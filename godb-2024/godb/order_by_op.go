package godb

import (
	"sort"
)

type OrderBy struct {
	orderBy        []Expr // OrderBy should include these two fields (used by parser)
	child          Operator
	ascending_list []bool
}

// Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	return &OrderBy{
		orderBy:        orderByFields,
		child:          child,
		ascending_list: ascending,
	}, nil

}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {

	child_iter, _ := o.child.Iterator(tid)
	res := make([]*Tuple, 0)
	for tuple, _ := child_iter(); tuple != nil; tuple, _ = child_iter() {
		res = append(res, tuple)
	}
	count := 0
	sort.Sort(sortTuples{orderBy: o.orderBy, ascending_list: o.ascending_list, all: res})

	return func() (*Tuple, error) {
		if count >= len(res) {
			return nil, nil
		}

		tuple := res[count]
		count += 1
		return tuple, nil
	}, nil
}

type sortTuples struct {
	orderBy        []Expr
	ascending_list []bool
	all            []*Tuple
}

func (s sortTuples) Less(a, b int) bool {
	tupleA := s.all[a]
	tupleB := s.all[b]

	for index := 0; index < len(s.orderBy); index++ {
		expr := s.orderBy[index]

		valA, _ := expr.EvalExpr(tupleA)
		valB, _ := expr.EvalExpr(tupleB)

		// If the values are equal, move to the next expression
		if valA.EvalPred(valB, OpEq) {
			continue
		}

		if s.ascending_list[index] {
			return valA.EvalPred(valB, OpLt) // Ascending order
		} else {
			return !valA.EvalPred(valB, OpLt) // Descending order
		}
	}

	return false // If all values are equal
}

func (s sortTuples) Swap(a, b int) {
	temp := s.all[a]
	s.all[a] = s.all[b]
	s.all[b] = temp
}

func (s sortTuples) Len() int {
	return len(s.all)
}
