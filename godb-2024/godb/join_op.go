package godb

import (
	"errors"
	"sort"
)

type EqualityJoin struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator // Operators for the two inputs of the join

	// The maximum number of records of intermediate state that the join should
	// use (only required for optional exercise).
	maxBufferSize int
}

// Constructor for a join of integer expressions.
//
// Returns an error if either the left or right expression is not an integer.
func NewJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin, error) {
	if leftField.GetExprType().Ftype != rightField.GetExprType().Ftype {
		return nil, errors.New("not proper types")
	}
	switch leftField.GetExprType().Ftype {
	case IntType:
		return &EqualityJoin{leftField, rightField, &left, &right, maxBufferSize}, nil
	case StringType:
		return &EqualityJoin{leftField, rightField, &left, &right, maxBufferSize}, nil
	}
	return nil, errors.New("not proper types")
}

// Return a TupleDesc for this join. The returned descriptor should contain the
// union of the fields in the descriptors of the left and right operators.
//
// HINT: use [TupleDesc.merge].
func (hj *EqualityJoin) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return (*hj.left).Descriptor().merge((*hj.right).Descriptor())
}

// Join operator implementation. This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
//
// HINT: When implementing the simple nested loop join, you should keep in mind
// that you only iterate through the left iterator once (outer loop) but iterate
// through the right iterator once for every tuple in the left iterator (inner
// loop).
//
// HINT: You can use [Tuple.joinTuples] to join two tuples.
//
// OPTIONAL EXERCISE: the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out. To pass this test, you will need to use something other than a nested
// loops join.

// sort merge join
// hash join
func (joinOp *EqualityJoin) Iterator(transactionID TransactionID) (func() (*Tuple, error), error) {
	leftIterator, _ := (*joinOp.left).Iterator(transactionID)
	leftTuples, _ := fetchAllTuples(leftIterator)

	rightIterator, _ := (*joinOp.right).Iterator(transactionID)
	rightTuples, _ := fetchAllTuples(rightIterator)

	sortTupleList(leftTuples, joinOp.leftField)
	sortTupleList(rightTuples, joinOp.rightField)

	joinedTuples := mergeAndJoinTuples(leftTuples, rightTuples, joinOp.leftField, joinOp.rightField)

	currentIndex := 0
	return func() (*Tuple, error) {
		if currentIndex >= len(joinedTuples) {
			return nil, nil
		}
		currentIndex += 1
		return joinedTuples[currentIndex-1], nil
	}, nil
}

func fetchAllTuples(iterator func() (*Tuple, error)) ([]*Tuple, error) {
	tuples := []*Tuple{}
	for tuple, _ := iterator(); tuple != nil; tuple, _ = iterator() {
		tuples = append(tuples, tuple)
	}
	return tuples, nil
}

func sortTupleList(tuples []*Tuple, field Expr) {
	sort.Slice(tuples, func(i, j int) bool {
		compareResult, _ := tuples[i].compareField(tuples[j], field)

		return compareResult < OrderedEqual
	})
}

func mergeAndJoinTuples(leftTuples, rightTuples []*Tuple, leftField, rightField Expr) []*Tuple {
	joinedTuples := []*Tuple{}
	leftIndex, rightIndex := 0, 0

	for leftIndex < len(leftTuples) && rightIndex < len(rightTuples) {
		order, err := compare(leftTuples[leftIndex], rightTuples[rightIndex], leftField, rightField)
		if err != nil {
			break
		}

		switch order {
		case OrderedEqual:
			mergeEqualTuples(leftTuples, rightTuples, leftIndex, rightIndex, leftField, rightField, &joinedTuples)
			leftIndex = findEqualRange(leftTuples, leftIndex, leftField)
			rightIndex = findEqualRange(rightTuples, rightIndex, rightField)
		case OrderedLessThan:
			leftIndex += 1
		case OrderedGreaterThan:
			rightIndex += 1
		}
	}

	return joinedTuples
}

func mergeEqualTuples(leftTuples, rightTuples []*Tuple, leftIndex, rightIndex int, leftField, rightField Expr, joinedTuples *[]*Tuple) {
	leftEnd := findEqualRange(leftTuples, leftIndex, leftField)
	rightEnd := findEqualRange(rightTuples, rightIndex, rightField)

	for i := leftIndex; i < leftEnd; i++ {
		for j := rightIndex; j < rightEnd; j++ {
			*joinedTuples = append(*joinedTuples, joinTuples(leftTuples[i], rightTuples[j]))
		}
	}
}

func compare(leftTuple, rightTuple *Tuple, leftField, rightField Expr) (orderByState, error) {
	leftExpr, err := leftField.EvalExpr(leftTuple)
	if err != nil {
		return 0, err
	}
	rightExpr, err := rightField.EvalExpr(rightTuple)
	if err != nil {
		return 0, err
	}

	switch leftVal := leftExpr.(type) {
	case IntField:
		rightVal := rightExpr.(IntField)
		switch {
		case leftVal.Value < rightVal.Value:
			return OrderedLessThan, nil
		case leftVal.Value > rightVal.Value:
			return OrderedGreaterThan, nil
		default:
			return OrderedEqual, nil
		}
	case StringField:
		rightVal := rightExpr.(StringField)
		switch {
		case leftVal.Value < rightVal.Value:
			return OrderedLessThan, nil
		case leftVal.Value > rightVal.Value:
			return OrderedGreaterThan, nil
		default:
			return OrderedEqual, nil
		}
	default:
		return OrderedEqual, nil
	}
}

// find the range of tuples that are equal based on a field
func findEqualRange(tuples []*Tuple, startIndex int, field Expr) int {
	endIndex := startIndex + 1
	for endIndex < len(tuples) {
		result, err := tuples[endIndex].compareField(tuples[startIndex], field)
		if err != nil || result != OrderedEqual {
			break
		}
		endIndex += 1
	}
	return endIndex
}
