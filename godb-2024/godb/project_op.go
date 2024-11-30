package godb

import (
	"errors"
)

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	distinct     bool
}

// Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	if len(selectFields) != len(outputNames) {
		return nil, errors.New("these should be the same length")
	}

	return &Project{
		selectFields: selectFields,
		outputNames:  outputNames,
		distinct:     distinct,
		child:        child,
	}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	proj_desc := &TupleDesc{
		Fields: make([]FieldType, len(p.selectFields)),
	}

	for i := 0; i < len(p.selectFields); i++ {
		get := p.selectFields[i].GetExprType()
		get.Fname = p.outputNames[i]
		proj_desc.Fields[i] = get
	}

	return proj_desc
}

// Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	child_iter, _ := p.child.Iterator(tid)
	proj_desc := *p.Descriptor()
	var seenKeys map[any]struct{}
	if p.distinct {
		seenKeys = make(map[any]struct{})
	}

	return func() (*Tuple, error) {
		for {
			tuple, _ := child_iter()
			if tuple == nil {
				return nil, nil
			}

			new := &Tuple{
				Desc:   proj_desc,
				Fields: make([]DBValue, len(p.selectFields)),
			}

			for i := 0; i < len(p.selectFields); i++ {
				field := p.selectFields[i]
				temp, err := field.EvalExpr(tuple)
				if err != nil {
					return nil, err
				}
				new.Fields[i] = temp
			}

			if p.distinct {
				tupleKey := new.tupleKey()
				if _, exists := seenKeys[tupleKey]; exists {
					continue
				}
				seenKeys[tupleKey] = struct{}{}
			}

			return new, nil
		}
	}, nil
}
