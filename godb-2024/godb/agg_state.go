package godb

// interface for an aggregation state
type AggState interface {
	// Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState struct {
	sum   int64
	alias string
	expr  Expr
}

func (a *SumAggState) Copy() AggState {
	// TODO: some code goes here
	return &SumAggState{a.sum, a.alias, a.expr}
}

func intAggGetter(v DBValue) any {
	get := v.(IntField)
	return get.Value
}

func stringAggGetter(v DBValue) any {
	get := v.(IntField)
	return get.Value
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	a.sum = 0
	a.alias = alias
	a.expr = expr

	return nil
}

func (a *SumAggState) AddTuple(t *Tuple) {
	get, _ := a.expr.EvalExpr(t)
	add, _ := get.(IntField)
	a.sum += add.Value
}

func (a *SumAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{{a.alias, "", IntType}},
	}
}

func (a *SumAggState) Finalize() *Tuple {
	return &Tuple{*a.GetTupleDesc(), []DBValue{IntField{a.sum}}, nil}
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState struct {
	alias   string
	expr    Expr
	count   int
	average float32
	sum     int64
	fun     func(DBValue) any
}

func (a *AvgAggState) Copy() AggState {
	return &AvgAggState{a.alias, a.expr, a.count, a.average, a.sum, a.fun}
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.average = 0
	a.sum = 0
	a.count = 0
	return nil
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	get, _ := a.expr.EvalExpr(t)
	value, _ := get.(IntField)
	a.sum += value.Value
	a.average = float32(a.sum / int64(a.count))
	a.count += 1
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{{a.alias, "", IntType}},
	}
}

func (a *AvgAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	res := IntField{a.sum / int64(a.count)}
	return &Tuple{*td, []DBValue{res}, nil}
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState struct {
	maximum DBValue
	alias   string
	expr    Expr
	fun     func(DBValue) any
}

func (a *MaxAggState) Copy() AggState {
	return &MaxAggState{a.maximum, a.alias, a.expr, a.fun}
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	a.maximum = nil
	a.alias = alias
	a.expr = expr
	return nil
}

func (a *MaxAggState) AddTuple(t *Tuple) {
	if tmpVal, _ := a.expr.EvalExpr(t); a.maximum == nil {
		a.maximum = tmpVal
		return
	} else if tmpVal.EvalPred(a.maximum, OpGt) {
		a.maximum = tmpVal
	}
}

func (a *MaxAggState) GetTupleDesc() *TupleDesc {
	res := &TupleDesc{
		Fields: []FieldType{{a.alias, "", IntType}},
	}
	return res
}

func (a *MaxAggState) Finalize() *Tuple {
	return &Tuple{*a.GetTupleDesc(), []DBValue{a.maximum}, nil}
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState struct {
	minimum DBValue
	alias   string
	expr    Expr
	fun     func(DBValue) any
}

func (a *MinAggState) Copy() AggState {
	return &MinAggState{a.minimum, a.alias, a.expr, a.fun}
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	a.minimum = nil
	a.alias = alias
	a.expr = expr
	return nil
}

func (a *MinAggState) AddTuple(t *Tuple) {
	if tmpVal, _ := a.expr.EvalExpr(t); a.minimum == nil {
		a.minimum = tmpVal
		return
	} else if tmpVal.EvalPred(a.minimum, OpLt) {
		a.minimum = tmpVal
	}
}

func (a *MinAggState) GetTupleDesc() *TupleDesc {
	res := &TupleDesc{
		Fields: []FieldType{{a.alias, "", IntType}},
	}
	return res
}

func (a *MinAggState) Finalize() *Tuple {
	return &Tuple{*a.GetTupleDesc(), []DBValue{a.minimum}, nil}
}
