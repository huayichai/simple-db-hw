package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator _child;
    private int _afield;
    private int _gfield;
    private Aggregator.Op _aop;
    private Aggregator _aggregator;
    private OpIterator groupResultIt;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    _child = child;
        _afield = afield;
        _gfield = gfield;
        _aop = aop;
        Type gbfieldType = null;
        if (_gfield != Aggregator.NO_GROUPING) {
            gbfieldType = _child.getTupleDesc().getFieldType(_gfield);
        }
        if (_child.getTupleDesc().getFieldType(_afield).equals(Type.INT_TYPE)) {
            _aggregator = new IntegerAggregator(_gfield, gbfieldType, _afield, _aop);
        } else {
            _aggregator = new StringAggregator(_gfield, gbfieldType, _afield, _aop);
        }
        try {
            _child.open();
            while (_child.hasNext()) {
                _aggregator.mergeTupleIntoGroup(_child.next());
            }
            groupResultIt = _aggregator.iterator();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    if (_gfield == Aggregator.NO_GROUPING) {
            return Aggregator.NO_GROUPING;
        } else {
            return _gfield;
        }
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        if (_gfield == Aggregator.NO_GROUPING) {
            return null;
        } else {
            // return _child.getTupleDesc().getFieldName(_gfield);
            return groupResultIt.getTupleDesc().getFieldName(0);
        }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return _afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // return _child.getTupleDesc().getFieldName(_afield);
        return groupResultIt.getTupleDesc().getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return _aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        groupResultIt.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple t;
        try {
            t = groupResultIt.next();
        } catch(Exception e) {
            t = null;
        }
        return t;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    groupResultIt.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc td = groupResultIt.getTupleDesc();
        Type[] typeAr;
        String[] fieldAr;
        if (_gfield != Aggregator.NO_GROUPING) {
            typeAr = new Type[] {td.getFieldType(0), td.getFieldType(1)};
            fieldAr = new String[] {_child.getTupleDesc().getFieldName(_gfield), _aop.toString() + " (" + _child.getTupleDesc().getFieldName(_afield) +")"};
        } else {
            typeAr = new Type[] {td.getFieldType(0)};
            fieldAr = new String[] {_aop.toString() + " (" + _child.getTupleDesc().getFieldName(_afield) +")"};
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    public void close() {
        groupResultIt.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{_child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    _child = children[0];
    }
    
}
