package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int _gbfield;
    private Type _gbFieldType;
    private int _afield;
    private Aggregator.Op _op;
    private HashMap<Field, Integer> groupMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        _gbfield = gbfield;
        _gbFieldType = gbfieldtype;
        _afield = afield;
        _op = what;
        if (!_op.equals(Aggregator.Op.COUNT)) {
            throw new IllegalArgumentException("StringAggregator not support operator " + _op.toString());
        }
        groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = _gbfield == Aggregator.NO_GROUPING ? null : tup.getField(_gbfield);
        if (!groupMap.containsKey(groupField)) {
            groupMap.put(groupField, 1);
        } else {
            groupMap.put(groupField, groupMap.get(groupField) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new StringOpIterator();
    }

    private class StringOpIterator implements OpIterator {

        private TupleDesc td;
        private Iterator<Map.Entry<Field, Integer>> it;

        public StringOpIterator() {
            it = null;
            Type[] typeAr;
            if (_gbfield != Aggregator.NO_GROUPING) {
                typeAr = new Type[] {_gbFieldType, Type.STRING_TYPE};
            } else {
                typeAr = new Type[] {Type.STRING_TYPE};
            }
            td = new TupleDesc(typeAr);
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = groupMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it != null) {
                return it.hasNext();
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null) {
                return null;
            } else if (it.hasNext()) {
                Map.Entry<Field, Integer> e = it.next();
                Field t1 = e.getKey();
                Integer t2 = e.getValue();
                Tuple t = new Tuple(td);
                if (_gbfield == Aggregator.NO_GROUPING) {
                    t.setField(0, new IntField(t2));
                } else {
                    t.setField(0, t1);
                    t.setField(1, new IntField(t2));
                }
                return t;
            } else {
                throw new NoSuchElementException("");
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        @Override
        public void close() {
            it = null;
        }

    }
}
