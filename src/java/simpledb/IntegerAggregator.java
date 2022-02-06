package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;


/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Aggregator.Op _op;
    private HashMap<Field, Integer> groupMap;
    private HashMap<Field, ArrayList<Integer>> avgMap;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        _gbfield = gbfield;
        _gbfieldtype = gbfieldtype;
        _afield = afield;
        _op = what;
        groupMap = new HashMap<>();
        avgMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = _gbfield == Aggregator.NO_GROUPING ? null : tup.getField(_gbfield);
        IntField intField = (IntField) tup.getField(_afield);
        switch(_op) {
            case MIN:
                if (!groupMap.containsKey(groupField)) {
                    groupMap.put(groupField, intField.getValue());
                } else {
                    groupMap.put(groupField, Math.min(intField.getValue(), groupMap.get(groupField)));
                }
                break;
            case MAX:
                if (!groupMap.containsKey(groupField)) {
                    groupMap.put(groupField, intField.getValue());
                } else {
                    groupMap.put(groupField, Math.max(intField.getValue(), groupMap.get(groupField)));
                }
                break;
            case SUM:
                if (!groupMap.containsKey(groupField)) {
                    groupMap.put(groupField, intField.getValue());
                } else {
                    groupMap.put(groupField, intField.getValue() + groupMap.get(groupField));
                }
                break;
            case AVG:
                if (!avgMap.containsKey(groupField)) {
                    ArrayList<Integer> list = new ArrayList<>();
                    list.add(intField.getValue());
                    avgMap.put(groupField, list);
                } else {
                    avgMap.get(groupField).add(intField.getValue());
                }
                break;
            case COUNT:
                if (!groupMap.containsKey(groupField)) {
                    groupMap.put(groupField, 1);
                } else {
                    groupMap.put(groupField, 1 + groupMap.get(groupField));
                }
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        Type[] typeAr;
        if (_gbfield != Aggregator.NO_GROUPING) {
            typeAr = new Type[] {_gbfieldtype, Type.INT_TYPE};
        } else {
            typeAr = new Type[] {Type.INT_TYPE};
        }
        TupleDesc td = new TupleDesc(typeAr);
        return new IntegerOpIterator(td);
    }

    private class IntegerOpIterator implements OpIterator  {

        private TupleDesc _td;
        private Iterator<Map.Entry<Field, Integer>> it;
        private Iterator<Map.Entry<Field, ArrayList<Integer>>> avg_it;

        private boolean isAvg;

        public IntegerOpIterator(TupleDesc td) {
            _td = td;
            it = null;
            avg_it = null;

            isAvg = _op.equals(Aggregator.Op.AVG);
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (isAvg) {
                avg_it = avgMap.entrySet().iterator();
            } else {
                it = groupMap.entrySet().iterator();  
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (isAvg && avg_it != null) {
                return avg_it.hasNext();
            } else if (it != null) {
                return it.hasNext();
            }
            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Field t1;
            int t2;
            if (isAvg) {
                if (avg_it == null) return null;
                if (!avg_it.hasNext()) {
                    throw new NoSuchElementException("");
                }
                Map.Entry<Field, ArrayList<Integer>> e = avg_it.next();
                t1 = e.getKey();
                ArrayList<Integer> list = e.getValue();
                int sum = 0;
                for (int val : list) {
                   sum += val;
                }
                t2 = sum / list.size();
            } else {
                if (it == null) return null;
                if (!it.hasNext()) {
                    throw new NoSuchElementException("");
                }
                Map.Entry<Field, Integer> e = it.next();
                t1 = e.getKey();
                t2 = e.getValue();
            }
            Tuple t = new Tuple(_td);
            if (_gbfield == Aggregator.NO_GROUPING) {
                t.setField(0, new IntField(t2));
            } else {
                t.setField(0, t1);
                t.setField(1, new IntField(t2));
            }
            return t;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return _td;
        }

        @Override
        public void close() {
            it = null;
            avg_it = null;
        }

    }

}
