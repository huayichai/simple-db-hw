package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId _tid;
    private OpIterator _child;
    private int _tableId;
    private TupleDesc _td;
    private int counter;
    private boolean called;


    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException("TupleDesc of child differs from table");
        }
        _tid = t;
        _child = child;
        _tableId = tableId;
        counter = 0;
        called = false;
        _td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"number of inserted tuples"});
    }

    public TupleDesc getTupleDesc() {
        return _td;
    }

    public void open() throws DbException, TransactionAbortedException {
        counter = 0;
        _child.open();
        super.open();
    }

    public void close() {
        counter = 0;
        _child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        counter = 0;
        _child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (called) {
            return null;
        }
        called = true;
        while (_child.hasNext()) {
            Tuple tuple = _child.next();
            try {
                Database.getBufferPool().insertTuple(_tid, _tableId, tuple);
                counter++;
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        Tuple tuple = new Tuple(_td);
        tuple.setField(0, new IntField(counter));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {_child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        _child = children[0];
    }
}
