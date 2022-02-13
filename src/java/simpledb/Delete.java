package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId _tid;
    private OpIterator _child;
    private TupleDesc _td;
    private int counter;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        _tid = t;
        _child = child;
        _td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"number of deleted tuples"});
        counter = 0;
        called = false;
    }

    public TupleDesc getTupleDesc() {
        return _td;
    }

    public void open() throws DbException, TransactionAbortedException {
        _child.open();
        super.open();
        counter = 0;
    }

    public void close() {
        _child.close();
        super.close();
        counter = 0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        _child.rewind();
        counter = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (called) {
            return null;
        }
        called = true;
        while (_child.hasNext()) {
            Tuple tuple = _child.next();
            try {
                Database.getBufferPool().deleteTuple(_tid, tuple);
                counter++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Tuple t = new Tuple(_td);
        t.setField(0, new IntField(counter));
        return t;
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
