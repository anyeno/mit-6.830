package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private OpIterator child;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // TODO: some code goes here
        this.t = t;
        this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        Type[] typear = new Type[]{Type.INT_TYPE};
        return new TupleDesc(typear);
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        close();
        open();
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
    private boolean is_deleted = false;
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if(is_deleted) return null;
        is_deleted = true;
        int count = 0;
        while(child.hasNext()) {
            Tuple tuple = child.next();
            try {
                assert tuple != null;
//                int tid = Database.getCatalog().getTableId_td(child.getTupleDesc());
                Database.getBufferPool().deleteTuple(t, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
            count ++;
        }
        Type[] typear = new Type[]{Type.INT_TYPE};
//        String [] fieldar = new String[]{"count"};
        Tuple res = new Tuple(new TupleDesc(typear));
        res.setField(0, new IntField(count));
        return res;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        child = children[0];
    }

}
