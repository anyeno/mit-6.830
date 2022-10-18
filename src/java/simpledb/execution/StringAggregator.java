package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private TupleDesc td;
    private Map<Field, ArrayList<String>> map;
    private ArrayList<Tuple> tuples;
    private boolean get_td = false;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        if(what != Op.COUNT) {
            throw new IllegalStateException("op 必须是 count");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if(gbfield == NO_GROUPING) {
            tuples = new ArrayList<>();
        }
        else
            map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        String aname;
        if(gbfield == NO_GROUPING){
            if(!get_td) {
                aname = tup.getTupleDesc().getFieldName(afield);
                Type [] typeAr = new Type[]{Type.STRING_TYPE};
                String[] fieldAr = new String[]{aname};
                td = new TupleDesc(typeAr, fieldAr);
                get_td = true;
            }
            Tuple t = new Tuple(td);
            t.setField(0, tup.getField(afield));
            tuples.add(t);
        }
        else{
            if(!get_td) {
                aname = tup.getTupleDesc().getFieldName(afield);
                String gbname = tup.getTupleDesc().getFieldName(gbfield);
                Type [] typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
                String[] fieldAr = new String[]{gbname, aname};
                td = new TupleDesc(typeAr, fieldAr);
                get_td = true;
            }
            Field gbField = tup.getField(gbfield);
            Field aField = tup.getField(afield);
            String aFieldValue = ((StringField)aField).getValue();
            if(!map.containsKey(gbField))
                map.put(gbField, new ArrayList<>());
            map.get(gbField).add(aFieldValue);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    private class g_OpIterator implements OpIterator{

        private ArrayList<Field> gbField;
        private int g_cur;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            gbField = new ArrayList<>();
            for(Field key: map.keySet()) {
                gbField.add(key);
            }
            g_cur = 0;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return g_cur < gbField.size();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext()) {
                throw new NoSuchElementException();
            }
            int aFieldCount = 0;
            ArrayList<String> in_tuples = map.get(gbField.get(g_cur));

            if(what == Op.COUNT) {
                aFieldCount = in_tuples.size();
            }
            Tuple res = new Tuple(td);
            res.setField(0, gbField.get(g_cur));
            res.setField(1, new IntField(aFieldCount));
            g_cur ++;
            return res;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        @Override
        public void close() {
            g_cur = 0;
            gbField = null;
        }
    }

    private class nog_OpIterator implements OpIterator {

        private int cur;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            cur = 0;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return cur < tuples.size();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return tuples.get(cur++);
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        @Override
        public void close() {
            cur = 0;
        }
    }


    public OpIterator iterator() {
        // TODO: some code goes here
        if(gbfield != NO_GROUPING)
            return new g_OpIterator();
        else return new nog_OpIterator();
    }

}
