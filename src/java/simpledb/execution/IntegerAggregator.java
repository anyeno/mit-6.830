package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private TupleDesc td;
    private Map<Field, ArrayList<Integer>> map;
    private ArrayList<Tuple> tuples;
    private boolean get_td = false;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     *                    分组字段 根据这个字段来分组
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     *                    聚合字段  把这个字段聚合成一个 Op `MIN, MAX, SUM, AVG, COUNT ...`
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        String aname;
        if(gbfield == NO_GROUPING){
            if(!get_td) {
                aname = tup.getTupleDesc().getFieldName(afield);
                Type [] typeAr = new Type[]{Type.INT_TYPE};
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
            Field gbFieldValue = tup.getField(gbfield);
            Field aField = tup.getField(afield);
            int aFieldValue = ((IntField)aField).getValue();
            if(!map.containsKey(gbFieldValue))
                map.put(gbFieldValue, new ArrayList<>());
            map.get(gbFieldValue).add(aFieldValue);
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
    private class g_OpIterator implements OpIterator{

        private ArrayList<Field> gbField;
        private int g_cur;
        private boolean is_opened = false;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            gbField = new ArrayList<>();
            is_opened = true;
//            System.out.println("已创建 array");
            for(Field key: map.keySet()) {
                gbField.add(key);
            }
            g_cur = 0;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(!is_opened) {
                throw new IllegalStateException("没打开");
            }
            return g_cur != gbField.size();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext()) {
                throw new NoSuchElementException();
            }
            int aFieldValue = 0;
            ArrayList<Integer> in_tuples = map.get(gbField.get(g_cur));
            if(what == Op.MIN) {
                aFieldValue = Collections.min(in_tuples);
            } else if(what == Op.MAX) {
                aFieldValue = Collections.max(in_tuples);
            } else if(what == Op.AVG) {
                int sum = 0;
                for (Integer in_tuple : in_tuples) {
                    sum += in_tuple;
                }
                aFieldValue = sum / in_tuples.size();
            } else if(what == Op.SUM) {
                int sum = 0;
                for (Integer in_tuple : in_tuples) {
                    sum += in_tuple;
                }
                aFieldValue = sum;
            } else if(what == Op.COUNT) {
                aFieldValue = in_tuples.size();
            }
            Tuple res = new Tuple(td);
            res.setField(0, gbField.get(g_cur++));
            res.setField(1, new IntField(aFieldValue));
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
            is_opened = false;
        }
    }

    private class nog_OpIterator implements OpIterator {

        private int cur = 0;

        @Override
        public void open() throws DbException, TransactionAbortedException {

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
