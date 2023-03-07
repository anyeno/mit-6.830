package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *  heapfile的ID
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here
        int tableid = pid.getTableId();
        int pgNo = pid.getPageNumber();
        final int pageSize = BufferPool.getPageSize();
        byte[] rawPgData = HeapPage.createEmptyPageData();

        // random access read from disk
        try {
            FileInputStream in = new FileInputStream(f);
            in.skip(pgNo * pageSize);
            in.read(rawPgData);
            return new HeapPage(new HeapPageId(tableid, pgNo), rawPgData);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        } catch (IOException e) {
            throw new IllegalArgumentException("HeapFile: readPage: file not found");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        PageId pid = page.getId();
        int tableid = pid.getTableId();
        int pgNo = pid.getPageNumber();

        final int pageSize = BufferPool.getPageSize();
        byte[] pgData = page.getPageData();

        RandomAccessFile dbfile = new RandomAccessFile(f, "rws");
        dbfile.skipBytes(pgNo * pageSize);
        dbfile.write(pgData);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        int fileSizeinByte = (int) f.length();
        return fileSizeinByte / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        ArrayList<Page> res = new ArrayList<>();
        int numPages = numPages();

        for(int pgNo = 0; pgNo < numPages + 1; pgNo ++) {
            HeapPageId pid = new HeapPageId(getId(), pgNo);
            HeapPage pg;
            if(pgNo < numPages) {
                // 先从缓冲区取
                pg = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            } else {
                pg = new HeapPage(pid, HeapPage.createEmptyPageData());
            }

            if(pg.getNumUnusedSlots() > 0) {
                pg.insertTuple(t);
                if(pgNo < numPages) {
                    res.add(pg);
                } else {
                    writePage(pg);
                    res.add(pg);
                }
                return  res;
            }
        }
        throw new DbException("无法加入");
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        HeapPageId heapPageId = (HeapPageId) t.getRecordId().getPageId();
        if(heapPageId.getTableId() == getId()) {
            List<Page> res = new ArrayList<>();
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            heapPage.deleteTuple(t);
            res.add(heapPage);
            return res;
        }
        throw new DbException("tuple 所在的表不在这个文件内");
    }

    private class HeapFileIterator implements DbFileIterator {

        private Integer pgCursor;
        private Iterator<Tuple> tupleIter;
        private final TransactionId transactionId;
        private final int tableId;
        private final int numPages;

        public HeapFileIterator(TransactionId tid) {
            this.pgCursor = null;
            this.tupleIter = null;
            this.transactionId = tid;
            this.tableId = getId();
            this.numPages = numPages();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pgCursor = 0;
            tupleIter = getTupleIter(pgCursor);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // < numpage - 1
            if (pgCursor != null) {
                while (pgCursor < numPages - 1) {
                    if (tupleIter.hasNext()) {
                        return true;
                    } else {
                        pgCursor += 1;
                        tupleIter = getTupleIter(pgCursor);
                    }
                }
                return tupleIter.hasNext();
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext())  {
                return tupleIter.next();
            }
            throw new NoSuchElementException("HeapFileIterator: error: next: no more elemens");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            pgCursor = null;
            tupleIter = null;
        }

        private Iterator<Tuple> getTupleIter(int pgNo)
                throws TransactionAbortedException, DbException {
            PageId pid = new HeapPageId(tableId, pgNo);
            return ((HeapPage)
                    Database
                            .getBufferPool()
                            .getPage(transactionId, pid, Permissions.READ_ONLY))
                    .iterator();
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

