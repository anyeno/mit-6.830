package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private ArrayList<Page> pages;

    private LockManager lockManager;

//    private ReentrantReadWriteLock listLock = new ReentrantReadWriteLock();
//    private ArrayList<ReentrantReadWriteLock> pageLocks;
    private Lock pagesLock = new ReentrantLock();

    private enum LockType {
        Read,
        Write
    }

    private class LockNode {
        public LockType needLockType;
        private LockType nowLockType;
        public TransactionId tid;
        public boolean isAwarded;
        public PageId pageId;
        public LockNode next;

        public LockNode(LockType lockType, TransactionId tid, boolean isAwarded, PageId pageId) {
            this.needLockType = lockType;
            this.nowLockType = null;
            this.tid = tid;
            this.isAwarded = isAwarded;
            this.pageId = pageId;
            this.next = null;
        }
    }

    private class LockManager {
        private ArrayList<LockNode> locks;

        public LockManager() {
            this.locks = new ArrayList<>();
        }

        public  boolean acquireLock(TransactionId tid, PageId pageId, LockType lockType){
            while(true) {
                synchronized(pageId) {
                    LockNode tidLockNode = this.holdsLock(tid, pageId);
                    // 如果这个事务已经获取锁了
                    if(tidLockNode != null) {
                        // 插入链表
                        LockNode pn = tidLockNode;
                        while(pn.next != null) {
                            pn = pn.next;
                        }
                        pn.next = new LockNode(lockType, tid, false, pageId);

                        // 与已经获取的锁类型相同或者获得了写锁 授予锁
                        if(tidLockNode.nowLockType == lockType || tidLockNode.nowLockType == LockType.Write)
                        {
                            pn.next.isAwarded = true;
                            pn.next.nowLockType = tidLockNode.nowLockType;
                            return true;
                        }
                        // 阻塞
                        else{
                            try {
                                pageId.wait();
                                continue;
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }

                    }


                    boolean flag = false;  // 是否存在pageId
                    for(LockNode lockNode: locks) {
                        if(pageId.equals(lockNode.pageId)) {
                            flag = true;
                            LockNode newLockNode = new LockNode(lockType, tid, false, pageId);
                            LockNode p = lockNode;
                            LockType nowLockType = p.nowLockType;
                            while(p.next != null) {
                                p = p.next;
                            }
                            p.next = newLockNode;
                            // 锁已经被获取了，这时候只能都是读锁才能获取
                            if(newLockNode.needLockType == LockType.Read && nowLockType == LockType.Read) {
                                newLockNode.isAwarded = true;
                                return true;
                            } else{
                                // 阻塞
                                try {
                                    pageId.wait();
                                    break;
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                    // 此时第一个请求锁  直接授予就行
                    if(!flag){
                        LockNode newLockNode = new LockNode(lockType, tid, true, pageId);
                        newLockNode.nowLockType = newLockNode.needLockType;
                        locks.add(newLockNode);
                        return true;
                    }
                }
            }
        }

        public boolean releaseLock(TransactionId tid, PageId pageId) {
            synchronized(pageId) {
                boolean isExist = false;
                for(LockNode lockNode: locks) {
                    if(pageId.equals(lockNode.pageId)) {
                        LockNode pre = lockNode;
                        LockNode ne = pre;
                        while(ne.next != null) {
                            if(tid.equals(ne.tid)) {
                                isExist = true;
                                pre.next = ne.next; // 从链表中删除
                                pageId.notifyAll();
//                                return true;
                            }
                            pre = ne;
                            ne = ne.next;
                        }
                        if(tid.equals(ne.tid)) {
                            isExist = true;
                            // 如果lock table中该项只有一个
                            if(pre == ne) {
                                locks.remove(lockNode);
                                return true;
                            }
                            else
                                pre.next = null; // 从链表中删除
                            pageId.notifyAll();
                        }
                    }
                }
                if(isExist) return true;
                else throw new RuntimeException("该事务在未申请该page的锁");
            }
        }

        public LockNode holdsLock(TransactionId tid, PageId p) {
            synchronized(p) {
                for(LockNode lockNode: locks) {
                    if(p.equals(lockNode.pageId)) {
//                        LockNode pre = lockNode;
                        LockNode ne = lockNode;
                        while(ne.next != null) {
                            if(tid.equals(ne.tid)) {
                                if(ne.isAwarded)
                                    return ne;
                            }
//                            pre = ne;
                            ne = ne.next;
                        }
                        if(tid.equals(ne.tid)) {
                            if(ne.isAwarded)
                                return ne;
                        }
                    }
                }
                return null;
            }
        }

        public void transactionComplete(TransactionId tid) {
            Iterator<LockNode> iterator = locks.iterator();
            while(iterator.hasNext()) {
                LockNode lockNode = iterator.next();
                LockNode ne = lockNode;
                LockNode pre = lockNode;
                while(ne.next != null) {
                    if(tid.equals(ne.tid)) {
                        pre.next = ne.next;
                        ne = pre.next;
                    }
                    pre = ne;
                    ne = ne.next;
                }
                if(tid.equals(ne.tid)) {
                    // 如果lock table中该项只有一个
                    if(pre == ne) {
                        // 迭代器删除   否则有bug
                        iterator.remove();
                    }
                    else
                        pre.next = null; // 从链表中删除
                }
            }
        }
    }

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // TODO: some code goes here
        this.numPages = numPages;
        pages = new ArrayList<>();
        lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // TODO: some code goes here
//        listLock.readLock();
        pagesLock.lock();
        if(perm == Permissions.READ_WRITE)
            lockManager.acquireLock(tid, pid, LockType.Write);
        if(perm == Permissions.READ_ONLY)
            lockManager.acquireLock(tid, pid, LockType.Read);
        for(Page p: pages) {
            if(p.getId().equals(pid)) {
                return p;
            }
        }
        if(pages.size() == numPages) {
//            throw new DbException("驱逐策略\n");
            evictPage();
        }

        Page res = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pages.add(res);
        pagesLock.unlock();
        return res;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
//        if(holdsLock(tid, pid))
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        lockManager.transactionComplete(tid);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p) != null;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> affectPgs = (ArrayList<Page>) dbFile.insertTuple(tid, t);
        for(Page page: affectPgs) {
            page.markDirty(true, tid);
            pages.add(page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> dirty_page = (ArrayList<Page>) dbFile.deleteTuple(tid, t);

        for(Page p: pages) {
            for(Page dp: dirty_page) {
                if(dp.getId().equals(p.getId())) {
                    p.markDirty(true, tid);
                    pages.remove(p);
                    pages.add(dp);
                }
            }
        }


    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        for(Page p: pages) {
            flushPage(p.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1
        for(Page page: pages) {
            if(page.getId().equals(pid)) {
                pages.remove(page);
                break;
            }
        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
        for(Page page: pages) {
            if(page.getId().equals(pid)) {
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
                page.markDirty(false, null);
                break;
            }
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // TODO: some code goes here
        // not necessary for lab1
        for(Page p: pages) {
            if(p.isDirty() == null) {
                try {
                    flushPage(p.getId());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                pages.remove(p);
                return ;
            }
        }
        throw new DbException("全是脏页");
    }

}
