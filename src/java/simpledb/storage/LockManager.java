package simpledb.storage;

import simpledb.common.DbException;
import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    public static enum LockType {
        Read,
        Write
    }
    private class LockNode {
        public LockType needLockType;
        public TransactionId tid;
        public boolean isAwarded;

        public LockNode(LockType lockType, TransactionId tid, boolean isAwarded) {
            this.needLockType = lockType;
            this.tid = tid;
            this.isAwarded = isAwarded;
        }
    }

    public Map<PageId, List<LockNode>> lockTable;

    public LockManager() {
        this.lockTable = new ConcurrentHashMap<>();
    }

    // 插入lock table   不支持同一个事务加两个相同的锁
    public synchronized void acquireLock(TransactionId tid, PageId pageId, LockType lockType){
        LockNode newLockNode = new LockNode(lockType, tid, false);
        // 插入lock table
        if(lockTable.containsKey(pageId)) {
            List<LockNode> nodes = lockTable.get(pageId);
            for(LockNode node: nodes) {
                if(node.tid.equals(tid) && node.needLockType == lockType)
                    return ;
            }
            lockTable.get(pageId).add(newLockNode);
        } else {
            List<LockNode> nodes = new ArrayList<>();
            nodes.add(newLockNode);
            lockTable.put(pageId, nodes);
        }
    }

    // 判断是否可以获取锁 可以就加锁 ** 在这个函数里实际加锁 **
    public synchronized boolean tryLock(TransactionId tid, PageId pageId, LockType lockType) {
        List<LockNode> nodes = lockTable.get(pageId);
        // 就这一所等待锁的 直接给他
        if(nodes.size() == 1) {
            nodes.get(0).isAwarded = true;
            return true;
        }

        boolean flag = false; //是否能获取锁

        Iterator<LockNode> iterator = nodes.listIterator();
        LockNode itself = null;
        while(iterator.hasNext()){
            LockNode now = iterator.next();
            // 遍历到自己了   不支持同一个事务加两个相同的锁
            if(now.tid.equals(tid) && now.needLockType == lockType) {
                itself = now;
                break;
            }
            // 该事务已经获取到了锁
            // 写锁 写锁        no 不支持相同锁
            // 写锁  读锁       yes
            // 读锁  读锁       yes
            // 多个读锁  写锁    no
            // 只有该事务获取了读锁 + 写锁 yes 升级成写锁
            if(now.tid.equals(tid) && now.isAwarded) {
                if(now.needLockType == LockType.Write && lockType == LockType.Read) {
                    flag = true;
//                    break;
                }
            }
            if(now.isAwarded) {
                if(now.needLockType == LockType.Read && lockType == LockType.Read) {
                    flag = true;
                }
            }
        }

        if(!flag)
            return false;
        else {
            itself.isAwarded = true;
            return true;
        }
    }

    // 释放某事务在某page上的锁
    public synchronized void releaseLock(TransactionId tid, PageId pageId) {
        if(!lockTable.containsKey(pageId)) return ;
        List<LockNode> nodes = lockTable.get(pageId);
        Iterator<LockNode> iterator = nodes.iterator();
        while(iterator.hasNext()) {
            LockNode now = iterator.next();
            if(now.tid.equals(tid)) {
                iterator.remove();
            }
        }
    }

    public boolean holdsLock(TransactionId tid, PageId pageId) {
        if(!lockTable.containsKey(pageId)) {
            try {
                throw new DbException("该页上没有锁");
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
        List<LockNode> nodes = lockTable.get(pageId);
        for(LockNode node: nodes) {
            if(node.isAwarded && node.tid.equals(tid)) {
                return true;
            }
        }
        return false;
    }

    public synchronized List<PageId> releaseAllLocks(TransactionId tid) {
        List<PageId> res = affectedPages(tid);
        for(PageId pageId: res) {
            releaseLock(tid, pageId);
        }
        return res;
    }

    // 事务结束
    public synchronized List<PageId> affectedPages(TransactionId tid) {
        List<PageId> res = new ArrayList<>();
        for(PageId pageId: lockTable.keySet()) {
            res.add(pageId);
        }
        return res;
    }
}

