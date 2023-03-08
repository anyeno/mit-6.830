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
    // 锁升级 如果事务t是唯一一个在对象o上持有共享锁的事务，则 t可以将其在o上的锁升级为独占锁。
    public synchronized boolean tryLock(TransactionId tid, PageId pageId, LockType lockType) {
        List<LockNode> nodes = lockTable.get(pageId);
        // 就这一个等待锁的 直接给他
        if(nodes.size() == 1) {
            nodes.get(0).isAwarded = true;
            return true;
        }

        Iterator<LockNode> iterator = nodes.listIterator();
        LockNode itself = null;
        while(iterator.hasNext()) {
            LockNode now = iterator.next();
            if(now.tid.equals(tid) && now.needLockType == lockType) {
                itself = now;
                break;
            }
        }

        Iterator<LockNode> iterator2 = nodes.listIterator();
        while(iterator2.hasNext()){
            LockNode now = iterator2.next();
            if(now.tid.equals(tid) && now.needLockType == lockType) {
                now.isAwarded = true;
                return true;
            }
            if(now.isAwarded) {
                // 是该事务 但肯定不是同一把锁
                if(now.tid.equals(tid)) {
                    // 该事务已经获得了写锁 那肯定只有一个事物能获得写锁 直接给他读锁
                    if(now.needLockType == LockType.Write && lockType == LockType.Read) {
                        itself.isAwarded = true;
                        return true;
                    }
                    // 如果该事务获取了读锁  可能有多个事务获取了读锁 只能只有该事务获取读锁的时候进行锁升级
                    else if(now.needLockType == LockType.Read && lockType == LockType.Write) {
                    }

                } else {
                    if(now.needLockType == LockType.Read) {
                        if(lockType == LockType.Read) {
                            itself.isAwarded = true;
                            return true;
                        }
                        else
                            return false;
                    }
                    else
                        return false;
                }
            } else {
                return false;
            }
        }

        return false;
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

