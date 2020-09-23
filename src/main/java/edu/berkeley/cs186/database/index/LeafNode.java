package edu.berkeley.cs186.database.index;

import java.nio.ByteBuffer;
import java.util.*;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

/**
 * A leaf of a B+ tree. Every leaf in a B+ tree of order d stores between d and
 * 2d (key, record id) pairs and a pointer to its right sibling (i.e. the page
 * number of its right sibling). Moreover, every leaf node is serialized and
 * persisted on a single page; see toBytes and fromBytes for details on how a
 * leaf is serialized. For example, here is an illustration of two order 2
 * leafs connected together:
 * <p>
 * leaf 1 (stored on some page)          leaf 2 (stored on some other page)
 * +-------+-------+-------+-------+     +-------+-------+-------+-------+
 * | k0:r0 | k1:r1 | k2:r2 |       | --> | k3:r3 | k4:r4 |       |       |
 * +-------+-------+-------+-------+     +-------+-------+-------+-------+
 */
class LeafNode extends BPlusNode {
    // Metadata about the B+ tree that this node belongs to.
    private BPlusTreeMetadata metadata;    // B+树的元信息

    // Buffer manager
    private BufferManager bufferManager;          // buffer 管理器

    // Lock context of the B+ tree
    private LockContext treeContext;         // B+ tree 锁上下文对象

    // The page on which this leaf is serialized.
    private Page page;

    // The keys and record ids of this leaf. `keys` is always sorted in ascending
    // order. The record id at index i corresponds to the key at index i. For
    // example, the keys [a, b, c] and the rids [1, 2, 3] represent the pairing
    // [a:1, b:2, c:3].
    //
    // Note the following subtlety. keys and rids are in-memory caches of the
    // keys and record ids stored on disk. Thus, consider what happens when you
    // create two LeafNode objects that point to the same page:
    //
    //   BPlusTreeMetadata meta = ...;
    //   int pageNum = ...;
    //   LockContext treeContext = new DummyLockContext();
    //
    //   LeafNode leaf0 = LeafNode.fromBytes(meta, bufferManager, treeContext, pageNum);
    //   LeafNode leaf1 = LeafNode.fromBytes(meta, bufferManager, treeContext, pageNum);
    //
    // This scenario looks like this:
    //
    //   HEAP                        | DISK
    //   ===============================================================
    //   leaf0                       | page 42
    //   +-------------------------+ | +-------+-------+-------+-------+
    //   | keys = [k0, k1, k2]     | | | k0:r0 | k1:r1 | k2:r2 |       |
    //   | rids = [r0, r1, r2]     | | +-------+-------+-------+-------+
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //   leaf1                       |
    //   +-------------------------+ |
    //   | keys = [k0, k1, k2]     | |
    //   | rids = [r0, r1, r2]     | |
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //
    // Now imagine we perform on operation on leaf0 like leaf0.put(k3, r3). The
    // in-memory values of leaf0 will be updated and they will be synced to disk.
    // But, the in-memory values of leaf1 will not be updated. That will look
    // like this:
    //
    //   HEAP                        | DISK
    //   ===============================================================
    //   leaf0                       | page 42
    //   +-------------------------+ | +-------+-------+-------+-------+
    //   | keys = [k0, k1, k2, k3] | | | k0:r0 | k1:r1 | k2:r2 | k3:r3 |
    //   | rids = [r0, r1, r2, r3] | | +-------+-------+-------+-------+
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //   leaf1                       |
    //   +-------------------------+ |
    //   | keys = [k0, k1, k2]     | |
    //   | rids = [r0, r1, r2]     | |
    //   | pageNum = 42            | |
    //   +-------------------------+ |
    //                               |
    //
    // Make sure your code (or your tests) doesn't use stale in-memory cached
    // values of keys and rids.
    private List<DataBox> keys;
    private List<RecordId> rids;

    // If this leaf is the rightmost leaf, then rightSibling is Optional.empty().
    // Otherwise, rightSibling is Optional.of(n) where n is the page number of
    // this leaf's right sibling.
    private Optional<Long> rightSibling;

    // Constructors //////////////////////////////////////////////////////////////

    /**
     * Construct a brand new leaf node.
     */
    LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
             List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum(), false),
                keys, rids,
                rightSibling, treeContext);
    }

    /**
     * Construct a leaf node that is persisted to page `page`.
     */
    private LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                     List<DataBox> keys,
                     List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
        assert (keys.size() == rids.size());

        this.metadata = metadata;
        this.bufferManager = bufferManager;
        this.treeContext = treeContext;
        this.page = page;
        this.keys = new ArrayList<>(keys);
        this.rids = new ArrayList<>(rids);
        this.rightSibling = rightSibling;

        sync();         // 建立的时候就会同步一次了。
        page.unpin();
    }

    // Core API //////////////////////////////////////////////////////////////////
    // See BPlusNode.get.
    @Override
    public LeafNode get(DataBox key) {   // 即使不包含该key也是需要返回本身
        return this;
    }

    // See BPlusNode.getLeftmostLeaf.
    @Override
    public LeafNode getLeftmostLeaf() {
        return this;
    }

    // See BPlusNode.put.
    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        if (keys.contains(key)) {
            throw new BPlusTreeException("A duplicate key is inserted!");
        }

        int index = InnerNode.numLessThanEqual(key, keys);
        keys.add(index, key);
        rids.add(index, rid);
        if (keys.size() <= metadata.getOrder() * 2) {
            sync();   // 先刷新一遍回到磁盘
            return Optional.empty();
        }

        // 否则进行裂变
        List<DataBox> newKeys = new ArrayList<>();
        List<RecordId> newRids = new ArrayList<>();
        int d = metadata.getOrder();
        while (keys.size() > d) {
            newKeys.add(keys.remove(d));
            newRids.add(rids.remove(d));
        }

        LeafNode rightNode = new LeafNode(metadata, bufferManager, newKeys, newRids, rightSibling, treeContext);
        long rightPageNum = rightNode.getPage().getPageNum();
        this.rightSibling = Optional.of(rightPageNum);
        sync();    // 写会磁盘一遍数据,但是为什么rightNode不写入一次呢? 因为rightNode在构建的时候就已经同步了一次

        return Optional.of(new Pair<>(newKeys.get(0), rightPageNum));
    }

    // See BPlusNode.bulkLoad.   这是在 LeafNode 已经有数据的基础之下进行的,不过有一个疑问就是 iterator 里面的数据是排序好的吗?
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
                                                  float fillFactor) {
        // TODO(proj2): implement

        int limitNumNode = (int) Math.ceil(metadata.getOrder() * 2 * fillFactor);
        // 最多只可以达到 limitNumNode 个节点
        while (data.hasNext() && keys.size() < limitNumNode) {
            Pair<DataBox, RecordId> nextNode = data.next();
            DataBox key = nextNode.getFirst();
            RecordId recordId = nextNode.getSecond();

            if (keys.contains(key)) {
                throw new BPlusTreeException("A duplicate key is inserted!");
            }

            int index = InnerNode.numLessThanEqual(key, keys);
            keys.add(index, key);
            rids.add(index, recordId);
        }

        Optional<Pair<DataBox, Long>> pair = Optional.empty();
        // 构造右节点
        if (keys.size() == limitNumNode && data.hasNext()) {
            Pair<DataBox, RecordId> next = data.next();
            List<DataBox> newKeys = new ArrayList<>();
            List<RecordId> newRids = new ArrayList<>();
            newKeys.add(next.getFirst());
            newRids.add(next.getSecond());

            LeafNode newRightLeafNode =
                    new LeafNode(metadata, bufferManager, newKeys, newRids, rightSibling, treeContext);
            this.rightSibling = Optional.of(newRightLeafNode.getPage().getPageNum());
            pair = Optional.of(new Pair<>(next.getFirst(), rightSibling.get()));
        }

        sync();
        return pair;
    }

    // See BPlusNode.remove.
    @Override
    public void remove(DataBox key) {
        // TODO(proj2): implement
        int index = keys.indexOf(key);
        if (index != -1) {
            keys.remove(index);
            rids.remove(index);

            sync();
        }
    }

    // Iterators /////////////////////////////////////////////////////////////////

    /**
     * Return the record id associated with `key`.
     */
    Optional<RecordId> getKey(DataBox key) {
        int index = keys.indexOf(key);
        return index == -1 ? Optional.empty() : Optional.of(rids.get(index));
    }

    /**
     * Returns an iterator over the record ids of this leaf in ascending order of
     * their corresponding keys.
     */
    Iterator<RecordId> scanAll() {
        return rids.iterator();
    }

    /**
     * Returns an iterator over the record ids of this leaf that have a
     * corresponding key greater than or equal to `key`. The record ids are
     * returned in ascending order of their corresponding keys.
     */
    Iterator<RecordId> scanGreaterEqual(DataBox key) {
        int index = InnerNode.numLessThan(key, keys);
        return rids.subList(index, rids.size()).iterator();
    }

    // Helpers ///////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    /**
     * Returns the right sibling of this leaf, if it has one.
     */
    Optional<LeafNode> getRightSibling() {
        if (!rightSibling.isPresent()) {
            return Optional.empty();
        }

        long pageNum = rightSibling.get();
        return Optional.of(LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum));
    }

    /**
     * Serializes this leaf to its page.
     */
    private void sync() {
        page.pin();
        try {
            Buffer b = page.getBuffer();
            byte[] newBytes = toBytes();    // 将内存中的LeafNode序列化为byte数组
            byte[] bytes = new byte[newBytes.length];
            b.get(bytes);     // 这里为什么是将旧的page放到和新的字节数据一样长度的字节数组里面呢?
            if (!Arrays.equals(bytes, newBytes)) {
                page.getBuffer().put(toBytes());
            }
        } finally {
            page.unpin();
        }
    }

    // Just for testing.
    List<DataBox> getKeys() {
        return keys;
    }

    // Just for testing.
    List<RecordId> getRids() {
        return rids;
    }

    /**
     * Returns the largest number d such that the serialization of a LeafNode
     * with 2d entries will fit on a single page.
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // A leaf node with n entries takes up the following number of bytes:
        //
        //   1 + 8 + 4 + n * (keySize + ridSize)
        //
        // where
        //
        //   - 1 is the number of bytes used to store isLeaf,
        //   - 8 is the number of bytes used to store a sibling pointer,
        //   - 4 is the number of bytes used to store n,
        //   - keySize is the number of bytes used to store a DataBox of type
        //     keySchema, and
        //   - ridSize is the number of bytes of a RecordId.
        //
        // Solving the following equation
        //
        //   n * (keySize + ridSize) + 13 <= pageSizeInBytes
        //
        // we get
        //
        //   n = (pageSizeInBytes - 13) / (keySize + ridSize)
        //
        // The order d is half of n.
        int keySize = keySchema.getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + ridSize);
        return n / 2;
    }

    // Pretty Printing ///////////////////////////////////////////////////////////
    @Override
    public String toString() {
        return String.format("LeafNode(pageNum=%s, keys=%s, rids=%s)",
                page.getPageNum(), keys, rids);
    }

    @Override
    public String toSexp() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i).toString();
            String rid = rids.get(i).toSexp();
            ss.add(String.format("(%s %s)", key, rid));
        }
        return String.format("(%s)", String.join(" ", ss));
    }

    /**
     * Given a leaf with page number 1 and three (key, rid) pairs (0, (0, 0)),
     * (1, (1, 1)), and (2, (2, 2)), the corresponding dot fragment is:
     * <p>
     * node1[label = "{0: (0 0)|1: (1 1)|2: (2 2)}"];
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("%s: %s", keys.get(i), rids.get(i).toSexp()));
        }
        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        return String.format("  node%d[label = \"{%s}\"];", pageNum, s);
    }

    // Serialization ///////////////////////////////////////////////////////////// 序列化
    @Override
    public byte[] toBytes() {
        // When we serialize a leaf node, we write:
        //
        //   a. the literal value 1 (1 byte) which indicates that this node is a
        //      leaf node,
        //   b. the page id (8 bytes) of our right sibling (or -1 if we don't have
        //      a right sibling),
        //   c. the number (4 bytes) of (key, rid) pairs this leaf node contains,
        //      and
        //   d. the (key, rid) pairs themselves.
        //
        // For example, the following bytes:
        //
        //   +----+-------------------------+-------------+----+-------------------------------+
        //   | 01 | 00 00 00 00 00 00 00 04 | 00 00 00 01 | 03 | 00 00 00 00 00 00 00 03 00 01 |
        //   +----+-------------------------+-------------+----+-------------------------------+
        //    \__/ \_______________________/ \___________/ \__________________________________/
        //     a               b                   c                         d
        //
        // represent a leaf node with sibling on page 4 and a single (key, rid)
        // pair with key 3 and page id (3, 1).   recordId 里面包含 (pageId, PageIndex)

        // All sizes are in bytes.
        int isLeafSize = 1;
        int siblingSize = Long.BYTES;   // next page address.
        int lenSize = Integer.BYTES;
        int keySize = metadata.getKeySchema().getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int entriesSize = (keySize + ridSize) * keys.size();
        int size = isLeafSize + siblingSize + lenSize + entriesSize;

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) 1);
        buf.putLong(rightSibling.orElse(-1L));
        buf.putInt(keys.size());
        for (int i = 0; i < keys.size(); ++i) {
            buf.put(keys.get(i).toBytes());
            buf.put(rids.get(i).toBytes());
        }
        return buf.array();
    }

    /**
     * Loads a leaf node from page `pageNum`.
     */
    public static LeafNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                     LockContext treeContext, long pageNum) {
        Page page = bufferManager.fetchPage(treeContext, pageNum, false);
        Buffer buffer = page.getBuffer();
        buffer.get();   // 第一个byte表明是哪种类型的节点
        long siblingSize = buffer.getLong();    // 右边的 LeafNode
        int lenSize = buffer.getInt();

        List<DataBox> keys = new ArrayList<>();  // keys
        List<RecordId> rids = new ArrayList<>();   // RecordIds
        Type keyType = metadata.getKeySchema();
        for (int i = 0; i < lenSize; ++i) {
            keys.add(DataBox.fromBytes(buffer, keyType));
            rids.add(RecordId.fromBytes(buffer));
        }

        return new LeafNode(metadata, bufferManager, page, keys, rids, siblingSize == -1 ? Optional.empty() : Optional.of(siblingSize), treeContext);
    }

    // Builtins //////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LeafNode)) {
            return false;
        }
        LeafNode n = (LeafNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
                keys.equals(n.keys) &&
                rids.equals(n.rids) &&
                rightSibling.equals(n.rightSibling);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, rids, rightSibling);
    }
}
