package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;      // Buffer B pages

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
                numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     * See lecture slides.
     * <p>
     * An implementation of Iterator that provides an iterator interface for this operator.
     * <p>
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     * This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        // The current record on the right page
        private Record rightRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            // 获取left table Page iterator
            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightIterator.markNext();   // 这个点比较重要的！
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         * <p>
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            if (leftIterator.hasNext()) {
                leftRecordIterator = getBlockIterator(getLeftTableName(), leftIterator, numBuffers - 2);
                leftRecordIterator.markNext();
                leftRecord = leftRecordIterator.next();
            } else {
                leftRecordIterator = null;
                leftRecord = null;
            }
        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         * <p>
         * If there are no more pages in the left relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            if (rightIterator.hasNext()) {
                rightRecordIterator = getBlockIterator(getRightTableName(), rightIterator, 1);
                rightRecordIterator.markNext();
                rightRecord = rightRecordIterator.next();
            } else {
                rightRecordIterator = null;
                rightRecord = null;
            }
        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         * <p>
         * 判断依据就是：
         * 1. 如果右表的那一个page还有record的话，则可以继续进行比对
         * 2. 否则看左表的block是否还有record，有的话则取吓一条，右表的page需要重新从第一条record进行比对
         * 3. 如果左表也没有record的话，则看右表是否还有page，有的话，取下一个page，左表的block从第一条记录开始
         * 4. 如果右表也没有page了的话，则左表取下一个block，右表从第一个page开始
         * 5. 如果上面的条件都不满足的话，则已经没有记录了
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            if (leftRecord == null || rightRecordIterator == null) {
                throw new NoSuchElementException("No new record to fetch!");
            }

            this.nextRecord = null;
            while (!hasNext()) {
                if (rightRecord != null) {
                    DataBox leftJoinValue = this.leftRecord.getValues().get(getLeftColumnIndex());
                    DataBox rightJoinValue = this.rightRecord.getValues().get(getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        this.nextRecord = joinRecords(leftRecord, rightRecord);
                    }

                    // rightRecord 需要向前走一步
                    rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                } else if (leftRecordIterator.hasNext()) {
                    // 右边是null,说明对于左边的一条记录已经遍历了全部右边的记录了，所以左边需要向前一步
                    leftRecord = leftRecordIterator.next();
                    this.rightRecordIterator.reset();
                    rightRecord = rightRecordIterator.hasNext() ? rightRecordIterator.next() : null;
                } else if (rightIterator.hasNext()) {
                    // 获取右表的下一个Page了
                    fetchNextRightPage();
                    // 重置左表
                    leftRecordIterator.reset();
                    if (leftRecordIterator.hasNext()) {
                        leftRecord = leftRecordIterator.next();
                    } else {
                        leftRecord = null;
                    }
                } else if (leftIterator.hasNext()) {
                    // 获取左表的下一个Block
                    fetchNextLeftBlock();

                    // 恢复右表指针指向第一个page
                    rightIterator.reset();
                    // 获取右表的下一个page
                    fetchNextRightPage();
                } else {
                    throw new NoSuchElementException("No new record to fetch!");
                }
            }
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         *
         * @param leftRecord  Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
