package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     * See lecture slides.
     * <p>
     * Before proceeding, you should read and understand SNLJOperator.java
     * You can find it in the same directory as this file.
     * <p>
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     * This means you'll probably want to add more methods than those given (Once again,
     * SNLJOperator.java might be a useful reference).
     */
    private class SortMergeIterator extends JoinIterator {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;         // 标记是否 match
        private RecordComparator recordComparator;

        private SortMergeIterator() {
            super();

            this.recordComparator = new RecordComparator();
            this.leftIterator = SortMergeOperator.this.getRecordIterator(new SortOperator(SortMergeOperator.this.getTransaction(), this.getLeftTableName(), new LeftRecordComparator()).sort());
            this.rightIterator = SortMergeOperator.this.getRecordIterator(new SortOperator(SortMergeOperator.this.getTransaction(), this.getRightTableName(), new RightRecordComparator()).sort());

            this.nextRecord = null;
            this.marked = false;

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }


        /*
        // 这是根据自己的理解来进行的！
        private void fetchNextRecord() {
            if (this.leftRecord == null) {
                throw new NoSuchElementException("No new record to fetch");
            }
            this.nextRecord = null;
            while (!hasNext() && this.leftRecord != null) {
                if (this.rightRecord != null) {
                    DataBox leftJoinValue = this.leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());

                    if (leftJoinValue.equals(rightJoinValue)) {
                        if (!marked) {
                            marked = true;
                            rightIterator.markPrev();
                        }

                        List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);

                        // 右表向前走一步
                        this.rightRecord = this.rightIterator.hasNext() ? this.rightIterator.next() : null;
                    } else {
                        if (marked) {
                            resetRightRecord();
                        } else {
                            if (leftJoinValue.compareTo(rightJoinValue) < 0) {
                                this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
                            } else {
                                this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                            }
                        }
                    }
                } else {
                    if (marked) {
                        resetRightRecord();
                    } else {
                        break;
                    }
                }
            }
        }

         */

        private void resetRightRecord() {
            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;      // 左表向前一步
            this.rightIterator.reset();     // 右表重置
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;      // 右表向前一步
            marked = false;
        }

        /**
         * 根据 Notes 给的伪代码来编写的代码
         */
        private void fetchNextRecord() {
            if (this.leftRecord == null) {
                throw new NoSuchElementException("No new record to fetch");
            }

            this.nextRecord = null;
            while (!hasNext() && leftRecord != null) {
                if (rightRecord == null && !marked) {
                    break;
                }

                if (!marked) {
                    while (leftRecord != null && this.recordComparator.compare(leftRecord, rightRecord) < 0) {
                        this.leftRecord = this.leftIterator.hasNext() ? this.leftIterator.next() : null;
                    }

                    if (leftRecord != null) {
                        while (rightRecord != null && this.recordComparator.compare(leftRecord, rightRecord) > 0) {
                            this.rightRecord = this.rightIterator.hasNext() ? this.rightIterator.next() : null;
                        }
                    }

                    if (leftRecord != null && rightRecord != null) {
                        this.marked = true;
                        this.rightIterator.markPrev();
                    }
                }

                if (leftRecord != null && rightRecord != null && this.recordComparator.compare(leftRecord, rightRecord) == 0) {
                    List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                    List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                    leftValues.addAll(rightValues);
                    this.nextRecord = new Record(leftValues);

                    // 右表向前走一步
                    this.rightRecord = this.rightIterator.hasNext() ? this.rightIterator.next() : null;
                } else {
                    resetRightRecord();
                }
            }
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

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }

        /**
         * 自定义 Comparator
         */
        private class RecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
