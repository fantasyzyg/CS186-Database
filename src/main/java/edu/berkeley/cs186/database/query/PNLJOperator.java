package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;

class PNLJOperator extends BNLJOperator {
    PNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource,
              rightSource,
              leftColumnName,
              rightColumnName,
              transaction);

        joinType = JoinType.PNLJ;
        numBuffers = 3;    // 其实 Page Nested Loop Join 是 Block Nested Loop Join 的一个特例，只需要把 buffer 数量设置为3即可
    }
}
