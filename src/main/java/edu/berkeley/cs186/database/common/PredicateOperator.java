package edu.berkeley.cs186.database.common;

/**
 *    谓语操作符，如何使用一个枚举类呢?
 */
public enum PredicateOperator {
    EQUALS,     // 相等
    NOT_EQUALS,    // 不相等
    LESS_THAN,        // 小于
    LESS_THAN_EQUALS,      // 小于等于
    GREATER_THAN,        // 大于
    GREATER_THAN_EQUALS;      // 大于等于

    public <T extends Comparable<T>> boolean evaluate(T a, T b) {
        switch (this) {
        case EQUALS:
            return a.compareTo(b) == 0;
        case NOT_EQUALS:
            return a.compareTo(b) != 0;
        case LESS_THAN:
            return a.compareTo(b) < 0;
        case LESS_THAN_EQUALS:
            return a.compareTo(b) <= 0;
        case GREATER_THAN:
            return a.compareTo(b) > 0;
        case GREATER_THAN_EQUALS:
            return a.compareTo(b) >= 0;
        }
        return false;
    }
}
