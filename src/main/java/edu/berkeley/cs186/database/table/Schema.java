package edu.berkeley.cs186.database.table;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;

/**
 * The schema of a table includes the name and type of every one of its
 * fields. For example, the following schema:
 * <p>
 * List<String> fieldNames = Arrays.asList("x", "y");
 * List<Type> fieldTypes = Arrays.asList(Type.intType(), Type.floatType());
 * Schema s = new Schema(fieldNames, fieldSize);
 * <p>
 * represents a table with an int field named "x" and a float field named "y".
 */
public class Schema {
    private List<String> fieldNames;
    private List<Type> fieldTypes;
    private short sizeInBytes;

    /**
     * @param fieldNames 表字段列表
     * @param fieldTypes 表字段类型  其实这两者都是一些元信息
     */
    public Schema(List<String> fieldNames, List<Type> fieldTypes) {
        assert (fieldNames.size() == fieldTypes.size());
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;

        sizeInBytes = 0;
        for (Type t : fieldTypes) {
            sizeInBytes += t.getSizeInBytes();
        }
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public List<Type> getFieldTypes() {
        return fieldTypes;
    }

    public short getSizeInBytes() {
        return sizeInBytes;
    }

    Record verify(List<DataBox> values) {
        if (values.size() != fieldNames.size()) {
            String err = String.format("Expected %d values, but got %d.",
                    fieldNames.size(), values.size());
            throw new DatabaseException(err);
        }

        for (int i = 0; i < values.size(); ++i) {
            Type actual = values.get(i).type();
            Type expected = fieldTypes.get(i);
            if (!actual.equals(expected)) {
                String err = String.format(
                        "Expected field %d to be of type %s, but got value of type %s.",
                        i, expected, actual);
                throw new DatabaseException(err);
            }
        }

        return new Record(values);
    }

    public byte[] toBytes() {
        // A schema is serialized as follows. We first write the number of fields
        // (4 bytes). Then, for each field, we write
        //
        //   1. the length of the field name (4 bytes),
        //   2. the field's name,
        //   3. and the field's type.

        // First, we compute the number of bytes we need to serialize the schema.
        int size = Integer.BYTES; // The length of the schema.
        for (int i = 0; i < fieldNames.size(); ++i) {
            size += Integer.BYTES; // The length of the field name.
            size += fieldNames.get(i).length(); // The field name.
            size += fieldTypes.get(i).toBytes().length; // The type.
        }

        // Then we serialize it.
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); ++i) {
            buf.putInt(fieldNames.get(i).length());
            buf.put(fieldNames.get(i).getBytes(StandardCharsets.UTF_8));
            buf.put(fieldTypes.get(i).toBytes());
        }
        return buf.array();
    }

    public static Schema fromBytes(Buffer buf) {
        int size = buf.getInt();
        List<String> fieldNames = new ArrayList<>();
        List<Type> fieldTypes = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            int fieldSize = buf.getInt();
            byte[] bytes = new byte[fieldSize];
            buf.get(bytes);
            fieldNames.add(new String(bytes, StandardCharsets.UTF_8));
            fieldTypes.add(Type.fromBytes(buf));
        }
        return new Schema(fieldNames, fieldTypes);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < fieldNames.size(); ++i) {
            sb.append(String.format("%s: %s", fieldNames.get(i), fieldTypes.get(i)));
            if (i != fieldNames.size()) {
                sb.append(", ");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Schema)) {
            return false;
        }
        Schema s = (Schema) o;
        return fieldNames.equals(s.fieldNames) && fieldTypes.equals(s.fieldTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldNames, fieldTypes);
    }
}
