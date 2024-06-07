/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.data.columnar;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.DataSetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.columnar.BytesColumnVector.Bytes;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Columnar row to support access to vector column data. It is a row view in {@link
 * VectorizedColumnBatch}.
 */
public final class ColumnarRow implements InternalRow, DataSetters, Serializable {

    private static final long serialVersionUID = 1L;

    private RowKind rowKind = RowKind.INSERT;
    private VectorizedColumnBatch vectorizedColumnBatch;
    // 这里控制的是行数
    private int rowId;

    public ColumnarRow() {}

    public ColumnarRow(VectorizedColumnBatch vectorizedColumnBatch) {
        this(vectorizedColumnBatch, 0);
        System.out.println("vectorizedColumnBatch: " +
                Arrays.stream(vectorizedColumnBatch.columns)
                        .map(Object::getClass).collect(Collectors.toList())
        );
    }

    public ColumnarRow(VectorizedColumnBatch vectorizedColumnBatch, int rowId) {
        this.vectorizedColumnBatch = vectorizedColumnBatch;
        this.rowId = rowId;
    }

    public void setVectorizedColumnBatch(VectorizedColumnBatch vectorizedColumnBatch) {
        this.vectorizedColumnBatch = vectorizedColumnBatch;
        this.rowId = 0;
    }

    public void setRowId(int rowId) {
        this.rowId = rowId;
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }

    @Override
    public int getFieldCount() {
        return vectorizedColumnBatch.getArity();
    }

    @Override
    public boolean isNullAt(int pos) {
        return vectorizedColumnBatch.isNullAt(rowId, pos);
    }

    @Override
    public boolean getBoolean(int pos) {
        System.out.println("getBoolean(int pos) ----------");
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        return vectorizedColumnBatch.getBoolean(rowId, pos);
    }

    @Override
    public byte getByte(int pos) {
        System.out.println("etByte(int pos) ----------");
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        return vectorizedColumnBatch.getByte(rowId, pos);
    }

    @Override
    public short getShort(int pos) {
        System.out.println("getShort(int pos) ----------");
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        return vectorizedColumnBatch.getShort(rowId, pos);
    }

    @Override
    public int getInt(int pos) {
        System.out.println("getInt(int pos) ----------" + pos);
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        return vectorizedColumnBatch.getInt(rowId, pos);
    }

    @Override
    public long getLong(int pos) {
        System.out.println("getLong(int pos) ----------" + pos);
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        return vectorizedColumnBatch.getLong(rowId, pos);
    }

    @Override
    public float getFloat(int pos) {
        System.out.println("getFloat(int pos) ----------");
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        return vectorizedColumnBatch.getFloat(rowId, pos);
    }

    @Override
    public double getDouble(int pos) {
        System.out.println("getDouble(int pos) ----------");
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        return vectorizedColumnBatch.getDouble(rowId, pos);
    }

    @Override
    public BinaryString getString(int pos) {
        System.out.printf("getString(int pos) rowId: %d---pos: %d------- \n", rowId, pos);
        Bytes byteArray = vectorizedColumnBatch.getByteArray(rowId, pos);
        return BinaryString.fromBytes(byteArray.data, byteArray.offset, byteArray.len);
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
        System.out.println("getDecimal(int pos) ----------");
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        return vectorizedColumnBatch.getDecimal(rowId, pos, precision, scale);
    }

    @Override
    public Timestamp getTimestamp(int pos, int precision) {
        System.out.println("getTimestamp(int pos) ----------");
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        return vectorizedColumnBatch.getTimestamp(rowId, pos, precision);
    }

    @Override
    public byte[] getBinary(int pos) {
        System.out.println("getBinary(int pos) ----------");
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        Bytes byteArray = vectorizedColumnBatch.getByteArray(rowId, pos);
        if (byteArray.len == byteArray.data.length) {
            return byteArray.data;
        } else {
            byte[] ret = new byte[byteArray.len];
            System.arraycopy(byteArray.data, byteArray.offset, ret, 0, byteArray.len);
            return ret;
        }
    }

    @Override
    public InternalRow getRow(int pos, int numFields) {
        System.out.println("getRow(int pos) ----------");
        Arrays.stream(Thread.currentThread().getStackTrace()).forEach(System.out::println);
        return vectorizedColumnBatch.getRow(rowId, pos);
    }

    @Override
    public InternalArray getArray(int pos) {
        return vectorizedColumnBatch.getArray(rowId, pos);
    }

    @Override
    public InternalMap getMap(int pos) {
        return vectorizedColumnBatch.getMap(rowId, pos);
    }

    @Override
    public void setNullAt(int pos) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setBoolean(int pos, boolean value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setByte(int pos, byte value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setShort(int pos, short value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setInt(int pos, int value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setLong(int pos, long value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setFloat(int pos, float value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDouble(int pos, double value) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setDecimal(int pos, Decimal value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public void setTimestamp(int pos, Timestamp value, int precision) {
        throw new UnsupportedOperationException("Not support the operation!");
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException(
                "ColumnarRowData do not support equals, please compare fields one by one!");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException(
                "ColumnarRowData do not support hashCode, please hash fields one by one!");
    }

    VectorizedColumnBatch vectorizedColumnBatch() {
        return vectorizedColumnBatch;
    }

    public ColumnarRow copy(ColumnVector[] vectors) {
        VectorizedColumnBatch vectorizedColumnBatchCopy = vectorizedColumnBatch.copy(vectors);
        ColumnarRow columnarRow = new ColumnarRow(vectorizedColumnBatchCopy, rowId);
        columnarRow.setRowKind(rowKind);
        return columnarRow;
    }
}
