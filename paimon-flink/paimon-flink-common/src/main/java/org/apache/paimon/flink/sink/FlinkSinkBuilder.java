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

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.index.GlobalDynamicBucketSink;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.paimon.flink.sink.FlinkStreamPartitioner.partition;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Builder for {@link FlinkSink}. */
public class FlinkSinkBuilder {

    private final FileStoreTable table;

    private DataStream<RowData> input;
    @Nullable private Map<String, String> overwritePartition;
    @Nullable private LogSinkFunction logSinkFunction;
    @Nullable private Integer parallelism;
    private boolean boundedInput = false;
    private boolean compactSink = false;

    public FlinkSinkBuilder(FileStoreTable table) {
        this.table = table;
    }

    public FlinkSinkBuilder withInput(DataStream<RowData> input) {
        this.input = input;
        return this;
    }

    /**
     * Whether we need to overwrite partitions.
     *
     * @param overwritePartition If we pass null, it means not overwrite. If we pass an empty map,
     *     it means to overwrite every partition it received. If we pass a non-empty map, it means
     *     we only overwrite the partitions match the map.
     * @return returns this.
     */
    public FlinkSinkBuilder withOverwritePartition(
            @Nullable Map<String, String> overwritePartition) {
        this.overwritePartition = overwritePartition;
        return this;
    }

    public FlinkSinkBuilder withLogSinkFunction(@Nullable LogSinkFunction logSinkFunction) {
        this.logSinkFunction = logSinkFunction;
        return this;
    }

    public FlinkSinkBuilder withParallelism(@Nullable Integer parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public FlinkSinkBuilder withBoundedInputStream(boolean bounded) {
        this.boundedInput = bounded;
        return this;
    }

    public FlinkSinkBuilder forCompact(boolean compactSink) {
        this.compactSink = compactSink;
        return this;
    }

    public DataStreamSink<?> build() {
        // SR24.03.18 这里将Flink的数据结构转换成paimon的数据结构
        DataStream<InternalRow> input = MapToInternalRow.map(this.input, table.rowType());
        if (table.coreOptions().localMergeEnabled() && table.schema().primaryKeys().size() > 0) {
            input =
                    // SR24.03.21 forward 使得不进行重新分区
                    input.forward()
                            .transform(
                                    "local merge",
                                    input.getType(),
                                    new LocalMergeOperator(table.schema()))
                            .setParallelism(input.getParallelism());
        }

        BucketMode bucketMode = table.bucketMode();
        // SR24.03.16 会根据不同的bucket(分桶)模式 生成不同的sink
        switch (bucketMode) {
            case FIXED:
                // 固定分桶
                return buildForFixedBucket(input);
            case DYNAMIC:
                // 动态分桶
                return buildDynamicBucketSink(input, false);
            case GLOBAL_DYNAMIC:
                // 全局动态分桶
                return buildDynamicBucketSink(input, true);
            case UNAWARE:
                // 忽略桶,只适用于append表
                return buildUnawareBucketSink(input);
            default:
                throw new UnsupportedOperationException("Unsupported bucket mode: " + bucketMode);
        }
    }

    private DataStreamSink<?> buildDynamicBucketSink(
            DataStream<InternalRow> input, boolean globalIndex) {
        checkArgument(logSinkFunction == null, "Dynamic bucket mode can not work with log system.");
        return compactSink && !globalIndex
                // todo support global index sort compact
                ? new DynamicBucketCompactSink(table, overwritePartition).build(input, parallelism)
                : globalIndex
                        ? new GlobalDynamicBucketSink(table, overwritePartition)
                                .build(input, parallelism)
                        : new RowDynamicBucketSink(table, overwritePartition)
                                .build(input, parallelism);
    }

    private DataStreamSink<?> buildForFixedBucket(DataStream<InternalRow> input) {
        // SR24.03.16 首先根据分区信息、bucket字段进行bucket分组
        DataStream<InternalRow> partitioned =
                partition(
                        input,
                        new RowDataChannelComputer(table.schema(), logSinkFunction != null),
                        parallelism);
        // SR24.03.16 FileStoreSink实现将记录写入Paimon，FileStoreSink提供了生成写入算子的方法
        FixedBucketSink sink = new FixedBucketSink(table, overwritePartition, logSinkFunction);
        return sink.sinkFrom(partitioned);
    }

    private DataStreamSink<?> buildUnawareBucketSink(DataStream<InternalRow> input) {
        checkArgument(
                table.primaryKeys().isEmpty(),
                "Unaware bucket mode only works with append-only table for now.");
        return new RowUnawareBucketSink(
                        table, overwritePartition, logSinkFunction, parallelism, boundedInput)
                .sinkFrom(input);
    }
}
