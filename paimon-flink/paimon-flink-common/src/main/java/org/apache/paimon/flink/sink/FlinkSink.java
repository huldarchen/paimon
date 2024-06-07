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

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.TagCreationMode;
import org.apache.paimon.flink.utils.StreamExecutionEnvironmentUtils;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SerializableRunnable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.configuration.ClusterOptions.ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT;
import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.flink.FlinkConnectorOptions.CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_AUTO_TAG_FOR_SAVEPOINT;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_CPU;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_COMMITTER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_MANAGED_WRITER_BUFFER_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.SINK_USE_MANAGED_MEMORY;
import static org.apache.paimon.flink.FlinkConnectorOptions.prepareCommitWaitCompaction;
import static org.apache.paimon.flink.utils.ManagedMemoryUtils.declareManagedMemory;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Abstract sink of paimon. */
public abstract class FlinkSink<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String WRITER_NAME = "Writer";
    private static final String WRITER_WRITE_ONLY_NAME = "Writer(write-only)";
    private static final String GLOBAL_COMMITTER_NAME = "Global Committer";

    protected final FileStoreTable table;
    private final boolean ignorePreviousFiles;

    public FlinkSink(FileStoreTable table, boolean ignorePreviousFiles) {
        this.table = table;
        this.ignorePreviousFiles = ignorePreviousFiles;
    }

    private StoreSinkWrite.Provider createWriteProvider(
            CheckpointConfig checkpointConfig, boolean isStreaming, boolean hasSinkMaterializer) {
        SerializableRunnable assertNoSinkMaterializer =
                () ->
                        Preconditions.checkArgument(
                                !hasSinkMaterializer,
                                String.format(
                                        "Sink materializer must not be used with Paimon sink. "
                                                + "Please set '%s' to '%s' in Flink's config.",
                                        ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE
                                                .key(),
                                        ExecutionConfigOptions.UpsertMaterialize.NONE.name()));

        boolean waitCompaction;
        if (table.coreOptions().writeOnly()) {
            // SR24.03.21 如果是只读 即将跳过 合并和镜像操作
            waitCompaction = false;
        } else {
            Options options = table.coreOptions().toConfiguration();
            ChangelogProducer changelogProducer = table.coreOptions().changelogProducer();
            waitCompaction = prepareCommitWaitCompaction(options);
            // SR24.03.21 增量提交后将不断触发完全压缩。
            int deltaCommits = -1;
            if (options.contains(FULL_COMPACTION_DELTA_COMMITS)) {
                deltaCommits = options.get(FULL_COMPACTION_DELTA_COMMITS);
            } else if (options.contains(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL)) {
                long fullCompactionThresholdMs =
                        options.get(CHANGELOG_PRODUCER_FULL_COMPACTION_TRIGGER_INTERVAL).toMillis();
                deltaCommits =
                        (int)
                                (fullCompactionThresholdMs
                                        / checkpointConfig.getCheckpointInterval());
            }

            // SR24.03.16 如果changelog 是每次完全压缩时生成变更日志文件(代价最大的) 或者 完全增量提交出发次数
            if (changelogProducer == ChangelogProducer.FULL_COMPACTION || deltaCommits >= 0) {
                int finalDeltaCommits = Math.max(deltaCommits, 1);
                return (table, commitUser, state, ioManager, memoryPool, metricGroup) -> {
                    assertNoSinkMaterializer.run();
                    return new GlobalFullCompactionSinkWrite(
                            table,
                            commitUser,
                            state,
                            ioManager,
                            ignorePreviousFiles,
                            waitCompaction,
                            finalDeltaCommits,
                            isStreaming,
                            memoryPool,
                            metricGroup);
                };
            }
        }
        // SR24.03.16 大多数的写入实现
        return (table, commitUser, state, ioManager, memoryPool, metricGroup) -> {
            assertNoSinkMaterializer.run();
            return new StoreSinkWriteImpl(
                    table,
                    commitUser,
                    state,
                    ioManager,
                    ignorePreviousFiles,
                    waitCompaction,
                    isStreaming,
                    memoryPool,
                    metricGroup);
        };
    }

    public DataStreamSink<?> sinkFrom(DataStream<T> input) {
        // This commitUser is valid only for new jobs.
        // After the job starts, this commitUser will be recorded into the states of write and
        // commit operators.
        // When the job restarts, commitUser will be recovered from states and this value is
        // ignored.
        String initialCommitUser = UUID.randomUUID().toString();
        return sinkFrom(input, initialCommitUser);
    }

    public DataStreamSink<?> sinkFrom(DataStream<T> input, String initialCommitUser) {
        // SR24.03.16 执行真正的写入操作，在这个阶段不会进行提交，相当于两阶段提交的第一阶段，进行数据写入，不会有snapshot生成
        // do the actually writing action, no snapshot generated in this stage
        DataStream<Committable> written = doWrite(input, initialCommitUser, input.getParallelism());
        // SR24.03.16 执行提交操作，会生成snapshot，下游可见，如果日志配置
        // commit the committable to generate a new snapshot
        return doCommit(written, initialCommitUser);
    }

    private boolean hasSinkMaterializer(DataStream<T> input) {
        // traverse the transformation graph with breadth first search
        Set<Integer> visited = new HashSet<>();
        Queue<Transformation<?>> queue = new LinkedList<>();
        queue.add(input.getTransformation());
        visited.add(input.getTransformation().getId());
        while (!queue.isEmpty()) {
            Transformation<?> transformation = queue.poll();
            if (transformation.getName().startsWith("SinkMaterializer")) {
                return true;
            }
            for (Transformation<?> prev : transformation.getInputs()) {
                if (!visited.contains(prev.getId())) {
                    queue.add(prev);
                    visited.add(prev.getId());
                }
            }
        }
        return false;
    }

    public DataStream<Committable> doWrite(
            DataStream<T> input, String commitUser, @Nullable Integer parallelism) {
        StreamExecutionEnvironment env = input.getExecutionEnvironment();
        boolean isStreaming =
                StreamExecutionEnvironmentUtils.getConfiguration(env)
                                .get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;

        // SR24.03.16
        // 是否只是writeOnly，如果是，则会忽略compact和snapshot过期，这个配置需要结合专门的compact任务执行，不然会造成小文件剧增，同时降低数据读的性能
        boolean writeOnly = table.coreOptions().writeOnly();
        SingleOutputStreamOperator<Committable> written =
                input.transform(
                                (writeOnly ? WRITER_WRITE_ONLY_NAME : WRITER_NAME)
                                        + " : "
                                        + table.name(),
                                new CommittableTypeInfo(),
                                // SR24.03.16 写入算子，真正的写入流程;上游的数据会通过这个算子将数据写入到Paimon表
                                createWriteOperator(
                                        // SR24.03.21 写入代理
                                        createWriteProvider(
                                                env.getCheckpointConfig(),
                                                isStreaming,
                                                hasSinkMaterializer(input)),
                                        commitUser))
                        .setParallelism(parallelism == null ? input.getParallelism() : parallelism);

        if (!isStreaming) {
            assertBatchConfiguration(env, written.getParallelism());
        }

        // SR24.03.16 Flink会创建一个独立的内存分配器用于merge tree的数据写入操作
        // SR24.03.16 否则会使用TM的管理内存支持写入操作
        Options options = Options.fromMap(table.options());
        if (options.get(SINK_USE_MANAGED_MEMORY)) {
            declareManagedMemory(written, options.get(SINK_MANAGED_WRITER_BUFFER_MEMORY));
        }
        return written;
    }

    protected DataStreamSink<?> doCommit(DataStream<Committable> written, String commitUser) {
        StreamExecutionEnvironment env = written.getExecutionEnvironment();
        ReadableConfig conf = StreamExecutionEnvironmentUtils.getConfiguration(env);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        boolean isStreaming =
                conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        boolean streamingCheckpointEnabled =
                isStreaming && checkpointConfig.isCheckpointingEnabled();
        if (streamingCheckpointEnabled) {
            assertStreamingConfiguration(env);
        }

        OneInputStreamOperator<Committable, Committable> committerOperator =
                // SR24.03.16 执行提交操作的算子，该算子会生成snapshot，下游可见
                new CommitterOperator<>(
                        streamingCheckpointEnabled,
                        commitUser,
                        createCommitterFactory(streamingCheckpointEnabled),
                        createCommittableStateManager());
        if (Options.fromMap(table.options()).get(SINK_AUTO_TAG_FOR_SAVEPOINT)) {
            committerOperator =
                    new AutoTagForSavepointCommitterOperator<>(
                            (CommitterOperator<Committable, ManifestCommittable>) committerOperator,
                            table::snapshotManager,
                            table::tagManager,
                            () -> table.store().newTagDeletion(),
                            () -> table.store().createTagCallbacks());
        }
        if (conf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.BATCH
                && table.coreOptions().tagCreationMode() == TagCreationMode.BATCH) {
            committerOperator =
                    new BatchWriteGeneratorTagOperator<>(
                            (CommitterOperator<Committable, ManifestCommittable>) committerOperator,
                            table);
        }
        SingleOutputStreamOperator<?> committed =
                written.transform(
                                GLOBAL_COMMITTER_NAME + " : " + table.name(),
                                new CommittableTypeInfo(),
                                committerOperator)
                        .setParallelism(1)
                        .setMaxParallelism(1);
        Options options = Options.fromMap(table.options());
        configureGlobalCommitter(
                committed,
                options.get(SINK_COMMITTER_CPU),
                options.get(SINK_COMMITTER_MEMORY),
                conf);
        return committed.addSink(new DiscardingSink<>()).name("end").setParallelism(1);
    }

    public static void configureGlobalCommitter(
            SingleOutputStreamOperator<?> committed,
            double cpuCores,
            @Nullable MemorySize heapMemory,
            ReadableConfig conf) {
        if (heapMemory == null) {
            return;
        }

        if (!conf.get(ENABLE_FINE_GRAINED_RESOURCE_MANAGEMENT)) {
            throw new RuntimeException(
                    "To support the 'sink.committer-cpu' and 'sink.committer-memory' configurations, you must enable fine-grained resource management. Please set 'cluster.fine-grained-resource-management.enabled' to 'true' in your Flink configuration.");
        }

        SlotSharingGroup slotSharingGroup =
                SlotSharingGroup.newBuilder(committed.getName())
                        .setCpuCores(cpuCores)
                        .setTaskHeapMemory(
                                new org.apache.flink.configuration.MemorySize(
                                        heapMemory.getBytes()))
                        .build();
        committed.slotSharingGroup(slotSharingGroup);
    }

    public static void assertStreamingConfiguration(StreamExecutionEnvironment env) {
        checkArgument(
                !env.getCheckpointConfig().isUnalignedCheckpointsEnabled(),
                "Paimon sink currently does not support unaligned checkpoints. Please set "
                        + ExecutionCheckpointingOptions.ENABLE_UNALIGNED.key()
                        + " to false.");
        checkArgument(
                env.getCheckpointConfig().getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE,
                "Paimon sink currently only supports EXACTLY_ONCE checkpoint mode. Please set "
                        + ExecutionCheckpointingOptions.CHECKPOINTING_MODE.key()
                        + " to exactly-once");
    }

    private void assertBatchConfiguration(StreamExecutionEnvironment env, int sinkParallelism) {
        try {
            checkArgument(
                    sinkParallelism != -1 || !AdaptiveParallelism.isEnabled(env),
                    "Paimon Sink does not support Flink's Adaptive Parallelism mode. "
                            + "Please manually turn it off or set Paimon `sink.parallelism` manually.");
        } catch (NoClassDefFoundError ignored) {
            // before 1.17, there is no adaptive parallelism
        }
    }

    protected abstract OneInputStreamOperator<T, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, String commitUser);

    protected abstract Committer.Factory<Committable, ManifestCommittable> createCommitterFactory(
            boolean streamingCheckpointEnabled);

    protected abstract CommittableStateManager<ManifestCommittable> createCommittableStateManager();
}
