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

package org.apache.paimon.flink.source;

import org.apache.paimon.append.AppendCompactTask;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.types.Either;

/** Factory of {@link AppendBypassCoordinateOperator}. */
public class AppendBypassCoordinateOperatorFactory<CommitT>
        extends AbstractStreamOperatorFactory<Either<CommitT, AppendCompactTask>>
        implements OneInputStreamOperatorFactory<CommitT, Either<CommitT, AppendCompactTask>> {

    private final FileStoreTable table;

    public AppendBypassCoordinateOperatorFactory(FileStoreTable table) {
        this.table = table;
    }

    @Override
    public <T extends StreamOperator<Either<CommitT, AppendCompactTask>>> T createStreamOperator(
            StreamOperatorParameters<Either<CommitT, AppendCompactTask>> parameters) {
        AppendBypassCoordinateOperator<CommitT> operator =
                new AppendBypassCoordinateOperator<>(parameters, table, processingTimeService);
        return (T) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return AppendBypassCoordinateOperator.class;
    }
}
