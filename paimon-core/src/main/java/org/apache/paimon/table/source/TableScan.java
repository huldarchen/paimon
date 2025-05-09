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

package org.apache.paimon.table.source;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.table.Table;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A scan of {@link Table} to generate {@link Split} splits.
 *
 * @since 0.4.0
 */
@Public
public interface TableScan {

    /** Set {@link MetricRegistry} to table scan. */
    TableScan withMetricRegistry(MetricRegistry registry);

    /** Plan splits, throws {@link EndOfScanException} if the scan is ended. */
    Plan plan();

    /** List partitions. */
    default List<BinaryRow> listPartitions() {
        return listPartitionEntries().stream()
                .map(PartitionEntry::partition)
                .collect(Collectors.toList());
    }

    List<PartitionEntry> listPartitionEntries();

    /**
     * Plan of scan.
     *
     * @since 0.4.0
     */
    @Public
    interface Plan {
        List<Split> splits();
    }
}
