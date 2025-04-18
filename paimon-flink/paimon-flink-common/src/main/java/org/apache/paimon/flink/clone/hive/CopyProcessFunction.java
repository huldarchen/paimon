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

package org.apache.paimon.flink.clone.hive;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.io.DataFileMetaSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import java.util.HashMap;
import java.util.Map;

import static org.apache.paimon.flink.FlinkCatalogFactory.createPaimonCatalog;
import static org.apache.paimon.flink.clone.hive.CloneHiveUtils.getRootHiveCatalog;

/** Abstract function for copying tables. */
public abstract class CopyProcessFunction<I, O> extends ProcessFunction<I, O> {

    protected final Map<String, String> sourceCatalogConfig;
    protected final Map<String, String> targetCatalogConfig;

    protected transient HiveCatalog hiveCatalog;
    protected transient Catalog targetCatalog;

    protected transient Map<Identifier, Table> tableCache;
    protected transient DataFileMetaSerializer dataFileSerializer;

    public CopyProcessFunction(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.hiveCatalog =
                getRootHiveCatalog(createPaimonCatalog(Options.fromMap(sourceCatalogConfig)));
        this.targetCatalog = createPaimonCatalog(Options.fromMap(targetCatalogConfig));
        this.dataFileSerializer = new DataFileMetaSerializer();
        this.tableCache = new HashMap<>();
    }

    protected Table getTable(Identifier identifier) {
        return tableCache.computeIfAbsent(
                identifier,
                k -> {
                    try {
                        return targetCatalog.getTable(k);
                    } catch (Catalog.TableNotExistException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.hiveCatalog.close();
        this.targetCatalog.close();
    }
}
