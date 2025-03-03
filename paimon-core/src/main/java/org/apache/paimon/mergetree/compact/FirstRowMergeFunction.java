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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValue;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

/**
 * A {@link MergeFunction} where key is primary key (unique) and value is the full record, only keep
 * the first one.
 */
public class FirstRowMergeFunction implements MergeFunction<KeyValue> {

    private KeyValue first;
    public boolean containsHighLevel;
    private final boolean ignoreDelete;

    protected FirstRowMergeFunction(boolean ignoreDelete) {
        this.ignoreDelete = ignoreDelete;
    }

    @Override
    public void reset() {
        this.first = null;
        this.containsHighLevel = false;
    }

    @Override
    public void add(KeyValue kv) {
        if (kv.valueKind().isRetract()) {
            // In 0.7- versions, the delete records might be written into data file even when
            // ignore-delete configured, so ignoreDelete still needs to be checked
            if (ignoreDelete) {
                return;
            } else {
                throw new IllegalArgumentException(
                        "By default, First row merge engine can not accept DELETE/UPDATE_BEFORE records.\n"
                                + "You can config 'first-row.ignore-delete' to ignore the DELETE/UPDATE_BEFORE records.");
            }
        }

        if (first == null) {
            this.first = kv;
        }
        if (kv.level() > 0) {
            containsHighLevel = true;
        }
    }

    @Override
    public KeyValue getResult() {
        return first;
    }

    @Override
    public boolean requireCopy() {
        return true;
    }

    public static MergeFunctionFactory<KeyValue> factory(Options options) {
        return new FirstRowMergeFunction.Factory(options.get(CoreOptions.IGNORE_DELETE));
    }

    private static class Factory implements MergeFunctionFactory<KeyValue> {

        private static final long serialVersionUID = 1L;
        private final boolean ignoreDelete;

        public Factory(boolean ignoreDelete) {
            this.ignoreDelete = ignoreDelete;
        }

        @Override
        public MergeFunction<KeyValue> create(@Nullable int[][] projection) {
            return new FirstRowMergeFunction(ignoreDelete);
        }
    }
}
