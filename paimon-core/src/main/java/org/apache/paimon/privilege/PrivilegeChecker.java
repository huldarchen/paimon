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

package org.apache.paimon.privilege;

import org.apache.paimon.catalog.Identifier;

import java.io.Serializable;

/** Check if current user has privilege to perform related operations. */
public interface PrivilegeChecker extends Serializable {
    default void assertCanSelectOrInsert(Identifier identifier) {
        try {
            assertCanSelect(identifier);
        } catch (NoPrivilegeException e) {
            try {
                assertCanInsert(identifier);
            } catch (NoPrivilegeException e1) {
                throw new NoPrivilegeException(
                        e1.getUser(),
                        e1.getObjectType(),
                        e1.getIdentifier(),
                        PrivilegeType.SELECT,
                        PrivilegeType.INSERT);
            }
        }
    }

    void assertCanSelect(Identifier identifier);

    void assertCanInsert(Identifier identifier);

    void assertCanAlterTable(Identifier identifier);

    void assertCanDropTable(Identifier identifier);

    void assertCanCreateTable(String databaseName);

    void assertCanDropDatabase(String databaseName);

    void assertCanAlterDatabase(String databaseName);

    void assertCanCreateDatabase();

    void assertCanCreateUser();

    void assertCanDropUser();

    void assertCanGrant(String identifier, PrivilegeType privilege);

    void assertCanRevoke();
}
