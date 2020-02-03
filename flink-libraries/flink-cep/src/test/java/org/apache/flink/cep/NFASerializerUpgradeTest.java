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

package org.apache.flink.cep;

import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A {@link TypeSerializerUpgradeTestBase} for NFA-related serializers.
 */
@RunWith(Parameterized.class)
public class NFASerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

	public NFASerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
		super(testSpecification);
	}

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {
		ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		for (MigrationVersion migrationVersion : migrationVersions) {
			testSpecifications.add(
				new TestSpecification<>(
					"dewey-number-serializer",
					migrationVersion,
					NFASerializerUpgradeTestSpecifications.DeweyNumberSerializerSetup.class,
					NFASerializerUpgradeTestSpecifications.DeweyNumberSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"event-id-serializer",
					migrationVersion,
					NFASerializerUpgradeTestSpecifications.EventIdSerializerSetup.class,
					NFASerializerUpgradeTestSpecifications.EventIdSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"nfa-state-serializer",
					migrationVersion,
					NFASerializerUpgradeTestSpecifications.NFAStateSerializerSetup.class,
					NFASerializerUpgradeTestSpecifications.NFAStateSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"node-id-serializer",
					migrationVersion,
					NFASerializerUpgradeTestSpecifications.NodeIdSerializerSetup.class,
					NFASerializerUpgradeTestSpecifications.NodeIdSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"shared-buffer-edge-serializer",
					migrationVersion,
					NFASerializerUpgradeTestSpecifications.SharedBufferEdgeSerializerSetup.class,
					NFASerializerUpgradeTestSpecifications.SharedBufferEdgeSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"shared-buffer-node-serializer",
					migrationVersion,
					NFASerializerUpgradeTestSpecifications.SharedBufferNodeSerializerSetup.class,
					NFASerializerUpgradeTestSpecifications.SharedBufferNodeSerializerVerifier.class));
		}
		return testSpecifications;
	}
}
