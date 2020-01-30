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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A {@link TypeSerializerUpgradeTestBase} for BaseType Serializers.
 */
@RunWith(Parameterized.class)
public class BaseTypeSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

	public BaseTypeSerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
		super(testSpecification);
	}

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

		ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		for (MigrationVersion migrationVersion : migrationVersions) {
			testSpecifications.add(
				new TestSpecification<>(
					"big-dec-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.BigDecSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.BigDecSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"big-int-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.BigIntSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.BigIntSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"boolean-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.BooleanSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.BooleanSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"boolean-value-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.BooleanValueSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.BooleanValueSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"byte-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.ByteSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.ByteSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"byte-value-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.ByteValueSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.ByteValueSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"char-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.CharSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.CharSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"char-value-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.CharValueSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.CharValueSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"date-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.DateSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.DateSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"double-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.DoubleSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.DoubleSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"double-value-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.DoubleValueSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.DoubleValueSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"float-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.FloatSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.FloatSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"float-value-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.FloatValueSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.FloatValueSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"int-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.IntSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.IntSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"int-value-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.IntValueSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.IntValueSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"long-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.LongSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.LongSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"long-value-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.LongValueSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.LongValueSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"null-value-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.NullValueSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.NullValueSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"short-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.ShortSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.ShortSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"short-value-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.ShortValueSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.ShortValueSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"sql-date-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.SqlDateSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.SqlDateSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"sql-time-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.SqlTimeSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.SqlTimeSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"sql-timestamp-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.SqlTimestampSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.SqlTimestampSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"string-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.StringSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.StringSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"string-value-serializer",
					migrationVersion,
					BaseTypeSerializerUpgradeTestSpecifications.StringValueSerializerSetup.class,
					BaseTypeSerializerUpgradeTestSpecifications.StringValueSerializerVerifier.class));
		}

		return testSpecifications;
	}
}
