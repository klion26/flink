/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.examples.windowing;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.fs.s3presto.S3FileSystemFactory;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.windowing.TopSpeedWindowing;
import org.apache.flink.test.checkpointing.utils.MigrationTestUtils;
import org.apache.flink.test.checkpointing.utils.SavepointMigrationTestBase;
import org.apache.flink.testutils.s3.S3TestCredentials;

import org.junit.Test;

import static com.facebook.presto.hive.s3.PrestoS3FileSystem.S3_USE_INSTANCE_CREDENTIALS;

/**
 * Migration ITCases for a stateful job. The tests are parameterized to cover
 * migrating for multiple previous Flink versions, as well as for different state backends.
 */
public class TopSpeedWindowingSavepointRestoreITCase extends SavepointMigrationTestBase {

	public TopSpeedWindowingSavepointRestoreITCase() throws Exception {
	}

	@Test
	public void testSavepoint() throws Exception {
		final int parallelism = 4;

		{
			Configuration configuration = new Configuration();
			configuration.setString(S3_USE_INSTANCE_CREDENTIALS, "false");
			configuration.setString("s3.access-key", S3TestCredentials.getS3AccessKey());
			configuration.setString("s3.secret-key", S3TestCredentials.getS3SecretKey());
			FileSystem.initialize(configuration);
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.setParallelism(parallelism);
		env.setStateBackend(new FsStateBackend(TEMP_FOLDER.newFolder().toURI()));

		TopSpeedWindowing.setupJob(ParameterTool.fromArgs(new String[] {}), env)
			.addSink(new MigrationTestUtils.AccumulatorCountingSink<>());

		restoreAndExecute(
			env,
			System.getenv("SAVEPOINT_SOURCE"),
			new Tuple2<>(MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, 1));
	}
}
