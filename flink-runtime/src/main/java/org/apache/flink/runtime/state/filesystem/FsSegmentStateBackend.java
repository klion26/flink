/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TernaryBoolean;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;

/**
 * The file state backend is a state backend like {@link FsStateBackend}, but stores the state of streaming jobs in segments on file system.
 */
public class FsSegmentStateBackend extends FsStateBackend {

	public FsSegmentStateBackend(String checkpointDataUri) {
		super(checkpointDataUri);
	}

	public FsSegmentStateBackend(URI checkpointDataUri) {
		super(checkpointDataUri);
	}

	public FsSegmentStateBackend(
		URI checkpointDirectory,
		@Nullable URI defaultSavepointDirectory,
		int fileStateSizeThreshold,
		TernaryBoolean asynchronousSnapshots) {

		super(checkpointDirectory, defaultSavepointDirectory, fileStateSizeThreshold, asynchronousSnapshots);
	}

	@Override
	public FsSegmentStateBackend configure(Configuration config, ClassLoader classLoader) {

		FsStateBackend configured = super.configure(config, classLoader);

		return new FsSegmentStateBackend(
			configured.getCheckpointPath().toUri(),
			configured.getSavepointPath() == null ? null : configured.getSavepointPath().toUri(),
			configured.getMinFileSizeThreshold(),
			TernaryBoolean.fromBoolean(configured.isUsingAsynchronousSnapshots()));
	}

	@Override
	public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
		Preconditions.checkNotNull(jobId, "jobId can't be null when creating checkpoint storage.");
		return new FsSegmentCheckpointStorage(
			getCheckpointPath(),
			getSavepointPath(),
			jobId,
			getMinFileSizeThreshold());
	}
}

