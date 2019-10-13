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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract class for all file based state handle.
 */
public abstract class AbstractFileBasedStateHandle implements StreamStateHandle {

	private static final long serialVersionUID = 1L;

	/**
	 * Underlying file used by current state handle, this file maybe used by multiple state handle.
	 */
	protected final Path filePath;

	/** The size of the state in the file */
	protected final long stateSize;

	AbstractFileBasedStateHandle(Path filePath, long stateSize) {
		this.filePath = checkNotNull(filePath);
		checkArgument(stateSize >= -1);
		this.stateSize = stateSize;
	}
	/**
	 * Return the underlying file path used by current state handler.
	 */
	public Path getFilePath() {
		return filePath;
	}

	/**
	 * Returns the file size in bytes.
	 */
	@Override
	public long getStateSize() {
		return stateSize;
	}
}

