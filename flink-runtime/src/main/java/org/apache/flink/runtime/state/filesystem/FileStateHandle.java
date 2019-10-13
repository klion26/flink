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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.IOException;

/**
 * {@link StreamStateHandle} for state that was written to a file stream. The written data is
 * identified by the file path. The state can be read again by calling {@link #openInputStream()}.
 */
public class FileStateHandle extends AbstractFileBasedStateHandle {

	private static final long serialVersionUID = 350284443258002355L;

	/**
	 * Creates a new file state for the given file path.
	 *
	 * @param filePath The path to the file that stores the state.
	 */
	public FileStateHandle(Path filePath, long stateSize) {
		super(filePath, stateSize);
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		return getFileSystem().open(filePath);
	}

	/**
	 * Discard the state by deleting the file that stores the state. If the parent directory
	 * of the state is empty after deleting the state file, it is also deleted.
	 *
	 * @throws Exception Thrown, if the file deletion (not the directory deletion) fails.
	 */
	@Override
	public void discardState() throws Exception {
		FileSystem fs = getFileSystem();
		fs.delete(filePath, false);
	}

	/**
	 * Gets the file system that stores the file state.
	 *
	 * @return The file system that stores the file state.
	 * @throws IOException Thrown if the file system cannot be accessed.
	 */
	private FileSystem getFileSystem() throws IOException {
		return FileSystem.get(filePath.toUri());
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof FileStateHandle)) {
			return false;
		}

		FileStateHandle that = (FileStateHandle) o;
		return filePath.equals(that.filePath);

	}

	@Override
	public int hashCode() {
		return filePath.hashCode();
	}

	@Override
	public String toString() {
		return String.format("File State: %s [%d bytes]", filePath, stateSize);
	}
}
