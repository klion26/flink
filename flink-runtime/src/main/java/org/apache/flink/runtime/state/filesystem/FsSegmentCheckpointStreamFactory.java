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

import org.apache.flink.core.fs.EntropyInjector;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.OutputStreamAndPath;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link CheckpointStreamFactory} that supports to create different streams on the same underlying file.
 */
public class FsSegmentCheckpointStreamFactory implements CheckpointStreamFactory {
	private static final Logger LOG = LoggerFactory.getLogger(FsSegmentCheckpointStreamFactory.class);

	/** Default size for the write buffer. */
	private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

	/**
	 * FileSegmentCheckpointStreamFactory-wide lock to safeguard the output stream updates.
	 */
	private final Object lock;

	/**
	 * Cached handle to the file system for file operations.
	 */
	private final FileSystem filesystem;

	/** The directory for shared checkpoint data. */
	private final Path sharedStateDirectory;

	/** The directory for taskowned data. */
	private final Path taskOwnedStateDirectory;

	/** The directory for checkpoint exclusive state data. */
	private final Path checkpointDirector;

	private final int writeBufferSize;
	/**
	 * The paths of the file under writing.
	 */
	@GuardedBy("lock")
	private final Map<Long, Path> filePaths;

	@GuardedBy("lock")
	private final Map<Long, CheckpointStateOutputStream> openedOutputStreams;

	/**
	 * The currentCheckpointOutputStream of the current output stream.
	 */
	private ConcurrentHashMap<Long, FSDataOutputStream> fileOutputStreams;

	/** Will be used when delivery to FsCheckpointStateOutputStream. */
	private final int fileSizeThreshold;

	public FsSegmentCheckpointStreamFactory(
		FileSystem fileSystem,
		Path checkpointDirectory,
		Path sharedStateDirectory,
		Path taskOwnedStateDirectory,
		int fileSizeThreshold) {
		this(fileSystem, checkpointDirectory, sharedStateDirectory, taskOwnedStateDirectory, DEFAULT_WRITE_BUFFER_SIZE, fileSizeThreshold);
	}

	public FsSegmentCheckpointStreamFactory(
		FileSystem fileSystem,
		Path checkpointDirector,
		Path sharedStateDirectory,
		Path taskOwnedStateDirectory,
		int writeBufferSize,
		int fileSizeThreshold) {
		this.filesystem = checkNotNull(fileSystem);
		this.checkpointDirector = checkNotNull(checkpointDirector);
		this.sharedStateDirectory = checkNotNull(sharedStateDirectory);
		this.taskOwnedStateDirectory = checkNotNull(taskOwnedStateDirectory);
		this.writeBufferSize = writeBufferSize;
		this.fileSizeThreshold = fileSizeThreshold;
		this.lock = new Object();

		this.filePaths = new ConcurrentHashMap<>();
		this.fileOutputStreams = new ConcurrentHashMap<>();
		this.openedOutputStreams = new HashMap<>();
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(
		long checkpointId,
		CheckpointedStateScope scope) throws IOException {
		if (CheckpointedStateScope.EXCLUSIVE.equals(scope)) {
			return new FsCheckpointStreamFactory.FsCheckpointStateOutputStream(
				checkpointDirector,
				filesystem,
				writeBufferSize,
				fileSizeThreshold);
		}
		synchronized (lock) {
			try {
				CheckpointStateOutputStream outputStream = new FsSegmentCheckpointStateOutputStream(checkpointId, sharedStateDirectory);
				openedOutputStreams.put(checkpointId, outputStream);
				return outputStream;
			} catch (IOException e) {
				openedOutputStreams.remove(checkpointId);
				closeFileOutputStream(checkpointId);
				throw e;
			}
		}
	}

	public void closeFileOutputStream(long checkpointId) {
		// close the underlying file used for checkpointId.
		synchronized (fileOutputStreams) {
			try {
				fileOutputStreams.get(checkpointId).close();
			} catch (IOException e) {
				LOG.warn("Can not close the checkpoint file for checkpoint {}.", checkpointId);
			} finally {
				filePaths.remove(checkpointId);
				fileOutputStreams.remove(checkpointId);
			}
		}
	}

	public Path getSharedStateDirectory() {
		return sharedStateDirectory;
	}

	public Path getTaskOwnedStateDirectory() {
		return taskOwnedStateDirectory;
	}

	public FileSystem getFileSystem() {
		return filesystem;
	}

	private Path createStatePath(Path basePath) {
		return new Path(basePath, UUID.randomUUID().toString());
	}

	private void createFileOutputStream(long checkpointId, Path basePath) throws IOException {
		Exception latestException = null;
		for (int attempt = 0; attempt < 10; attempt++) {
			try {
				if (!filePaths.containsKey(checkpointId)) {
					// we know we're guarded by the lock, so no need to sync here.
					Path nextFilePath = createStatePath(basePath);

					OutputStreamAndPath streamAndPath = EntropyInjector.createEntropyAware(
						filesystem, nextFilePath, FileSystem.WriteMode.NO_OVERWRITE);

					FSDataOutputStream nextFileOutputStream = streamAndPath.stream();

					filePaths.put(checkpointId, streamAndPath.path());
					fileOutputStreams.put(checkpointId, nextFileOutputStream);
				}
				return;
			} catch (Exception e) {
				latestException = e;
			}
		}

		throw new IOException("Could not open output stream for state backend", latestException);
	}

	/**
	 * A {@link org.apache.flink.runtime.state.CheckpointStreamFactory.CheckpointStateOutputStream} will reuse the underlying file if possible.
	 */
	public final class FsSegmentCheckpointStateOutputStream extends CheckpointStreamFactory.CheckpointStateOutputStream {
		private final Path checkpointBasePath;

		private final long checkpointId;
		/**
		 * The owner thread of the output stream.
		 */
		private final long ownerThreadID;

		private final byte[] buffer;

		private int bufferIndex;

		private final long startPos;

		private boolean closed;

		private FSDataOutputStream currentFileOutputStream;

		FsSegmentCheckpointStateOutputStream(long checkpointId, Path basePath) throws IOException {
			this.checkpointId = checkpointId;
			this.checkpointBasePath = checkNotNull(basePath);
			this.ownerThreadID = Thread.currentThread().getId();

			this.closed = false;

			this.buffer = new byte[writeBufferSize];
			this.bufferIndex = 0;

			currentFileOutputStream = fileOutputStreams.get(checkpointId);
			this.startPos = currentFileOutputStream == null ? 0 : currentFileOutputStream.getPos();
		}

		@Nullable
		@Override
		public StreamStateHandle closeAndGetHandle() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			synchronized (lock) {
				Preconditions.checkState(openedOutputStreams.get(checkpointId) == this);

				try {
					flush();
					long endPos = currentFileOutputStream.getPos();
					return new FsSegmentStateHandle(filePaths.get(checkpointId), startPos, endPos);
				} finally {
					closed = true;
					currentFileOutputStream = null;
				}
			}
		}

		@Override
		public void close() {
			System.out.println("Close....");
			if (closed) {
				return;
			}

			synchronized (lock) {
				try {
					closeFileOutputStream(checkpointId);
				} finally {
					closed = true;
					currentFileOutputStream = null;
				}
			}
		}

		/**
		 * Checks whether the stream is closed.
		 *
		 * @return True if the stream was closed, false if it is still open.
		 */
		public boolean isClosed() {
			return closed;
		}

		@Override
		public long getPos() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			if (currentFileOutputStream == null) {
				return bufferIndex;
			} else {
				synchronized (lock) {
					checkState(openedOutputStreams.get(checkpointId) == this);

					try {
						return bufferIndex + currentFileOutputStream.getPos() - startPos;
					} catch (IOException e) {
						closeFileOutputStream(checkpointId);
						throw e;
					}
				}
			}
		}

		@Override
		public void flush() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			synchronized (lock) {
				checkState(openedOutputStreams.get(checkpointId) == this);

				// create a new file if there does not exist a opened one
				if (currentFileOutputStream == null && bufferIndex > 0) {
					createFileOutputStream(checkpointId, checkpointBasePath);
					currentFileOutputStream = fileOutputStreams.get(checkpointId);
				}

				// flush the data in the buffer to the file.
				if (bufferIndex > 0) {
					try {
						currentFileOutputStream.write(buffer, 0, bufferIndex);
						currentFileOutputStream.flush();

						bufferIndex = 0;
					} catch (IOException e) {
						closeFileOutputStream(checkpointId);
						throw e;
					}
				}
			}
		}

		@Override
		public void sync() throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			synchronized (lock) {
				Preconditions.checkState(openedOutputStreams.get(checkpointId) == this);

				try {
					currentFileOutputStream.sync();
				} catch (Exception e) {
					closeFileOutputStream(checkpointId);
					throw e;
				}
			}
		}

		@Override
		public void write(int b) throws IOException {
			checkState(Thread.currentThread().getId() == ownerThreadID);
			checkState(!closed);

			if (bufferIndex >= buffer.length) {
				flush();
			}

			if (!closed) {
				buffer[bufferIndex++] = (byte) b;
			}
		}
	}
}

