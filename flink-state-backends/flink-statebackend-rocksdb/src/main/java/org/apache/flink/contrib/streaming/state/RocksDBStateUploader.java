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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.CheckedSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * Help class for uploading RocksDB state files.
 */
public class RocksDBStateUploader extends RocksDBStateDataTransfer {
	private static final int READ_BUFFER_SIZE = 16 * 1024;
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateUploader.class);

	public RocksDBStateUploader(int numberOfSnapshottingThreads) {
		super(numberOfSnapshottingThreads);
	}

	/**
	 * Upload all the files to checkpoint fileSystem using specified number of threads.
	 *
	 * @param files The files will be uploaded to checkpoint filesystem.
	 * @param checkpointStreamFactory The checkpoint streamFactory used to create outputstream.
	 *
	 * @throws Exception Thrown if can not upload all the files.
	 */
	public Map<StateHandleID, StreamStateHandle> uploadFilesToCheckpointFs(
		long checkpointId,
		SnapshotDirectory localDirectory,
		@Nonnull Map<StateHandleID, Path> files,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) throws Exception {

		LOG.info("Come in uploadFilesToCheckpointFs for {} thread {} directory {}.",
			checkpointId,
			Thread.currentThread(),
			localDirectory);
		Map<StateHandleID, StreamStateHandle> handles = new HashMap<>();

		Map<StateHandleID, CompletableFuture<StreamStateHandle>> futures =
			createUploadFutures(checkpointId, localDirectory, files, checkpointStreamFactory, closeableRegistry);

		LOG.info("Start to uploadFilesToCheckpointFs for {} thread {} directory {}.",
			checkpointId,
			Thread.currentThread(),
			localDirectory);
		try {
			FutureUtils.waitForAll(futures.values()).get();

			for (Map.Entry<StateHandleID, CompletableFuture<StreamStateHandle>> entry : futures.entrySet()) {
				handles.put(entry.getKey(), entry.getValue().get());
			}
		} catch (ExecutionException e) {
			LOG.info("Exception to uploadFilesToCheckpointFs for {} thread {} directory {}.",
				checkpointId,
				Thread.currentThread(),
				localDirectory,
				e);
			Throwable throwable = ExceptionUtils.stripExecutionException(e);
			throwable = ExceptionUtils.stripException(throwable, RuntimeException.class);
			if (throwable instanceof IOException) {
				throw (IOException) throwable;
			} else {
				throw new FlinkRuntimeException("Failed to download data for state handles.", e);
			}
		}

		LOG.info("Complete to uploadFilesToCheckpointFs for {} thread {} directory {}.",
			checkpointId,
			Thread.currentThread(),
			localDirectory);

		return handles;
	}

	private Map<StateHandleID, CompletableFuture<StreamStateHandle>> createUploadFutures(
		long checkpointId,
		SnapshotDirectory localDirectory,
		Map<StateHandleID, Path> files,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) {
		Map<StateHandleID, CompletableFuture<StreamStateHandle>> futures = new HashMap<>(files.size());

		LOG.info("Before createUploadFutures for {} thread {} directory {}.",
			checkpointId,
			Thread.currentThread(),
			localDirectory);
		for (Map.Entry<StateHandleID, Path> entry : files.entrySet()) {
			final Supplier<StreamStateHandle> supplier =
				CheckedSupplier.unchecked(() -> uploadLocalFileToCheckpointFs(entry.getValue(), checkpointStreamFactory, closeableRegistry));
			futures.put(entry.getKey(), CompletableFuture.supplyAsync(supplier, executorService));
			LOG.info("Create one future for {} thread {} path {} directory {}.",
				checkpointId,
				Thread.currentThread(),
				entry.getValue(),
				localDirectory);
		}
		LOG.info("After createUploadFutures for {} thread {} directory {}.",
			checkpointId,
			Thread.currentThread(),
			localDirectory);

		return futures;
	}

	private StreamStateHandle uploadLocalFileToCheckpointFs(
		Path filePath,
		CheckpointStreamFactory checkpointStreamFactory,
		CloseableRegistry closeableRegistry) throws IOException {
		FSDataInputStream inputStream = null;
		CheckpointStreamFactory.CheckpointStateOutputStream outputStream = null;

		try {
			final byte[] buffer = new byte[READ_BUFFER_SIZE];

			FileSystem backupFileSystem = filePath.getFileSystem();
			inputStream = backupFileSystem.open(filePath);
			closeableRegistry.registerCloseable(inputStream);

			outputStream = checkpointStreamFactory
				.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);
			closeableRegistry.registerCloseable(outputStream);

			while (true) {
				int numBytes = inputStream.read(buffer);

				if (numBytes == -1) {
					break;
				}

				outputStream.write(buffer, 0, numBytes);
			}

			StreamStateHandle result = null;
			if (closeableRegistry.unregisterCloseable(outputStream)) {
				result = outputStream.closeAndGetHandle();
				outputStream = null;
			}
			return result;

		} finally {

			if (closeableRegistry.unregisterCloseable(inputStream)) {
				IOUtils.closeQuietly(inputStream);
			}

			if (closeableRegistry.unregisterCloseable(outputStream)) {
				IOUtils.closeQuietly(outputStream);
			}
		}
	}
}

