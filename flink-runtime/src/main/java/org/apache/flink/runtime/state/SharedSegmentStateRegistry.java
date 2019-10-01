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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.state.filesystem.FsSegmentStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Used for {@link org.apache.flink.runtime.state.filesystem.FsSegmentStateBackend}.
 */
public class SharedSegmentStateRegistry extends SharedStateRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(SharedSegmentStateRegistry.class);

	/** A singleton object for the default implementation of a {@link SharedStateRegistryFactory} */
	public static final SharedStateRegistryFactory SEGMENT_STATE_FACTORY = SharedSegmentStateRegistry::new;

	/**
	 * Used in {@link org.apache.flink.runtime.state.filesystem.FsSegmentStateBackend}.
	 * will track the reference of the underlying file.
	 */
	private final Map<String, Integer> fileRefCounts;

	/** A set used to tracking the underlying files which used by duplicated state hanldes,
	 * we'll try to delete these files after all state handles of one checkpoint have been registered.
	 * Please see comments of {@link #cleanUpAfterEveryCheckpoint()} ()} for more detail.*/
	private Set<String> underlyingFileOfDuplicatedStatehandles;

	/** Default uses direct executor to delete unreferenced state */
	public SharedSegmentStateRegistry() {
		this(Executors.directExecutor());
	}

	public SharedSegmentStateRegistry(Executor asyncDisposalExecutor) {
		super(asyncDisposalExecutor);
		this.fileRefCounts = new HashMap<>();
		this.underlyingFileOfDuplicatedStatehandles = new HashSet<>();
	}

	/**
	 * Register a reference to the given shared state in the registry.
	 * This does the following: We check if the state handle is actually new by the
	 * registrationKey. If it is new, we register it with a reference count of 1. If there is
	 * already a state handle registered under the given key, we dispose the given "new" state
	 * handle, uptick the reference count of the previously existing state handle and return it as
	 * a replacement with the result.
	 *
	 * <p>IMPORTANT: caller should check the state handle returned by the result, because the
	 * registry is performing de-duplication and could potentially return a handle that is supposed
	 * to replace the one from the registration request.
	 *
	 * @param state the shared state for which we register a reference.
	 * @return the result of this registration request, consisting of the state handle that is
	 * registered under the key by the end of the operation and its current reference count.
	 */
	@Override
	public Result registerReference(SharedStateRegistryKey registrationKey, StreamStateHandle state) {

		checkArgument(state instanceof FsSegmentStateHandle, "SharedSegmentStateRegistry only support FsSegmentStateHandle, passed in " + state.getClass().getSimpleName());
		Preconditions.checkNotNull(state);

		StreamStateHandle scheduledStateDeletion = null;
		SharedStateEntry entry;

		synchronized (registeredStates) {

			Preconditions.checkState(open, "Attempt to register state to closed SharedStateRegistry.");

			entry = registeredStates.get(registrationKey);

			if (entry == null) {
				entry = registerUnduplicatedState(registrationKey, state);
			} else {
				scheduledStateDeletion = registerDuplicatedState(registrationKey, state, entry);
			}
			increaseFileRefCountForSegmentStateHandle(entry.getStateHandle());
		}

		scheduleAsyncDelete(scheduledStateDeletion);
		LOG.trace("Registered shared state {} under key {}.", entry, registrationKey);
		return new Result(entry);
	}

	/**
	 * Releases one reference to the given shared state in the registry. This decreases the
	 * reference count by one. Once the count reaches zero, the shared state is deleted.
	 *
	 * @param registrationKey the shared state for which we release a reference.
	 * @return the result of the request, consisting of the reference count after this operation
	 * and the state handle, or null if the state handle was deleted through this request. Returns null if the registry
	 * was previously closed.
	 */
	@Override
	public Result unregisterReference(SharedStateRegistryKey registrationKey) {

		Preconditions.checkNotNull(registrationKey);

		final Result result;
		final StreamStateHandle scheduledStateDeletion;
		SharedStateEntry entry;

		synchronized (registeredStates) {
			Tuple3<Result, StreamStateHandle, SharedStateEntry> tuple = doUnregisterRef(registrationKey);
			result = tuple.f0;
			scheduledStateDeletion = tuple.f1;
			entry = tuple.f2;
			descFileRefCountForSegmentStateHandle(entry.getStateHandle(), true);
		}

		LOG.trace("Unregistered shared state {} under key {}.", entry, registrationKey);
		scheduleAsyncDelete(scheduledStateDeletion);
		return result;
	}

	private void increaseFileRefCountForSegmentStateHandle(StreamStateHandle handle) {
		checkArgument(handle instanceof FsSegmentStateHandle, "SharedSegmentStateRegistry only support FsSegmentStateHandle, passed in " + handle.getClass().getSimpleName());

		String filePath = ((FsSegmentStateHandle) handle).getFilePath().toUri().toString();
		fileRefCounts.put(filePath, fileRefCounts.getOrDefault(filePath, 0) + 1);
	}

	@Override
	protected StreamStateHandle registerDuplicatedState(SharedStateRegistryKey registrationKey, StreamStateHandle state, SharedStateEntry entry) {
		StreamStateHandle scheduledStateDeletion = null;
		// delete if this is a real duplicate
		if (!Objects.equals(state, entry.getStateHandle())) {
			scheduledStateDeletion = state;
			// increase file reference of scheduledStateDeletetion so that
			// we can unify the operation in descFileRefCountForSegmentStateHandle
			increaseFileRefCountForSegmentStateHandle(scheduledStateDeletion);
			descFileRefCountForSegmentStateHandle(scheduledStateDeletion, false);
			LOG.trace("Identified duplicate state registration under key {}. New state {} was determined to " +
					"be an unnecessary copy of existing state {} and will be dropped.",
				registrationKey,
				state,
				entry.getStateHandle());
		}
		entry.increaseReferenceCount();
		return scheduledStateDeletion;
	}

	/**
	 * Decrease the reference count of underlying file used by handle,
	 * if the ref count becomes zero
	 * 	1) if canDeleteDirectly is true, will delete the underlying file directly.
	 * 	2) else will add the underlying file to a set for deletion purpose in the future.
	 */
	private void descFileRefCountForSegmentStateHandle(StreamStateHandle handle, boolean canDeleteDirectly) {
		checkArgument(handle instanceof FsSegmentStateHandle, "SharedSegmentStateRegistry only support FsSegmentStateHandle, passed in " + handle.getClass().getSimpleName());
		String segmentFilePath = ((FsSegmentStateHandle) handle).getFilePath().toUri().toString();

		Integer count = fileRefCounts.get(segmentFilePath);
		Preconditions.checkState(count != null, "file ref count should never be null");
		int newRefCount = fileRefCounts.get(segmentFilePath) - 1;
		if (newRefCount <= 0) {
			fileRefCounts.remove(segmentFilePath);
			if (canDeleteDirectly) {
				deleteQuietly(segmentFilePath);
			} else {
				underlyingFileOfDuplicatedStatehandles.add(segmentFilePath);
			}
		} else {
			fileRefCounts.put(segmentFilePath, newRefCount);
		}
	}

	@VisibleForTesting
	public Map<String, Integer> getFileRefCounts() {
		return fileRefCounts;
	}

	private void deleteQuietly(String path) {
		Path path2Del = new Path(path);
		try {
			path2Del.getFileSystem().delete(path2Del, false);
		} catch (IOException e) {
			LOG.error("Can not delete underlying checkpoint file {}.", path);
		}
	}

	@Override
	public String toString() {
		synchronized (registeredStates) {
			return "SharedStateRegistry{" +
				"registeredStates=" + registeredStates +
				"fileRefCounts=" + fileRefCounts +
				'}';
		}
	}

	// this function should be called after the state handles of one checkpoint have been all registered.
	// this help function wants to solve the following problem:
	// max concurrent checkpoint = 2
	// checkpoint 1 includes 1.sst, 2.sst, 3.sst
	// checkpoint 2 includes 2.sst, 3.sst, 4.sst
	// checkpoint 3 includes 4.sst
	// checkpoint 2 and checkpoint 3 are both based on checkpoint 1
	// so we'll register 4.sst twice with different state handle(checkpoint 2 and checkpoint 3)
	// when register 4.sst in checkpoint 3, we can't directly delete the underlying file,
	// because we don't know if there exist any more state handle in checkpoint 3 will use
	// the same underlying file(maybe 5.sst).
	@Override
	public void cleanUpAfterEveryCheckpoint() {
		final Set<String> allInUsePaths = fileRefCounts.keySet();
		for (String path : underlyingFileOfDuplicatedStatehandles) {
			if (!allInUsePaths.contains(path)) {
				deleteQuietly(path);
			}
		}
		underlyingFileOfDuplicatedStatehandles.clear();
	}
}

