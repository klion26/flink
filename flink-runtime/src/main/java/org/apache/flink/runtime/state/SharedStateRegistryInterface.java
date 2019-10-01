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

/**
 * Registry which used to track shared state handle usages, if the state is in the
 * registry, that means this handle is still being used in some checkpoint, and when
 * the handle is no longer in the registry, we need to delete the resource used by the handle.
 */
public interface SharedStateRegistryInterface extends AutoCloseable {

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
	 */
	Result registerReference(SharedStateRegistryKey registrationKey, StreamStateHandle state);

	/**
	 * Releases one reference to the given shared state in the registry. This decreases the
	 * reference count by one. Once the count reaches zero, the shared state is deleted.
	 */
	Result unregisterReference(SharedStateRegistryKey registrationKey);

	/**
	 * Register all the given shared states in the registry.
	 */
	void registerAll(Iterable<? extends CompositeStateHandle> stateHandles);

	/**
	 * This function has to be called after all the handles have been registered.
	 * used to clean up the useless resource used in checkpoint.
	 */
	void cleanUpAfterEveryCheckpoint();
}

