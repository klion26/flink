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
 * An entry in the registry, tracking the handle and the corresponding reference count.
 */
public class SharedStateEntry {
	/** The shared state handle */
	private final StreamStateHandle stateHandle;

	/** The current reference count of the state handle */
	private int referenceCount;

	SharedStateEntry(StreamStateHandle value) {
		this.stateHandle = value;
		this.referenceCount = 1;
	}

	StreamStateHandle getStateHandle() {
		return stateHandle;
	}

	int getReferenceCount() {
		return referenceCount;
	}

	void increaseReferenceCount() {
		++referenceCount;
	}

	void decreaseReferenceCount() {
		--referenceCount;
	}

	@Override
	public String toString() {
		return "SharedStateEntry{" +
			"stateHandle=" + stateHandle +
			", referenceCount=" + referenceCount +
			'}';
	}
}

