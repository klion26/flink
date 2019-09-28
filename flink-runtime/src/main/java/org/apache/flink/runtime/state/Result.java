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

import org.apache.flink.util.Preconditions;

/**
 * The result of an attempt to (un)/reference state.
 */
public class Result {

	/** The (un)registered state handle from the request */
	private final StreamStateHandle reference;

	/** The reference count to the state handle after the request to (un)register */
	private final int referenceCount;

	public Result(SharedStateEntry sharedStateEntry) {
		this.reference = sharedStateEntry.getStateHandle();
		this.referenceCount = sharedStateEntry.getReferenceCount();
	}

	public Result(StreamStateHandle reference, int referenceCount) {
		Preconditions.checkArgument(referenceCount >= 0);

		this.reference = reference;
		this.referenceCount = referenceCount;
	}

	public StreamStateHandle getReference() {
		return reference;
	}

	public int getReferenceCount() {
		return referenceCount;
	}

	@Override
	public String toString() {
		return "Result{" +
			"reference=" + reference +
			", referenceCount=" + referenceCount +
			'}';
	}
}

