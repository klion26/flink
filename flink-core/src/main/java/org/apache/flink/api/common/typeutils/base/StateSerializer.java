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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * StateSerializer.
 * @param <T>
 */
public abstract class StateSerializer<T> extends TypeSerializer<T> {
	protected byte[] keyAndKeyGroupBytes;

	protected SerializedCompositeKeyBuilder keyBuilder;

	public StateSerializer() {
	}

	public void setKeyBuilder(SerializedCompositeKeyBuilder keyBuilder) {
		this.keyBuilder = keyBuilder;
	}

	public void updateKeyAndGroup(byte[] keyAndKeyGroupBytes) {
		this.keyAndKeyGroupBytes = keyAndKeyGroupBytes;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean canEqual(Object obj) {
		return (obj != null && obj.getClass() == getClass());
	}
}

