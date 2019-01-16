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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListStateSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy.STOP_ON_FIRST_INCLUDED;

public class StateSnapshotTransformerWrapper<T> implements StateSnapshotTransformer<byte[]> {
	private final StateSnapshotTransformer<T> elementTransformer;
	private final ListStateSerializer<T> stateSerializer;
	private final DataOutputSerializer out = new DataOutputSerializer(128);
	private final CollectionStateSnapshotTransformer.TransformStrategy transformStrategy;

	StateSnapshotTransformerWrapper(StateSnapshotTransformer<T> elementTransformer, ListStateSerializer<T> stateSerializer) {
		this.elementTransformer = elementTransformer;
		this.stateSerializer = stateSerializer;
		this.transformStrategy = elementTransformer instanceof CollectionStateSnapshotTransformer ?
			((CollectionStateSnapshotTransformer) elementTransformer).getFilterStrategy() :
			CollectionStateSnapshotTransformer.TransformStrategy.TRANSFORM_ALL;
	}

	@Override
	@Nullable
	public byte[] filterOrTransform(@Nullable byte[] value) {
		if (value == null) {
			return null;
		}
		List<T> result = new ArrayList<>();
		DataInputDeserializer in = new DataInputDeserializer(value);
		T next;
		int prevPosition = 0;
		while ((next = stateSerializer.deserializeNextElement(in)) != null) {
			T transformedElement = elementTransformer.filterOrTransform(next);
			if (transformedElement != null) {
				if (transformStrategy == STOP_ON_FIRST_INCLUDED) {
					return Arrays.copyOfRange(value, prevPosition, value.length);
				} else {
					result.add(transformedElement);
				}
			}
			prevPosition = in.getPosition();
		}
		try {
			return result.isEmpty() ? null : stateSerializer.serializeValue(result);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to serialize transformed list", e);
		}
	}

	byte[] serializeValueList(
		List<T> valueList,
		TypeSerializer<T> elementSerializer,
		byte delimiter) throws IOException {

		out.clear();
		boolean first = true;

		for (T value : valueList) {
			Preconditions.checkNotNull(value, "You cannot add null to a value list.");

			if (first) {
				first = false;
			} else {
				out.write(delimiter);
			}
			elementSerializer.serialize(value, out);
		}

		return out.getCopyOfBuffer();
	}
}

