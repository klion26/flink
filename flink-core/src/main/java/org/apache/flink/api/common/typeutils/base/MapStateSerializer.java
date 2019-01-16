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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * MapStateSerializer.
 * should call setKeyBuilder() before all operations.
 * @param <MK>
 * @param <MV>
 */
public class MapStateSerializer<MK, MV, N> extends StateSerializer<Map<MK, MV>> {
	private static final long serialVersionUID = -6885593032367050078L;

	/** The serializer for the keys in the map. */
	private final TypeSerializer<MK> keySerializer;

	/** The serializer for the values in the map. */
	private final TypeSerializer<MV> valueSerializer;

	/** The serializer for the namespace. */
	private TypeSerializer<N> namespaceSerializer;

	private N currentNamespace;

	private ByteArrayOutputStreamWithPos outputStreamWithPos = new ByteArrayOutputStreamWithPos();
	private DataOutputView outputView = new DataOutputViewStreamWrapper(outputStreamWithPos);

	private ByteArrayOutputStreamWithPos valueOutputStreamWithPos = new ByteArrayOutputStreamWithPos();
	private DataOutputView valueOutputView = new DataOutputViewStreamWrapper(valueOutputStreamWithPos);

	public MapStateSerializer(
		TypeSerializer<MK> keySerializer,
		TypeSerializer<MV> valueSerializer,
		TypeSerializer<N> namespaceSerializer) {

		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.namespaceSerializer = namespaceSerializer;
	}

	// ------------------------------------------------------------------------
	//  MapSerializer specific properties
	// ------------------------------------------------------------------------

	public TypeSerializer<MK> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializer<MV> getValueSerializer() {
		return valueSerializer;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	// ------------------------------------------------------------------------
	//  Type Serializer implementation
	// ------------------------------------------------------------------------

	@Override
	public TypeSerializer<Map<MK, MV>> duplicate() {
		TypeSerializer<MK> duplicateKeySerializer = keySerializer.duplicate();
		TypeSerializer<MV> duplicateValueSerializer = valueSerializer.duplicate();
		TypeSerializer<N> duplicatedNamesapceSerializer = namespaceSerializer.duplicate();

		return (duplicateKeySerializer == keySerializer) && (duplicateValueSerializer == valueSerializer)
			? this
			: new MapStateSerializer<>(duplicateKeySerializer, duplicateValueSerializer, duplicatedNamesapceSerializer);
	}

	@Override
	public Map<MK, MV> createInstance() {
		return new HashMap<>();
	}

	@Override
	public Map<MK, MV> copy(Map<MK, MV> from) {
		Map<MK, MV> newMap = new HashMap<>(from.size());

		for (Map.Entry<MK, MV> entry : from.entrySet()) {
			MK newKey = keySerializer.copy(entry.getKey());
			MV newValue = entry.getValue() == null ? null : valueSerializer.copy(entry.getValue());

			newMap.put(newKey, newValue);
		}

		return newMap;
	}

	@Override
	public Map<MK, MV> copy(Map<MK, MV> from, Map<MK, MV> reuse) {
		return copy(from);
	}

	@Override
	public void serialize(Map<MK, MV> map, DataOutputView target) throws IOException {
		final int size = map.size();
		target.writeInt(size);

		// write namespace once, and reuse it through all the map.
		for (Map.Entry<MK, MV> entry : map.entrySet()) {
			byte[] compositeKey = keyBuilder.buildCompositeKeyNamesSpaceUserKey(currentNamespace, namespaceSerializer, entry.getKey(), keySerializer);
			byte[] rawValue = serializeRawValue(entry.getValue());
			target.write(compositeKey);
			target.write(rawValue);
		}
	}

	public byte[] serializeCurrentKeyWithGroupAndNamespace(N namespace) throws IOException {
		return keyBuilder.buildCompositeKeyNamespace(namespace, namespaceSerializer);
	}

	public byte[] serializeCurrentKeyWithGroupAndNamespacePlusUserKey(MK userKey, N namespace) throws IOException {
		return keyBuilder.buildCompositeKeyNamesSpaceUserKey(namespace, namespaceSerializer, userKey, keySerializer);
	}

	public byte[] serializeRawKey(MK key) throws IOException {
		keySerializer.serialize(key, outputView);

		return outputStreamWithPos.getBuf();
	}

	public byte[] serializeRawValue(MV value) throws IOException {
		valueOutputStreamWithPos.reset();
		if (value == null) {
			valueOutputView.writeBoolean(true);
		} else {
			valueOutputView.writeBoolean(false);
			valueSerializer.serialize(value, valueOutputView);
		}
		return valueOutputStreamWithPos.getBuf();
	}

	public void setCurrentNamespace(N currentNamespace) {
		this.currentNamespace = currentNamespace;
	}

	@Override
	public Map<MK, MV> deserialize(DataInputView source) throws IOException {
		final int size = source.readInt();
		Map<MK, MV> result = new HashMap<>(size);

		for (int i = 0; i < size; ++i) {
			// skip groupid, key
			int keyAndGroupLength = source.readInt();
			source.skipBytesToRead(keyAndGroupLength);

			// skip namespace
			int namespaceBytesLength = source.readInt();
			source.skipBytesToRead(namespaceBytesLength);

			MK key = keySerializer.deserialize(source);

			boolean isNull = source.readBoolean();
			MV value = isNull ? null : valueSerializer.deserialize(source);
			result.put(key, value);
		}
		return result;
	}

	@Override
	public Map<MK, MV> deserialize(Map<MK, MV> reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {

	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
			(obj != null && obj.getClass() == getClass() &&
				keySerializer.equals(((MapStateSerializer<?, ?, ?>) obj).getKeySerializer()) &&
				valueSerializer.equals(((MapStateSerializer<?, ?, ?>) obj).getValueSerializer()) &&
				namespaceSerializer.equals(((MapStateSerializer<?, ?, ?>) obj).getNamespaceSerializer()));
	}

	@Override
	public int hashCode() {
		return keySerializer.hashCode() * 31 + valueSerializer.hashCode();
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting
	// --------------------------------------------------------------------------------------------

	@Override
	public TypeSerializerSnapshot<Map<MK, MV>> snapshotConfiguration() {
		return new MapStateSerializerSnapshot<>(this);
	}
}

