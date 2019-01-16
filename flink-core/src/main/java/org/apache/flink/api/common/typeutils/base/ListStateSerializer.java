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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * ListStateSerializer.
 * @param <T>
 */
public class ListStateSerializer<T> extends StateSerializer<List<T>> {

	private static final long serialVersionUID = 1119562170939152304L;

	/** The serializer for the elements of the list. */
	private final TypeSerializer<T> elementSerializer;

	/**
	 * Separator of StringAppendTestOperator in ListState.
	 */
	private static final byte DELIMITER = ',';

	/**
	 * Length of the separator.
	 */
	private static final int DELEMITER_LENGTH = 1;

	private ByteArrayOutputStreamWithPos outputStreamWithPos = new ByteArrayOutputStreamWithPos();
	private DataOutputView outputView = new DataOutputViewStreamWrapper(outputStreamWithPos);

	public <K> ListStateSerializer(TypeSerializer<T> elementSerializer) {
		this.elementSerializer = checkNotNull(elementSerializer);
	}

	/**
	 * Gets the serializer for the elements of the list.
	 * @return The serializer for the elements of the list
	 */
	public TypeSerializer<T> getElementSerializer() {
		return elementSerializer;
	}

	@Override
	public TypeSerializer<List<T>> duplicate() {
		TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
		return duplicateElement == elementSerializer ? this : new ListStateSerializer<>(duplicateElement);
	}

	@Override
	public List<T> createInstance() {
		return new ArrayList<>(0);
	}

	@Override
	public List<T> copy(List<T> from) {
		List<T> newList = new ArrayList<>(from.size());

		// We iterate here rather than accessing by index, because we cannot be sure that
		// the given list supports RandomAccess.
		// The Iterator should be stack allocated on new JVMs (due to escape analysis)
		for (T element : from) {
			newList.add(elementSerializer.copy(element));
		}
		return newList;
	}

	@Override
	public List<T> copy(List<T> from, List<T> reuse) {
		return copy(from);
	}

	@Override
	public void serialize(List<T> valueList, DataOutputView target) throws IOException {
		serializeKeyAndNamespace(target);

		target.write(serializeValue(valueList));
	}

	private void serializeKeyAndNamespace(DataOutputView target) throws IOException {
		target.writeInt(keyAndKeyGroupBytes.length);
		target.write(keyAndKeyGroupBytes);
	}

	public byte[] serializeValue(List<T> valueList) throws IOException {
		boolean first = true;
		outputStreamWithPos.reset();
		outputView.writeInt(valueList.size());
		for (T value : valueList) {
			checkNotNull(value, "You cannot add null to a value list.");

			if (first) {
				first = false;
			} else {
				outputView.write(DELIMITER);
			}
			elementSerializer.serialize(value, outputView);
		}
		return outputStreamWithPos.getBuf();
	}

	@Override
	public List<T> deserialize(DataInputView source) throws IOException {

		// groupId's bytes length may be var.
		source.skipBytesToRead(2);

		int keyLenght = source.readInt();
		source.skipBytesToRead(keyLenght);
		int namespaceLength = source.readInt();
		source.skipBytesToRead(namespaceLength);
		return deserializeValue(source);
	}

	public List<T> deserializeValue(DataInputView source) throws IOException {
		final int size = source.readInt();
		List<T> result = new ArrayList<>(size);
		T next = elementSerializer.deserialize(source);
		result.add(next);

		if (size > 1) {
			for (int i = 1; i < size; ++i) {
				source.skipBytesToRead(DELEMITER_LENGTH);
				next = elementSerializer.deserialize(source);
				result.add(next);
			}
		}

		return result;
	}

	public T deserializeNextElement(DataInputDeserializer in) {
		try {
			if (in.available() > 0) {
				T element = elementSerializer.deserialize(in);
				if (in.available() > 0) {
					in.readByte();
				}
				return element;
			}
		} catch (IOException e) {
			throw new FlinkRuntimeException("Unexpected list element deserialization failure");
		}
		return null;
	}

	@Override
	public List<T> deserialize(List<T> reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		// copy number of elements
		final int num = source.readInt();
		target.writeInt(num);
		for (int i = 0; i < num; i++) {
			elementSerializer.copy(source, target);
		}
	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
			(obj != null && obj.getClass() == getClass() &&
				elementSerializer.equals(((ListStateSerializer<?>) obj).elementSerializer));
	}

	@Override
	public int hashCode() {
		return elementSerializer.hashCode();
	}

	@Override
	public TypeSerializerSnapshot<List<T>> snapshotConfiguration() {
		return new ListStateSerializerSnapshot<>(this);
	}
}

