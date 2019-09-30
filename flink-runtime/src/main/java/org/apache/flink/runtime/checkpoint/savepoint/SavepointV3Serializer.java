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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.flink.runtime.checkpoint.savepoint.SavepointSerializerUtil.deserializeSavepoint;
import static org.apache.flink.runtime.checkpoint.savepoint.SavepointSerializerUtil.serializeSavepoint;

/**
 * (De)serializer for checkpoint metadata format version 3.
 *
 * <p>This format version adds
 *
 * <p>Basic checkpoint metadata layout:
 * <pre>
 *  +--------------+---------------+-----------------+
 *  | checkpointID | master states | operator states |
 *  +--------------+---------------+-----------------+
 *
 *  Master state:
 *  +--------------+---------------------+---------+------+---------------+
 *  | magic number | num remaining bytes | version | name | payload bytes |
 *  +--------------+---------------------+---------+------+---------------+
 * </pre>
 */
@Internal
@VisibleForTesting
public class SavepointV3Serializer implements SavepointSerializer<SavepointV3>  {

	/** The singleton instance of the serializer */
	public static final SavepointV3Serializer INSTANCE = new SavepointV3Serializer();

	// ------------------------------------------------------------------------

	/** Singleton, not meant to be instantiated */
	private SavepointV3Serializer() {}

	// ------------------------------------------------------------------------
	//  (De)serialization entry points
	// ------------------------------------------------------------------------

	@Override
	public void serialize(SavepointV3 checkpointMetadata, DataOutputStream dos) throws IOException {
		serializeSavepoint(
			checkpointMetadata.getCheckpointId(),
			checkpointMetadata.getMasterStates(),
			checkpointMetadata.getOperatorStates(),
			dos);
	}

	@Override
	public SavepointV3 deserialize(DataInputStream dis, ClassLoader cl) throws IOException {
		return (SavepointV3) deserializeSavepoint(dis, cl, SavepointV3.VERSION);
	}
}

