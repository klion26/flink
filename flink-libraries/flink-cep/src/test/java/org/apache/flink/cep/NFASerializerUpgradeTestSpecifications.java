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

package org.apache.flink.cep;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.cep.nfa.ComputationState;
import org.apache.flink.cep.nfa.DeweyNumber;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferEdge;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferNode;

import org.hamcrest.Matcher;

import java.util.PriorityQueue;
import java.util.Queue;

import static org.apache.flink.cep.nfa.NFAState.COMPUTATION_STATE_COMPARATOR;
import static org.hamcrest.Matchers.is;

/**
 * NFASerializerUpgradeTestSpecifications.
 */
public class NFASerializerUpgradeTestSpecifications {
	// ----------------------------------------------------------------------------------------------
	//  Specification for "event-id-serializer"
	// ----------------------------------------------------------------------------------------------
	/**
	 * EventIdSerializerSetup.
	 */
	public static final class EventIdSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<EventId> {
		@Override
		public TypeSerializer<EventId> createPriorSerializer() {
			return EventId.EventIdSerializer.INSTANCE;
		}

		@Override
		public EventId createTestData() {
			return new EventId(1, 1512346789);
		}
	}

	/**
	 * EventIdSerializerVerifier.
	 */
	public static final class EventIdSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<EventId> {
		@Override
		public TypeSerializer<EventId> createUpgradedSerializer() {
			return EventId.EventIdSerializer.INSTANCE;
		}

		@Override
		public Matcher<EventId> testDataMatcher() {
			return is(new EventId(1, 1512346789));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<EventId>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
	// ----------------------------------------------------------------------------------------------
	//  Specification for "node-id-serializer"
	// ----------------------------------------------------------------------------------------------
	/**
	 * NodeIdSerializerSetup.
	 */
	public static final class NodeIdSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<NodeId> {
		@Override
		public TypeSerializer<NodeId> createPriorSerializer() {
			return new NodeId.NodeIdSerializer();
		}

		@Override
		public NodeId createTestData() {
			return new NodeId(new EventId(2, 1512345678), "flink");
		}
	}

	/**
	 * NodeIdSerializerVerifier.
	 */
	public static final class NodeIdSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<NodeId> {
		@Override
		public TypeSerializer<NodeId> createUpgradedSerializer() {
			return new NodeId.NodeIdSerializer();
		}

		@Override
		public Matcher<NodeId> testDataMatcher() {
			return is(new NodeId(new EventId(2, 1512345678), "flink"));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<NodeId>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
	// ----------------------------------------------------------------------------------------------
	//  Specification for "dewey-number-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * DeweyNumberSerializerSetup.
	 */
	public static final class DeweyNumberSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<DeweyNumber> {
		@Override
		public TypeSerializer<DeweyNumber> createPriorSerializer() {
			return DeweyNumber.DeweyNumberSerializer.INSTANCE;
		}

		@Override
		public DeweyNumber createTestData() {
			return new DeweyNumber(124).addStage();
		}
	}

	/**
	 * DeweyNumberSerializerVerifier.
	 */
	public static final class DeweyNumberSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<DeweyNumber> {
		@Override
		public TypeSerializer<DeweyNumber> createUpgradedSerializer() {
			return DeweyNumber.DeweyNumberSerializer.INSTANCE;
		}

		@Override
		public Matcher<DeweyNumber> testDataMatcher() {
			return is(new DeweyNumber(124).addStage());
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<DeweyNumber>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
	// ----------------------------------------------------------------------------------------------
	//  Specification for "shared-buffer-edge-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * SharedBufferEdgeSerializerSetup.
	 */
	public static final class SharedBufferEdgeSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<SharedBufferEdge> {
		@Override
		public TypeSerializer<SharedBufferEdge> createPriorSerializer() {
			return new SharedBufferEdge.SharedBufferEdgeSerializer();
		}

		@Override
		public SharedBufferEdge createTestData() {
			return new SharedBufferEdge(new NodeId(new EventId(1, 23456789), "shared"), new DeweyNumber(234));
		}
	}

	/**
	 * SharedBufferEdgeSerializerVerifier.
	 */
	public static final class SharedBufferEdgeSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<SharedBufferEdge> {
		@Override
		public TypeSerializer<SharedBufferEdge> createUpgradedSerializer() {
			return new SharedBufferEdge.SharedBufferEdgeSerializer();
		}

		@Override
		public Matcher<SharedBufferEdge> testDataMatcher() {
			return is(new SharedBufferEdge(new NodeId(new EventId(1, 23456789), "shared"), new DeweyNumber(234)));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<SharedBufferEdge>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "shared-buffer-node-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * SharedBufferNodeSerializerSetup.
	 */
	public static final class SharedBufferNodeSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<SharedBufferNode> {
		@Override
		public TypeSerializer<SharedBufferNode> createPriorSerializer() {
			return new SharedBufferNode.SharedBufferNodeSerializer();
		}

		@Override
		public SharedBufferNode createTestData() {
			return new SharedBufferNode();
		}
	}

	/**
	 * SharedBufferNodeSerializerVerifier.
	 */
	public static final class SharedBufferNodeSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<SharedBufferNode> {
		@Override
		public TypeSerializer<SharedBufferNode> createUpgradedSerializer() {
			return new SharedBufferNode.SharedBufferNodeSerializer();
		}

		@Override
		public Matcher<SharedBufferNode> testDataMatcher() {
			return is(new SharedBufferNode());
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<SharedBufferNode>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "nfa-state-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * NFAStateSerializerSetup.
	 */
	public static final class NFAStateSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<NFAState> {
		@Override
		public TypeSerializer<NFAState> createPriorSerializer() {
			return new NFAStateSerializer();
		}

		@Override
		public NFAState createTestData() {
			Queue<ComputationState> partialMatches = new PriorityQueue<>(COMPUTATION_STATE_COMPARATOR);
			partialMatches.add(ComputationState.createStartState("partial", new DeweyNumber(123)));
			Queue<ComputationState> completedMatches = new PriorityQueue<>(COMPUTATION_STATE_COMPARATOR);
			completedMatches.add(ComputationState.createStartState("complete", new DeweyNumber(456)));
			return new NFAState(partialMatches, completedMatches);
		}
	}

	/**
	 * NFAStateSerializerVerifier.
	 */
	public static final class NFAStateSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<NFAState> {
		@Override
		public TypeSerializer<NFAState> createUpgradedSerializer() {
			return new NFAStateSerializer();
		}

		@Override
		public Matcher<NFAState> testDataMatcher() {
			Queue<ComputationState> partialMatches = new PriorityQueue<>(COMPUTATION_STATE_COMPARATOR);
			partialMatches.add(ComputationState.createStartState("partial", new DeweyNumber(123)));
			Queue<ComputationState> completedMatches = new PriorityQueue<>(COMPUTATION_STATE_COMPARATOR);
			completedMatches.add(ComputationState.createStartState("complete", new DeweyNumber(456)));
			return is(new NFAState(partialMatches, completedMatches));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<NFAState>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
}

