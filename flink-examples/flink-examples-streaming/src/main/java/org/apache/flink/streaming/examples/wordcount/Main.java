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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Main.
 */
public class Main {
	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new RocksDBStateBackend("file:///tmp/blink_tests/", true));
		env.enableCheckpointing(1000);
		CheckpointConfig cpcfg = env.getCheckpointConfig();
		cpcfg.setCheckpointTimeout(30_000); // Default of 10 minutes requires too large checkpoints.
		cpcfg.setFailOnCheckpointingErrors(false);

		DataStream<Integer> triggers = env.addSource(new RichSourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> sourceContext) throws Exception {
				for (int i = 28; i <= 33; i++) {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException ignored) {
					}
					for (int j = 0; j < 8; j++) {
						sourceContext.collect(i - 3);
					}
				}
				System.out.println("No more messages.");
				while (true) {
					try {
						Thread.sleep(60000);
					} catch (InterruptedException ignored) {
					}
				}
			}

			@Override
			public void cancel() {
				System.err.println("No, why!");
			}
		});

		SingleOutputStreamOperator<Tuple0> out = triggers.keyBy(x -> x).process(new KeyedProcessFunction<Integer, Integer, Tuple0>() {
			private transient MapState<Integer, byte[]> bstate;
			@Override
			public void open(Configuration config) {
				MapStateDescriptor<Integer, byte[]> descriptor =
					new MapStateDescriptor<>("bloop",
						TypeInformation.of(new TypeHint<Integer>() {}),
						TypeInformation.of(new TypeHint<byte[]>() {})
					);
				bstate = getRuntimeContext().getMapState(descriptor);
			}

			@Override
			public void processElement(Integer exp, Context context, Collector<Tuple0> collector) throws Exception {
				long size = 1L << exp;
				System.out.println("" + size + " B");
				int chunk = 8192; // Java can't even. jni-RocksDB couldn't either. And my RAM doesn't want to.
				for (int i = 0; size > 0; i++, size -= chunk) {
					byte[] bloo = new byte[chunk];
					new Random().nextBytes(bloo);
					bstate.put(i, bloo);
				}
			}
		});

		out.print();

		// execute program
		env.execute("5 seconds");
	}
}

