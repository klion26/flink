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

package org.apache.flink.client.cli;

import org.apache.commons.cli.CommandLine;

import static org.apache.flink.client.cli.CliFrontendParser.CHECKPOINT;

/**
 * Command line options for the CHECKPOINT command.
 */
public class CheckpointOptions extends CommandLineOptions {
	private String checkpointPath;

	public CheckpointOptions(CommandLine line) {
		super(line);
		if (!line.hasOption(CHECKPOINT.getOpt())) {
			throw new IllegalStateException("Did not set checkpoint path.");
		}

		this.checkpointPath = line.getOptionValue(CHECKPOINT.getOpt());
	}

	public String getCheckpointPath() {
		return checkpointPath;
	}
}

