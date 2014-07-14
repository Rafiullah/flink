/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.examples.join;

import org.apache.log4j.Level;

import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.streaming.util.LogUtils;

public class JoinLocal {

	public static JobGraph getJobGraph() {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph", FaultToleranceType.NONE);
		graphBuilder.setSource("JoinSourceOne", JoinSourceOne.class);
		graphBuilder.setSource("JoinSourceTwo", JoinSourceTwo.class);
		graphBuilder.setTask("JoinTask", JoinTask.class, 1, 1);
		graphBuilder.setSink("JoinSink", JoinSink.class);

		graphBuilder.fieldsConnect("JoinSourceOne", "JoinTask", 1);
		graphBuilder.fieldsConnect("JoinSourceTwo", "JoinTask", 1);
		graphBuilder.shuffleConnect("JoinTask", "JoinSink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {

		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);
		ClusterUtil.runOnMiniCluster(getJobGraph());

	}
}