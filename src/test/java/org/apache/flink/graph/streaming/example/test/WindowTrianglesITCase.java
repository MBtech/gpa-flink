/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.streaming.example.test;

import org.apache.flink.graph.streaming.example.WindowTriangles;
import org.apache.flink.graph.streaming.util.ExamplesTestData;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;

public class WindowTrianglesITCase extends AbstractTestBase {
	
	protected String textPath;
	protected String resultPath;

//	@Override
//	protected void preSubmit() throws Exception {
//		setParallelism(1); //needed to ensure total ordering for windows
//
//	}
//
//	@Override
//	protected void postSubmit() throws Exception {
//	}

	@Test
	public void testProgram() throws Exception {
		textPath = createTempFile("edges.txt", ExamplesTestData.TRIANGLES_DATA);
		resultPath = getTempDirPath("result");

		WindowTriangles.main(new String[]{textPath, resultPath, "400"});

		compareResultsByLinesInMemory(ExamplesTestData.TRIANGLES_RESULT, resultPath);

	}
}