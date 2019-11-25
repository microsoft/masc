/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.accumulo.spark.util;

/**
 * Stop watch functionality optimized for large number of segments.
 */
public class StopWatch {
	private long start;
	private double avg;
	private long n = 1;

	/**
	 * Start recording a new segment. If stop() is not called subsequently this
	 * cancels the previous run.
	 */
	public void start() {
		this.start = System.nanoTime();
	}

	/**
	 * Stops the current run.
	 */
	public void stop() {
		double time = System.nanoTime() - this.start;

		// see //
		// https://stackoverflow.com/questions/1930454/what-is-a-good-solution-for-calculating-an-average-where-the-sum-of-all-values-e
		this.avg += (time - this.avg) / n;

		// important that we only count here as callers might repeatly call start()
		// as a run was cancelled
		this.n++;
	}

	/**
	 * @return average number of milliseconds.
	 */
	public double getAverage() {
		return this.avg / 1000;
	}

	/**
	 * @return Returns the number segments (=stop() calls).
	 */
	public long getN() {
		return this.n;
	}
}
