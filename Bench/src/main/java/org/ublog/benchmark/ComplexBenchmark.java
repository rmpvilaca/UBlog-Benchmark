/*******************************************************************************
 * Copyright 2010 Universidade do Minho, Ricardo Vilaça and Francisco Cruz
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ublog.benchmark;

import org.ublog.benchmark.BenchMark;
import org.ublog.benchmark.BenchMarkClient;
import org.ublog.benchmark.StatsCollector;

import org.ublog.utils.Clock;

public abstract class ComplexBenchmark extends BenchMark {

	private int clientCount;

	public ComplexBenchmark(StatsCollector statsCollector, int usernameStarter) {
		super(statsCollector);
		this.clientCount = usernameStarter;
	}

	@Override
	public BenchMarkClient createNewClient(Clock clock) {
		return this.createNewClient(clock, this.clientCount++);
	}

	public abstract ComplexBenchMarkClient createNewClient(Clock clock,
			int clientCount);

}
