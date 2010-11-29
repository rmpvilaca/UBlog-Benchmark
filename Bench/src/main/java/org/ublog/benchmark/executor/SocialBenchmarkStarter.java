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
package org.ublog.benchmark.executor;

import java.util.Random;

import org.apache.commons.configuration.ConfigurationException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.ublog.benchmark.BenchMark;
import org.ublog.benchmark.DataStore;
import org.ublog.benchmark.StatsCollector;
import org.ublog.benchmark.social.SocialBenchmark;
import org.ublog.benchmark.social.SocialOperationsFactory;
import org.ublog.utils.Clock;

public class SocialBenchmarkStarter {

	private Logger logger = Logger.getLogger(SocialBenchmarkStarter.class);

	public SocialBenchmarkStarter(SocialOperationsFactory opFactory,
			DataStore store, int size, int sizeTotal, int usernameStarter,
			int nOperations, double thinkTime) {
		System.out.println("Starting Twitterbenchmarktarter with type:"
				+ opFactory.getClass().getName() + ";size:" + size
				+ ";nOperations:" + nOperations + ";thinkTime:" + thinkTime);

		StatsCollector stats = new StatsCollector("stats."
				+ new Random(System.nanoTime()) + "txt");
		try {
			if(logger.isInfoEnabled())
			{
				logger.info("Loading properties file: ublog.properties");
			}
			Configuration config = new PropertiesConfiguration("ublog.properties");
			BenchMark bench = new SocialBenchmark(stats, opFactory, sizeTotal,
					size, usernameStarter, nOperations, thinkTime);
			bench.initialize(config);
			store.initialize();
			BenchMarkExecutor exec = new BenchMarkExecutor(bench, store, size,
					stats);

			exec.start();
			stats.setFinishTime(new Clock().getTime());
			stats.showResults();
		}catch(NumberFormatException ne)
		{
			logger.error("Error loading nodes. Port must be an integer",ne);
		}
		catch(ConfigurationException ce)
		{
			logger.error("Error loading properties file: ublog.properties.",ce);
		}
		catch (Exception e) {
			logger.error("Exception occured.",e);
		}

	}

}
