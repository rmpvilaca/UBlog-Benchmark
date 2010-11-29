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

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import org.ublog.utils.Pair;
import org.ublog.benchmark.BenchMark;
import org.ublog.benchmark.BenchMarkClient;
import org.ublog.benchmark.DataStore;
import org.ublog.benchmark.operations.CollectionOperation;
import org.ublog.benchmark.operations.DataStoreOperation;
import org.ublog.benchmark.StatsCollector;

import org.ublog.utils.Clock;
import org.ublog.utils.Time;

public class BenchMarkExecutor {

	private Logger logger = Logger.getLogger(BenchMarkExecutor.class);

	private int nClients;
	private ExecutorService executor;
	private BenchMark bench;
	private DataStore dataStore;
	private StatsCollector stats;

	@SuppressWarnings("static-access")
	public BenchMarkExecutor(BenchMark bench, DataStore dataStore,
			int nClients, StatsCollector statsCollector) throws Exception {

		this.executor = Executors.newFixedThreadPool(nClients);
		this.nClients = nClients;
		this.bench = bench;
		this.dataStore = dataStore;
		this.stats = statsCollector;
	}

	public void start() {
		Runnable init = new Runnable() {
			@Override
			public void run() {
				logger.info("Creating connection to initialize benchmark.");
				DataStore.Connection conn = null;
				try {
					conn = dataStore.createConnection();
				} catch (Exception e1) {
					e1.printStackTrace();
				}
				while (bench.hasMoreInitOperations()) {

					logger.info("Executing next initialize operation.");
					try {
						DataStoreOperation op = bench.nextInitOperation();
						conn.executeOperation(op);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				try {
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				logger.info("Finished benchmark initialization.");
				System.out.println("Finished benchmark initialization.");
			}
		};
		init.run();
		Time time = new Clock().getTime();
		stats.setStartTime(time);
		for (int i = 0; i < this.nClients; i++) {
			Runnable client = new Runnable() {
				@Override
				public void run() {
					BenchMarkClient client = bench.createNewClient(new Clock());
					logger.info("Creating new connection for a client:"
							+ client);
					DataStore.Connection conn = null;
					try {
						conn = dataStore.createConnection();
					} catch (Exception e2) {
						e2.printStackTrace();
					}
					while (client.hasMoreOperations()) {
						Pair<CollectionOperation, Double> next = null;
						try {
							next = client.nextOperation();
						} catch (UnsupportedEncodingException e1) {
							e1.printStackTrace();
						}
						if (next.getSecond() > 0) {
							try {
								Thread.sleep(next.getSecond().longValue());
							} catch (InterruptedException e) {
								logger.warn("Sleep for think-time has been interrupted.");
							}
						}
						try {
							conn.executeOperation(next.getFirst());
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					try {
						conn.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			this.executor.execute(client);
		}
		try {
			this.executor.shutdown();
			this.executor.awaitTermination(1, TimeUnit.DAYS);
			this.dataStore.finalclose();
			System.out.println("TERMINOU");
		} catch (InterruptedException e) {
			logger.warn("Benchmark clients timeout expired.");
		}
		logger.info("All clients have finished.");
	}

}
