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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import org.ublog.utils.Time;

public class StatsCollector {

	private Logger logger = Logger.getLogger(StatsCollector.class);

	private Map<String, Double> mapTotalTime;
	private Map<String, Integer> mapCount;

	private Time finishTime;
	private Time startTime;

	private PrintWriter logFile;

	public StatsCollector(String fileName) {
		this.mapCount = new HashMap<String, Integer>();
		this.mapTotalTime = new HashMap<String, Double>();
		try {
			this.logFile = new PrintWriter(new FileWriter(fileName));
		} catch (IOException e) {
			logger.fatal("Can't open logFile: " + fileName);

			e.printStackTrace();
		}

	}

	public void reset() {
		this.mapCount.clear();
		this.mapTotalTime.clear();
	}

	public synchronized void registerRequest(double startTime, int opID,
			int nodeID, double latency, boolean write, String requestType) {

		int scaleFactor = 1000000;
		char readOnlyChar = write ? 'w' : 'r';
		logFile.println(Math.round(startTime * scaleFactor) + "," + opID + ","
				+ nodeID + "," + Math.round(latency * scaleFactor) + ",commit,"
				+ readOnlyChar + "," + requestType + ",0,0,0,0,0,0,0,0,0,0,0,");
		if (!this.mapCount.containsKey(requestType))
			this.mapCount.put(requestType, 0);
		if (!this.mapTotalTime.containsKey(requestType))
			this.mapTotalTime.put(requestType, 0.0);
		this.mapCount.put(requestType, this.mapCount.get(requestType) + 1);
		this.mapTotalTime.put(requestType, this.mapTotalTime.get(requestType)
				+ latency);
	}

	public int getTotalRequests(String requestType) {
		if (requestType == null) {
			int total = 0;
			for (String req : this.mapCount.keySet()) {
				total += this.getTotalRequests(req);
			}
			return total;
		} else
			return this.mapCount.get(requestType);
	}

	public double getRequestsMean(String requestType) {
		if (requestType == null) {
			double total = 0;
			for (String req : this.mapTotalTime.keySet()) {
				total += this.getRequestsMean(req);
			}
			return total / this.mapTotalTime.keySet().size();
		} else
			return this.mapTotalTime.get(requestType)
					/ this.mapCount.get(requestType);
	}

	public synchronized void showResults() {
		this.logFile.flush();
		this.logFile.close();
		double time = Time.inSeconds(this.finishTime.subtract(this.startTime));
		System.out.println("Start Time(s):" + Time.inSeconds(this.startTime)
				+ ";FinishTime(s):" + Time.inSeconds(this.finishTime)
				+ ";MeasurementTime(s):" + time);
		System.out.println("OP Type;Count;Latency;Throughput(OPS)");
		for (String req : this.mapCount.keySet()) {
			System.out.println(req + ";" + this.getTotalRequests(req) + ";"
					+ this.getRequestsMean(req) + ";"
					+ this.getTotalRequests(req) * 1.0 / time);
		}
		System.out.println("TOTAL;" + this.getTotalRequests(null) + ";"
				+ this.getRequestsMean(null) + ";"
				+ this.getTotalRequests(null) * 1.0 / time);

	}

	public void setFinishTime(Time finishTime) {
		this.finishTime = finishTime;
	}

	public Time getFinishTime() {
		return finishTime;
	}

	public void setStartTime(Time startTime) {
		this.startTime = startTime;
	}

	public Time getStartTime() {
		return startTime;
	}

}
