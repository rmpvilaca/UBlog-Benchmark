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


import java.net.UnknownHostException;


import org.ublog.benchmark.DataStore;
import org.ublog.benchmark.mysql.DataStoreMysql;
import org.ublog.benchmark.social.mysql.MySQLTwitterOperationFactoryImpl;



public class MySQLSocialBenchmarkStarter {

	/**
	 * @param args
	 * @throws UnknownHostException 
	 */
	public static void main(String[] args) throws UnknownHostException {
		int size;
		int sizeTotal;
		int usernameStarter;
		int nOperations;
		double thinkTime;
		String type;		
		
		DataStore store;
		if (args.length!=5)
		{
			System.out.println("Invalid number of parameters");
			System.out.println("Usage: java MySQLSocialBenchmarkStarter sizeTotal size usernameStarter nOperations thinkTime");
			System.out.println("Using default values.");
			
			size = 500;
			sizeTotal=500;
			usernameStarter = 0;
			nOperations = 100;
			thinkTime = 1000;
		}
		else
		{
			sizeTotal = Integer.valueOf(args[0]);
			size = Integer.valueOf(args[1]);
			usernameStarter = Integer.valueOf(args[2]);
			nOperations = Integer.valueOf(args[3]);
			thinkTime = Integer.valueOf(args[4]);
		}
		new SocialBenchmarkStarter(new MySQLTwitterOperationFactoryImpl(),new DataStoreMysql(), size, sizeTotal, usernameStarter, nOperations, thinkTime);
		
	}

}
