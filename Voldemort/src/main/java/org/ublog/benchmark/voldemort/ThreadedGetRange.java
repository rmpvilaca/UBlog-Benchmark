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
package org.ublog.benchmark.voldemort;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;
import org.ublog.utils.Pair;
import org.ublog.benchmark.operations.MultiPutOperation;

import voldemort.client.StoreClient;


public class ThreadedGetRange implements Callable<Map<String,String>> {

	private Logger logger= Logger.getLogger(ThreadedGetRange.class);
	private String key;
	private StoreClient<String,Map<String,String>> store;
	
	public ThreadedGetRange(StoreClient<String, Map<String,String>> store,String key) {
		super();
		this.key = key;
		this.store = store;
	}
	
	@Override
	public Map<String, String> call() throws Exception {
		// TODO Auto-generated method stub
		if (logger.isInfoEnabled())
			logger.info("ThreadGetRange:"+key);
		return (Map<String, String>) this.store.getValue(this.key);
		
	}
}
