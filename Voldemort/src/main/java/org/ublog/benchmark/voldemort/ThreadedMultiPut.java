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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.ublog.utils.Pair;

import voldemort.client.StoreClient;
import voldemort.versioning.Versioned;

public class ThreadedMultiPut<K, V> implements Callable<Pair> {

	private StoreClient<String,Object> store;
	private String key;
	private Pair<K, V> value;
	private Pair<String, Boolean> result;
	
	private Logger logger= Logger.getLogger(ThreadedMultiPut.class);
	
	public ThreadedMultiPut(StoreClient<String,Object> store, String key, Pair<K, V> value){	
		this.store = store;
		this.key =  key;
		this.value = value;
	}

	@Override
	public Pair call()  {
		// TODO Auto-generated method stub
		Object first = value.getFirst();
		//Object second = value.getSecond();

		try{
			this.store.put(key, first);
		}
		catch (Exception e) {
			//e.printStackTrace();
		}
		//System.out.println("THR multiPut:"+key+":"+first);
		if (logger.isInfoEnabled())
			logger.info("ThreadMultiPut:"+key+":"+first);
		//System.out.println("ThreadMultiPut:"+key+":"+first);
		Boolean res = true;
		return new Pair(key,res);
	}
	
}
