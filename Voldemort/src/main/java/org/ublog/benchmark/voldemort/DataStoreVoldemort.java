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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.ublog.utils.Pair;
import org.ublog.benchmark.DataStore;
import org.ublog.benchmark.operations.*;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.versioning.Versioned;


public class DataStoreVoldemort implements DataStore{

	private StoreClientFactory factory; 
	private StoreClient<String,Map<String,String>>users;
	private StoreClient<String,List<String>> friendsTimeLine;
	private StoreClient<String,Map<String,String>> tweets;
	private StoreClient<String,Set<Map<String,String>>> tagsStore;

	private Logger logger= Logger.getLogger(DataStoreVoldemort.class);

	@Override
	public Connection createConnection() {
		return new Connection().createConnections();
	}

	public void initialize() throws Exception {
		Configuration conf = new PropertiesConfiguration("voldemort.properties");
		String[] nodes = conf.getStringArray("node");
		List<String> listNodes=new ArrayList<String>();
		for(String tmp:nodes)
		{
			String[] split=tmp.split(":");
			if (split.length==2)
			{
				int port=new Integer(split[1]);
				listNodes.add("tcp://"+split[0]+":"+port);
			}
			else
			{
				logger.error("Nodes configuration is wrong");
				throw new ConfigurationException("Nodes must be in the format hostName:port");
			}
		}


		ClientConfig config = new ClientConfig();
		config.setBootstrapUrls(listNodes);
		config.setMaxConnectionsPerNode(3);
		config.setMaxThreads(3);
		this.factory = new SocketStoreClientFactory(config);
		this.users = factory.getStoreClient("users");
		this.friendsTimeLine = factory.getStoreClient("friendsTimeLine");
		this.tweets =  factory.getStoreClient("tweets");
		this.tagsStore = factory.getStoreClient("tags");
	}

	public void finalclose() {
		factory.close();
	}

	class Connection implements DataStore.Connection{

		private ExecutorService pool;


		@Override
		public Connection createConnections() {
			this.pool =  Executors.newFixedThreadPool(5);
			return this;
		}

		@Override
		public void close() {
			this.pool.shutdown();
		}

		@SuppressWarnings("unchecked")
		@Override
		public void executeOperation(DataStoreOperation op) {
			Object value;
			if(op instanceof GetOperation){
				this.getOperation(op);
			}

			else if(op instanceof MultiGetOperation){
				this.multiGetOperation(op);
			}

			else if(op instanceof PutOperation){
				this.putOperation(op);
			}

			else if(op instanceof MultiPutOperation){ 
				this.multiPutOperation(op);
			}

			else if(op instanceof GetRangeOperation){
				this.getRangeOperation(op);
			}

			else if(op instanceof GetByTagsOperation){
				this.getByTagsOperation(op);
			}

			else if(op instanceof DeleteOperation){

				System.out.println("DELOP");
				op.notifyListeners();
			}
		}


		private void getOperation(DataStoreOperation op){
			Object value;
			Comparable key = ((GetOperation) op).getKey();
			String tablename = ((GetOperation) op).getTableName();

			//System.out.println(op.toString());

			if(tablename.equals("friendsTimeLine")){
				//System.out.println("FRIEDNS GET: key = "+(String) key);
				//System.out.println(friendsTimeLine.toString());
				Versioned versioned = friendsTimeLine.get((String) key);
				//System.out.println(versioned.getValue());
				((GetOperation) op).setResult((List<String>)versioned.getValue());
				op.notifyListeners();
			}
			else {
				Versioned versioned = null;
				if(tablename.equals("users")){
					versioned = users.get((String) key);
					//System.out.println(("GETOP")+(Map<String,String>) versioned.getValue()+versioned.getVersion());
				}
				else{
					versioned = tweets.get((String) key);
				}
				if(versioned!=null){
					((GetOperation) op).setResult((Map<String,String>) versioned.getValue());
				}
				else{
					((GetOperation) op).setResult(null);
				}
				op.notifyListeners();
			}
		}

		private void multiGetOperation(DataStoreOperation op){
			String key;
			Set<String> keys = ((MultiGetOperation) op).getKeys();
			String tablename = ((MultiGetOperation) op).getTableName();
			//System.out.println("MultiGet:"+tablename+";"+keys);

			if(tablename.equals("friendsTimeLine")){
				HashMap<String,List<String>> result = new HashMap<String,List<String>>();
				Map<String,Versioned<List<String>>> getAllres = friendsTimeLine.getAll(keys); 

				//System.out.println("GETALLRES: "+getAllres+ ":: size: "+getAllres.size());

				Set<String> keysRes = getAllres.keySet();
				List<String> values;
				Iterator<String> iter = keysRes.iterator();
				while(iter.hasNext()){
					key = iter.next();
					//System.out.println("MULTIGET FRIENDS:"+getAllres.get(key).getValue());
					values = getAllres.get(key).getValue();
					result.put(key, values);
				}
				//System.out.println("Result DTVolde MultiGet:"+result);
				((MultiGetOperation) op).setResult(result);
				op.notifyListeners();
			}
			else{
				if(tablename.equals("users")){
					HashMap<String,Map<String,String>> result = new HashMap<String,Map<String,String>>();
					Map<String, Versioned<Map<String, String>>> getAllres = users.getAll(keys); 

					Set<String> keysRes = getAllres.keySet();
					Map<String, String> values;
					Iterator<String> iter = keysRes.iterator();
					while(iter.hasNext()){
						key = iter.next();
						//System.out.println(key);
						values = getAllres.get(key).getValue();
						result.put(key, values);
					}
					((MultiGetOperation) op).setResult(result);
					op.notifyListeners();
				}
				else{
					HashMap<String,Map<String,String>> result = new HashMap<String,Map<String,String>>();
					Map<String, Versioned<Map<String, String>>> getAllres = tweets.getAll(keys); 

					Set<String> keysRes = getAllres.keySet();
					Map<String, String> values;
					Iterator<String> iter = keysRes.iterator();
					while(iter.hasNext()){
						key = iter.next();
						//System.out.println(key);
						values = getAllres.get(key).getValue();
						result.put(key, values);
					}
					((MultiGetOperation) op).setResult(result);
					op.notifyListeners();
				}
			}
		}

		private void putOperation(DataStoreOperation op){
			boolean res = false;
			Comparable key = ((PutOperation) op).getKey();
			String tablename = ((PutOperation) op).getTableName();

			//System.out.println("PutOP:"+tablename+";"+key);

			if(tablename.equals("friendsTimeLine")){
				//System.out.println("PutOP: "+(List<String>) ((PutOperation) op).getData());
				//res = friendsTimeLine.put((String) key, (List<String>) ((PutOperation) op).getData());

				//System.out.println("Put res bool:"+res +  " "+ (String) key + ((PutOperation) op).getData());
				List<String> list = (List<String>) ((PutOperation) op).getData();
				friendsTimeLine.put((String) key, list);


				//res = friendsTimeLine.putIfNotObsolete((String) key,new Versioned((List<String>) ((PutOperation) op).getData()));

				res = true;
				((PutOperation) op).setResult(res);
				op.notifyListeners();
			}
			else {
				if(tablename.equals("users")){
					//Versioned version = new Versioned(((PutOperation) op).getData());
					//System.out.println("Version:"+version.toString());
					//System.out.println("Put res bool:"+res +  " "+ (String) key + ((PutOperation) op).getData());
					users.put((String) key,(Map<String, String>) ((PutOperation) op).getData());
					//res = users.putIfNotObsolete((String) key,new Versioned(((PutOperation) op).getData()));

					//users.put((String) key,(Map<String, String>) ((PutOperation) op).getData());
					res = true;
				}
				else{
					////INSERT IN TAGS
					Set<Map<String, String>> result = null;
					Set<String> tags = ((PutOperation) op).getTags();
					//int i = 0;
					//String auxTag;
					for(String tag:tags){
						if(tag != null){
							/*if(i==0){
							 }
							 else {
								 if(i==1) auxTag = "#"+tag.toString();//TOPIC
								 else auxTag = "@"+tag.toString();*///USER
							//System.out.println("TAG:"+tag);
							result = null;
							result = tagsStore.getValue(tag);
							if(result==null) result = new HashSet<Map<String,String>>();
							result.add((Map<String, String>) ((PutOperation) op).getData()); //INSERE O TWEET TAGS STORE
							tagsStore.put(tag, (Set<Map<String, String>>) result);
						}

						//i++;
					}
					/////FINISH TAGS

					tweets.put((String) key,(Map<String, String>) ((PutOperation) op).getData());
					res=true;
				}
				((PutOperation) op).setResult(res);
				op.notifyListeners();
			}
		}

		private void multiPutOperation(DataStoreOperation op){
			boolean res = false;
			Set<Future<Pair<String,Boolean>>> set = new HashSet<Future<Pair<String,Boolean>>>();
			Map<String,Boolean> mapResults = new HashMap<String,Boolean>();
			String tablename = ((MultiPutOperation) op).getTableName();

			if(tablename.equals("friendsTimeLine")){
				//System.out.println("I need multiputOP para friendstomeline");

				Map<String, Pair<List<String>,Set<String>>> map = ((MultiPutOperation) op).getMapKeyToDataAndTags();
				Set<String> keys = map.keySet();
				Iterator<String> iter = keys.iterator();
				String key;
				while(iter.hasNext()){ 
					key = iter.next();
					Pair<List<String>, Set<String>> hash = map.get(key);
					Callable<Pair<String,Boolean>> callable = new ThreadedMultiPut(friendsTimeLine,key,hash);
					Future<Pair<String,Boolean>> future = this.pool.submit(callable);
					set.add(future);
				}
				for (Future<Pair<String,Boolean>> future : set) {
					try {
						Pair<String,Boolean> aux = future.get();
						mapResults.put(aux.getFirst(), aux.getSecond());
					} catch (InterruptedException e) {
						//e.printStackTrace();
					} catch (ExecutionException e) {
						//e.printStackTrace();
					}
				}
				((MultiPutOperation) op).setMapResults(mapResults);
				op.notifyListeners();

			}
			else{
				if(tablename.equals("users")){
					Map<String, Pair<Map<String,String>,List<String>>> map = ((MultiPutOperation) op).getMapKeyToDataAndTags();
					Set<String> keys = map.keySet();
					Iterator<String> iter = keys.iterator();
					String key;
					while(iter.hasNext()){
						key = iter.next();
						//System.out.println(key);
						Pair<Map<String, String>, List<String>> hash = map.get(key);
						Callable<Pair<String,Boolean>> callable = new ThreadedMultiPut(users,key,hash);
						Future<Pair<String,Boolean>> future = this.pool.submit(callable);
						set.add(future);
					}
					for (Future<Pair<String,Boolean>> future : set) {
						try {
							Pair<String,Boolean> aux = future.get();
							mapResults.put(aux.getFirst(), aux.getSecond());
						} catch (InterruptedException e) {
							//e.printStackTrace();
						} catch (ExecutionException e) {
							//e.printStackTrace();
						}
					}
					((MultiPutOperation) op).setMapResults(mapResults);
					op.notifyListeners();
				}
				else{
					Map<String, Pair<Map<String,String>,List<String>>> map = ((MultiPutOperation) op).getMapKeyToDataAndTags();
					Set<String> keys = map.keySet();
					Iterator<String> iter = keys.iterator();
					String key;
					while(iter.hasNext()){
						key = iter.next();
						//System.out.println(key);
						Pair<Map<String, String>, List<String>> hash = map.get(key);
						Callable<Pair<String,Boolean>> callable = new ThreadedMultiPut(tweets,key,hash);
						Future<Pair<String,Boolean>> future = this.pool.submit(callable);
						set.add(future);
					}
					for (Future<Pair<String,Boolean>> future : set) {
						try {
							Pair<String,Boolean> aux = future.get();
							mapResults.put(aux.getFirst(), aux.getSecond());
						} catch (InterruptedException e) {
							//e.printStackTrace();
						} catch (ExecutionException e) {
							//e.printStackTrace();
						}
					}
					((MultiPutOperation) op).setMapResults(mapResults);
					op.notifyListeners();
				}
			}
		}

		private void getRangeOperation(DataStoreOperation op){///////////////POR ISTO EM PARALELO COMO O MULIPUT 
			Comparable keyMax = ((GetRangeOperation) op).getMax();
			Comparable keyMin = ((GetRangeOperation) op).getMin();
			String tablename = ((GetRangeOperation) op).getTableName();

			int idMax = this.getTweetID((String) keyMax);
			int idMin = this.getTweetID((String) keyMin);
			String userID = this.getUserID((String) keyMax);

			Set<Future<Map<String,String>>> set = new HashSet<Future<Map<String,String>>>();

			if(tablename.equals("friendsTimeLine")){

			}
			else{
				if(tablename.equals("users")){

				}
				else{
					Set<Map<String, String>> result = new HashSet<Map<String,String>>();
					for(int i=idMin;i<=idMax;i++){
						//System.out.println("idMax:"+idMax+";"+"idMin"+idMin);
						//System.out.println(userID+"-"+i);
						//System.out.println(userID+"-"+this.getTweetPadding(i));
						Callable<Map<String,String>> callable = new ThreadedGetRange(tweets,userID+"-"+this.getTweetPadding(i));
						Future<Map<String, String>> future = this.pool.submit(callable);
						set.add(future);
						//result.add(this.tweets.getValue(userID+"-"+this.getTweetPadding(i)));
					}
					for (Future<Map<String, String>> future : set) {
						try {
							Map<String, String> aux = future.get();
							result.add(aux);
						} catch (InterruptedException e) {
							//e.printStackTrace();
						} catch (ExecutionException e) {
							//e.printStackTrace();
						}
					}

					((GetRangeOperation) op).setResult(result);
					//System.out.println("GETRANGE RES:"+result);
					op.notifyListeners();
				}

			}
		}

		private int getTweetID(String tweetID)
		{
			String split[]=tweetID.split("-");
			return new Integer(split[1]);
		}
		private String getUserID(String tweetID)
		{
			String split[]=tweetID.split("-");
			return split[0];
		}
		private String getTweetPadding(int tweetIdx) { //copiado do Utils.java
			int MaxNTweets=10000;
			StringBuilder strBuild=new StringBuilder();
			int current=(int)Math.floor(Math.log10(tweetIdx))+1;
			int expected=(int)Math.floor(Math.log10(MaxNTweets));
			if (tweetIdx==0)
				current=1;
			for(int i=0;i<(expected-current);i++)
				strBuild.append(0);
			strBuild.append(tweetIdx);
			/*if (logger.isInfoEnabled())
				logger.info("getTweetPadding with tweetIdx:"+tweetIdx+" is:"+strBuild);*/
			return strBuild.toString();
		}

		private void getByTagsOperation(DataStoreOperation op){
			Set<String> tags = ((GetByTagsOperation) op).getTags();
			Set<Map<String, String>> result = null;
			//String tablename = ((GetByTagsOperation) op).getTableName();
			//int i=0;
			//String auxTag;
			for(String tag:tags){
				if(tag!=null) {
					/*if(i==0){
					}
					else {
						if(i==1) auxTag = "#"+tag.toString();//TOPIC
						else auxTag = "@"+tag.toString();*///USER
					//System.out.println("Tag:"+tag);
					//System.out.println(op.toString());
					result = tagsStore.getValue(tag);
					if(result != null){
						//System.out.println("RES getByTags:"+result);
					}

				}

				//i++;
			}
			((GetByTagsOperation) op).setResult(result);
			op.notifyListeners();
		}
	}
}
