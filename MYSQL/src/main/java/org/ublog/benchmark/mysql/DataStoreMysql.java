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
package org.ublog.benchmark.mysql;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.ublog.utils.Pair;
import org.ublog.benchmark.DataStore;
import org.ublog.benchmark.operations.*;

import java.sql.SQLException;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.DataSources;

import java.sql.*;



public class DataStoreMysql implements DataStore{
	
	private String url;
	private String dbName;
	private String driver;
	private String userName;
	private String password;
	
	private DataSource pooled;
	
	private Logger logger= Logger.getLogger(DataStoreMysql.class);
	
	@Override
	public Connection createConnection() throws UnsupportedEncodingException {
		return new Connection().createConnections();
	}

	public void initialize() throws Exception {
		
		Configuration config = new PropertiesConfiguration("mysql.properties");
		String host=config.getString("host.name");
		int port=config.getInt("host.port");
		this.url = "jdbc:mysql://"+host+":"+port+"/";
	    this.dbName = config.getString("dbName");
	    this.driver = "com.mysql.jdbc.Driver";
	    this.userName =  config.getString("userName");
	    this.password = config.getString("password"); 
	    
	    ///CREATE TABLES
		try {
			Class.forName(driver).newInstance();
			
			DataSource ds_unpooled = DataSources.unpooledDataSource(url+dbName,userName,password);

			Map<String, Comparable> overrides = new HashMap<String, Comparable>();
			overrides.put("maxStatements", "200");         //Stringified property values work
			overrides.put("maxPoolSize", new Integer(3)); //"boxed primitives" also work

			//create the PooledDataSource using the default configuration and our overrides
			this.pooled = DataSources.pooledDataSource( ds_unpooled, overrides ); 

			//The DataSource ds_pooled is now a fully configured and usable pooled DataSource,
			//with Statement caching enabled for a maximum of up to 200 statements and a maximum
			//of 50 Connections.

			java.sql.Connection conn = pooled.getConnection();
			
			System.out.println("Connected to the database");
		
			String users = "create table users (" +
								"userID VARCHAR(50) PRIMARY KEY, " +
								"name VARCHAR(50), " +
								"password VARCHAR(50), "+
								"following VARCHAR(500), "+
								"followers VARCHAR(500), "+
								"username VARCHAR(50), "+
								"lastTweet VARCHAR(50), "+
								"created VARCHAR(50) )" + "ENGINE=NDBCLUSTER";
			String tweets = "create table tweets (" +
								"tweetID VARCHAR(50) PRIMARY KEY, " +
								"id VARCHAR(50), "+
								"text VARCHAR(50), "+
								"date VARCHAR(50), "+
								"user VARCHAR(50) )" + "ENGINE=NDBCLUSTER";
			String friendsTimeLine = "create table friendsTimeLine (" +
								"tweetID VARCHAR(50),"+
								"userID VARCHAR(50), INDEX(userID),"+
								"date VARCHAR(50), " +
								"PRIMARY KEY (userID,tweetID)" +
								")" + "ENGINE=NDBCLUSTER";
			
			String tweetsTags = "create table tweetsTags(" +
								"tweetID VARCHAR(50) PRIMARY KEY, " +
								"topic VARCHAR(50), "+
								"user VARCHAR(50) )" + "ENGINE=NDBCLUSTER";
			try {
				Statement stmt = conn.createStatement();
				if (logger.isInfoEnabled())
					logger.info("Creating table users");
		   		stmt.executeUpdate(users);
		   		if (logger.isInfoEnabled())
					logger.info("Creating table tweets");
		   		stmt.executeUpdate(tweets);
		   		if (logger.isInfoEnabled())
					logger.info("Creating table friendsTimeLine");
		   		stmt.executeUpdate(friendsTimeLine);
		   		if (logger.isInfoEnabled())
					logger.info("Creating table tweet's tags");
		   		stmt.executeUpdate(tweetsTags);
				stmt.close();
				conn.close();
			} catch(SQLException ex) {
				logger.error("SQLException ",ex);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	  }
	public void finalclose(){
		System.out.println("CLOSEALL");
	}
	
	
	class Connection implements DataStore.Connection{
		private ExecutorService pool;	
		private String tweetsColumns;
		private String userColumns;
		
		@Override
		public Connection createConnections() throws UnsupportedEncodingException {
			this.pool =  Executors.newFixedThreadPool(5);			
			this.addTweetsColumns();
			this.addUserColumns();
			
			return this;
		}

		@Override
		public void close() {	
			try {
				this.pool.shutdown();
				this.pool.awaitTermination(1,TimeUnit.DAYS);
			} catch (InterruptedException e) {
				logger.warn("Benchmark clients timeout expired.");
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void executeOperation(DataStoreOperation op) throws UnsupportedEncodingException, SQLException {
			if(op instanceof GetOperation){
				this.getOperation(op);
			}

			else if(op instanceof GetTimeLineAndTweetsOperation){
				this.getTimeLineAndTweetsOperation(op);
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
				this.deleteOperation(op);
			}
		}

		@SuppressWarnings("unchecked")
		private void getOperation(DataStoreOperation op) throws UnsupportedEncodingException,  SQLException{
			java.sql.Connection conn = null;
			Comparable key = ((GetOperation) op).getKey();
			String tablename = ((GetOperation) op).getTableName();

			if(tablename.equals("friendsTimeLine")){
				//int count = ((GetOperation) op).getCount();
		        conn = this.borrowClient();
		        String query = "select tweetID,date from " + tablename + " where userID = ?";
		        PreparedStatement stmt = conn.prepareStatement(query);
		        ResultSet results = null;
		        //Map<String,String> res;
		        ArrayList<String> res;
		        try {
		        	stmt.setString(1, (String) key);
		        	results = stmt.executeQuery();
		        	//res = new HashMap<String,String>();
		        	res = new ArrayList<String>();
		        	while (results.next())
		        	{	
		        		//res.put(results.getString("tweetID"), results.getString("date"));
		        		res.add(results.getString("tweetID")+":"+results.getString("date"));
		        	}
		        } finally {
		        	try {
		        		stmt.close();
		        	} catch(Exception e) {}
		        	this.releaseClient(conn);
		        }
		        ((GetOperation) op).setResult(res);
				op.notifyListeners();
			}
			else {
				if(tablename.equals("users")){
					String columns = new String();
					List<String> cols = ((GetOperation) op).getColumns();
					if(cols.isEmpty()) columns = this.userColumns; ///////SE FOR VAZIO TRAZ COLUNAS TODAS
					else columns = this.listToString((ArrayList<String>) cols);
					Map<String,String> value = new HashMap<String,String>();
					
					String query = "select "+columns+" from " + tablename + " where userID = ?";
					
			        conn = this.borrowClient();
			        PreparedStatement stmt = conn.prepareStatement(query);
			        ResultSet results = null;
			        try {
			        	stmt.setString(1, (String) key);
			        	//System.out.println("Query select:"+stmt.toString());
			        	results = stmt.executeQuery();

			        	String[] arrStr = columns.split(",");
			        	int size = arrStr.length;
			        	while(results.next()){
			        		for(int i=0;i<size;i++){
			        			value.put(arrStr[i],results.getString(arrStr[i]));
			        		}
			        	}
			        } finally {
			        	try {
			        		stmt.close();
			        	} catch(Exception e) {}
			        	this.releaseClient(conn);
			        }
			        ((GetOperation) op).setResult((Map<String,String>) value);
					op.notifyListeners();
				}
			}
		}
		
		@SuppressWarnings("unchecked")
		private void getTimeLineAndTweetsOperation(DataStoreOperation op) throws UnsupportedEncodingException, SQLException{
			java.sql.Connection conn = null;
			String userid = ((GetTimeLineAndTweetsOperation) op).getUserid();
			int start = ((GetTimeLineAndTweetsOperation) op).getStart();
			int count = ((GetTimeLineAndTweetsOperation) op).getCount();
			String tablename = ((GetTimeLineAndTweetsOperation) op).getTableName();

			conn = this.borrowClient();
	        String query = "select tweets.tweetID,id,text,date,user from (select tweetID from friendsTimeLine where userID = ?) as res inner join tweets on res.tweetID = tweets.tweetID order by date DESC limit ?,? ";
	        PreparedStatement stmt = conn.prepareStatement(query);
	        ResultSet results = null;
	        LinkedHashMap<String,Map<String,String>> result;
	        try {
	            stmt.setString(1, (String) userid);
	            //stmt.setString(2, new String().valueOf(start));
	            stmt.setInt(2, start);
	            //stmt.setString(3, new String().valueOf(start+count));
	            stmt.setInt(3, start+count);
	            //System.out.println(start+count);
	            //System.out.println("QERy:"+stmt.toString());
	            results = stmt.executeQuery();

	            result = new LinkedHashMap<String,Map<String,String>>();
	            while (results.next())
	            {	
	            	Map<String,String> value = new HashMap<String,String>();
	            	value.put("id",results.getString("id"));
	            	value.put("text",results.getString("text"));
	            	value.put("date",results.getString("date"));
	            	value.put("user",results.getString("user"));
	            	result.put(results.getString("tweetID"), value);
	            }
	        } finally {
	        	try {
	        		stmt.close();
	        	} catch(Exception e) {}
	        	this.releaseClient(conn);
	        }
	        ((GetTimeLineAndTweetsOperation) op).setResult(result);
			op.notifyListeners();
		}
		
		@SuppressWarnings("unchecked")
		private void putOperation(DataStoreOperation op) throws UnsupportedEncodingException, SQLException {
			//long timestamp;
			boolean res = false;
			java.sql.Connection conn = null;
			Comparable key = ((PutOperation) op).getKey();
			String tablename = ((PutOperation) op).getTableName();
			
			if(tablename.equals("friendsTimeLine")){ //SERA QUASE UM MULTIPUT DE TIMELINE
				conn = this.borrowClient();
				String query = "insert into "+tablename+" (tweetID,userID,date) values (?,?,?)"; //on duplicate key udpdate userID = values(userID), date = values(date)";
				PreparedStatement stmt = conn.prepareStatement(query);
				List<Pair<String,String>> value = (List<Pair<String,String>>) ((PutOperation) op).getData();
				for(Pair<String,String> pair : value){
					String date = pair.getFirst();
					String tweetID = pair.getSecond();
					stmt.setString(1, tweetID/*+":"+date*/);
					stmt.setString(2, (String)  key);
					stmt.setString(3, date);
					stmt.executeUpdate();
				}
				stmt.close();
				this.releaseClient(conn);
				res = true;
				((PutOperation) op).setResult(res);
				op.notifyListeners();
			}
			else {
				if(tablename.equals("users")){
					Map<String,String> value = (Map<String, String>) ((PutOperation) op).getData();
					String query;
					int i;
					//String query = "insert into "+tablename+" (userID,name,password,following,followers,username,lasttweet,created) values (?,?,?,?,?,?,?,?)";
					if(value.size()<7){
						i=0;
						query = "update " +tablename+" set ";
					}
					else{
						i=1;
						query = "insert into "+tablename+" ";// (userID,name,password,following,followers,username,lasttweet,created) values (?,?,?,?,?,?,?,?)";
					}
					Set<String> keys = value.keySet();
					Iterator<String> iter = keys.iterator();
					ArrayList<String> columnNames = new ArrayList<String>();
					ArrayList<String> columnValues = new ArrayList<String>();
					
					while(iter.hasNext()){ // FUNCIONA PARA O INSERT e UPDATE
						String columnName = iter.next();
						columnNames.add(columnName);
						columnValues.add(value.get(columnName));						
					}
					conn = this.borrowClient();
					Statement stmt = conn.createStatement();
					try {
						if(i==0) query += this.buildUpdateQuery("userID", (String) key, columnNames, columnValues);
						else query += this.buildInsertNamesQuery("userID", columnNames) +" values " +this.buildInsertValuesQuery((String) key, columnValues)
										+" on duplicate key update "+this.buildDuplicateNamesQuery(columnNames);
						//System.out.println("QUERY:" +query);
						stmt.executeUpdate(query);
					} finally {
						try {
							stmt.close();
						} catch(Exception e) {}
						this.releaseClient(conn);
					}
					res = true;
					//System.out.println("PUTOP SETRESULT");
					((PutOperation) op).setResult(res);
					op.notifyListeners();
				}
				else{
					Map<String,String> value = (Map<String, String>) ((PutOperation) op).getData();
					//conn = this.borrowClient(op);
					//String query = "insert into "+tablename+" (tweetID,id,text,date,user) values (?,?,?,?,?,?,?,?)";
					String query = "insert into "+tablename + " "; //tweets
										
					ArrayList<String> columnNames = new ArrayList<String>();
					ArrayList<String> columnValues = new ArrayList<String>();
					Set<String> keys = value.keySet();
					Iterator<String> iter = keys.iterator();					
					while(iter.hasNext()){ 
						String columnName = iter.next();
						columnNames.add(columnName);
						columnValues.add(value.get(columnName));
					}
					conn = this.borrowClient();
					Statement stmt = conn.createStatement();
					try {
						query += this.buildInsertNamesQuery("tweetID", columnNames) +" values " +this.buildInsertValuesQuery((String) key, columnValues)
										+" on duplicate key update "+this.buildDuplicateNamesQuery(columnNames);
						stmt.executeUpdate(query);
						//System.out.println(stmt.executeUpdate(query));
					} finally {
						try {
							stmt.close();
						} catch(Exception e) {}
						//this.releaseClient(conn);
					}
					
					////INSERT IN TWEET TAGS
					Set<String> tags = ((PutOperation) op).getTags();
					query = "insert into tweetsTags "; //topic,user) values (?,?,?) on duplicate key update topic=values(topic),user=values(user)";
					PreparedStatement st = conn.prepareStatement(query);
					
					columnNames = new ArrayList<String>(3);
					columnValues = new ArrayList<String>(2);
					//columnNames.add("tweetID");
					//columnValues.add((String) key); ///TWEETID
					columnNames.add("topic");
					columnNames.add("user");
					
					if(!tags.isEmpty()){
						Iterator<String> it = tags.iterator();
						while(it.hasNext()){
							String tag = it.next();
							if(tag.startsWith("topico")) {
								columnValues.add(tag); //TOPIC
							}
							else if (tag.startsWith("user")){
								columnValues.add(tag); //USER
							}
						}
					}
					query += this.buildInsertNamesQuery("tweetID", columnNames) + " values "+this.buildInsertValuesQueryForTweetsTags((String) key, columnValues) 
					+" on duplicate key update "+this.buildDuplicateNamesQuery(columnNames);
					//System.out.println("TAGS PUTOP QUERY:"+query);
					try {
						//System.out.println(st.toString());
						st.executeUpdate(query);
					} finally {
						try {
							st.close();
						} catch(Exception e) {}
						this.releaseClient(conn);
					}
					/////FINISH TAGS
					res=true;

					((PutOperation) op).setResult(res);
					op.notifyListeners();
				}
			}
		}
		
		
		@SuppressWarnings("unchecked")
		private void multiPutOperation(DataStoreOperation op){
			Set<Future<Pair<String,Boolean>>> set = new HashSet<Future<Pair<String,Boolean>>>();
			Map<String,Boolean> mapResults = new HashMap<String,Boolean>();
			String tablename = ((MultiPutOperation) op).getTableName();
			
			if(tablename.equals("friendsTimeLine")){
				Map<String, Pair<String,String>> map = ((MultiPutOperation) op).getMapKeyToDataAndTags();
				Set<String> keys = map.keySet();
				Iterator<String> iter = keys.iterator();
				String follower;
				while(iter.hasNext()){ 
					follower = iter.next();
					
					Pair<String, String> pair = map.get(follower);
					String date = pair.getFirst();
					String tweetId = pair.getSecond();
					
					Callable<Pair<String,Boolean>> callable = new TMultiPutMySqlFriendsTimeline(this.borrowClient(),follower,date,tweetId);
					Future<Pair<String,Boolean>> future = this.pool.submit(callable);
					set.add(future);
				}
				for (Future<Pair<String,Boolean>> future : set) {
					try {
						Pair<String,Boolean> aux = future.get();
						mapResults.put(aux.getFirst(), aux.getSecond());
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
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
					String userid;
					while(iter.hasNext()){
						userid = iter.next();
						Pair<Map<String, String>, List<String>> hash = map.get(userid);
						Map<String,String> userFields = hash.getFirst();
						Callable<Pair<String,Boolean>> callable = new TMultiPutMySqlUsers(this.borrowClient(),userid,userFields);
						Future<Pair<String,Boolean>> future = this.pool.submit(callable);
						set.add(future);
					}
					for (Future<Pair<String,Boolean>> future : set) {
						try {
							Pair<String,Boolean> aux = future.get();
							mapResults.put(aux.getFirst(), aux.getSecond());
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
					}
					((MultiPutOperation) op).setMapResults(mapResults);
					op.notifyListeners();
				}
			}
		}
		
		@SuppressWarnings("unchecked")
		private void getRangeOperation(DataStoreOperation op) throws UnsupportedEncodingException, SQLException{///////////////POR ISTO EM PARALELO COMO O MULIPUT 
			Comparable keyMax = ((GetRangeOperation) op).getMax();
			Comparable keyMin = ((GetRangeOperation) op).getMin();
			String tablename = ((GetRangeOperation) op).getTableName();
			
			int idMax = this.getTweetID((String) keyMax);
			int idMin = this.getTweetID((String) keyMin);
			String userID = this.getUserID((String) keyMax);
			
			if(tablename.equals("tweets")){
				java.sql.Connection conn = null;
				
				conn = this.borrowClient();
				//String query = "select id,text,date,user from "+tablename+" tweetID between ? AND ?";
				String query = "select id,text,date,user from "+tablename+" where tweetID = ?";
				PreparedStatement stmt = conn.prepareStatement(query);
				ResultSet results = null;
				Set<Map<String, String>> result = new HashSet<Map<String,String>>();
				for(int i=idMin;i<=idMax;i++){
					try {
						stmt.setString(1, new String(userID+"-"+this.getTweetPadding(i)));
						//System.out.println(stmt.toString());
						results = stmt.executeQuery();

						while (results.next())
						{	
							Map<String,String> value = new HashMap<String,String>();
							value.put("id",results.getString("id"));
							value.put("text",results.getString("text"));
							value.put("date",results.getString("date"));
							value.put("user",results.getString("user"));
							result.add(value);
						}
					}
					finally {
						try {
							//stmt.close();
						} catch(Exception e) {}
					} 
				}
				stmt.close();
				this.releaseClient(conn);
				((GetRangeOperation) op).setResult(result);
				op.notifyListeners();
			}
		}
		
		@SuppressWarnings("unchecked")
		private void getByTagsOperation(DataStoreOperation op) throws UnsupportedEncodingException, SQLException{
			Set<String> tags = ((GetByTagsOperation) op).getTags();
			Set<Map<String, String>> result = new HashSet<Map<String,String>>();
			//int i=0;
			//String auxTag;
			for(String tag:tags){
				if(tag!=null) {
					/*if(i==0){
					}
					else {*/
						String query = null;
						if(tag.startsWith("topic")) {//TOPIC
							//auxTag = "#"+tag.toString();
							query = "select tweets.tweetID,id,text,date,user from (select tweetID from tweetsTags where  topic = ?) as res inner join tweets on res.tweetID = tweets.tweetID order by date DESC";
						}
						else if (tag.startsWith("user")) {//USER
							//auxTag = "@"+tag.toString();
							query = "select tweets.tweetID,id,text,date,user from (select tweetID from tweetsTags where  user = ?) as res inner join tweets on res.tweetID = tweets.tweetID order by date DESC";
						}
						
						java.sql.Connection conn = null;
						conn = this.borrowClient();
				        PreparedStatement stmt = conn.prepareStatement(query);
				        ResultSet results = null;
				        try {
				        	stmt.setString(1, tag);
				        	results = stmt.executeQuery();

				        	while (results.next())
				        	{	
				        		Map<String,String> tweet = new HashMap<String,String>();
				        		tweet.put("id",results.getString("id"));
				        		tweet.put("text",results.getString("text"));
				        		tweet.put("date",results.getString("date"));
				        		tweet.put("user",results.getString("user"));
				        		result.add(tweet);
				        	}
				        } finally {
				        	try {
				        		stmt.close();
				        	} catch(Exception e) {}
				        	this.releaseClient(conn);
				        }
					//}
				}
				//i++;
			}
			((GetByTagsOperation) op).setResult(result);
			op.notifyListeners();
		}
		
		@SuppressWarnings("unchecked")

		private void deleteOperation(DataStoreOperation op) throws SQLException{
			Comparable key = ((DeleteOperation) op).getKey();
			String tablename = ((DeleteOperation) op).getTableName();
			java.sql.Connection conn = null;
			
			//System.out.println("OP:"+op.toString());

			if(tablename.equals("friendsTimeLine")){
				List<String> pos = ((DeleteOperation) op).getColumns();
				conn = this.borrowClient();
				String query = "delete from "+tablename+" where userID = '"+(String)key+"' and date = ?";
				PreparedStatement stmt = null;

				try {
					stmt = conn.prepareStatement(query);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
					System.out.println("prepare");
				}

				//System.out.println(query);
				//System.out.println(pos.toString());
				for(String str:pos){
					//System.out.println("delete 2");
					try {
						stmt.setString(1, str);
					} catch (SQLException e) {
						System.out.println("setString"+query+"="+str);
					}
					//System.out.println(query+"="+str);
					//System.out.println(stmt.toString());
					try {
						stmt.executeUpdate();
					} catch (SQLException e) {

						//e.printStackTrace();
						System.out.println("EXCEP"+query+"="+str);
						System.out.println("EXCEP"+stmt.toString());
					}
				}

				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				finally{
					this.releaseClient(conn);
				}
			}

			((DeleteOperation) op).setResult(true);
			op.notifyListeners();
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
			return strBuild.toString();
		}
		
		@SuppressWarnings("unused")
		private java.util.UUID toUUID( byte[] uuid )
		{
			long msb = 0;
			long lsb = 0;
			assert uuid.length == 16;
			for (int i=0; i<8; i++)
				msb = (msb << 8) | (uuid[i] & 0xff);
			for (int i=8; i<16; i++)
				lsb = (lsb << 8) | (uuid[i] & 0xff);
			long mostSigBits = msb;
			long leastSigBits = lsb;

			com.eaio.uuid.UUID u = new com.eaio.uuid.UUID(msb,lsb);
			return java.util.UUID.fromString(u.toString());
		}
		private byte[] asByteArray(java.util.UUID uuid) 
		{
			long msb = uuid.getMostSignificantBits();
			long lsb = uuid.getLeastSignificantBits();
			byte[] buffer = new byte[16];

			for (int i = 0; i < 8; i++) {
				buffer[i] = (byte) (msb >>> 8 * (7 - i));
			}
			for (int i = 8; i < 16; i++) {
				buffer[i] = (byte) (lsb >>> 8 * (7 - i));
			}
			return buffer;
		}
	
		private void addTweetsColumns() {
			this.tweetsColumns = "id,text,date,user";
		}
		private void addUserColumns() {
			this.userColumns = "following,followers,username,lastTweet,created,name,password";
		}
		
		private java.sql.Connection borrowClient(){
			java.sql.Connection conn = null;
			try {
				//Class.forName(driver).newInstance();
				//conn = DriverManager.getConnection(url+dbName,userName,password);
				conn = pooled.getConnection();
				//System.out.println("get connection: "+op.toString());
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			return conn;
		}
		private void releaseClient(java.sql.Connection conn){
			try {
				conn.close();
				//System.out.println("Disconnected from database");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		private String listToString(ArrayList<String> list){
			String res = new String();
			Iterator<String> iter = list.iterator();
			res = iter.next();
			while(iter.hasNext()){
				res = res+","+iter.next();
			}
			return res;
		}
		
		private String buildInsertNamesQuery(String keyName, ArrayList<String> columnNames){
			String res = "("+keyName;
			if(!columnNames.isEmpty()){
				res+=",";
				Iterator<String> iter = columnNames.iterator();
				while(iter.hasNext()){
					String columnName = iter.next();
					res += columnName;
					if(iter.hasNext()){
						res+=",";	
					}
					else{
						res+=")";
					}
				}
			}
			else res+=")";
			return res;
		}
		private String buildInsertValuesQuery(String keyName, ArrayList<String> columnNames){
			//System.out.println("SIZE COLUMNVALEUS:"+columnNames.size());
			String res = "('"+keyName;
			if(!columnNames.isEmpty()){
				res+="',";
				Iterator<String> iter = columnNames.iterator();
				while(iter.hasNext()){
					String columnName = iter.next();
					res += "'"+columnName+"'";
					if(iter.hasNext()){
						res+=",";	
					}
					else{
						res+=")";
					}
				}
			}
			else res+=")";
			return res;
		}
		
		private String buildInsertValuesQueryForTweetsTags(String keyName, ArrayList<String> columnNames){
			//System.out.println("SIZE COLUMNVALEUS:"+columnNames.size());
			String res = "('"+keyName;
			if(!columnNames.isEmpty()){
				res+="',?1,?2)";
				Iterator<String> iter = columnNames.iterator();
				while(iter.hasNext()){
					//i++;
					String columnName = iter.next();
					if(columnName.startsWith("topico") ) res = res.replace("?1", "'"+columnName+"'");
					else res = res.replace("?1", "''");
					
					if(columnName.startsWith("user")) res = res.replace("?2", "'"+columnName+"'");
					else res = res.replace("?2", "''");
				}
			}
			else res+="','','')";
			return res;
		}
		
		private String buildUpdateQuery(String key, String value, ArrayList<String> columnNames, ArrayList<String> columnValues){
			String res = "";
			if(!columnNames.isEmpty() && !columnValues.isEmpty()){
				Iterator<String> iterName = columnNames.iterator();
				Iterator<String> iterValue = columnValues.iterator();
				while(iterName.hasNext()){
					String columnName = iterName.next();
					String columnValue = iterValue.next();
					res += columnName+"='"+columnValue+"'";

					if(iterName.hasNext()){
						res+=",";	
					}
				}
				res+=" where "+key+" = "+"'"+value+"'";
			}
			return res;
		}
		private String buildDuplicateNamesQuery(ArrayList<String> columnNames){
			String res = "";// = "("+keyName;
			if(!columnNames.isEmpty()){
				Iterator<String> iter = columnNames.iterator();
				while(iter.hasNext()){
					String columnName = iter.next();
					res += columnName + "=values("+columnName+")";
					if(iter.hasNext()){
						res+=",";	
					}
				}
			}
			return res;
		}
	}
}
