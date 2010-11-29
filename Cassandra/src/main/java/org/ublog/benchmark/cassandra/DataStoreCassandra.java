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
package org.ublog.benchmark.cassandra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
import java.util.concurrent.TimeUnit;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ExhaustedPolicy;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.service.PoolExhaustedException;
import me.prettyprint.cassandra.service.TimestampResolution;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.ublog.utils.Pair;
import org.ublog.benchmark.DataStore;
import org.ublog.benchmark.operations.*;

public class DataStoreCassandra implements DataStore {

	private String keyspace;
	private String partitioner = "ordered";
	private CassandraClientPool connPool;
	private String[] inst;
	
	private int max_active_connections=10;

	private Logger logger = Logger.getLogger(DataStoreCassandra.class);

	@Override
	public Connection createConnection() throws UnsupportedEncodingException,
	UnknownHostException {
		return new Connection().createConnections();
	}

	public void initialize() throws Exception {
		this.keyspace = "Twitter";

		Configuration conf = new PropertiesConfiguration("cassandra.properties");
		String[] nodes = conf.getStringArray("node");
		this.max_active_connections=conf.getInt("maxActiveConnections");
		this.partitioner=conf.getString("partitioner");
		this.inst =new String[nodes.length];
		for(int i=0;i<nodes.length;i++)
		{
			String[] split=nodes[i].split(":");
			if (split.length==2)
			{
				int port=new Integer(split[1]);
				this.inst[i]=InetAddress.getByName(split[0]).getHostName()+":"+port;
			}
			else
			{
				logger.error("Nodes configuration is wrong");
				throw new ConfigurationException("Nodes must be in the format hostName:port");
			}
		}
		
		String hosts = "";
		int i = 0;
		for (String str : inst) {
			if (i < this.inst.length - 1) {
				hosts += str + ",";
			} else {
				hosts += str;
			}
			i++;
		}
		System.out.println("HOSTS: " + hosts);
		CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator(
				hosts);

		cassandraHostConfigurator.setMaxActive(this.max_active_connections);
		cassandraHostConfigurator.setMaxIdle(CassandraHost.DEFAULT_MAX_IDLE);
		cassandraHostConfigurator
		.setExhaustedPolicy(ExhaustedPolicy.WHEN_EXHAUSTED_BLOCK);
		cassandraHostConfigurator
		.setMaxWaitTimeWhenExhausted(CassandraHost.DEFAULT_MAX_WAITTIME_WHEN_EXHAUSTED);
		cassandraHostConfigurator.setTimestampResolution("MICROSECONDS");
		System.out.println(cassandraHostConfigurator.toString());
		this.connPool = CassandraClientPoolFactory.getInstance().createNew(
				cassandraHostConfigurator);
	}

	public void finalclose() {
		System.out.println("CLOSEALL");
	}

	class Connection implements DataStore.Connection {
		CassandraClient clientCass;
		private Keyspace twitter;
		private ExecutorService pool;
		private List<byte[]> tweetsColumns;
		private List<byte[]> userColumns;

		@Override
		public Connection createConnections()
		throws UnsupportedEncodingException, UnknownHostException {
			this.pool = Executors.newFixedThreadPool(5);
			this.addTweetsColumns();
			this.addUserColumns();
			return this;
		}

		@Override
		public void close() {
			try {
				this.pool.shutdown();
				this.pool.awaitTermination(1, TimeUnit.DAYS);
			} catch (InterruptedException e) {
				logger.warn("Benchmark clients timeout expired.");
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void executeOperation(DataStoreOperation op) throws Exception {
			if (op instanceof GetOperation) {
				this.getOperation(op);
			}

			else if (op instanceof MultiGetOperation) {
				this.multiGetOperation(op);
			}

			else if (op instanceof PutOperation) {
				this.putOperation(op);
			}

			else if (op instanceof MultiPutOperation) {
				this.multiPutOperation(op);
			}

			else if (op instanceof GetRangeOperation) {
				this.getRangeOperation(op);
			}

			else if (op instanceof GetByTagsOperation) {
				this.getByTagsOperation(op);
			}

			else if (op instanceof DeleteOperation) {
				this.deleteOperation(op);
			}
		}

		@SuppressWarnings("unchecked")
		private void getOperation(DataStoreOperation op)
		throws InvalidRequestException, UnavailableException,
		TimedOutException, TException, UnsupportedEncodingException,
		NotFoundException {
			ColumnParent parent;
			SlicePredicate predicate = new SlicePredicate();
			Comparable key = ((GetOperation) op).getKey();
			String tablename = ((GetOperation) op).getTableName();

			if (tablename.equals("friendsTimeLine")) {
				int count = ((GetOperation) op).getCount();
				predicate.slice_range = new SliceRange(new byte[0],
						new byte[0], true, count);
				parent = new ColumnParent("friendsTimeLine");

				this.borrowClient();
				List<Column> results = this.twitter.getSlice((String) key,
						parent, predicate);
				this.releaseClient();
				List<String> res = new ArrayList<String>(results.size());
				for (Column result : results) {
					res.add(new String(result.value, "UTF-8"));
				}
				((GetOperation) op).setResult(res);
				op.notifyListeners();
			} else {
				if (tablename.equals("users")) {
					List<byte[]> columns = ((GetOperation) op).getColumns();
					if (columns.isEmpty())
						columns = this.userColumns; // /////SE FOR VAZIO TRAZ
					// COLUNAS TODAS
					Map<String, String> value = new HashMap<String, String>();
					predicate.column_names = columns;
					parent = new ColumnParent("users");
					this.borrowClient();
					List<Column> results = this.twitter.getSlice((String) key,
							parent, predicate);
					this.releaseClient();
					for (Column result : results) {
						value.put(new String(result.name, "UTF-8"), new String(
								result.value, "UTF-8"));
					}
					((GetOperation) op).setResult((Map<String, String>) value);
					op.notifyListeners();
				}
			}
		}

		@SuppressWarnings("unchecked")
		private void multiGetOperation(DataStoreOperation op)
		throws InvalidRequestException, UnavailableException,
		TimedOutException, TException, UnsupportedEncodingException {
			String key;
			ColumnParent parent;
			SlicePredicate predicate = new SlicePredicate();
			Set<String> keys = ((MultiGetOperation) op).getKeys();
			List<String> keysAsList = new ArrayList(keys);
			String tablename = ((MultiGetOperation) op).getTableName();

			if (tablename.equals("tweets")) {
				// CASSANDRA READY
				Map<String, String> value;
				predicate.column_names = this.tweetsColumns;
				parent = new ColumnParent(tablename);

				this.borrowClient();
				Map<String, List<Column>> sliceRes = this.twitter
				.multigetSlice(keysAsList, parent, predicate);
				this.releaseClient();

				HashMap<String, Map<String, String>> result = new HashMap<String, Map<String, String>>();
				Set<String> keysRes = sliceRes.keySet();
				Iterator<String> iter = keysRes.iterator();
				while (iter.hasNext()) {
					key = iter.next();
					List<Column> results = sliceRes.get(key);
					value = new HashMap<String, String>();
					for (Column columns : results) {
						value.put(new String(columns.name, "UTF-8"),
								new String(columns.value, "UTF-8"));
					}
					result.put(key, value);
				}
				((MultiGetOperation) op).setResult(result);
				op.notifyListeners();
			}
		}

		@SuppressWarnings("unchecked")
		private void putOperation(DataStoreOperation op)
		throws UnsupportedEncodingException, InvalidRequestException,
		UnavailableException, TimedOutException, TException {
			long timestamp;
			boolean res = false;
			Comparable key = ((PutOperation) op).getKey();
			String tablename = ((PutOperation) op).getTableName();

			if (tablename.equals("friendsTimeLine")) { // SERA QUASE UM MULTIPUT
				// DE TIMELINE
				ColumnOrSuperColumn column;
				Mutation m;
				timestamp = System.currentTimeMillis();
				List<Mutation> mutationList = new ArrayList<Mutation>();

				List<Pair<String, String>> value = (List<Pair<String, String>>) ((PutOperation) op)
				.getData();
				for (Pair<String, String> pair : value) {
					String columnName = pair.getFirst();
					String tweetID = pair.getSecond();

					column = new ColumnOrSuperColumn();
					column.column = new Column();
					column.column.name = this.asByteArray(java.util.UUID
							.fromString(columnName));
					column.column.value = tweetID.getBytes("UTF-8");
					column.column.timestamp = timestamp;
					m = new Mutation();
					m.column_or_supercolumn = column;
					mutationList.add(m);
				}
				Map<String, List<Mutation>> mapPut = new HashMap<String, List<Mutation>>();
				mapPut.put("friendsTimeLine", mutationList);
				Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>();
				mutationMap.put((String) key, mapPut);

				this.borrowClient();
				this.twitter.batchMutate(mutationMap);
				this.releaseClient();

				res = true;
				((PutOperation) op).setResult(res);
				op.notifyListeners();
			} else {
				if (tablename.equals("users")) {
					Map<String, String> value;
					ColumnOrSuperColumn column;
					Mutation m;
					timestamp = System.currentTimeMillis();
					List<Mutation> mutationList = new ArrayList<Mutation>();
					value = (Map<String, String>) ((PutOperation) op).getData();

					Set<String> keys = value.keySet();
					Iterator<String> iter = keys.iterator();
					String columnName;
					while (iter.hasNext()) {
						columnName = iter.next();
						column = new ColumnOrSuperColumn();
						column.column = new Column();
						column.column.name = columnName.getBytes("UTF-8");
						column.column.value = value.get(columnName).getBytes(
						"UTF-8");
						column.column.timestamp = timestamp;
						m = new Mutation();
						m.column_or_supercolumn = column;
						mutationList.add(m);
					}
					Map<String, List<Mutation>> mapPut = new HashMap<String, List<Mutation>>();
					mapPut.put("users", mutationList);
					Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>();
					mutationMap.put((String) key, mapPut);
					this.borrowClient();
					this.twitter.batchMutate(mutationMap);
					this.releaseClient();
					res = true;
				} else {
					Map<String, String> value;
					ColumnOrSuperColumn column;
					Mutation m;
					timestamp = System.currentTimeMillis();
					List<Mutation> mutationList = new ArrayList<Mutation>();
					List<Column> columnsForTags = new ArrayList<Column>(); // /TAGS
					Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>();
					value = (Map<String, String>) ((PutOperation) op).getData();

					Set<String> keys = value.keySet();
					Iterator<String> iter = keys.iterator();
					String columnName;
					while (iter.hasNext()) {
						columnName = iter.next();
						column = new ColumnOrSuperColumn();
						column.column = new Column();
						column.column.name = columnName.getBytes("UTF-8");
						column.column.value = value.get(columnName).getBytes(
						"UTF-8");
						column.column.timestamp = timestamp;
						m = new Mutation();
						m.column_or_supercolumn = column;
						mutationList.add(m);// TWEETS
						columnsForTags.add(column.column); // /TAGS
					}

					Map<String, List<Mutation>> mapPut = new HashMap<String, List<Mutation>>();
					mapPut.put("tweets", mutationList);
					mutationMap.put((String) key, mapPut);

					// //INSERT IN TWEET TAGS
					Set<String> tags = ((PutOperation) op).getTags();
					int i = 0;
					String auxTag;

					List<Mutation> mutationListTags = new ArrayList<Mutation>();
					ColumnOrSuperColumn supercolumn = new ColumnOrSuperColumn();
					supercolumn.super_column = new SuperColumn();
					supercolumn.super_column.name = ((String) key)
					.getBytes("UTF-8");
					supercolumn.super_column.setColumns(columnsForTags);
					Map<String, List<Mutation>> mapTags = new HashMap<String, List<Mutation>>();
					m = new Mutation();
					m.column_or_supercolumn = supercolumn;
					mutationListTags.add(m);
					mapTags.put("tweetsTags", mutationListTags);
					for (String tag : tags) {
						if (tag != null) {
							mutationMap.put(tag, mapTags);
							/*
							 * if(i==0){ } else { if(i==1) auxTag =
							 * "#"+tag.toString();//TOPIC else auxTag =
							 * "@"+tag.toString();//USER mutationMap.put(auxTag,
							 * mapTags); }
							 */
						}
						i++;
					}
					// ///FINISH TAGS

					this.borrowClient();
					// System.out.println("MutationMap PUTOP SIZE:"+mutationMap.size());
					this.twitter.batchMutate(mutationMap);// INSERT IN
					// TWEETSTAGS AND
					// TWEETS
					this.releaseClient();
					res = true;
				}
				((PutOperation) op).setResult(res);
				op.notifyListeners();
			}
		}

		@SuppressWarnings("unchecked")
		private void multiPutOperation(DataStoreOperation op) {
			Set<Future<Pair<String, Boolean>>> set = new HashSet<Future<Pair<String, Boolean>>>();
			Map<String, Boolean> mapResults = new HashMap<String, Boolean>();
			String tablename = ((MultiPutOperation) op).getTableName();

			if (tablename.equals("friendsTimeLine")) {
				Map<String, Pair<String, String>> map = ((MultiPutOperation) op)
				.getMapKeyToDataAndTags();
				Set<String> keys = map.keySet();
				Iterator<String> iter = keys.iterator();
				String key;
				while (iter.hasNext()) {
					key = iter.next();

					Pair<String, String> pair = map.get(key);
					String columnName = pair.getFirst();
					String value = pair.getSecond();

					ColumnPath cp = new ColumnPath();
					cp.column_family = tablename;
					cp.column = this.asByteArray(java.util.UUID
							.fromString(columnName));

					Callable<Pair<String, Boolean>> callable = new TMultiPutCassFriendsTimeline(
							keyspace, key, value, cp, connPool, inst);
					Future<Pair<String, Boolean>> future = this.pool
					.submit(callable);
					set.add(future);
				}
				for (Future<Pair<String, Boolean>> future : set) {
					try {
						Pair<String, Boolean> aux = future.get();
						mapResults.put(aux.getFirst(), aux.getSecond());
					} catch (InterruptedException e) {
						e.printStackTrace();
					} catch (ExecutionException e) {
						e.printStackTrace();
					}
				}
				((MultiPutOperation) op).setMapResults(mapResults);
				op.notifyListeners();

			} else {
				if (tablename.equals("users")) {
					Map<String, Pair<Map<String, String>, List<String>>> map = ((MultiPutOperation) op)
					.getMapKeyToDataAndTags();
					Set<String> keys = map.keySet();
					Iterator<String> iter = keys.iterator();
					String key;
					while (iter.hasNext()) {
						key = iter.next();
						Pair<Map<String, String>, List<String>> hash = map
						.get(key);
						Map<String, String> value = hash.getFirst();
						Callable<Pair<String, Boolean>> callable = new TMultiPutCassUsers(
								keyspace, key, value, connPool, inst);
						Future<Pair<String, Boolean>> future = this.pool
						.submit(callable);
						set.add(future);
					}
					for (Future<Pair<String, Boolean>> future : set) {
						try {
							Pair<String, Boolean> aux = future.get();
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
		private void getRangeOperation(DataStoreOperation op)
		throws InvalidRequestException, UnavailableException,
		TimedOutException, TException, UnsupportedEncodingException {// /////////////POR
			// ISTO
			// EM
			// PARALELO
			// COMO
			// O
			// MULIPUT
			Comparable keyMax = ((GetRangeOperation) op).getMax();
			Comparable keyMin = ((GetRangeOperation) op).getMin();
			String tablename = ((GetRangeOperation) op).getTableName();

			ColumnParent parent;
			int idMax = this.getTweetID((String) keyMax);
			int idMin = this.getTweetID((String) keyMin);
			String userID = this.getUserID((String) keyMax);
			if (partitioner.equals("ordered")) {

				// ////ORDERED PARTITIONER
				if (tablename.equals("tweets")) {
					// /CASSSANDRA READY
					String key;
					Map<String, String> value;
					Set<Map<String, String>> result = new HashSet<Map<String, String>>();
					SlicePredicate predicate = new SlicePredicate();
					predicate.column_names = this.tweetsColumns;
					parent = new ColumnParent("tweets");

					this.borrowClient();
					Map<String, List<Column>> keySlice = this.twitter
					.getRangeSlice(parent, predicate, (String) keyMin,
							(String) keyMax, idMax - idMin + 1);
					this.releaseClient();
					Set<String> keys = keySlice.keySet();
					Iterator<String> iter = keys.iterator();

					while (iter.hasNext()) {
						key = iter.next();
						List<Column> results = keySlice.get(key);
						value = new HashMap<String, String>();
						for (Column columns : results) {
							value.put(new String(columns.name, "UTF-8"),
									new String(columns.value, "UTF-8"));
						}
						result.add(value);
					}
					((GetRangeOperation) op).setResult(result);
					op.notifyListeners();
				}
			} else if (partitioner.equals("random")) {
				// ////RANDOM PARTITIONER
				if (tablename.equals("tweets")) {
					// CASSANDRA READY
					List<String> keysAsList = new ArrayList<String>();
					Map<String, String> value;
					Set<Map<String, String>> result = new HashSet<Map<String, String>>();

					for (int i = idMin; i <= idMax; i++) {
						keysAsList.add(new String(userID + "-"
								+ this.getTweetPadding(i)));
					}
					SlicePredicate predicate = new SlicePredicate();
					predicate.column_names = this.tweetsColumns;
					parent = new ColumnParent("tweets");

					this.borrowClient();
					Map<String, List<Column>> sliceRes = this.twitter
					.multigetSlice(keysAsList, parent, predicate);
					this.releaseClient();

					Set<String> keysRes = sliceRes.keySet();
					Iterator<String> iter = keysRes.iterator();
					while (iter.hasNext()) {
						String key = iter.next();
						List<Column> results = sliceRes.get(key);
						value = new HashMap<String, String>();
						for (Column columns : results) {
							value.put(new String(columns.name, "UTF-8"),
									new String(columns.value, "UTF-8"));
						}
						result.add(value);
					}
					((GetRangeOperation) op).setResult(result);
					op.notifyListeners();
				}
			}
		}

		@SuppressWarnings("unchecked")
		private void getByTagsOperation(DataStoreOperation op)
		throws InvalidRequestException, UnavailableException,
		TimedOutException, TException, UnsupportedEncodingException,
		NotFoundException {
			Set<String> tags = ((GetByTagsOperation) op).getTags();
			Set<Map<String, String>> result = new HashSet<Map<String, String>>();
			// int i=0;
			String auxTag;
			for (String tag : tags) {
				if (tag != null) {
					/*
					 * if(i==0){ } else {
					 */
					/*
					 * if(i==1) auxTag = "#"+tag.toString();//TOPIC else auxTag
					 * = "@"+tag.toString();//USER
					 */
					SlicePredicate predicate = new SlicePredicate();
					predicate.slice_range = new SliceRange(new byte[0],
							new byte[0], false, 10000);// NR ARBITRARIAMENTE
					// GRANDE(10000 =
					// MAX_NTWEETS) PARA
					// TRAZER TODAS AS
					// SUPERCOLUNAS
					ColumnParent parent = new ColumnParent("tweetsTags");

					this.borrowClient();
					List<SuperColumn> results = this.twitter.getSuperSlice(tag,
							parent, predicate);
					this.releaseClient();

					for (SuperColumn res : results) {
						Map<String, String> tweet = new HashMap<String, String>();
						List<Column> columns = res.columns;
						for (Column c : columns) {
							tweet.put(new String(c.name, "UTF-8"), new String(
									c.value, "UTF-8"));
						}
						result.add(tweet);
					}
					if (result != null) {
						// System.out.println("RES getByTags:"+result);
						// System.out.println("RES getByTags for tag:"+tag+" Count:"+result.size());
					}

					// }
				}
				// i++;
			}
			((GetByTagsOperation) op).setResult(result);
			op.notifyListeners();
		}

		@SuppressWarnings("unchecked")
		private void deleteOperation(DataStoreOperation op)
		throws InvalidRequestException, UnavailableException,
		TimedOutException, TException {
			Comparable key = ((DeleteOperation) op).getKey();
			String tablename = ((DeleteOperation) op).getTableName();

			if (tablename.equals("friendsTimeLine")) {
				List<byte[]> columns = ((DeleteOperation) op).getColumns();
				for (byte[] column : columns) {
					this.borrowClient();
					ColumnPath cp = new ColumnPath();
					cp.column_family = tablename;
					cp.column = column;
					this.twitter.remove((String) key, cp);
					this.releaseClient();
				}
			}
			((DeleteOperation) op).setResult(true);
			op.notifyListeners();
		}

		private int getTweetID(String tweetID) {
			String split[] = tweetID.split("-");
			return new Integer(split[1]);
		}

		private String getUserID(String tweetID) {
			String split[] = tweetID.split("-");
			return split[0];
		}

		private String getTweetPadding(int tweetIdx) { // copiado do Utils.java
			int MaxNTweets = 10000;
			StringBuilder strBuild = new StringBuilder();
			int current = (int) Math.floor(Math.log10(tweetIdx)) + 1;
			int expected = (int) Math.floor(Math.log10(MaxNTweets));
			if (tweetIdx == 0)
				current = 1;
			for (int i = 0; i < (expected - current); i++)
				strBuild.append(0);
			strBuild.append(tweetIdx);
			return strBuild.toString();
		}

		@SuppressWarnings("unused")
		private java.util.UUID toUUID(byte[] uuid) {
			long msb = 0;
			long lsb = 0;
			assert uuid.length == 16;
			for (int i = 0; i < 8; i++)
				msb = (msb << 8) | (uuid[i] & 0xff);
			for (int i = 8; i < 16; i++)
				lsb = (lsb << 8) | (uuid[i] & 0xff);
			long mostSigBits = msb;
			long leastSigBits = lsb;

			com.eaio.uuid.UUID u = new com.eaio.uuid.UUID(msb, lsb);
			return java.util.UUID.fromString(u.toString());
		}

		private byte[] asByteArray(java.util.UUID uuid) {
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

		private void addTweetsColumns() throws UnsupportedEncodingException {
			this.tweetsColumns = new ArrayList<byte[]>();
			this.tweetsColumns.add("id".getBytes("UTF-8"));
			this.tweetsColumns.add("text".getBytes("UTF-8"));
			this.tweetsColumns.add("date".getBytes("UTF-8"));
			this.tweetsColumns.add("user".getBytes("UTF-8"));
		}

		private void addUserColumns() throws UnsupportedEncodingException {
			this.userColumns = new ArrayList<byte[]>();
			this.userColumns.add("following".getBytes("UTF-8"));
			this.userColumns.add("followers".getBytes("UTF-8"));
			this.userColumns.add("username".getBytes("UTF-8"));
			this.userColumns.add("lastTweet".getBytes("UTF-8"));
			this.userColumns.add("created".getBytes("UTF-8"));
			this.userColumns.add("name".getBytes("UTF-8"));
			this.userColumns.add("password".getBytes("UTF-8"));
		}

		private void borrowClient() {
			try {
				this.clientCass = connPool.borrowClient(inst);
				this.twitter = clientCass.getKeyspace(keyspace,
						ConsistencyLevel.ONE); // CONSISTENCY LEVEL
			} catch (IllegalStateException e1) {
				e1.printStackTrace();
			} catch (PoolExhaustedException e1) {
				e1.printStackTrace();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}

		private void releaseClient() {
			try {
				this.clientCass = this.twitter.getClient();
				connPool.releaseClient(this.clientCass);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
