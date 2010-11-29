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

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.Keyspace;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.ublog.utils.Pair;

public class TMultiPutCassUsers implements Callable<Pair<String, Boolean>> {

	private String key;
	private Map<String, String> value;

	private String keyspace;

	private CassandraClientPool connPool;
	private Keyspace twitter;
	private String[] inst;

	private Logger logger = Logger.getLogger(TMultiPutCassUsers.class);

	public TMultiPutCassUsers(String keyspace, String key,
			Map<String, String> value, CassandraClientPool connPool,
			String[] inst) {
		this.key = key;
		this.value = value;
		this.keyspace = keyspace;
		this.connPool = connPool;
		this.inst = inst;
	}

	@Override
	public Pair<String, Boolean> call() {
		CassandraClient clientCass = null;
		List<Mutation> mutationList = new ArrayList<Mutation>();
		long timestamp = System.currentTimeMillis();
		ColumnOrSuperColumn column;
		Mutation m;

		Set<String> keys = value.keySet();
		Iterator<String> iter = keys.iterator();
		String columnName;
		while (iter.hasNext()) {
			columnName = iter.next();
			column = new ColumnOrSuperColumn();
			column.column = new Column();
			try {
				column.column.name = columnName.getBytes("UTF-8");
				column.column.value = value.get(columnName).getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			column.column.timestamp = timestamp;
			m = new Mutation();
			m.column_or_supercolumn = column;
			mutationList.add(m);
		}
		Map<String, List<Mutation>> mapPut = new HashMap<String, List<Mutation>>();
		mapPut.put("users", mutationList);
		Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>();
		mutationMap.put((String) key, mapPut);

		try {
			// CONECTION
			clientCass = connPool.borrowClient(inst);
			this.twitter = clientCass.getKeyspace(this.keyspace,
					ConsistencyLevel.ONE);

			// /INSERTION
			this.twitter.batchMutate(mutationMap);
		} catch (InvalidRequestException e) {
			e.printStackTrace();
		} catch (UnavailableException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} catch (TimedOutException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// return client to pool. do it in a finally block to make sure it's
			// executed
			try {
				connPool.releaseClient(clientCass);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (logger.isInfoEnabled())
			logger.info("ThreadMultiPut:" + key + ":" + value);
		Boolean res = true;
		return new Pair<String, Boolean>(key, res);
	}

}
