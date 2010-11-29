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

import java.util.concurrent.Callable;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.Keyspace;

import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.log4j.Logger;
import org.ublog.utils.Pair;

public class TMultiPutCassFriendsTimeline implements
		Callable<Pair<String, Boolean>> {

	private String key;
	private String value;

	private String keyspace;
	private ColumnPath columnPath;

	private CassandraClientPool connPool;
	private Keyspace twitter;
	private String[] inst;

	private Logger logger = Logger
			.getLogger(TMultiPutCassFriendsTimeline.class);

	public TMultiPutCassFriendsTimeline(String keyspace, String key,
			String value, ColumnPath columnPath, CassandraClientPool connPool,
			String[] inst) {
		this.key = key;
		this.value = value;
		this.keyspace = new String(keyspace);
		this.columnPath = columnPath;
		this.connPool = connPool;
		this.inst = inst;

	}

	@Override
	public Pair<String, Boolean> call() throws Exception {

		CassandraClient clientCass = connPool.borrowClient(inst);
		this.twitter = clientCass.getKeyspace(this.keyspace,
				ConsistencyLevel.ONE);

		try {
			this.twitter.insert(key, this.columnPath, value.getBytes("UTF-8"));
		} finally {
			// return client to pool. do it in a finally block to make sure it's
			// executed
			connPool.releaseClient(clientCass);
		}

		if (logger.isInfoEnabled())
			logger.info("ThreadMultiPut:" + key + ":" + value);
		Boolean res = true;
		return new Pair<String, Boolean>(key, res);
	}
}
