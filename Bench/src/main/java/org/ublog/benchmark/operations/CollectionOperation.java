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
package org.ublog.benchmark.operations;

import org.ublog.benchmark.BenchMarkClient;

public abstract class CollectionOperation extends DataStoreOperation {

	private String tableName;
	private BenchMarkClient client;
	private boolean isInit;

	public CollectionOperation(BenchMarkClient client, String tableName) {
		super();
		this.client = client;
		this.tableName = tableName;
		this.setInit(false);
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public BenchMarkClient getClient() {
		return client;
	}

	public void setClient(BenchMarkClient client) {
		this.client = client;
	}

	public void setInit(boolean isInit) {
		this.isInit = isInit;
	}

	public boolean isInit() {
		return isInit;
	}

}
