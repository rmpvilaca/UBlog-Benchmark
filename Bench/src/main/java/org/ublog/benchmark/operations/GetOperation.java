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

import java.util.List;

import org.ublog.benchmark.BenchMarkClient;

@SuppressWarnings("unchecked")
public class GetOperation<K extends Comparable> extends CollectionOperation {

	private K key;
	private List<String> columns;
	private int count;

	private Object result;

	public GetOperation(BenchMarkClient client, String tableName, K key,
			List<String> columns) {
		super(client, tableName);
		this.key = key;
		this.columns = columns;
	}

	public GetOperation(BenchMarkClient client, String tableName, K key,
			int count) {
		super(client, tableName);
		this.key = key;
		this.count = count;
	}

	public List<String> getColumns() {
		return columns;
	}

	public void setColumns(List<String> columns) {
		this.columns = columns;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "GetOperation [key=" + key + ", getClient()=" + getClient()
				+ ", getTableName()=" + getTableName() + ", result=" + result
				+ "]";
	}

	public void setResult(Object result) {
		this.result = result;
	}

	public Object getResult() {
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		GetOperation<K> other = (GetOperation<K>) obj;
		if (other != null) {
			return this.key.compareTo(other.key) == 0
					&& this.getClient().equals(other.getClient())
					&& this.getTableName().equals(other.getTableName());
		} else
			return false;
	}

}
