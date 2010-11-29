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
public class DeleteOperation<K extends Comparable> extends CollectionOperation {

	private K key;
	private boolean result;
	private List<byte[]> columns;

	public DeleteOperation(BenchMarkClient client, String tableName, K key,
			List<byte[]> columns) {
		super(client, tableName);
		this.key = key;
		this.columns = columns;
	}

	public List<byte[]> getColumns() {
		return columns;
	}

	public void setColumns(List<byte[]> columns) {
		this.columns = columns;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	@Override
	public String toString() {
		return "GetOperation [key=" + key + ", getClient()=" + getClient()
				+ ", getTableName()=" + getTableName() + "]";
	}

	public void setResult(boolean result) {
		this.result = result;
	}

	public boolean isResult() {
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		DeleteOperation<K> other = (DeleteOperation<K>) obj;
		if (other != null) {
			return this.key.compareTo(other.key) == 0
					&& this.getClient().equals(other.getClient())
					&& this.getTableName().equals(other.getTableName());
		} else
			return false;
	}

}
