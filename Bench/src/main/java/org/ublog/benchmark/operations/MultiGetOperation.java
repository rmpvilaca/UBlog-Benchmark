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

import java.util.Map;
import java.util.Set;

import org.ublog.benchmark.BenchMarkClient;

@SuppressWarnings("unchecked")
public class MultiGetOperation<K extends Comparable> extends
		CollectionOperation {

	private Set<K> keys;
	private Map<K, Object> result;

	public MultiGetOperation(BenchMarkClient client, String tableName,
			Set<K> keys) {
		super(client, tableName);
		this.keys = keys;
	}

	public Set<K> getKeys() {
		return keys;
	}

	public void setKeys(Set<K> keys) {
		this.keys = keys;
	}

	@Override
	public String toString() {
		return "MultiGetOperation [keys=" + keys + ", getClient()="
				+ getClient() + ", getTableName()=" + getTableName()
				+ ", result=" + result + "]";
	}

	public void setResult(Map<K, Object> result) {
		this.result = result;
	}

	public Map<K, Object> getResult() {
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		MultiGetOperation<K> other = (MultiGetOperation<K>) obj;
		if (other != null) {
			return this.keys.equals(other.keys)
					&& this.getClient().equals(other.getClient())
					&& this.getTableName().equals(other.getTableName());
		} else
			return false;
	}

}
