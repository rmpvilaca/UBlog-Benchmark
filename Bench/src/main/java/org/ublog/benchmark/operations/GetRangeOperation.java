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

import java.util.Set;

import org.ublog.benchmark.BenchMarkClient;

@SuppressWarnings("unchecked")
public class GetRangeOperation<K extends Comparable, V> extends
		CollectionOperation {

	private K min;
	private K max;
	private Set<V> result;

	public GetRangeOperation(BenchMarkClient client, String tableName, K min,
			K max) {
		super(client, tableName);
		this.min = min;
		this.max = max;
	}

	public K getMin() {
		return min;
	}

	public void setMin(K min) {
		this.min = min;
	}

	public K getMax() {
		return max;
	}

	public void setMax(K max) {
		this.max = max;
	}

	@Override
	public String toString() {
		return "GetRangeOperation [max=" + max + ", min=" + min
				+ ", getClient()=" + getClient() + ", getTableName()="
				+ getTableName() + "]";
	}

	public void setResult(Set<V> result) {
		this.result = result;
	}

	public Set<V> getResult() {
		return result;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((max == null) ? 0 : max.hashCode());
		result = prime * result + ((min == null) ? 0 : min.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		GetRangeOperation<K, V> other = (GetRangeOperation<K, V>) obj;
		if (other != null) {
			return this.min.compareTo(other.min) == 0
					&& this.max.compareTo(other.max) == 0
					&& this.getClient().equals(other.getClient())
					&& this.getTableName().equals(other.getTableName());
		} else
			return false;
	}

}
