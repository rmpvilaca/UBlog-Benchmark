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

import org.ublog.utils.Pair;
import org.ublog.benchmark.BenchMarkClient;

@SuppressWarnings("unchecked")
public class PutOperation<K extends Comparable, V> extends CollectionOperation {

	private K key;
	private V data;
	private Set<String> tags;
	private boolean result;
	private Pair<String, String> toAddToTimeline;

	public PutOperation(BenchMarkClient client, String tableName, K key,
			V value, Set<String> tags2) {
		super(client, tableName);
		this.key = key;
		this.data = value;
		this.tags = tags2;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getData() {
		return data;
	}

	public void setData(V value) {
		this.data = value;
	}

	public Set<String> getTags() {
		return tags;
	}

	public void setTags(Set<String> tags) {
		this.tags = tags;
	}

	public Pair<String, String> getToAddToTimeline() {
		return toAddToTimeline;
	}

	public void setToAddToTimeline(Pair<String, String> toAddToTimeline) {
		this.toAddToTimeline = toAddToTimeline;
	}

	@Override
	public String toString() {
		return "PutOperation [key=" + key + ", tags=" + tags + ", value="
				+ data + "]";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PutOperation<K, V> other = (PutOperation<K, V>) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (tags == null) {
			if (other.tags != null)
				return false;
		} else if (!tags.equals(other.tags))
			return false;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		return true;
	}

	public void setResult(boolean result) {
		this.result = result;
	}

	public boolean isResult() {
		return result;
	}

}
