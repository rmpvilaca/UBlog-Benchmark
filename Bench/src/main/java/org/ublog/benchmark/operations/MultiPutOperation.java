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

import org.ublog.utils.Pair;

import org.ublog.benchmark.BenchMarkClient;

@SuppressWarnings("unchecked")
public class MultiPutOperation<K extends Comparable, V> extends
		CollectionOperation {

	private Map<K, Pair<V, Object>> mapKeyToDataAndTags;
	private Map<K, Boolean> mapResults;

	public MultiPutOperation(BenchMarkClient client, String tableName,
			Map<K, Pair<V, Object>> mapKeyToDataAndTags) {
		super(client, tableName);
		this.setMapKeyToDataAndTags(mapKeyToDataAndTags);
	}

	public void setMapResults(Map<K, Boolean> mapResults) {
		this.mapResults = mapResults;
	}

	public Map<K, Boolean> getMapResults() {
		return mapResults;
	}

	public void setMapKeyToDataAndTags(
			Map<K, Pair<V, Object>> mapKeyToDataAndTags) {
		this.mapKeyToDataAndTags = mapKeyToDataAndTags;
	}

	public Map<K, Pair<V, Object>> getMapKeyToDataAndTags() {
		return mapKeyToDataAndTags;
	}

	@Override
	public String toString() {
		return "MultiPutOperation [mapKeyToDataAndTags=" + mapKeyToDataAndTags
				+ ", mapResults=" + mapResults + ", getTableName()="
				+ getTableName() + ", isInit()=" + isInit() + "]";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MultiPutOperation other = (MultiPutOperation) obj;
		if (mapKeyToDataAndTags == null) {
			if (other.mapKeyToDataAndTags != null)
				return false;
		} else if (!mapKeyToDataAndTags.equals(other.mapKeyToDataAndTags))
			return false;
		return true;
	}

}
