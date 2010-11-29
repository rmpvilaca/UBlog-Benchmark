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
public class GetTimeLineAndTweetsOperation<K extends Comparable> extends
		CollectionOperation {

	private String userid;
	private int start;
	private int count;
	private Map<K, Object> result;

	public GetTimeLineAndTweetsOperation(BenchMarkClient client,
			String tableName, String userid, int start, int count) {
		super(client, tableName);
		this.userid = userid;
	}

	public String getUserid() {
		return userid;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return "GetTimeLineOperation [userid=" + userid + ", getClient()="
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
		GetTimeLineAndTweetsOperation<K> other = (GetTimeLineAndTweetsOperation<K>) obj;
		if (other != null) {
			return this.userid.equals(other.userid)
					&& this.getClient().equals(other.getClient())
					&& this.getTableName().equals(other.getTableName());
		} else
			return false;
	}

}
