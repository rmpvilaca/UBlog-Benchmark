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

public class GetByTagsOperation<V> extends CollectionOperation {

	private Set<String> tags;
	private Set<V> result;

	public GetByTagsOperation(BenchMarkClient client, String tableName,
			Set<String> tags2) {
		super(client, tableName);
		this.tags = tags2;
	}

	public Set<String> getTags() {
		return tags;
	}

	public void setTags(Set<String> tags) {
		this.tags = tags;
	}

	public void setResult(Set<V> result) {
		this.result = result;
	}

	public Set<V> getResult() {
		return result;
	}

}
