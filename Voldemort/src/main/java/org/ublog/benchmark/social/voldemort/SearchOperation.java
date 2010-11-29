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
package org.ublog.benchmark.social.voldemort;


import java.util.Map;
import java.util.Set;

import org.ublog.benchmark.BenchOperation;
import org.ublog.benchmark.ComplexBenchMarkClient;
import org.ublog.benchmark.operations.CollectionOperation;
import org.ublog.benchmark.operations.DataStoreOperation;
import org.ublog.benchmark.operations.GetByTagsOperation;
import org.ublog.benchmark.operations.OperationListener;
import org.ublog.benchmark.social.SocialBenchmark;




public class SearchOperation implements BenchOperation, OperationListener{

	private ComplexBenchMarkClient client;
	private Set<String> tags;
	private Set<Map<String, String>> result;
	private boolean start;

	public SearchOperation(ComplexBenchMarkClient client,Set<String> tags2)
	{
		this.client=client;
		this.tags=tags2;
		this.start=false;
	}

	@Override
	public void handleFinishedOperation(DataStoreOperation op) {
		this.result= (Set<Map<String, String>>) ((GetByTagsOperation) op).getResult();
		client.handleFinishedComplexOperation(this);
	}

	@Override
	public String getName() {
		return "twitter:search";
	}


	@Override
	public CollectionOperation getNextDBOperation() {
		start=true;
		CollectionOperation res=new GetByTagsOperation<Comparable>(client, SocialBenchmark.tweetsTable,this.tags);
		res.addListener(this);
		return res;
	}


	@Override
	public boolean hasMoreDBOperations() {
		return !start;
	}


	@Override
	public String toString() {
		return "SearchOperation [tags=" + tags + "]";
	}

	@Override
	public boolean isInit() {
		return false;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public void setInit(boolean init) {
	}

}
