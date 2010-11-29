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
import org.ublog.benchmark.operations.GetOperation;
import org.ublog.benchmark.operations.OperationListener;
import org.ublog.benchmark.social.SocialBenchmark;
import org.ublog.benchmark.social.UserService;
import org.ublog.benchmark.social.Utils;



public class GetFollowersOperation implements BenchOperation,OperationListener{

	private ComplexBenchMarkClient client;
	private String userId;
	private Map<String, String> result;
	private boolean start;

	public GetFollowersOperation(ComplexBenchMarkClient client,String userId)
	{
		this.client=client;
		this.userId=userId;
		this.start=false;
	}


	@Override
	public void handleFinishedOperation(DataStoreOperation op) {
		this.result= (Map<String, String>) ((GetOperation) op).getResult();
		client.handleFinishedComplexOperation(this);//escrever o log com os tempos

	}

	public Set<String> getResult()
	{
		return Utils.asSet(this.result.get(UserService.FOLLOWERS));
	}


	@Override
	public String getName() {
		return "twitter:getFollowers";
	}


	@Override
	public CollectionOperation getNextDBOperation() {
		start=true;
		CollectionOperation res=new GetOperation<Comparable>(client, SocialBenchmark.userTable,this.userId,250);
		res.addListener(this);
		return res;
	}


	@Override
	public boolean hasMoreDBOperations() {
		return !start;
	}


	@Override
	public String toString() {
		return "GetFollowersOperation [userId=" + userId + "]";
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
