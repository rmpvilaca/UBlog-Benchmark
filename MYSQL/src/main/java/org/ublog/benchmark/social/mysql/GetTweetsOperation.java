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
package org.ublog.benchmark.social.mysql;


import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ublog.benchmark.BenchOperation;
import org.ublog.benchmark.ComplexBenchMarkClient;
import org.ublog.benchmark.operations.CollectionOperation;
import org.ublog.benchmark.operations.DataStoreOperation;
import org.ublog.benchmark.operations.GetOperation;
import org.ublog.benchmark.operations.GetRangeOperation;
import org.ublog.benchmark.operations.OperationListener;
import org.ublog.benchmark.social.Message;
import org.ublog.benchmark.social.SocialBenchmark;
import org.ublog.benchmark.social.UserService;
import org.ublog.benchmark.social.Utils;


public class GetTweetsOperation implements BenchOperation, OperationListener{

	private ComplexBenchMarkClient client;
	private String userId;
	private Map<String, String> user;
	private List<Message> tweetList;
	private int start,count;
	private boolean finished;

	public GetTweetsOperation(ComplexBenchMarkClient client,String userId, int start, int count)
	{
		this.client=client;
		this.userId=userId;
		this.start=start;
		this.count=count;
		this.finished=false;
		this.user=null;
		this.tweetList=new ArrayList<Message>();
	}

	@Override
	public String getName() {
		return "twitter:getTweets";
	}

	public List<Message> getResult()
	{
		return this.tweetList;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CollectionOperation getNextDBOperation() throws UnsupportedEncodingException {
		CollectionOperation res;
		if (this.user==null)
		{
			List<String> columns = new ArrayList<String>();
			columns.add("lastTweet");
			res=new GetOperation(client,SocialBenchmark.userTable,userId,columns);
		}
		else
		{
			int lastTweet = Integer.valueOf(this.user.get(UserService.LAST_TWEET));
			String min=userId + "-" + Utils.getTweetPadding(Math.max(1,lastTweet - this.start-this.count+1));
			String max=userId + "-" +   Utils.getTweetPadding(Math.max(1,lastTweet - this.start));
			res=new GetRangeOperation(client,SocialBenchmark.tweetsTable,min,max);
			finished=true;
		}
		res.addListener(this);
		return res;
	}

	@Override
	public boolean hasMoreDBOperations() {
		return !finished;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleFinishedOperation(DataStoreOperation op) {
		if (this.user==null)
		{
			this.user=((Map<String, String>) ((GetOperation) op).getResult());
		}
		else
		{
			for(Map<String,String> tweetMap:((Set<Map<String, String>>) ((GetRangeOperation) op).getResult())){
				tweetList.add(Utils.toTweet(tweetMap));
			}
			this.client.handleFinishedComplexOperation(this);
		}

	}

	@Override
	public String toString() {
		return "GetTweetsOperation [count=" + count + ", start=" + start
				+ ", userId=" + userId + "]";
	}
	
	@Override
	public boolean isReadOnly() {
		return true;
	}

	@Override
	public boolean isInit() {
		return false;
	}

	@Override
	public void setInit(boolean init) {
	}

}
