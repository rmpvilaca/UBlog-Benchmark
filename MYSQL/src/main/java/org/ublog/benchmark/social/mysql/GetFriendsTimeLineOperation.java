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


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.ublog.benchmark.BenchOperation;
import org.ublog.benchmark.ComplexBenchMarkClient;
import org.ublog.benchmark.operations.CollectionOperation;
import org.ublog.benchmark.operations.DataStoreOperation;
import org.ublog.benchmark.operations.GetTimeLineAndTweetsOperation;
import org.ublog.benchmark.operations.OperationListener;
import org.ublog.benchmark.social.Message;
import org.ublog.benchmark.social.SocialBenchmark;
import org.ublog.benchmark.social.Utils;



public class GetFriendsTimeLineOperation implements BenchOperation,OperationListener{


	private Logger logger= Logger.getLogger(GetFriendsTimeLineOperation.class);

	private ComplexBenchMarkClient client;
	private String userId;
	private List<Message> tweetList;
	private List<String> timeline;
	private int start;
	private int count;

	public GetFriendsTimeLineOperation(ComplexBenchMarkClient client,String userId,int start,int count)
	{
		this.client=client;
		this.userId=userId;
		this.count=count;
		this.timeline=null;
		this.tweetList = null;
		this.start=start;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleFinishedOperation(DataStoreOperation op) {

		Map<Comparable,Object> res=((GetTimeLineAndTweetsOperation<Comparable>) op).getResult();
		this.tweetList=new ArrayList<Message>(res.size());
		for(Comparable key:res.keySet())
		{
			this.tweetList.add(Utils.toTweet((Map<String,String>) res.get(key)));
		}


		this.client.handleFinishedComplexOperation(this);
		if (logger.isInfoEnabled())
			logger.info("Result of GetFriendsTimeline with userId:"+this.userId+", start:"+start+", and count:"+this.count/*+" is:"+this.tweetList*/);

	}

	public List<Message> getResult()
	{
		return this.tweetList;
	}

	@Override
	public String getName() {
		return "twitter:getFriendsTimeLine";
	}


	@SuppressWarnings("unchecked")
	@Override
	public CollectionOperation getNextDBOperation() {
		CollectionOperation res=null;
		res=new GetTimeLineAndTweetsOperation<String>(client,SocialBenchmark.tweetsTable,this.userId,this.start,this.count);
		res.addListener(this);
		return res;
	}

	@Override
	public boolean hasMoreDBOperations() {
		return this.tweetList==null;
	}

	@Override
	public String toString() {
		return "GetFriendsTimeLineOperation [count=" + count
		+ ", timelineIndex=" + start + ", userId=" + userId
		+ "]";
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
