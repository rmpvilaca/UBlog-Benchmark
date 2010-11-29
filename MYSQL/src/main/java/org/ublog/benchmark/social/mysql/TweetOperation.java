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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.ublog.utils.Pair;
import org.ublog.benchmark.BenchOperation;
import org.ublog.benchmark.ComplexBenchMarkClient;
import org.ublog.benchmark.operations.CollectionOperation;
import org.ublog.benchmark.operations.DataStoreOperation;
import org.ublog.benchmark.operations.GetOperation;
import org.ublog.benchmark.operations.MultiPutOperation;
import org.ublog.benchmark.operations.OperationListener;
import org.ublog.benchmark.operations.PutOperation;
import org.ublog.benchmark.social.Message;
import org.ublog.benchmark.social.SocialBenchmark;
import org.ublog.benchmark.social.UserService;
import org.ublog.benchmark.social.Utils;

public class TweetOperation implements BenchOperation,OperationListener{
	private Logger logger= Logger.getLogger(TweetOperation.class);

	private enum Phase {USER,TWEET,FOLLOWERS};

	private Phase phase;
	private boolean get;
	private boolean finish;
	private ComplexBenchMarkClient client;
	private Message tweet;
	private Map<String, String> tweetMap;

	private Map<String, String> user;
	private String tweetId;
	private Set<String> followers;

	private Map<String, Pair<List<String>, List<Integer>>> mapTimeLine;
	private Map<String, Pair<String,Object>> tweetFriendsTimeline;
	private Set<String> tags;

	private boolean init;

	public TweetOperation(ComplexBenchMarkClient client,Message tweet,Set<String> tags2)
	{
		this.client=client;
		this.tweet=tweet;
		this.phase=Phase.USER;
		this.get=false;
		this.finish=false;
		this.user=null;
		this.mapTimeLine=null;
		this.tags=tags2;
		this.setInit(false);

		logger.setLevel(Level.OFF);
	}


	private void insertTweet() {
		String dateStr = tweetMap.get(Utils.DATE);
		Long date = new Long(dateStr);

		String timelineId = tweetId + ":" + dateStr;

		cycle://////
			for(String key:this.mapTimeLine.keySet())
			{
				List<String> timeLine=this.mapTimeLine.get(key).getFirst();
				for (int i = 0; i < timeLine.size(); i++) {
					String id =timeLine.get(i/*0*/);
					String[] split = id.split(":");
					Long timelineDate = new Long(split[1]);

					if (timelineDate < date) {
						timeLine.add(i, timelineId);
						continue cycle;//////
						//return;
					}
				}
				timeLine.add(timelineId);
			}

	}

	@SuppressWarnings("unchecked")
	private void setTweetToFriendsTimeLine(){
		String dateStr = tweetMap.get(Utils.DATE);
		String timelineId = tweetId + ":" + dateStr;

		this.tweetFriendsTimeline = new HashMap<String,Pair<String,Object>>();
		for(String follower:this.followers){
			this.tweetFriendsTimeline.put(follower,new Pair(dateStr, tweetId));
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleFinishedOperation(DataStoreOperation op) {
		switch(this.phase)
		{
		case USER:
			if (this.user==null)
			{
				this.user=(Map<String,String>) ((GetOperation) op).getResult();
				if (this.user==null)
				{
					this.finish=true;
				}
				else
				{
					this.followers =Utils.asSet(user.get(UserService.FOLLOWERS));
					//Also update my timeLine
					this.followers.add(this.tweet.getUser());
				}
			}
			else
				this.phase=Phase.TWEET;
			break;
		case TWEET:
			this.phase=Phase.FOLLOWERS;
			break;
		case FOLLOWERS:
			if (!init){
				this.client.handleFinishedComplexOperation(this);
			}
			break;
		}
	}

	@Override
	public String getName() {
		return "twitter:tweet";
	}



	@Override
	public CollectionOperation getNextDBOperation() throws UnsupportedEncodingException {
		CollectionOperation res=null;
		switch(this.phase)
		{
		case USER:
			if (!this.get)
			{
				tweet.setDate(Utils.getTimeUUID());
				this.tweetMap = Utils.toMap(tweet);
				String userId = tweet.getUser();
				List<String> columns = new ArrayList<String>();
				columns.add("lastTweet");
				columns.add("followers");
				res=new GetOperation(client,SocialBenchmark.userTable,userId,columns);	
				this.get=true;
			}
			else
			{
				String lastTweetStr = user.get(UserService.LAST_TWEET);
				int lastTweet = Integer.valueOf(lastTweetStr);
				int tweetIdx = ++lastTweet;
				user.put(UserService.LAST_TWEET, new Integer(tweetIdx).toString());
				this.tweetId=tweet.getUser() + "-" + Utils.getTweetPadding(tweetIdx);

				res=new PutOperation(client,SocialBenchmark.userTable,tweet.getUser(),user,null);
				this.get=false;
			}
			break;
		case TWEET:
			tweetMap.put(Utils.ID, this.tweetId);
			res=new PutOperation(client,SocialBenchmark.tweetsTable,tweetId,tweetMap,this.tags);
			this.get=false;
			break;
		case FOLLOWERS:
			this.setTweetToFriendsTimeLine();
			res=new MultiPutOperation<String,String>(client,SocialBenchmark.friendsTimeLineTable,this.tweetFriendsTimeline);
			this.finish=true;
			break;
		}
		res.addListener(this);
		if (init)
			res.setInit(true);
		return res;
	}

	@Override
	public boolean hasMoreDBOperations() {
		return !finish;
	}


	@Override
	public String toString() {
		return "TweetOperation [tweet=" + tweet + ";tags:"+tags+"]";
	}


	public void setInit(boolean init) {
		this.init = init;
	}


	public boolean isInit() {
		return init;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}
}
