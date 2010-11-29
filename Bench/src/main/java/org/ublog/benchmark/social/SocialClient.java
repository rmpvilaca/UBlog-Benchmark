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
package org.ublog.benchmark.social;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.ublog.utils.Pair;
import org.ublog.utils.Time;
import org.ublog.benchmark.BenchOperation;
import org.ublog.benchmark.ComplexBenchMarkClient;
import org.ublog.benchmark.StatsCollector;

import org.ublog.utils.Clock;

public class SocialClient extends ComplexBenchMarkClient {

	private User myUser;
	private int currentOp;
	private int nOperations;
	private StatsCollector statsCollector;
	private Clock clock;
	private Time startTime;

	private Logger logger = Logger.getLogger(SocialClient.class);
	private double thinkTime;
	private Random rndOp;
	private SocialBenchmark socialBenchmark;
	private SocialOperationsFactory opFactory;

	public SocialClient(Clock clock, int clientCount, User user,
			int nOperations, StatsCollector statsCollector, double thinkTime,
			Random rndOp, SocialBenchmark socialBenchmark,
			SocialOperationsFactory opFactory) {
		super(clientCount);
		this.clock = clock;
		this.myUser = user;
		this.nOperations = nOperations;
		this.statsCollector = statsCollector;
		this.currentOp = 0;
		this.thinkTime = thinkTime;
		this.rndOp = rndOp;
		this.socialBenchmark = socialBenchmark;
		this.opFactory = opFactory;
	}

	@Override
	public Pair<BenchOperation, Double> getNextComplexOperation() {
		this.startTime = this.clock.getTime().add(
				Time.inMilliseconds(this.thinkTime));
		BenchOperation res;
		double p = rndOp.nextDouble();
		if (p < this.socialBenchmark.getProbabilitySearchPerTopic())
		{
			// Probability given by this.socialBenchmark.getProbabilitySearchPerTopic()
			Set<String> tags = new HashSet<String>();
			tags.add(null);
			tags.add(this.socialBenchmark.getPowerLawTag());
			tags.add(null);
			if (logger.isInfoEnabled())
				logger.info("Client with user:"
						+ this.myUser
						+ " generating new search operation per topic with tags:"
						+ tags + ".");
			res = this.opFactory.getNewSearchPerTopicOperation(this, tags);
		} else if (p < (this.socialBenchmark.getProbabilitySearchPerTopic()+this.socialBenchmark.getProbabilitySearchPerOwner()))
		{
			// Probability given by this.socialBenchmark.getProbabilitySearchPerOwner()
			Set<String> tags = new HashSet<String>();
			tags.add(null);
			tags.add(null);
			tags.add("user" + this.myUser.getUsername().substring(4));
			if (logger.isInfoEnabled())
				logger.info("Client with user:"
						+ this.myUser
						+ " generating new search operation per direct messages with tags:"
						+ tags + ";myID:"
						+ this.myUser.getUsername().substring(4));
			res = this.opFactory.getNewSearchPerOwnerOperation(this, tags);
		} else if (p < (this.socialBenchmark.getProbabilitySearchPerTopic()+this.socialBenchmark.getProbabilitySearchPerOwner()+
				this.socialBenchmark.getProbabilityGetRecentTweets())) 
		{
			// Probability given by this.socialBenchmark.getProbabilityGetRecentTweets()
			if (logger.isInfoEnabled())
				logger.info("Client with user:" + this.myUser
						+ " generating new get recent tweets operation.");
			res = this.opFactory.getNewGetTweetsOperation(this,
					this.myUser.getUsername(), 0, 50);
		} else if (p < (this.socialBenchmark.getProbabilitySearchPerTopic()+this.socialBenchmark.getProbabilitySearchPerOwner()+
				this.socialBenchmark.getProbabilityGetRecentTweets()+this.socialBenchmark.getProbabilityGetFriendsTimeline())) 
		{
			// Probability given by this.socialBenchmark.getProbabilityGetFriendsTimeline()
			if (logger.isInfoEnabled())
				logger.info("Client with user:" + this.myUser
						+ " generating new get friendsTimeLine operation.");
			res = this.opFactory.getNewGetFriendsTimeLineOperation(this,
					this.myUser.getUsername(), 0, 100);
		} else if (p < (this.socialBenchmark.getProbabilitySearchPerTopic()+this.socialBenchmark.getProbabilitySearchPerOwner()+
				this.socialBenchmark.getProbabilityGetRecentTweets()+this.socialBenchmark.getProbabilityGetFriendsTimeline()+
				this.socialBenchmark.getProbabilityStartFollowing())) 
		{
			// Probability given by this.socialBenchmark.getProbabilityStartFollowing()
			String toStartUser = this.socialBenchmark
			.getNextStartFollowingUser(this.myUser);
			if (toStartUser != null) {
				if (logger.isInfoEnabled())
					logger.info("Client with user:" + this.myUser
							+ " generating new start follow operation to user:"
							+ toStartUser + ".");
				res = this.opFactory.getNewStartFollowingOperation(this,
						this.myUser.getUsername(), toStartUser);

			} else {
				if (logger.isInfoEnabled())
					logger.info("Client with user:"
							+ this.myUser
							+ " generating new fake get friendsTimeLine operation.");
				res = this.opFactory.getNewGetFriendsTimeLineOperation(this,
						this.myUser.getUsername(), 0, 100);
			}
		} else if (p < (this.socialBenchmark.getProbabilitySearchPerTopic()+this.socialBenchmark.getProbabilitySearchPerOwner()+
				this.socialBenchmark.getProbabilityGetRecentTweets()+this.socialBenchmark.getProbabilityGetFriendsTimeline()+
				this.socialBenchmark.getProbabilityStartFollowing()+this.socialBenchmark.getProbabilityStopFollowing())) {
			// Probability given by this.socialBenchmark.getProbabilityStopFollowing()
			String toStopUser = this.socialBenchmark
			.getNextStopFollowingUser(this.myUser);
			if (toStopUser != null) {
				if (logger.isInfoEnabled())
					logger.info("Client with user:" + this.myUser
							+ " generating new stop follow operation to user:"
							+ toStopUser + ".");
				res = this.opFactory.getNewStopFollowingOperation(this,
						this.myUser.getUsername(), toStopUser);
			} else {
				if (logger.isInfoEnabled())
					logger.info("Client with user:"
							+ this.myUser
							+ " generating new fake get friendsTimeLine operation.");
				res = this.opFactory.getNewGetFriendsTimeLineOperation(this,
						this.myUser.getUsername(), 0, 100);
			}
		} else 
		{
			// Remaining Probability
			res = this.socialBenchmark.getNewTweetOperation(this.myUser, this);
			if (logger.isInfoEnabled())
				logger.info("Client with user:" + this.myUser
						+ " generating new tweet operation:" + res);
		} 
		this.currentOp++;
		return new Pair<BenchOperation, Double>(res, this.thinkTime);
	}

	@Override
	public void handleFinishedComplexOperation(BenchOperation op) {
		boolean write = !op.isReadOnly();
		statsCollector.registerRequest(Time.inMilliseconds(this.startTime),
				this.currentOp - 1, this.getPeerID(),
				Time.inMilliseconds(this.clock.getTime().subtract(startTime)),
				write, op.getName());
	}

	@Override
	public boolean hasMoreComplexOperations() {
		return this.currentOp < this.nOperations;
	}

}
