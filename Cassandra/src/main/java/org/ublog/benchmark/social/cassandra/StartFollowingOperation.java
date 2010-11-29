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
package org.ublog.benchmark.social.cassandra;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ublog.utils.Pair;
import org.ublog.benchmark.BenchOperation;
import org.ublog.benchmark.ComplexBenchMarkClient;
import org.ublog.benchmark.operations.CollectionOperation;
import org.ublog.benchmark.operations.DataStoreOperation;
import org.ublog.benchmark.operations.GetOperation;
import org.ublog.benchmark.operations.GetRangeOperation;
import org.ublog.benchmark.operations.OperationListener;
import org.ublog.benchmark.operations.PutOperation;
import org.ublog.benchmark.social.Message;
import org.ublog.benchmark.social.SocialBenchmark;
import org.ublog.benchmark.social.UserService;
import org.ublog.benchmark.social.Utils;

public class StartFollowingOperation implements BenchOperation,
		OperationListener {

	private enum Phase {
		UPDATE_FOLLOWERS, UPDATE_FOLLOWING, UPDATE_TIMELINE
	}

	private ComplexBenchMarkClient client;
	private String userId, toStartUser;
	private Phase currentPhase;
	private Map<String, String> user;
	private Map<String, String> toStart;
	private boolean get;
	private boolean finish;
	private List<String> timeLine;
	private List<Message> recentTweets;

	public StartFollowingOperation(ComplexBenchMarkClient client,
			String userId, String toStartUser) {
		this.client = client;
		this.userId = userId;
		this.toStartUser = toStartUser;
		this.currentPhase = Phase.UPDATE_FOLLOWING;
		this.get = false;
		this.finish = false;
		this.user = null;
		this.toStart = null;
		this.recentTweets = null;
	}

	private String getTimelineId(Message tweet) {
		return tweet.getId() + ":" + tweet.getDate().toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleFinishedOperation(DataStoreOperation op) {
		switch (this.currentPhase) {
		case UPDATE_FOLLOWING:
			if (this.user == null) {
				this.user = (Map<String, String>) ((GetOperation) op)
						.getResult();
			} else
				this.currentPhase = Phase.UPDATE_FOLLOWERS;
			break;
		case UPDATE_FOLLOWERS:
			if (this.toStart == null) {
				this.toStart = (Map<String, String>) ((GetOperation) op)
						.getResult();
			} else
				this.currentPhase = Phase.UPDATE_TIMELINE;
			break;
		case UPDATE_TIMELINE:
			if (this.timeLine == null) {
				this.timeLine = (List<String>) ((GetOperation) op).getResult();
				if (this.timeLine == null)
					this.timeLine = new ArrayList<String>();
			} else if (this.recentTweets == null) {
				this.recentTweets = new ArrayList<Message>();
				for (Map<String, String> tweetMap : ((Set<Map<String, String>>) ((GetRangeOperation) op)
						.getResult())) {
					if (!tweetMap.isEmpty()) {
						this.recentTweets.add(Utils.toTweet(tweetMap));
					}
				}
			} else
				this.client.handleFinishedComplexOperation(this);
			break;
		}
	}

	@Override
	public String getName() {
		return "twitter:startFollowing";
	}

	@SuppressWarnings("unchecked")
	@Override
	public CollectionOperation getNextDBOperation()
			throws UnsupportedEncodingException {
		CollectionOperation res = null;
		switch (this.currentPhase) {
		case UPDATE_FOLLOWING:
			if (!this.get) {
				// CASSANDRA
				List<byte[]> columns = new ArrayList<byte[]>();
				columns.add("following".getBytes("UTF-8"));
				// FINISHED
				res = new GetOperation(client, SocialBenchmark.userTable,
						this.userId, columns);
				this.get = true;
			} else {
				Set<String> following = Utils.asSet(user
						.get(UserService.FOLLOWING));
				following.add(toStartUser);
				this.user.put(UserService.FOLLOWING, Utils.toString(following));

				res = new PutOperation(client, SocialBenchmark.userTable,
						this.userId, this.user, null);
				this.get = false;
			}
			break;
		case UPDATE_FOLLOWERS:
			if (!this.get) {
				// CASSANDRA
				List<byte[]> columns = new ArrayList<byte[]>();
				columns.add("followers".getBytes("UTF-8"));
				columns.add("lastTweet".getBytes("UTF-8"));
				// FINISHED
				res = new GetOperation(client, SocialBenchmark.userTable,
						this.toStartUser, columns);
				this.get = true;
			} else {
				Set<String> followers = Utils.asSet(user
						.get(UserService.FOLLOWERS));
				followers.add(userId);
				this.toStart.put(UserService.FOLLOWERS,
						Utils.toString(followers));

				res = new PutOperation(client, SocialBenchmark.userTable,
						this.toStartUser, this.toStart, null);
				this.get = false;
			}
			break;
		case UPDATE_TIMELINE:
			if (!this.get) {

				this.get = true;
				this.timeLine = null;
				res = new GetOperation(client,
						SocialBenchmark.friendsTimeLineTable, this.userId, 250);
			} else {
				if (this.recentTweets == null) {
					int lastTweet = Integer.valueOf(this.toStart
							.get(UserService.LAST_TWEET));
					String min = this.toStartUser
							+ "-"
							+ Utils.getTweetPadding(Math.max(1,
									(lastTweet - 20 + 1)));
					String max = this.toStartUser + "-"
							+ Utils.getTweetPadding(lastTweet);
					res = new GetRangeOperation(client,
							SocialBenchmark.tweetsTable, min, max);
				} else {
					List<Pair<String, String>> toAddToTimeline = new ArrayList<Pair<String, String>>();

					int timelineIdx = 0;
					long timelineDate = 0;
					for (Message tweet : recentTweets) {
						long date = tweet.getDate().timestamp();

						String timelineId = getTimelineId(tweet);
						if (this.timeLine.contains(timelineId)) {
							continue;
						}

						while (timelineIdx < this.timeLine.size()
								&& timelineIdx < Utils.MAX_MESSAGES_IN_TIMELINE) {
							String id = this.timeLine.get(timelineIdx);
							String[] split = id.split(":");
							timelineDate = new Long(java.util.UUID.fromString(
									split[1]).timestamp());

							if (timelineDate > date) {
								// CASSANDRA
								toAddToTimeline
										.add(new Pair<String, String>(tweet
												.getDate().toString(),
												timelineId));
								// FINISHED
								timelineIdx++;
								break;
							}

							timelineIdx++;
						}

						if (timelineIdx == this.timeLine.size()) {
							// CASSANDRA
							toAddToTimeline.add(new Pair<String, String>(tweet
									.getDate().toString(), timelineId));
							// FINISHED
						}

						if (timelineIdx == Utils.MAX_MESSAGES_IN_TIMELINE) {
							break;
						}
					}

					res = new PutOperation(client,
							SocialBenchmark.friendsTimeLineTable, this.userId,
							toAddToTimeline, null);
					this.finish = true;
				}
			}
			break;
		}
		res.addListener(this);
		return res;

	}

	@Override
	public boolean hasMoreDBOperations() {
		return !this.finish;
	}

	@Override
	public String toString() {
		return "StartFollowingOperation [toStartUser=" + toStartUser
				+ ", userId=" + userId + "]";
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public boolean isInit() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setInit(boolean init) {
		// TODO Auto-generated method stub

	}

}
