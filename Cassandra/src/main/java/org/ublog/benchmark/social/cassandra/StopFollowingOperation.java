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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ublog.benchmark.BenchOperation;
import org.ublog.benchmark.ComplexBenchMarkClient;
import org.ublog.benchmark.operations.CollectionOperation;
import org.ublog.benchmark.operations.DataStoreOperation;
import org.ublog.benchmark.operations.DeleteOperation;
import org.ublog.benchmark.operations.GetOperation;
import org.ublog.benchmark.operations.OperationListener;
import org.ublog.benchmark.operations.PutOperation;
import org.ublog.benchmark.social.Message;
import org.ublog.benchmark.social.SocialBenchmark;
import org.ublog.benchmark.social.UserService;
import org.ublog.benchmark.social.Utils;

public class StopFollowingOperation implements BenchOperation,
		OperationListener {

	private enum Phase {
		UPDATE_FOLLOWERS, UPDATE_FOLLOWING, UPDATE_TIMELINE
	}

	private ComplexBenchMarkClient client;
	private String userId, toStopUser;
	private Phase currentPhase;
	private Map<String, String> user;
	private Map<String, String> toStop;
	private boolean get;
	private boolean finish;
	private List<String> timeLine;

	public StopFollowingOperation(ComplexBenchMarkClient client, String userId,
			String toStopUser) {
		this.client = client;
		this.userId = userId;
		this.toStopUser = toStopUser;
		this.currentPhase = Phase.UPDATE_FOLLOWERS;
		this.get = false;
		this.finish = false;
		this.user = null;
		this.toStop = null;
	}

	private void addToTimeline(List<String> timeline, int timelineIdx,
			String timelineId) {
		timeline.add(timelineIdx, timelineId);
	}

	private String getTimelineId(Message tweet) {
		return tweet.getId() + ":" + tweet.getDate().toString();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleFinishedOperation(DataStoreOperation op) {
		switch (this.currentPhase) {
		case UPDATE_FOLLOWERS:
			if (this.toStop == null) {
				this.toStop = (Map<String, String>) ((GetOperation) op)
						.getResult();
			} else
				this.currentPhase = Phase.UPDATE_FOLLOWING;
			break;
		case UPDATE_FOLLOWING:
			if (this.user == null) {
				this.user = (Map<String, String>) ((GetOperation) op)
						.getResult();
			} else
				this.currentPhase = Phase.UPDATE_TIMELINE;
			break;
		case UPDATE_TIMELINE:
			if (this.timeLine == null) {
				this.timeLine = (List<String>) ((GetOperation) op).getResult();
				if (this.timeLine == null)
					this.timeLine = new ArrayList<String>();
			} else
				this.client.handleFinishedComplexOperation(this);
			break;
		}
	}

	@Override
	public String getName() {
		return "twitter:stopFollowing";
	}

	@SuppressWarnings("unchecked")
	@Override
	public CollectionOperation getNextDBOperation()
			throws UnsupportedEncodingException {
		CollectionOperation res = null;
		switch (this.currentPhase) {

		case UPDATE_FOLLOWERS:
			if (!this.get) {
				// CASSANDRA
				List<byte[]> columns = new ArrayList<byte[]>();
				columns.add("followers".getBytes("UTF-8"));
				// FINISHED
				res = new GetOperation(client, SocialBenchmark.userTable,
						this.toStopUser, columns);
				this.get = true;
			} else {
				Set<String> followers = Utils.asSet(this.toStop
						.get(UserService.FOLLOWERS));
				followers.remove(userId);
				this.toStop.put(UserService.FOLLOWERS,
						Utils.toString(followers));

				res = new PutOperation(client, SocialBenchmark.userTable,
						this.toStopUser, this.toStop, null);
				this.get = false;
			}
			break;
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
				following.remove(toStopUser);
				this.user.put(UserService.FOLLOWING, Utils.toString(following));

				res = new PutOperation(client, SocialBenchmark.userTable,
						this.userId, this.user, null);
				this.get = false;
			}
			break;
		case UPDATE_TIMELINE:
			if (!this.get) {
				this.timeLine = null;
				res = new GetOperation(client,
						SocialBenchmark.friendsTimeLineTable, this.userId, 250);
				this.get = true;
			} else {
				// CASSANDRA: DEL SPECIFIED COLUMNS
				List<byte[]> columns = new ArrayList<byte[]>();
				// /FINISH
				for (Iterator<String> itr = this.timeLine.iterator(); itr
						.hasNext();) {
					String idAndTime = itr.next();

					String[] split = idAndTime.split(":");
					String id = split[0];
					String date = split[1];

					if (id.startsWith(this.toStopUser + "-")) {
						itr.remove();
						columns.add(Utils.asByteArray(java.util.UUID
								.fromString(date)));
					}
				}
				List<Integer> tags = new ArrayList<Integer>();
				tags.add(Utils.getTagUserID(this.userId));

				res = new DeleteOperation(client,
						SocialBenchmark.friendsTimeLineTable, this.userId,
						columns);
				// res=new
				// PutOperation(client,TwitterBenchmark.friendsTimeLineTable,this.userId,timeLine,tags);
				this.finish = true;

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
		return "StopFollowingOperation [toStopUser=" + toStopUser + ", userId="
				+ userId + "]";
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
