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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.ublog.benchmark.BenchOperation;
import org.ublog.benchmark.ComplexBenchMarkClient;
import org.ublog.benchmark.operations.CollectionOperation;
import org.ublog.benchmark.operations.DataStoreOperation;
import org.ublog.benchmark.operations.GetOperation;
import org.ublog.benchmark.operations.MultiGetOperation;
import org.ublog.benchmark.operations.OperationListener;
import org.ublog.benchmark.social.Message;
import org.ublog.benchmark.social.SocialBenchmark;
import org.ublog.benchmark.social.Utils;

public class GetFriendsTimeLineOperation implements BenchOperation,
		OperationListener {

	private Logger logger = Logger.getLogger(GetFriendsTimeLineOperation.class);

	private ComplexBenchMarkClient client;
	private String userId;
	private List<Message> tweetList;
	private List<String> timeline;
	private int start;
	private int count;

	public GetFriendsTimeLineOperation(ComplexBenchMarkClient client,
			String userId, int start, int count) {
		this.client = client;
		this.userId = userId;
		this.count = count;
		this.timeline = null;
		this.tweetList = null;
		this.start = start;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleFinishedOperation(DataStoreOperation op) {
		// System.out.println("handleFinishedOP");
		if (this.timeline == null) {
			timeline = (List<String>) ((GetOperation) op).getResult();
			if (this.timeline == null)
				this.timeline = new ArrayList<String>();
		} else {
			Map<Comparable, Object> res = ((MultiGetOperation<Comparable>) op)
					.getResult();
			this.tweetList = new ArrayList<Message>(res.size());
			Map<String, String> resin;
			for (Comparable key : res.keySet()) {
				resin = (Map<String, String>) res.get(key);
				// System.out.println("resin: "+resin);
				if (!resin.isEmpty()) {
					this.tweetList.add(Utils.toTweet(resin));
				}
			}
			Collections.sort(this.tweetList, new Comparator<Message>() {
				public int compare(Message o1, Message o2) {
					return -o1.getDate().compareTo(o2.getDate());
				}
			});
			this.client.handleFinishedComplexOperation(this);
			if (logger.isInfoEnabled())
				logger.info("Result of GetFriendsTimeline with userId:"
						+ this.userId + ", start:" + start + ", and count:"
						+ this.count/* +" is:"+this.tweetList */);
		}
	}

	public List<Message> getResult() {
		return this.tweetList;
	}

	@Override
	public String getName() {
		return "twitter:getFriendsTimeLine";
	}

	@SuppressWarnings("unchecked")
	@Override
	public CollectionOperation getNextDBOperation() {
		CollectionOperation res = null;
		if (this.timeline == null) {

			res = new GetOperation(client,
					SocialBenchmark.friendsTimeLineTable, this.userId, 250);
		} else {

			int count = Math.min(this.count, timeline.size() - this.start);
			int end = this.start + count;
			if (logger.isInfoEnabled())
				logger.info("Indexes- start:" + this.start + ";count:"
						+ this.count + ";timeline size:" + this.timeline.size()
						+ ";end:" + end);
			Set<String> keys = new HashSet<String>();
			for (int i = this.start; i < end; i++) {
				String idAndTime = timeline.get(i/* start */);
				String[] split = idAndTime.split(":");
				keys.add(split[0]);
			}
			// System.out.println("GET TWEETS:"+keys.toString());
			res = new MultiGetOperation<String>(client,
					SocialBenchmark.tweetsTable, keys);
		}
		res.addListener(this);
		return res;
	}

	@Override
	public boolean hasMoreDBOperations() {
		return this.tweetList == null;
	}

	@Override
	public String toString() {
		return "GetFriendsTimeLineOperation [count=" + count
				+ ", timelineIndex=" + start + ", userId=" + userId + "]";
	}

	@Override
	public boolean isReadOnly() {
		return true;
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
