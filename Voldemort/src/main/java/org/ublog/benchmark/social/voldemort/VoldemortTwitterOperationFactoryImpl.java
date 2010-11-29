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

import java.util.Set;

import org.ublog.benchmark.BenchOperation;
import org.ublog.benchmark.social.SocialClient;
import org.ublog.benchmark.social.Message;
import org.ublog.benchmark.social.SocialOperationsFactory;


public class VoldemortTwitterOperationFactoryImpl implements SocialOperationsFactory {

	@Override
	public BenchOperation getNewGetFriendsTimeLineOperation(SocialClient client,
			String userName, int start, int count) {
		return new GetFriendsTimeLineOperation(client, userName, start, count);
	}

	@Override
	public BenchOperation getNewGetTweetsOperation(SocialClient client,
			String userName, int start, int count) {
		return new GetTweetsOperation(client, userName, start, count);
	}

	@Override
	public BenchOperation getNewSearchPerOwnerOperation(SocialClient client,
			Set<String> tags) {
		return new SearchPerOwnerOperation(client, tags);
	}

	@Override
	public BenchOperation getNewSearchPerTopicOperation(SocialClient client,
			Set<String> tags) {
		return new SearchPerTopicOperation(client, tags);
	}

	@Override
	public BenchOperation getNewStartFollowingOperation(SocialClient client,
			String userName, String toStartUser) {
		return new StartFollowingOperation(client, userName, toStartUser);
	}

	@Override
	public BenchOperation getNewStopFollowingOperation(SocialClient client,
			String userName, String toStopUser) {
		return new StopFollowingOperation(client, userName, toStopUser);
	}

	@Override
	public BenchOperation getNewTweetOperation(SocialClient client, Message tweet,
			java.util.Set<String> tags) {
		return new TweetOperation(client, tweet, tags);
	}

}
