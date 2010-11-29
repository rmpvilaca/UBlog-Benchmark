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

import java.util.Set;

import org.ublog.benchmark.BenchOperation;

public interface SocialOperationsFactory {

	public BenchOperation getNewSearchPerTopicOperation(SocialClient client,
			Set<String> tags);

	public BenchOperation getNewSearchPerOwnerOperation(SocialClient client,
			Set<String> tags);

	public BenchOperation getNewGetTweetsOperation(SocialClient client,
			String userName, int start, int count);

	public BenchOperation getNewGetFriendsTimeLineOperation(
			SocialClient client, String userName, int start, int count);

	public BenchOperation getNewStartFollowingOperation(SocialClient client,
			String userName, String toStartUser);

	public BenchOperation getNewStopFollowingOperation(SocialClient client,
			String userName, String toStopUser);

	public BenchOperation getNewTweetOperation(SocialClient client,
			Message tweet, Set<String> tags);
}
