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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.ublog.benchmark.BenchMarkClient;
import org.ublog.benchmark.operations.GetOperation;

public class UserService {

	public static final String USERNAME = "username";
	public static final String PASSWORD = "password";
	public static final String NAME = "name";
	public static final String CREATED = "created";
	public static final String LAST_TWEET = "lastTweet";
	public static final String FOLLOWING = "following";
	public static final String FOLLOWERS = "followers";

	private BenchMarkClient client;

	public Map<String, String> initialize() throws Exception {
		User user = new User();
		user.setUsername("admin");
		user.setPassword("password");
		user.setName("Administrator");
		user.setCreated(new Date());
		return newUser(user);
	}

	public User newUser(String userName) {
		User user = new User();
		user.setUsername(userName);
		user.setPassword(PASSWORD);
		user.setName(NAME);
		user.setCreated(new Date());
		return user;
	}

	public Map<String, String> newUser(final User user) {
		final Map<String, String> userMap = new HashMap<String, String>();
		userMap.put(USERNAME, user.getUsername());
		userMap.put(PASSWORD, user.getPassword());
		userMap.put(NAME, user.getName());
		userMap.put(CREATED, new Long(new Date().getTime()).toString());
		userMap.put(LAST_TWEET, "0");
		return userMap;
	}

	public Map<String, String> newUser(final User user, String followers,
			String following) {
		final Map<String, String> userMap = new HashMap<String, String>();
		userMap.put(USERNAME, user.getUsername());
		userMap.put(PASSWORD, user.getPassword());
		userMap.put(NAME, user.getName());
		userMap.put(CREATED, new Long(new Date().getTime()).toString());
		userMap.put(LAST_TWEET, "0");
		userMap.put(FOLLOWERS, followers);
		userMap.put(FOLLOWING, following);
		return userMap;
	}

	public GetOperation<String> getUser(String username) {
		return new GetOperation<String>(client, "users", username, null);
	}

}
