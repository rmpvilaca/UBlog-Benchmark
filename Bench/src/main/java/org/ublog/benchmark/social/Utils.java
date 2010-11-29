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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class Utils {
	private static Logger logger = Logger.getLogger(Utils.class);

	public static final String DATE = "date";
	public static final String TEXT = "text";
	public static final String USER = "user";
	public static final String ID = "id";
	public static int MAX_MESSAGES_IN_TIMELINE = 250;

	public static int NUsers = 10000;
	public static int MaxNTweets = 10000;

	public static String toString(Set<String> followers) {
		StringBuilder sb = new StringBuilder();
		for (String s : followers) {
			if (sb.length() > 0) {
				sb.append(",");
			}
			sb.append(s);
		}
		return sb.toString();
	}

	public static java.util.UUID getTimeUUID() {
		return java.util.UUID.fromString(new com.eaio.uuid.UUID().toString());
	}

	public static Set<String> asSet(String string) {
		if ((string == null) || (string.equals(""))) {
			return new HashSet<String>();
		}

		String[] split = string.split(",");
		Set<String> set = new HashSet<String>();
		for (String s : split) {
			set.add(s);
		}
		return set;
	}

	public static Map<String, String> toMap(Message tweet) {
		Map<String, String> tweetMap = new HashMap<String, String>();
		tweetMap.put(DATE, tweet.getDate().toString());
		tweetMap.put(TEXT, tweet.getText());
		tweetMap.put(USER, tweet.getUser());
		tweetMap.put(ID, tweet.getId());
		return tweetMap;
	}

	public static Message toTweet(Map<String, String> tweetMap) {
		Message tweet = new Message();
		try {
			tweet.setDate(java.util.UUID.fromString(tweetMap.get(DATE)));
			tweet.setText(tweetMap.get(TEXT));
			tweet.setUser(tweetMap.get(USER));
			tweet.setId(tweetMap.get(ID));
			return tweet;
		} catch (Exception e) {
			System.out.println("Null no toTweet!!");
			return null;
		}
	}

	public static String getTweetPadding(int tweetIdx) {
		StringBuilder strBuild = new StringBuilder();
		int current = (int) Math.floor(Math.log10(tweetIdx)) + 1;
		int expected = (int) Math.floor(Math.log10(Utils.MaxNTweets));
		if (tweetIdx == 0)
			current = 1;
		for (int i = 0; i < (expected - current); i++)
			strBuild.append(0);
		strBuild.append(tweetIdx);
		if (logger.isInfoEnabled())
			logger.info("getTweetPadding with tweetIdx:" + tweetIdx + " is:"
					+ strBuild);
		return strBuild.toString();
	}

	public static int getTagUserID(String userID) {
		return new Integer(userID.substring(4));
	}

	public static int getTagTweetID(String tweetID) {
		String split[] = tweetID.split("-");
		return new Integer(split[0].substring(4)) * Utils.MaxNTweets
				+ new Integer(split[1]);
	}

	public static java.util.UUID toUUID(byte[] uuid) {
		long msb = 0;
		long lsb = 0;
		assert uuid.length == 16;
		for (int i = 0; i < 8; i++)
			msb = (msb << 8) | (uuid[i] & 0xff);
		for (int i = 8; i < 16; i++)
			lsb = (lsb << 8) | (uuid[i] & 0xff);
		@SuppressWarnings("unused")
		long mostSigBits = msb;
		@SuppressWarnings("unused")
		long leastSigBits = lsb;

		com.eaio.uuid.UUID u = new com.eaio.uuid.UUID(msb, lsb);
		return java.util.UUID.fromString(u.toString());
	}

	public static byte[] asByteArray(java.util.UUID uuid) {
		long msb = uuid.getMostSignificantBits();
		long lsb = uuid.getLeastSignificantBits();
		byte[] buffer = new byte[16];

		for (int i = 0; i < 8; i++) {
			buffer[i] = (byte) (msb >>> 8 * (7 - i));
		}
		for (int i = 8; i < 16; i++) {
			buffer[i] = (byte) (lsb >>> 8 * (7 - i));
		}

		return buffer;
	}
}
