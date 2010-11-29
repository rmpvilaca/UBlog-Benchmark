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
package org.ublog.benchmark.mysql;


import java.sql.PreparedStatement;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.ublog.utils.Pair;

public class TMultiPutMySqlFriendsTimeline implements Callable<Pair<String,Boolean>> {

	private String follower;
	private String date;
	private String tweetId;
	private java.sql.Connection conn;
	
	private Logger logger= Logger.getLogger(TMultiPutMySqlFriendsTimeline.class);
	
	public TMultiPutMySqlFriendsTimeline(java.sql.Connection conn,String follower,String date, String tweetId){	
		this.follower =  follower;
		this.date = date;
		this.tweetId = tweetId;
		this.conn = conn;
	}
	@Override
	public Pair<String,Boolean> call() throws Exception {
		
		String query = "insert into friendsTimeLine (tweetID,userID,date) values (?,?,?)"; //on duplicate key update userID = values(userID), date = values(date)";
		PreparedStatement stmt = this.conn.prepareStatement(query);

		stmt.setString(1, tweetId/*+":"+date*/);
		stmt.setString(2, follower);
		stmt.setString(3, date);
		try {
			stmt.executeUpdate();
		} finally {
			try {
				stmt.close();
			} catch(Exception e) {}
			this.conn.close();
		}
		if (logger.isInfoEnabled())
			logger.info("ThreadMultiPut:"+follower+":"+tweetId+":"+date);
		Boolean res = true;
		return new Pair<String,Boolean>(follower,res);
	}
}
