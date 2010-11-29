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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.ublog.utils.Pair;

public class TMultiPutMySqlUsers implements Callable<Pair<String,Boolean>> {

	private String userid;
	private Map<String,String> value;
	private java.sql.Connection conn;
	
	private Logger logger= Logger.getLogger(TMultiPutMySqlUsers.class);
	
	public TMultiPutMySqlUsers(java.sql.Connection conn,String userid, Map<String,String> value){	
		this.userid =  userid;
		this.value = value;
		this.conn = conn;
	}

	@Override
	public Pair<String,Boolean> call() {
		String query;
		int i;
		if(value.size()<7){
			i=0;
			query = "update users set ";
		}
		else{
			i=1;
			query = "insert into users ";// (userID,name,password,following,followers,username,lasttweet,created) values (?,?,?,?,?,?,?,?)";
		}
		Set<String> keys = value.keySet();
		Iterator<String> iter = keys.iterator();
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<String> columnValues = new ArrayList<String>();
		
		while(iter.hasNext()){
			String columnName = iter.next();
			columnNames.add(columnName);
			columnValues.add(value.get(columnName));						
		}
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e2) {
			e2.printStackTrace();
		}
		try {
			if(i==0) query += this.buildUpdateQuery("userID", this.userid, columnNames, columnValues);
			else query += this.buildInsertNamesQuery("userID", columnNames) +" values " +this.buildInsertValuesQuery(this.userid, columnValues)
						+" on duplicate key update "+this.buildDuplicateNamesQuery(columnNames);
			stmt.executeUpdate(query);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		finally {
			try {
				stmt.close();
			} catch(Exception e) {}
			try {
				this.conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		if (logger.isInfoEnabled())
			logger.info("ThreadMultiPut:"+userid+":"+value);
		Boolean res = true;
		return new Pair<String,Boolean>(userid,res);
	}
	
	private String buildInsertNamesQuery(String keyName, ArrayList<String> columnNames){
		String res = "("+keyName;
		if(!columnNames.isEmpty()){
			res+=",";
			Iterator<String> iter = columnNames.iterator();
			while(iter.hasNext()){
				String columnName = iter.next();
				res += columnName;
				if(iter.hasNext()){
					res+=",";	
				}
				else{
					res+=")";
				}
			}
		}
		else res+=")";
		return res;
	}
	private String buildInsertValuesQuery(String keyName, ArrayList<String> columnNames){
		String res = "('"+keyName;
		if(!columnNames.isEmpty()){
			res+="',";
			Iterator<String> iter = columnNames.iterator();
			while(iter.hasNext()){
				String columnName = iter.next();
				res += "'"+columnName+"'";
				if(iter.hasNext()){
					res+=",";	
				}
				else{
					res+=")";
				}
			}
		}
		else res+=")";
		return res;
	}
	
	private String buildUpdateQuery(String key, String value, ArrayList<String> columnNames, ArrayList<String> columnValues){
		String res = "";
		if(!columnNames.isEmpty() && !columnValues.isEmpty()){
			Iterator<String> iterName = columnNames.iterator();
			Iterator<String> iterValue = columnValues.iterator();
			while(iterName.hasNext()){
				String columnName = iterName.next();
				String columnValue = iterValue.next();
				res += columnName+"='"+columnValue+"'";

				if(iterName.hasNext()){
					res+=",";	
				}
			}
			res+=" where "+key+" = "+"'"+value+"'";
		}
		return res;
	}
	
	private String buildDuplicateNamesQuery(ArrayList<String> columnNames){
		String res = "";// = "("+keyName;
		if(!columnNames.isEmpty()){
			Iterator<String> iter = columnNames.iterator();
			while(iter.hasNext()){
				String columnName = iter.next();
				res += columnName + "=values("+columnName+")";
				if(iter.hasNext()){
					res+=",";	
				}
			}
		}
		return res;
	}
}
