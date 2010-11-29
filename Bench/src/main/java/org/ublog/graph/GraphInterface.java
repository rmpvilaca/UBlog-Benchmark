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
package org.ublog.graph;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Set;

import org.ublog.benchmark.social.User;

public interface GraphInterface extends Remote {

	int getSomaGraus() throws RemoteException;

	int degreeOfGrausTags(String arg0) throws RemoteException;

	int getRandomFriend(User user) throws RemoteException;

	User getClientCount(int clientCount) throws RemoteException;

	User getMapUser(int i) throws RemoteException;

	Set<String> getFollowers(User user) throws RemoteException;

	Set<String> getFollowing(User user) throws RemoteException;

	boolean graphContainsEdge(User user, int userID) throws RemoteException;

	String getNextStopFollowingUser(User user) throws RemoteException;

	int getUserID() throws RemoteException;

	int getOutDegreeOf(int currentUser) throws RemoteException;

	boolean init(int size) throws RemoteException;
}
