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

import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.VertexFactory;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.generate.ScaleFreeGraphGenerator;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import org.ublog.benchmark.social.User;

public class GraphImpl implements GraphInterface,VertexFactory<User>{

	private static final long serialVersionUID = 1L;
	private static Map<Integer,User> mapUser;
	private static DirectedGraph<User,DefaultEdge> graph;
	private static UndirectedGraph<String,DefaultEdge> grausTags;
	private int somaGraus;
	private int tagID;
	private int size;
	private static int userID;

	private static boolean hasInitiated;

	public GraphImpl() throws RemoteException {
		hasInitiated = false;
	}

	public synchronized boolean init(int size) throws RemoteException {
		if(hasInitiated == false ){
			this.size = size;

			userID=0;
			mapUser=new HashMap<Integer, User>();
			ScaleFreeGraphGenerator<User,DefaultEdge> gen = new ScaleFreeGraphGenerator<User,DefaultEdge>(size,123456789);
			graph=new DefaultDirectedGraph<User, DefaultEdge>(DefaultEdge.class);
			gen.generateGraph(graph,this,null);
			this.exportToDot("graph-init.dot");

			this.tagID=0;
			ScaleFreeGraphGenerator<String,DefaultEdge> gen2 = new ScaleFreeGraphGenerator<String,DefaultEdge>(64,987654321);
			grausTags=new  SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
			gen2.generateGraph(grausTags,new VertexFactory<String>(){

				@Override
				public String createVertex() {
					String res="topico"+tagID;
					tagID++;
					return res;
				}

			},null);
			for(String vertex:grausTags.vertexSet())
			{
				this.somaGraus+=grausTags.degreeOf(vertex);
			}
			this.exportToDotGraus("graus.dot",grausTags);
			hasInitiated = true;
			return false;
		}
		else {
			System.out.println("The graph was already initiated");
			return hasInitiated;
		}
	}

	public User createVertex() 
	{
		User res=new User();
		res.setName("User "+userID);
		res.setUsername("user"+userID);
		res.setCreated(new Date());
		res.setPassword("123456");
		mapUser.put(userID,res);
		userID++;
		return res;
	}

	public void exportToDot(String fileName)
	{
		try {
			new DOTExporter<User,DefaultEdge>().export(new FileWriter(fileName), graph);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	public void exportToDotGraus(String fileName, Graph graph)
	{
		try {
			new DOTExporter<String,DefaultEdge>().export(new FileWriter(fileName), grausTags);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getRandomFriend(User myUser) throws RemoteException{
		List<User> friends=new ArrayList<User>();
		for(DefaultEdge edge:graph.outgoingEdgesOf(myUser))
		{
			if (graph.containsEdge(graph.getEdgeTarget(edge),myUser)) 
			{
				friends.add(graph.getEdgeTarget(edge));
			}
		}
		if (friends.isEmpty())
		{
			return -1;
		}
		else
		{
			Random rnd=new Random();
			int pos=rnd.nextInt(friends.size());
			int userID=new Integer(friends.get(pos).getUsername().substring(4));
			return userID;
		}
	}


	@Override

	public synchronized String getNextStopFollowingUser(User user) throws RemoteException {
		// Da o utilizador com menos seguidores ao qual o user ainda segue
		int min=Integer.MAX_VALUE;
		User res=null;
		for(User tmp:graph.vertexSet())
		{
			if (!user.equals(tmp) && graph.containsEdge(user, tmp) && graph.incomingEdgesOf(tmp).size()<min)
			{
				min=graph.incomingEdgesOf(tmp).size();
				res=tmp;
			}
		}
		if (res!=null)
		{
			graph.removeEdge(user, res);
			return res.getUsername();
		}
		else return null;
	}

	@Override
	public int degreeOfGrausTags(String str) throws RemoteException {
		return grausTags.degreeOf(str);
	}

	@Override
	public User getClientCount(int clientCount) throws RemoteException {
		return mapUser.get(clientCount);
	}

	@Override
	public Set<String> getFollowers(User currentUser) throws RemoteException {
		Set<String> followers=new HashSet<String>();
		for (DefaultEdge edge:graph.incomingEdgesOf(currentUser))
		{
			followers.add(graph.getEdgeSource(edge).getUsername());
		}
		return followers;
	}

	@Override
	public Set<String> getFollowing(User currentUser) throws RemoteException {
		Set<String> following=new HashSet<String>();
		for (DefaultEdge edge:graph.outgoingEdgesOf(currentUser))
		{
			following.add(graph.getEdgeTarget(edge).getUsername());
		}
		return following;
	}

	@Override
	public User getMapUser(int i) throws RemoteException {
		return mapUser.get(i);
	}

	@Override
	public int getSomaGraus() throws RemoteException {
		return this.somaGraus;
	}

	@Override
	public boolean graphContainsEdge(User user, int userID)
	throws RemoteException {
		return graph.containsEdge(user, mapUser.get(userID));
	}

	@Override
	public int getUserID() throws RemoteException {
		return userID;
	}

	@Override
	public int getOutDegreeOf(int currentUser) throws RemoteException {
		return graph.outDegreeOf(mapUser.get(currentUser));
	}

}
