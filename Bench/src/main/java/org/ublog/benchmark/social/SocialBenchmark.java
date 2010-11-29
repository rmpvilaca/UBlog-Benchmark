/*******************************************************************************
 * Copyright 2010 Universidade do Minho, Ricardo Vilaa and Francisco Cruz
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

import java.io.UnsupportedEncodingException;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import org.ublog.utils.Pair;
import org.ublog.benchmark.BenchOperation;
import org.ublog.benchmark.ComplexBenchMarkClient;
import org.ublog.benchmark.ComplexBenchmark;
import org.ublog.benchmark.StatsCollector;
import org.ublog.benchmark.operations.CollectionOperation;
import org.ublog.benchmark.operations.MultiPutOperation;
import org.ublog.graph.GraphInterface;

import org.ublog.utils.Clock;

public class SocialBenchmark extends ComplexBenchmark {

	public final static String userTable = "users";
	public final static String tweetsTable = "tweets";
	public final static String friendsTimeLineTable = "friendsTimeLine";

	private Logger logger = Logger.getLogger(SocialBenchmark.class);

	//Values defined by configuration file
	private  double initialTweetsFactor=10.0;
	private double probabilitySearchPerTopic=0.15;
	private double probabilitySearchPerOwner=0.25;
	private double probabilityGetRecentTweets=0.05;
	private double probabilityGetFriendsTimeline=0.45;
	private double probabilityStartFollowing=0.05;
	private double probabilityStopFollowing=0.05;

	private int totalSize=0;
	private boolean missingCreatingTables;
	private boolean missingAddingUsers;

	private int currentUser;
	private int currentTweet;
	private int currentNTweets;

	private BenchOperation currentTweetOp;
	private UserService clouderUserService;
	private int nOperations;

	private Random rndOp;
	private Random rndOwner;
	private Random rndTopic;
	private Random rndStartFollow;

	private double thinkTime;

	private SocialOperationsFactory opFactory;
	private GraphInterface remoteGraphServer;

	private boolean hasInitiated;

	public SocialBenchmark(StatsCollector statsCollector,
			SocialOperationsFactory opFactory, int sizeTotal, int size,
			int usernameStarter, int nOperations, double thinkTime) {
		super(statsCollector, usernameStarter);
		this.opFactory = opFactory;
		Utils.NUsers = size;
		this.nOperations = nOperations;
		this.thinkTime = thinkTime;
		this.clouderUserService = new UserService();
		this.currentUser = 0;
		this.currentTweet = 0;
		this.currentNTweets = 0;
		this.missingCreatingTables = true;
		this.missingAddingUsers = true;
		this.totalSize=sizeTotal;
	}

	public double getProbabilitySearchPerTopic() {
		return probabilitySearchPerTopic;
	}



	public void setProbabilitySearchPerTopic(double probabilitySearchPerTopic) {
		this.probabilitySearchPerTopic = probabilitySearchPerTopic;
	}



	public double getProbabilitySearchPerOwner() {
		return probabilitySearchPerOwner;
	}



	public void setProbabilitySearchPerOwner(double probabilitySearchPerOwner) {
		this.probabilitySearchPerOwner = probabilitySearchPerOwner;
	}



	public double getProbabilityGetRecentTweets() {
		return probabilityGetRecentTweets;
	}



	public void setProbabilityGetRecentTweets(double probabilityGetRecentTweets) {
		this.probabilityGetRecentTweets = probabilityGetRecentTweets;
	}



	public double getProbabilityGetFriendsTimeline() {
		return probabilityGetFriendsTimeline;
	}



	public void setProbabilityGetFriendsTimeline(
			double probabilityGetFriendsTimeline) {
		this.probabilityGetFriendsTimeline = probabilityGetFriendsTimeline;
	}



	public double getProbabilityStartFollowing() {
		return probabilityStartFollowing;
	}



	public void setProbabilityStartFollowing(double probabilityStartFollowing) {
		this.probabilityStartFollowing = probabilityStartFollowing;
	}



	public double getProbabilityStopFollowing() {
		return probabilityStopFollowing;
	}



	public void setProbabilityStopFollowing(double probabilityStopFollowing) {
		this.probabilityStopFollowing = probabilityStopFollowing;
	}



	@Override
	public void initialize(Configuration conf) throws ConfigurationException {
		this.initialTweetsFactor=conf.getDouble("benchmark.social.initialTweetsFactor");

		Utils.MAX_MESSAGES_IN_TIMELINE=conf.getInt("benchmark.social.maximumMessagesTimeline");
		Utils.MaxNTweets=conf.getInt("benchmark.social.maximumTweetsPerUser");
		long seedNextOperation=conf.containsKey("benchmark.social.seedNextOperation") ? conf.getLong("benchmark.social.seedNextOperation"): System.nanoTime();
		long seedOwner=conf.containsKey("benchmark.social.seedOwner") ? conf.getLong("benchmark.social.seedOwner"): System.nanoTime();
		long seedTopic=conf.containsKey("benchmark.social.seedTopic") ? conf.getLong("benchmark.social.seedTopic"): System.nanoTime();
		long seedStartFollow=conf.containsKey("benchmark.social.seedStartFollow") ? conf.getLong("benchmark.social.seedStartFollow"): System.nanoTime();

		this.rndOp = new Random(seedNextOperation);
		this.rndOwner = new Random(seedOwner);
		this.rndTopic = new Random(seedTopic);
		this.rndStartFollow = new Random(seedStartFollow);


		this.probabilitySearchPerTopic=conf.getDouble("benchmark.social.probabilities.probabilitySearchPerTopic");
		this.probabilitySearchPerOwner=conf.getDouble("benchmark.social.probabilities.probabilitySearchPerOwner");
		this.probabilityGetRecentTweets=conf.getDouble("benchmark.social.probabilities.probabilityGetRecentTweets");
		this.probabilityGetFriendsTimeline=conf.getDouble("benchmark.social.probabilities.probabilityGetFriendsTimeline");
		this.probabilityStartFollowing=conf.getDouble("benchmark.social.probabilities.probabilityStartFollowing");
		this.probabilityStopFollowing=conf.getDouble("benchmark.social.probabilities.probabilityStopFollowing");
		if (this.probabilityGetFriendsTimeline+this.probabilityGetRecentTweets+this.probabilitySearchPerOwner+
				this.probabilitySearchPerTopic+this.probabilityStartFollowing+this.probabilityStopFollowing >1)
		{
			logger.warn("The sum of all probabilities must be less or equal than 1.");
			throw  new ConfigurationException("The sum of all probabilities must be less or equal than 1.");
		}


		String serverName=conf.getString("benchmark.server.name");
		int serverPort=conf.getInt("benchmark.server.port");
		this.remoteGraphServer = this.getRemoteGraphServer(serverName,serverPort);
		try {
			this.hasInitiated = this.remoteGraphServer.init(this.totalSize);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	private GraphInterface getRemoteGraphServer(String serverName,int serverPort) {
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new RMISecurityManager());
		}
		try {
			if (logger.isInfoEnabled())
			{
				logger.info("Trying to connect to graph server");
			}
			Registry reg = LocateRegistry.getRegistry(serverName,serverPort);
			System.out.println("reg " + reg.list().toString());
			return (GraphInterface) reg.lookup("Graph");

		} catch (NotBoundException ex) {
			Logger.getLogger("global").log(null, ex);
			return null;
		} catch (RemoteException ex) {
			ex.printStackTrace();
			Logger.getLogger("global").log(null, ex);
			return null;
		}
	}

	public String getPowerLawTag() {
		Random rnd = new Random();
		int val = 0;
		int i = 0;
		try {
			val = rnd.nextInt(this.remoteGraphServer.getSomaGraus()) + 1;
			int sum = 0;
			for (i = 0; i < 64; i++) {
				sum += this.remoteGraphServer.degreeOfGrausTags("topico" + i);
				if (sum >= val) {
					break;
				}
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return "topico" + i;
	}

	public int getRandomFriend(User myUser) {
		int res;
		try {
			res = this.remoteGraphServer.getRandomFriend(myUser);
		} catch (RemoteException e) {
			e.printStackTrace();
			res = -1;
		}
		if (res == -1)
			return Utils.NUsers;
		else
			return res;
	}

	@Override
	public ComplexBenchMarkClient createNewClient(Clock clock, int clientCount) {
		ComplexBenchMarkClient client;
		try {
			client = new SocialClient(clock, clientCount,
					this.remoteGraphServer.getClientCount(clientCount),
					this.nOperations, this.getStatsCollector(), this.thinkTime,
					this.rndOp, this, this.opFactory);
		} catch (RemoteException e) {
			e.printStackTrace();
			client = null;
		}
		return client;
	}

	@Override
	public boolean hasMoreInitOperations() {
		int userID;
		if (this.hasInitiated == false) {
			try {
				userID = this.remoteGraphServer.getUserID();
				// System.out.println("USERID HASMOREINIT:"+userID);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				userID = 1;
			}
			if (logger.isInfoEnabled())
				logger.info("currentUser:" + this.currentUser + ";userID:"
						+ userID + ";currentTweet:" + this.currentTweet
						+ ";currentNTweets:" + this.currentNTweets);
			return this.missingCreatingTables || this.missingAddingUsers
			|| this.currentUser < userID - 1
			|| this.currentTweetOp.hasMoreDBOperations()
			|| this.currentTweet < this.currentNTweets - 1;
		} else
			return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CollectionOperation nextInitOperation()
	throws UnsupportedEncodingException {
		CollectionOperation res;
		if (this.missingCreatingTables) {
			/*
			 * TableMetaData<String>
			 * metaData=this.collectionsMetadata.get(currentCollection);
			 * this.currentCollection++; res=new
			 * CreateTableOperation<String>(metaData);
			 * this.missingCreatingCollections = false;///////fui q pus
			 */
			res = null;
			this.missingCreatingTables = false;
		} else {

			if (missingAddingUsers) {
				int userID;
				try {
					userID = this.remoteGraphServer.getUserID();
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					userID = 1;
				}

				Map<String, Pair<Map<String, String>, List<Integer>>> puts = new HashMap<String, Pair<Map<String, String>, List<Integer>>>();
				for (int i = 0; i < userID; i++) {
					// User currentUser=this.mapUser.get(i);
					User currentUser;
					try {
						currentUser = this.remoteGraphServer.getMapUser(i);
					} catch (RemoteException e1) {
						e1.printStackTrace();
						currentUser = null;
					}
					Set<String> followers = new HashSet<String>();
					Set<String> following = new HashSet<String>();

					try {
						followers = this.remoteGraphServer
						.getFollowers(currentUser);
						following = this.remoteGraphServer
						.getFollowing(currentUser);
					} catch (RemoteException e) {
						e.printStackTrace();
					}
					/*
					 * for (DefaultEdge
					 * edge:this.graph.incomingEdgesOf(currentUser)) {
					 * followers.
					 * add(this.graph.getEdgeSource(edge).getUsername()); } for
					 * (DefaultEdge
					 * edge:this.graph.outgoingEdgesOf(currentUser)) {
					 * following.
					 * add(this.graph.getEdgeTarget(edge).getUsername()); }
					 */
					// List<Integer> tags=new ArrayList<Integer>();
					// tags.add(Utils.getTagUserID(currentUser.getUsername()));
					puts.put(
							currentUser.getUsername(),
							new Pair<Map<String, String>, List<Integer>>(
									this.clouderUserService.newUser(
											currentUser,
											Utils.toString(followers),
											Utils.toString(following)), null));
				}
				CollectionOperation tmp = new MultiPutOperation(null, "users",
						puts);
				tmp.setInit(true);
				res = tmp;
				this.missingAddingUsers = false;
				this.currentUser = 0;
				this.currentTweet = 0;
				try {
					// this.currentNTweets=(int)
					// Math.ceil(this.graph.outDegreeOf(this.mapUser.get(this.currentUser))*initialTweetsFactor);
					this.currentNTweets = (int) Math
					.ceil(this.remoteGraphServer
							.getOutDegreeOf(currentUser)
							* initialTweetsFactor);
					// this.currentTweetOp=this.getNewTweetOperation(this.mapUser.get(this.currentUser),
					// null);
					this.currentTweetOp = this.getNewTweetOperation(
							this.remoteGraphServer.getMapUser(currentUser),
							null);
					// System.out.println("currentNTweets: "+currentNTweets);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				this.currentTweetOp.setInit(true);
				// System.out.println("SET INIT op:"+this.currentTweetOp);
			} else {

				if (this.currentTweetOp.hasMoreDBOperations()) {
					res = this.currentTweetOp.getNextDBOperation();
				} else {
					this.currentTweet++;
					if (this.currentTweet < this.currentNTweets) {
						try {
							// this.currentTweetOp=this.getNewTweetOperation(this.mapUser.get(this.currentUser),
							// null);
							this.currentTweetOp = this.getNewTweetOperation(
									this.remoteGraphServer
									.getMapUser(currentUser), null);
						} catch (RemoteException e) {
							e.printStackTrace();
						}
						this.currentTweetOp.setInit(true);
						// System.out.println("SET INIT op:"+this.currentTweetOp);
						res = this.currentTweetOp.getNextDBOperation();
					} else {
						this.currentUser++;
						this.currentTweet = 0;
						try {
							// this.currentNTweets=(int)
							// Math.ceil(this.graph.outDegreeOf(this.mapUser.get(this.currentUser))*initialTweetsFactor);
							this.currentNTweets = (int) Math
							.ceil(this.remoteGraphServer
									.getOutDegreeOf(currentUser)
									* initialTweetsFactor);
							// this.currentTweetOp=this.getNewTweetOperation(this.mapUser.get(this.currentUser),
							// null);
							this.currentTweetOp = this.getNewTweetOperation(
									this.remoteGraphServer
									.getMapUser(currentUser), null);
						} catch (RemoteException e) {
							e.printStackTrace();
						}
						this.currentTweetOp.setInit(true);

						// System.out.println("SET INIT op:"+this.currentTweetOp);
						res = this.currentTweetOp.getNextDBOperation();
					}
				}
			}
		}
		if (logger.isInfoEnabled())
			logger.info("Next Init Operation is:" + res);
		return res;
	}

	public String getNextStartFollowingUser(User user) {
		// Da o utilizador com mais seguidores ao qual o user ainda nao segue
		String res = null;
		while (true) {
			int userID = this.rndStartFollow.nextInt(Utils.NUsers);
			boolean containsEdge;
			try {
				containsEdge = this.remoteGraphServer.graphContainsEdge(user,
						userID);
			} catch (RemoteException e) {
				e.printStackTrace();
				containsEdge = false;
			}
			if (!containsEdge) {
				try {
					res = this.remoteGraphServer.getMapUser(userID)
					.getUsername();
				} catch (RemoteException e) {
					e.printStackTrace();
				}
				break;
			}
		}
		return res;
	}

	public String getNextStopFollowingUser(User user) {
		// Da o utilizador com menos seguidores ao qual o user ainda segue
		try {
			return this.remoteGraphServer.getNextStopFollowingUser(user);
		} catch (RemoteException e) {
			e.printStackTrace();
			return null;
		}
	}

	public BenchOperation getNewTweetOperation(User user, SocialClient client) {
		// System.out.println("NEW TWEET OP");
		double hasOwner = rndOwner.nextDouble();
		double hasTopic = rndTopic.nextDouble();
		// System.out.println("TAGS PROB: "+hasOwner+";"+hasTopic);
		Set<String> tags = new HashSet<String>();
		if (hasTopic <= 0.05) // 0.05
		{
			tags.add(this.getPowerLawTag());
		}
		if (hasOwner <= 0.35) {
			int friend = this.getRandomFriend(user);
			tags.add("user" + friend);
			// System.out.println("user"+friend);
		}
		Message tweet = new Message();
		// tweet.setDate(new Date());
		tweet.setDate(Utils.getTimeUUID());
		tweet.setText("qq");
		tweet.setUser(user.getUsername());
		return this.opFactory.getNewTweetOperation(client, tweet, tags);
	}
}
