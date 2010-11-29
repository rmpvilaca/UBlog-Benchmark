java -cp lib/graph-server-1.0-SNAPSHOT-client.jar:lib/jgrapht-0.8.0.jar:lib/graph-server-1.0-SNAPSHOT.jar:lib/ublog-benchmark-1.0-SNAPSHOT.jar -Djava.security.policy=conf/policy -Djava.rmi.server.hostname=$@ org.ublog.graph.Server 

