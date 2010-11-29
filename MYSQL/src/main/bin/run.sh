CLASSPATH=`for i in \`find lib -follow -name \*.jar\`; do echo -n $i:; done`:conf
java -cp $CLASSPATH -Djava.security.policy=conf/policy org.ublog.benchmark.executor.MySQLSocialBenchmarkStarter $@ 



