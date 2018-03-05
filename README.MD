Run tar xzvf streams-test.tar.gz  to extract all files then cd streams-test.

twitter-config.properties file contains twitter account credentials. Please change to your own.

twitter-topic.properties file contains the stream name and two topics. The two topics are also the two keywords you want to monitor on Twitter.

Use maprcli command to create the stream and the two topics, if they don't exist yet.

Run compile-all.sh to recompile all java classes. Class files are under classes directory.

start-producer.sh will start the stream producer. Producer listens to tweets that mention the two keywords you defined, and publish them to corresponding topics.

start-consumer.sh will start the stream consumer. The consumer listens to the two topics defined. For each message in topic1, it writes the message to a file under output directory. For each message in topic2, it displays the message in current window (stdout).

Login to your twitter account and send a tweet mentions the first keyword (topic1), you should see it gets appended to the output file.

CTRL-C to stop producer and consumer.

