. twitter-topics.properties
echo 'Topic1 is : ' $Stream$Topic1
echo 'Topic2 is : ' $Stream$Topic2

java -classpath ./classes:/opt/mapr/lib/mapr-streams-5.1.0-mapr.jar:/opt/mapr/lib/mapr-streams-5.1.0-mapr-tests.jar:/opt/mapr/lib/kafka-clients-0.9.0.0-mapr-1602.jar:/opt/mapr/lib/log4j-1.2.17.jar:/opt/mapr/lib/slf4j-api-1.7.12.jar:/opt/mapr/lib/slf4j-log4j12-1.7.12.jar:/opt/mapr/lib/maprfs-5.1.0-mapr.jar:/opt/mapr/lib/protobuf-java-2.5.0.jar:./twitter4j/lib/twitter4j-core-4.0.4.jar:./twitter4j/lib/twitter4j-stream-4.0.4.jar TwitterConsumerDemo $Stream$Topic1 $Stream$Topic2
