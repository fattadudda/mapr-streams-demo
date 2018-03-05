import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
	
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.InterruptedException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.List;
import java.util.Arrays;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterException;
import twitter4j.StallWarning;

	
public class TwitterProducerDemo {
	
    // Declare a new producer.
    public static KafkaProducer producer;
    public static ProducerRecord<String, String>  rec;
    public static String[] topics = getTopics();
    public static TwitterStream twitterStream = configureTwitterStream();
	

	
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException, TwitterException {

        configureProducer(args);

	StatusListener listener = new StatusListener(){
            public void onStatus(Status status) {
	        String text = status.getText();
                //topics[0] is actually stream name
                String msg = null;

                msg = status.getUser().getName() + " : " + text;
                System.out.println(msg);

	        if (text.contains(topics[1])) { 
                   rec = new ProducerRecord<String, String>(topics[0]+topics[1], msg);
	           producer.send(rec);		
	        }

	        if (text.contains(topics[2])) { 
                   rec = new ProducerRecord<String, String>(topics[0]+topics[2], msg);
	           producer.send(rec);		
	        }
            }
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
	    public void onStallWarning(StallWarning sw){}
	    public void onScrubGeo(long userId, long upToStatusId){}
    	};
	twitterStream.addListener(listener);
	twitterStream.filter(new FilterQuery(0, null, Arrays.copyOfRange(topics, 1, 3)));

        //producer.close();
        System.out.println("Listening..." + Arrays.toString(Arrays.copyOfRange(topics,1,3)));
    }
    /* Set the value for a configuration parameter.
       This configuration parameter specifies which class
       to use to serialize the value of each message.*/
    public static void configureProducer(String[] args) {
        Properties props = new Properties();
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("streams.parallel.flushers.per.partition", "false");
        producer = new KafkaProducer<String, String>(props);
    }

    /* Twitter credential is stored in twitter-config.properties file
     *
     */
    public static TwitterStream configureTwitterStream(){
        ConfigurationBuilder cb = new ConfigurationBuilder();
        Properties props = new Properties();
        FileInputStream input = null;
        try {
            input = new FileInputStream("twitter-config.properties");
            props.load(input);
            cb.setDebugEnabled(Boolean.valueOf(props.getProperty("DebugEnabled")));
            cb.setOAuthConsumerKey(props.getProperty("OAuthConsumerKey"));
            cb.setOAuthConsumerSecret(props.getProperty("OAuthConsumerSecret"));
            cb.setOAuthAccessToken(props.getProperty("OAuthAccessToken"));
            cb.setOAuthAccessTokenSecret(props.getProperty("OAuthAccessTokenSecret"));
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return new TwitterStreamFactory(cb.build()).getInstance();
    }

    /* Topic names and stream name are stored in twitter-topics.properties file
     *
     */
    public static String[] getTopics(){
        Properties props = new Properties();
        FileInputStream input = null;
        String[] topics = null;
        try {
            input = new FileInputStream("twitter-topics.properties");
            props.load(input);
            topics = new String[]{props.getProperty("Stream"), props.getProperty("Topic1"), props.getProperty("Topic2")};
            System.out.println(Arrays.toString(topics));
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return topics;
    }
}
