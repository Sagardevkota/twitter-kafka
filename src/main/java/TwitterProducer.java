import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private Logger logger;
    private final String CONSUMER_KEY = "ZCuQXilmFv0nxIOk6G0dq8lPG";
    private final String CONSUMER_SECRET = "UIuY9PrAvzjishI94dG2n9C3ZdNxJMMu8bZbFZ9b2Wetrk3AiK";
    private final String ACCESS_TOKEN = "1331123795986313216-UJPcsvDgvnLI2GLe8jeKOteouhhb1d";
    private final String TOKEN_SECRET = "tEIqZ53ZsumfunZslCKgHXa89kYA2EkLWnNDfdPC1AnLi";
    private BlockingQueue<String> msgQueue;
    List<String> terms = Lists.newArrayList("java");

    public TwitterProducer(){
        logger = LoggerFactory.getLogger(TwitterProducer.class);
    }


    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    public void run(){

        //Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */

        msgQueue = new LinkedBlockingQueue<>(100000);

        //create twitter client
        Client twitterClient = createTwitterClient();
        // Attempts to establish a connection.
        twitterClient.connect();

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();


        while (!twitterClient.isDone()){
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                if (msg!=null){
                    logger.info(msg);
                    kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e!=null)
                                logger.error("Something bad happened");
                        }
                    });
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
                twitterClient.stop();
            }
        }


        //create a kafka producer

        //loop to send tweets to kafka
    }

    private KafkaProducer<String, String> createKafkaProducer() {

        String bootstrapServers = "127.0.0.1:9092";
        //create the producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        //high throughput producer at the expense of latency
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32 * 1024)); //32kb batch size


        //create the producer
        return new KafkaProducer<String, String>(properties);
    }

    public Client createTwitterClient(){


        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));// optional: use this if you want to process client events

        return builder.build();

    }
}
