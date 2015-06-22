package org.freethinking.dataprocessing;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PublishTweetsText implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(PublishTweetsText.class);

    public void run() {

        String fileName = "/Users/krusheel/Projects/ProgrammingTest/DataProcessing/sample_tweets_data.json";
        String hostName = "localhost";
        String queueName = "test1";

        int count = 0;
        TweetFileReader tweetFileReader;
        QueuePublisher queuePublisher;

        try {
            queuePublisher = new QueuePublisher(hostName, queueName);
            tweetFileReader = new TweetFileReader(fileName);
            String tweetText = tweetFileReader.getNextTextFieldValue();
            while (tweetText != null && count < 1000) {
                ProcessTweets.publishCounter++;
                queuePublisher.publishToRabbitMQ(tweetText);
                tweetText = tweetFileReader.getNextTextFieldValue();
                count++;
            }

            queuePublisher.close();
        } catch (IOException ex) {
            logger.error("Error creating QueuePublisher or TweetFileReader");
        }

        logger.debug("Number of messages published {}", ProcessTweets.publishCounter);
    }
}
