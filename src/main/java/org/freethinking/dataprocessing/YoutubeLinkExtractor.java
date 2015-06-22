package org.freethinking.dataprocessing;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.validator.UrlValidator;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.youtube.model.Video;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public class YoutubeLinkExtractor implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(YoutubeLinkExtractor.class);
    private static int MAX_REDIRECT_COUNT = 15;  // Max number of redirects

    private static String regex = "\\(?\\b(https?://|www[.])[-A-Za-z0-9+&amp;@#/%?=~_()|!:,.;]*[-A-Za-z0-9+&amp;@#/%=~_()|]";
    private static Pattern p = Pattern.compile(regex);
    private static Pattern youtubeIdPattern = Pattern.compile("(?<=v=).*?(?=&|$)", Pattern.CASE_INSENSITIVE);

    private ConcurrentSkipListSet<Video> youtubeList;
    private ConcurrentHashMap<String, Video> youtubeMap;
    private SynchronizedCounter processedCounter;

    YouTubeVideoStatistics youtubeVideoStatistics;
    private String queueName = "test1";
    private String hostName = "localhost";
    private int workerId;

    public ConcurrentHashMap<String, Video> getYoutubeMap() {
        return youtubeMap;
    }

    public void setYoutubeMap(ConcurrentHashMap<String, Video> youtubeMap) {
        this.youtubeMap = youtubeMap;
    }

    public ConcurrentSkipListSet<Video> getYoutubeList() {
        return youtubeList;
    }

    public void setYoutubeList(ConcurrentSkipListSet<Video> youtubeList) {
        this.youtubeList = youtubeList;
    }

    public SynchronizedCounter getProcessedCounter() {
        return processedCounter;
    }

    public void setProcessedCounter(SynchronizedCounter processedCounter) {
        this.processedCounter = processedCounter;
    }

    public YoutubeLinkExtractor(String queueName, String hostName, int workerId) throws FileNotFoundException,
            IOException {
        this.queueName = queueName;
        this.hostName = hostName;
        this.workerId = workerId;

        youtubeVideoStatistics = new YouTubeVideoStatistics();
        logger.debug("Created YoutubeLinkExtractor {}", workerId);
    }

    public void run() {

        Channel channel;
        QueueingConsumer consumer;
        int count = 0;

        logger.debug("Started YoutubeLinkExtractor {}", workerId);
        try {
            channel = getChannel(hostName);
            channel.queueDeclare(queueName, false, false, false, null);
            consumer = new QueueingConsumer(channel);

            channel.basicConsume(queueName, false, consumer);
            channel.basicQos(1);

            logger.debug(" [*] Waiting for messages. To exit press CTRL+C");

            QueueingConsumer.Delivery delivery;
            while (true) {
                try {

                    delivery = consumer.nextDelivery();
                    String message = new String(delivery.getBody());
                    process(message);
                    ProcessTweets.processedCounter.increment();
                    count++;

                    if (count % 10 == 0)
                        logger.debug("Worker Id : " + workerId + " Count : " + count);

                    int totalProcessedCount = ProcessTweets.processedCounter.getCount();
                    if (totalProcessedCount % 100 == 0) {
                        logger.debug("Worker Id : " + workerId + " Total processedCount Count : "
                                + totalProcessedCount);
                        logger.debug("{}", new DateTime());
                    }
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                } catch (ShutdownSignalException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (ConsumerCancelledException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        } catch (IOException ex) {
        }
    }

    private static Channel getChannel(String hostName) throws IOException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(hostName);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        return channel;
    }

    private void process(String message) throws MalformedURLException, IOException, InterruptedException {

        // logger.debug("In process {}", workerId);
        List<String> urls = getURLs(message);
        List<String> youtubeUrls = getYouTubeUrls(urls);
        updateYoutubeVideoStats(youtubeUrls);
    }

    public void updateYoutubeVideoStats(List<String> youtubeUrls) throws FileNotFoundException, IOException {

        List<Video> videoList = new ArrayList<Video>();
        for (String url : youtubeUrls) {
            String videoId = getVideoIdFromYoutubeUrl(url);
            videoList = youtubeVideoStatistics.getVideoStatsFromId(videoId);

            for (Video video : videoList) {

                if (youtubeMap.get(video.getId()) == null) {
                    youtubeList.add(video);
                    youtubeMap.put(video.getId(), video);
                    logger.debug("YouTube Video Id : " + video.getId());
                }
            }
        }
    }

    public String getVideoIdFromYoutubeUrl(String url) {

        String id = "";

        Matcher matcher = youtubeIdPattern.matcher(url);
        if (matcher.find()) {
            id = matcher.group();
        }

        return id;
    }

    // get all links from the body for easy retrieval
    private ArrayList<String> getURLs(String text) {

        // logger.debug("In getUrls {}", workerId);
        ArrayList<String> links = new ArrayList<String>();
        Matcher m = p.matcher(text);
        while (m.find()) {
            String urlStr = m.group();
            if (urlStr.startsWith("(") && urlStr.endsWith(")"))
            {
                urlStr = urlStr.substring(1, urlStr.length() - 1);
            }
            links.add(urlStr);
        }
        return links;
    }

    public List<String> getYouTubeUrls(List<String> urls) throws MalformedURLException, IOException,
            InterruptedException {

        // logger.debug("In getYouTubeUrls {}", workerId);

        List<String> youtubeURLs = new ArrayList<String>();

        for (String shortUrl : urls) {

            UrlValidator urlValidator = new UrlValidator();
            urlValidator.isValid(shortUrl);
            String longUrl = getLongUrl(shortUrl);
            if (isYouTubeLink(longUrl)) {
                youtubeURLs.add(longUrl);
            }
        }
        return youtubeURLs;
    }

    public String getLongUrl(String shortUrl) throws MalformedURLException, IOException {

        // logger.debug("In getLongUrl {}", workerId);

        String result = shortUrl;
        String header;

        int redCount = 0; // # of redirects
        do {
            URL url = null;
            try {
                url = new URL(result);
            } catch (MalformedURLException ex) {
                logger.error("Malformed Exception : {}", result);
                result = "";
                break;
            }

            HttpURLConnection.setFollowRedirects(false);
            URLConnection conn = url.openConnection();
            header = conn.getHeaderField(null);
            if (header == null) {
                // logger.error("Header is null : {}", result);
                result = "";
                break;
            }
            String location = conn.getHeaderField("location");
            if (location != null) {
                result = location;
                // System.out.println("Redirect Url : " + result);
            }

            if (redCount == MAX_REDIRECT_COUNT) {
                result = "";
            }
            redCount += 1;
        } while (header.contains("301") && redCount <= MAX_REDIRECT_COUNT);

        return result;
    }

    public static Boolean isYouTubeLink(String link) {

        Boolean youTubeUrl = false;
        String pattern = "https?:\\/\\/(?:[0-9A-Za-z-]+\\.)?(?:youtu\\.be\\/|youtube\\.com\\S*[^\\w\\-\\s])([\\w\\-]{11})(?=[^\\w\\-]|$)(?![?=&+%\\w]*(?:['\"][^<>]*>|<\\/a>))[?=&+%\\w]*";
        if (!link.isEmpty() && link.matches(pattern)) {
            youTubeUrl = true;
        }

        return youTubeUrl;
    }
}
