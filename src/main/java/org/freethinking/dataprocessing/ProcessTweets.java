package org.freethinking.dataprocessing;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.api.services.youtube.model.Video;

public class ProcessTweets {

    private static String queueName = "test1";
    private static String hostName = "localhost";
    private static int threadPoolLimit = 12; // One thread for publisher and One thread for printing number of videos
                                             // found till now
    private static int numWorkers = 0;

    protected static int publishCounter;
    protected static SynchronizedCounter processedCounter;
    protected static ConcurrentSkipListSet<Video> youtubeList;
    protected static ConcurrentHashMap<String, Video> youtubeMap;

    public static class SortByLikes implements Comparator<Video> {

        public int compare(Video v1, Video v2) {
            return v2.getStatistics().getLikeCount().compareTo(v1.getStatistics().getLikeCount());
        }
    }

    public static void main(String[] args) throws FileNotFoundException, IOException {

        processedCounter = new SynchronizedCounter();
        youtubeList = new ConcurrentSkipListSet<Video>(new ProcessTweets.SortByLikes());
        youtubeMap = new ConcurrentHashMap<String, Video>();

        ExecutorService executor = Executors.newFixedThreadPool(12);
        executor.submit(new PublishTweetsText());
        executor.submit(new PrintVideoListStatistics(youtubeList, youtubeMap));
        for (int i = 0; i < threadPoolLimit - 2; i++) {

            YoutubeLinkExtractor youtubeLinkExtractor = new YoutubeLinkExtractor(queueName, hostName, numWorkers++);
            youtubeLinkExtractor.setProcessedCounter(processedCounter);
            youtubeLinkExtractor.setYoutubeList(youtubeList);
            youtubeLinkExtractor.setYoutubeMap(youtubeMap);

            executor.submit(youtubeLinkExtractor);
        }
    }

}
