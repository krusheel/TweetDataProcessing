package org.freethinking.dataprocessing;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Joiner;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.Video;
import com.google.api.services.youtube.model.VideoListResponse;
import com.google.api.services.youtube.model.VideoStatistics;

public class YouTubeVideoStatistics {

    /** Global instance of the HTTP transport. */
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();

    /**
     * Define a global variable that identifies the name of a file that
     * contains the developer's API key.
     */
    private static final String PROPERTIES_FILENAME = "youtube.properties";
    private static final long NUMBER_OF_VIDEOS_RETURNED = 25;

    private static Logger logger = LoggerFactory.getLogger(YouTubeVideoStatistics.class);

    /**
     * Define a global instance of a Youtube object, which will be used
     * to make YouTube Data API requests.
     */
    private static YouTube youtube;
    private static PropertiesConfiguration prop = null;

    public YouTubeVideoStatistics() throws FileNotFoundException, IOException {
        try {
            prop = new PropertiesConfiguration(new File(PROPERTIES_FILENAME));
            prop.load();
            logger.info("loaded properties file succesfully");
        } catch (Exception e) {
            logger.error("Exception while loading properties file : {}", PROPERTIES_FILENAME);
        }

        // This object is used to make YouTube Data API requests. The last
        // argument is required, but since we don't need anything
        // initialized when the HttpRequest is initialized, we override
        // the interface and provide a no-op function.
        youtube = new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY, new HttpRequestInitializer() {
            public void initialize(HttpRequest request) throws IOException {
            }
        }).setApplicationName("Video Statistics").build();
    }

    public List<Video> getVideoStatsFromId(String Id) {

        List<Video> videoList = new ArrayList<Video>();

        try {

            // Define the API request for retrieving search results.
            // String apiKey = properties.getProperty("youtube.apikey");
            YouTube.Videos.List listVideosRequest = youtube.videos().list("statistics").setId(Id);
            listVideosRequest.setKey(prop.getString("youtube.apikey"));

            VideoListResponse listVideoResponse = listVideosRequest.execute();
            videoList = listVideoResponse.getItems();

        } catch (GoogleJsonResponseException e) {
            System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
                    + e.getDetails().getMessage());
        } catch (IOException e) {
            System.err.println("There was an IO error: " + e.getCause() + " : " + e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
        }

        return videoList;
    }

    /**
     * Initialize a YouTube object to search for videos on YouTube. Then
     * display the name and thumbnail image of each video in the result set.
     *
     * @param args
     *            command line args.
     */

    public static void main(String[] args) {

        try {

            // This object is used to make YouTube Data API requests. The last
            // argument is required, but since we don't need anything
            // initialized when the HttpRequest is initialized, we override
            // the interface and provide a no-op function.
            youtube = new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY, new HttpRequestInitializer() {
                public void initialize(HttpRequest request) throws IOException {
                }
            }).setApplicationName("Video Statistics").build();

            // Define the API request for retrieving search results.
            try {
                prop = new PropertiesConfiguration(new File(PROPERTIES_FILENAME));
                prop.load();
                logger.info("loaded properties file succesfully");
            } catch (Exception e) {
                logger.error("Exception while loading properties file : {}", PROPERTIES_FILENAME);
            }
            String apiKey = prop.getString("youtube.apikey");
            YouTube.Videos.List listVideosRequest = youtube.videos().list("statistics").setId("5WQSDa-FJ3Y");
            listVideosRequest.setKey(apiKey);

            VideoListResponse listVideoResponse = listVideosRequest.execute();
            List<Video> videoList = listVideoResponse.getItems();

            List<String> videoIdList = new ArrayList<String>();
            List<VideoStatistics> videoStatisticsList = new ArrayList<VideoStatistics>();
            if (videoList != null) {

                // Merge video IDs
                for (Video video : videoList) {
                    videoIdList.add(video.getId());
                    videoStatisticsList.add(video.getStatistics());
                }
                Joiner stringJoiner = Joiner.on(',');
                String videoId = stringJoiner.join(videoIdList);

                // Call the YouTube Data API's youtube.videos.list method to
                // retrieve the resources that represent the specified videos.
                if (videoList != null) {
                    prettyPrint(videoList.iterator(), "");
                }

            }
        } catch (GoogleJsonResponseException e) {
            System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
                    + e.getDetails().getMessage());
        } catch (IOException e) {
            System.err.println("There was an IO error: " + e.getCause() + " : " + e.getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    /*
     * Prints out all results in the Iterator. For each result, print the
     * VideoId, View Count, Comment Count, Like Count, Dislike Count
     * 
     * @param iteratorVideoResults Iterator of Videos to print
     * 
     * @param query Search query (String)
     */
    private static void prettyPrint(Iterator<Video> iteratorVideoResults, String query) {

        System.out.println("\n=============================================================");
        System.out.println(
                "   First " + NUMBER_OF_VIDEOS_RETURNED + " videos for search on \"" + query + "\".");
        System.out.println("=============================================================\n");

        if (!iteratorVideoResults.hasNext()) {
            System.out.println(" There aren't any results for your query.");
        }

        while (iteratorVideoResults.hasNext()) {

            Video singleVideo = iteratorVideoResults.next();

            System.out.println(" Video Id : " + singleVideo.getId());
            System.out.println(" View Count : " + singleVideo.getStatistics().getViewCount());
            System.out.println(" Comment Count : " + singleVideo.getStatistics().getCommentCount());
            System.out.println(" Like Count : " + singleVideo.getStatistics().getLikeCount());
            System.out.println(" Dislike Count : " + singleVideo.getStatistics().getDislikeCount());
            System.out.println("\n-------------------------------------------------------------\n");
        }
    }

}
