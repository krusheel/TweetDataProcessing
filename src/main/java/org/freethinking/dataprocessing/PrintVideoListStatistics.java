package org.freethinking.dataprocessing;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.youtube.model.Video;

public class PrintVideoListStatistics implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(PrintVideoListStatistics.class);

    ConcurrentHashMap<String, Video> youtubeStatsList;
    ConcurrentSkipListSet<Video> youtubeList;

    public PrintVideoListStatistics(ConcurrentSkipListSet<Video> youtubeList,
            ConcurrentHashMap<String, Video> youtubeStatsList) {
        this.youtubeStatsList = youtubeStatsList;
        this.youtubeList = youtubeList;
    }

    public void run() {

        while (true) {

            try {
                Thread.sleep(600000l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            logger.info("# of youtube links detected : {}", youtubeStatsList.size());
            for (Video video : youtubeList) {
                logger.info("Stats for Youtube Video Id : {}", video.getId());
                logger.info("View Count : {} Like Count : {}", video.getStatistics()
                        .getViewCount(), video.getStatistics().getLikeCount());
            }

        }

    }
}
