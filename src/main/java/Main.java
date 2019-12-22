import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.*;
import java.util.*;


public class Main {
    private static final int threadSleepMs = 100;
    private static final String pathToConfig = "src/main/resources/config.properties";
    private static final Logger logger = LogManager.getLogger(Main.class);

    private static Path dataDirectoryPath;
    private static String[] categories;
    private static int parallelTaskCount;
    private static int requestDelaySeconds;
    private static String pathToCrawledData;
    private static String csvFilename;
    static int maxPagesPerLevel;


    static int getThreadSleepMs() {
        return threadSleepMs;
    }

    static String[] getCategories() {
        return categories;
    }

    static int getParallelTaskCount() {
        return parallelTaskCount;
    }
    static String getCsvFilename() {
        return csvFilename;
    }

    static Path getDataDirectoryPath(){
        return dataDirectoryPath;
    }

    static int getRequestDelaySeconds() {
        return requestDelaySeconds;
    }



    private static void readProperties() throws UnsupportedEncodingException {
        Properties properties = new Properties();

        try {
            FileInputStream fileInputStream = new FileInputStream(pathToConfig);
            properties.load(fileInputStream);
            fileInputStream.close();
        } catch (IOException e) {
            logger.error(e);
        }

        categories = new String(properties.getProperty("categories").getBytes("ISO8859-1")).split(",");
        parallelTaskCount = Integer.valueOf(properties.getProperty("parallel_task_number"));
        requestDelaySeconds = Integer.valueOf(properties.getProperty("request_delay_seconds"));
        pathToCrawledData = properties.getProperty("path_to_crawled_data");
        csvFilename = properties.getProperty("csv_filename").replaceAll("[\\\\/]", "");
        maxPagesPerLevel = Integer.valueOf(properties.getProperty("max_pages_per_level"));

    }


    public static void main(String[] args) {
        logger.info("Crawler started");

        StringBuilder finalCsvData = new StringBuilder();

        try {
            readProperties();
        } catch (UnsupportedEncodingException e) {
            logger.error("Cannot read properties");
            logger.error(e);
            System.exit(1);
        }

        if (pathToCrawledData.equals(".")) {
            dataDirectoryPath = Paths.get(System.getProperty("user.dir"), "wiki_crawled_data");
        } else {
            dataDirectoryPath = Paths.get(pathToCrawledData, "wiki_crawled_data");
        }
//

        try {
            Crawler.run();
        } catch (InterruptedException e) {
            logger.error(e);
            System.exit(1);
        }
        logger.info("Crawler finished");
    }
}
