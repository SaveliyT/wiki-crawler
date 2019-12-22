import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


public class ProcessCategory implements Callable<String> {
    private static final Logger logger = LogManager.getLogger(ProcessCategory.class);

    private final String category;
    private final String globalCategory;
    private final int subcategoryLevel;
    private final Path path;
    private final String filenamePrefix;
    private StringBuilder csvData;

    private static final String url = "https://ru.wikipedia.org/w/api.php";
    private CloseableHttpClient httpClient;
    private final List<NameValuePair> paramsCat = new ArrayList<>();
    private final List<NameValuePair> paramsPage = new ArrayList<>();


    ProcessCategory (TaskParameters taskParameters){
        this.category = taskParameters.getCurrentCategory();
        this.globalCategory = taskParameters.getGlobalCategory();
        this.subcategoryLevel = taskParameters.getLevel();
        this.path = taskParameters.getPathToStoreCategoryPages();
        this.filenamePrefix = taskParameters.getPrefixForPageFilename();

        httpClient = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom()
                .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        paramsCat.add(new BasicNameValuePair("action", "query"));
        paramsCat.add(new BasicNameValuePair("format", "json"));
        paramsCat.add(new BasicNameValuePair("list", "categorymembers"));
        paramsCat.add(new BasicNameValuePair("cmprop", "title|type|ids"));
        paramsCat.add(new BasicNameValuePair("cmlimit", "500"));

        paramsPage.add(new BasicNameValuePair("action", "query"));
        paramsPage.add(new BasicNameValuePair("format", "json"));
        paramsPage.add(new BasicNameValuePair("prop", "extracts"));
        paramsPage.add(new BasicNameValuePair("exsectionformat", "plain"));
    }


    private String makeCategoryUrl(String categoryName, String cmcontinue) {
        paramsCat.add(new BasicNameValuePair("cmtitle", categoryName));
        paramsCat.add(new BasicNameValuePair("cmcontinue", cmcontinue));
        String encodedParams = URLEncodedUtils.format(paramsCat, "UTF-8");
        paramsCat.remove(paramsCat.size()-1);
        paramsCat.remove(paramsCat.size()-1);
        return url + "?" + encodedParams;
    }


    private String makePageUrl(String pageId){
        paramsPage.add(new BasicNameValuePair("pageids", pageId));
        String encodedParams = URLEncodedUtils.format(paramsPage, "UTF-8");
        paramsPage.remove(paramsPage.size()-1);
        return url + "?" + encodedParams;
    }

    private String quoteWrapper(String value){
        return "\"" + value + "\"";
    }


    private void downloadPage(String pageName, String pageId, int i) throws IOException, ParseException {
        logger.info("Downloading page: " + pageName + " global: " + globalCategory + " local: " + category + " level: " + subcategoryLevel);

        // Create directory if not exists
        if (!Files.exists(path)){
            try {
                Files.createDirectories(path);
            } catch (IOException e){
                e.printStackTrace();
            }
        }

        String pageUrl = makePageUrl(pageId);
        logger.debug(pageUrl);

        HttpGet get = new HttpGet(pageUrl);
        HttpResponse response = httpClient.execute(get);

        if (response.getStatusLine().getStatusCode() == 200) {
            HttpEntity entity = response.getEntity();
            String jsonStr = EntityUtils.toString(entity);
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonStr);
            JSONObject queryObject = (JSONObject) jsonObject.get("query");
            JSONObject pages = (JSONObject) queryObject.get("pages");
            JSONObject page = (JSONObject) pages.get(pageId);
            String title = (String) page.get("title");
            String text = (String) page.get("extract");

            byte[] dataToWrite = text.getBytes();

            String fileId = filenamePrefix + String.format("%03d", i);

            logger.info("Saving " + fileId + ".txt" + " to " + Paths.get(path.toString()));
            Files.write(Paths.get(path.toString(), fileId + "_" +
                    title.replaceAll("[\\\\/]", " ") + ".txt"), dataToWrite);

            String url = "https://ru.wikipedia.org/wiki/" + URLEncoder.encode(title, "UTF-8")
                    .replaceAll("\\+", "_");
            String categoryNumber = fileId.split("_")[0];
            String pageLength = String.valueOf(text.length());


            csvData.append(String.join(",",
                    quoteWrapper(fileId),
                    quoteWrapper(title),
                    quoteWrapper(url),
                    categoryNumber,
                    String.valueOf(subcategoryLevel),
                    pageLength));
            csvData.append("\n");

        } else {
            logger.error("Bad response status:\n URL:\n " + pageUrl + "\nStatus:\n" + response.getStatusLine());
        }
    }


    @Override
    public String call() throws IOException, ParseException, InterruptedException {
        HashMap<String, String> pageNameIdMap = new HashMap<>();
        List<String> subcategories = new ArrayList<>();
        String cmcontinue = "";
        boolean allDataGot = false;
        int i;

        // Check if downloaded pages count for previous level of same global category is less than needed
        int previousLevelCount = Crawler.getLevelCounts().getOrDefault(globalCategory + (subcategoryLevel-1), 0);
        if (previousLevelCount > Main.maxPagesPerLevel){
            logger.debug("Count for category : " + globalCategory + " and level " + (subcategoryLevel-1) +
                    " is " + previousLevelCount);
            return "";
        }

        while (!allDataGot) {

            String categoryUrl = makeCategoryUrl(category, cmcontinue);
            logger.debug(categoryUrl);
            HttpGet get = new HttpGet(categoryUrl);

            HttpResponse response = httpClient.execute(get);

            if (response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                String jsonStr = EntityUtils.toString(entity);

                JSONParser jsonParser = new JSONParser();
                JSONObject jsonObject = (JSONObject) jsonParser.parse(jsonStr);
                JSONObject queryObject = (JSONObject) jsonObject.get("query");
                JSONArray categoryMembers = (JSONArray) queryObject.get("categorymembers");
                if (jsonObject.containsKey("continue")) {
                    JSONObject continueObj = (JSONObject) jsonObject.get("continue");
                    cmcontinue = (String) continueObj.get("cmcontinue");
                } else {
                    allDataGot = true;
                }

                i = 0;
                for (JSONObject categoryMember : (Iterable<JSONObject>) categoryMembers) {
                    String type = (String) categoryMember.get("type");
                    long ns = (long) categoryMember.get("ns");
                    if (type.equals("page") && ns == 0) {
                        pageNameIdMap.put((String) categoryMember.get("title"),
                                String.valueOf((long) categoryMember.get("pageid")));
                        i++;
                    } else if (type.equals("subcat")) {
                        subcategories.add((String) categoryMember.get("title"));
                    }
                }
            } else {
                logger.error("Bad response status:\n" + response.getStatusLine());
            }
        }

        if (pageNameIdMap.isEmpty() && subcategories.isEmpty()){
            logger.error("No data returned from query\n" + "Check category name: " + category);
            Crawler.deleteDataDirectory(Main.getDataDirectoryPath());
            logger.info("Deleted directory " + Main.getDataDirectoryPath().toString());
            System.exit(1);
        }

        List<String> pageNames = new ArrayList<>(pageNameIdMap.keySet());
        pageNames.sort(null);
        subcategories.sort(null);

        i = 0;
        for (String pageName : pageNames){
            downloadPage(pageName, pageNameIdMap.get(pageName), i);
            TimeUnit.SECONDS.sleep(Main.getRequestDelaySeconds());
            i++;
        }


        int currentLevelCount = Crawler.getLevelCounts().getOrDefault(globalCategory + subcategoryLevel, 0);
        Crawler.addLevelCount(globalCategory + subcategoryLevel, currentLevelCount + i);
        logger.debug("Current level cnt: " + currentLevelCount + i);

        i = 0;
        for (String subcat: subcategories){
            Path newPath = Paths.get(path.toString(), filenamePrefix + String.format("%03d", i) + "_" +
                    subcat.replace("Категория:", ""));

            String newPrefix = filenamePrefix + String.format("%03d", i) + "_";

            Crawler.addTaskToQueue(new TaskParameters(subcat, globalCategory, subcategoryLevel+1, newPath, newPrefix));
            i++;
        }

        logger.debug("CSV: \n" + csvData.toString());
        String key = globalCategory + subcategoryLevel;
        List<String> currentRunningCats = Crawler.getCurrentProcessingLevels().get(key);
        currentRunningCats.remove(category);
        Crawler.addProcessingLevel(key, currentRunningCats);
        logger.debug(currentRunningCats);
        return csvData.toString();
    }
}
