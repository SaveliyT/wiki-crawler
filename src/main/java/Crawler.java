import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


class Crawler {
    private static final Logger logger = LogManager.getLogger(Crawler.class);

    private static ConcurrentLinkedQueue<TaskParameters> queue;
    private static ConcurrentHashMap<String, Integer> levelCounts;
    private static ConcurrentHashMap<String, List<String>> currentProcessingLevels;


    static ConcurrentHashMap<String, Integer> getLevelCounts() {
        return levelCounts;
    }

    static void addLevelCount(String key, int value){
        levelCounts.put(key, value);
    }

    static ConcurrentHashMap<String, List<String>> getCurrentProcessingLevels(){
        return currentProcessingLevels;
    }

    static void addProcessingLevel(String key, List<String> value){
        currentProcessingLevels.put(key, value);
    }


    static void addTaskToQueue(TaskParameters taskParameters){
        queue.add(taskParameters);
    }

    static void deleteDataDirectory(Path dataDirectoryPath) throws IOException {
        if (Files.exists(dataDirectoryPath)){
            Files.walkFileTree(dataDirectoryPath,
                    new SimpleFileVisitor<Path>() {
                        @Override
                        public FileVisitResult postVisitDirectory(
                                Path dir, IOException exc) throws IOException {
                            Files.delete(dir);
                            return FileVisitResult.CONTINUE;
                        }

                        @Override
                        public FileVisitResult visitFile(
                                Path file, BasicFileAttributes attrs)
                                throws IOException {
                            Files.delete(file);
                            return FileVisitResult.CONTINUE;
                        }
                    });
        }
    }



    static void run() throws InterruptedException {
        queue = new ConcurrentLinkedQueue<>();
        levelCounts = new ConcurrentHashMap<>();
        currentProcessingLevels = new ConcurrentHashMap<>();
        StringBuilder finalCsvData = new StringBuilder();
        List<Future<String>> futures = new ArrayList<>();

        Path dataDirectoryPath = Main.getDataDirectoryPath();

        // Recursively delete all previously crawled data
        try {
            deleteDataDirectory(dataDirectoryPath);
        } catch (IOException e) {
            logger.error("Cannot delete directory " + dataDirectoryPath.toString());
            logger.error(e);
        }

        try {
            Files.createDirectories(dataDirectoryPath);
            logger.info("Created directory " + dataDirectoryPath.toString());
        } catch (IOException e) {
            logger.error("Cannot create directory for crawled data");
            logger.error(e);
        }

        // Make queue with tasks properties
        int i = 0;
        for (String category: Main.getCategories()){
            Path path = Paths.get(dataDirectoryPath.toString(), String.format("%02d", i) + "_" + category);
            category = category.trim();
            category = "Категория:" + category;
            queue.add(new TaskParameters(category, category, 0, path,
                    String.format("%02d", i) + "_"));
            i++;
        }

        // Create executor
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(Main.getParallelTaskCount());

        i = 0;
        do {
            // If queue is empty but there are running tasks then wait
            if (queue.size() == 0){
                Thread.sleep(Main.getThreadSleepMs());
            } else {
                //Get data from queue
                TaskParameters taskParameteres = queue.poll();
                String keyNew = taskParameteres.getGlobalCategory() + taskParameteres.getLevel();
                String keyPrev = taskParameteres.getGlobalCategory() + (taskParameteres.getLevel()-1);

                // If there are running tasks in this category at previous level then return data to queue and wait
                if (currentProcessingLevels.containsKey(keyPrev) && !currentProcessingLevels.get(keyPrev).isEmpty()
                        && taskParameteres.getLevel() != 0) {
                    queue.add(taskParameteres);
                    Thread.sleep(Main.getThreadSleepMs());
                } else {
                    // Run task and collect returned csv data
                    List<String> currentRunningCats = currentProcessingLevels.getOrDefault(keyNew, new ArrayList<>());
                    currentRunningCats.add(taskParameteres.getCurrentCategory());
                    currentProcessingLevels.put(keyNew, currentRunningCats);
                    Future<String> csv_data = executor.submit(new ProcessCategory(taskParameteres));
                    futures.add(csv_data);
                }
            }
            i++;
        } while (!queue.isEmpty() || executor.getActiveCount() != 0);

        logger.info("Tasks finished");

        // Add header
        finalCsvData.append("File id,Название статьи,URL,Категория,Уровень,Размер статьи\n");
        for (Future<String> future: futures){
            try {
                finalCsvData.append(future.get());
            } catch (InterruptedException | ExecutionException e) {
                logger.error(e);
            }
        }


        byte[] csvBytesToWrite = finalCsvData.toString().getBytes();
        try {
            Files.write(Paths.get(dataDirectoryPath.toString(), Main.getCsvFilename()), csvBytesToWrite);
        } catch (IOException e) {
            logger.error("Cannot write csv data");
            logger.error(e);
        }

        logger.info("Csv file written to" + Paths.get(dataDirectoryPath.toString(), Main.getCsvFilename()).toString());

        executor.shutdown();
    }
}
