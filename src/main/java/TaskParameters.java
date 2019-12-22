import java.nio.file.Path;

class TaskParameters {
    private String currentCategory;
    private String globalCategory;
    private int level;
    private Path pathToStoreCategoryPages;
    private String prefixForPageFilename;

    TaskParameters(String currentCategory, String globalCategory, int level, Path pathToStoreCategoryPages,
                   String prefixForPageFilename){
        this.currentCategory = currentCategory;
        this.globalCategory = globalCategory;
        this.level = level;
        this.pathToStoreCategoryPages = pathToStoreCategoryPages;
        this.prefixForPageFilename = prefixForPageFilename;
    }

    String getCurrentCategory() {
        return currentCategory;
    }

    String getGlobalCategory() {
        return globalCategory;
    }

    int getLevel() {
        return level;
    }

    Path getPathToStoreCategoryPages() {
        return pathToStoreCategoryPages;
    }

    String getPrefixForPageFilename() {
        return prefixForPageFilename;
    }
}
