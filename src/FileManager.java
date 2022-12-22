import java.io.File;

public class FileManager {

    /* Variables */
    // Folders
    String folderPath;

    // Files
    String ordersString;
    File ordersFile;
    String productsString;
    File productsFile;
    String ordersOutputString;
    File ordersOutputFile;
    String productsOutputString;
    File productsOutputFile;


    // Getters & Setters
    public String getFolderPath() {
        return folderPath;
    }

    public void setFolderPath(String folderPath) {
        this.folderPath = folderPath;
    }

    public String getOrdersString() {
        return ordersString;
    }

    public void setOrdersString(String ordersString) {
        this.ordersString = ordersString;
    }

    public File getOrdersFile() {
        return ordersFile;
    }

    public void setOrdersFile(File ordersFile) {
        this.ordersFile = ordersFile;
    }

    public String getProductsString() {
        return productsString;
    }

    public void setProductsString(String productsString) {
        this.productsString = productsString;
    }

    public File getProductsFile() {
        return productsFile;
    }

    public void setProductsFile(File productsFile) {
        this.productsFile = productsFile;
    }

    public String getOrdersOutputString() {
        return ordersOutputString;
    }

    public void setOrdersOutputString(String ordersOutputString) {
        this.ordersOutputString = ordersOutputString;
    }

    public File getOrdersOutputFile() {
        return ordersOutputFile;
    }

    public void setOrdersOutputFile(File ordersOutputFile) {
        this.ordersOutputFile = ordersOutputFile;
    }

    public String getProductsOutputString() {
        return productsOutputString;
    }

    public void setProductsOutputString(String productsOutputString) {
        this.productsOutputString = productsOutputString;
    }

    public File getProductsOutputFile() {
        return productsOutputFile;
    }

    public void setProductsOutputFile(File productsOutputFile) {
        this.productsOutputFile = productsOutputFile;
    }
}
