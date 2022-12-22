import java.io.*;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Tema2 implements Runnable {
    // File manager
    static FileManager fileManager = new FileManager();

    // Thread manager
    static ThreadManager threadManager = new ThreadManager();

    // File progress manager
    static FileProgressManager fileProgressManager = new FileProgressManager();

    // Thread pools
    static ExecutorService ordersReaderThreadPool;
    static ExecutorService productsScannerThreadPool;

    // For byte reaading
    private long nrOfBytesToSkip;

    // For making sure we have unique items
    static HashMap<String, Integer > uniqueOrdersMap = new HashMap <> ();;

    class Products implements Runnable {
        String order_name;
        int product_index;
        int order_product_max_index;
        int product_order = 0;

        public Products(String _order_name, int _product_index, int _order_product_max_index) {
            order_name = _order_name;
            product_index = _product_index;
            order_product_max_index = _order_product_max_index;
        }

        @Override
        public void run() {
            // Open the file
            File fopen = new File(fileManager.getProductsString());

            Scanner myScanner;
            try {
                myScanner = new Scanner(fopen);
                if(myScanner.hasNextLine() == false) {
                    System.out.println("My products scanner doesn't work!");
                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            // Read by line
            while (myScanner.hasNextLine()) {
                // Split the line
                String line = myScanner.nextLine();
                String[] split = line.split(",");
                if (split[0].equals(order_name)) {
                    product_order++;
                }

                // We reach our product, ship the product
                if (product_order == product_index) {

                    try {
                        fileManager.getProductsWriter().write(order_name + "," + split[1] + ",shipped\n");
                        fileManager.getProductsWriter().flush();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    synchronized (uniqueOrdersMap) {
                        Tema2.uniqueOrdersMap.put(split[0], Tema2.uniqueOrdersMap.get(split[0]) + 1);

                        // If we finish with all products in that order, ship the order
                        if (Tema2.uniqueOrdersMap.get(split[0]) == order_product_max_index) {
                            try {
                                fileManager.getOrdersWriter().write(order_name + "," + order_product_max_index + ",shipped\n");
                                fileManager.getOrdersWriter().flush();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    // Constructor
    public Tema2(long _nrOfBytesToSkip) {
        nrOfBytesToSkip = _nrOfBytesToSkip;
    }

    @Override
    public void run() {
        try {
            // Go to the point in file where we need to go
            FileInputStream input = new FileInputStream(fileManager.getOrdersFile());
            input.skip(nrOfBytesToSkip);

            Scanner myScanner = new Scanner(input);

            while (nrOfBytesToSkip < (int)(nrOfBytesToSkip + fileProgressManager.getChunk_Size())) {
                if (myScanner.hasNextLine()) {
                    // Get each line and split the string
                    String line = myScanner.nextLine();
                    String[] stringSplit = line.split(",");

                    // Check if we read correctly the line
                    if (stringSplit[0].length() <= 1 || stringSplit[0].charAt(1) != '_' || line.length() <= 1) {
                        continue;
                    }

                    synchronized (uniqueOrdersMap) {
                        if (uniqueOrdersMap.get(stringSplit[0]) != null) {
                            continue;
                        } else {
                            nrOfBytesToSkip += line.length();

                            // Queue all products
                            if (stringSplit[0].charAt(1) == '_') {
                                uniqueOrdersMap.put(stringSplit[0], 0);
                                for (int i = 1; i <= Integer.parseInt(stringSplit[1]); ++i) {
                                    productsScannerThreadPool.submit(new Products(stringSplit[0], i, Integer.parseInt(stringSplit[1])));
                                }
                            }
                        }
                    }
                } else {
                    break;
                }
            }

            input.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {

        // File manager
        fileManager.setFolderPath(args[0]);
        fileManager.setOrdersString(fileManager.getFolderPath() + "/orders.txt");
        fileManager.setOrdersFile(new File(fileManager.getFolderPath() + "/orders.txt"));
        fileManager.setProductsString(fileManager.getFolderPath() + "/order_products.txt");
        fileManager.setProductsFile(new File(fileManager.getFolderPath() + "/order_products.txt"));
        fileManager.setOrdersOutputString(fileManager.getFolderPath() + "/../../orders_out.txt");
        fileManager.setOrdersOutputFile(new File(fileManager.getFolderPath() + "/../../orders_out.txt"));
        fileManager.setProductsOutputString(fileManager.getFolderPath() + "/../../order_products_out.txt");
        fileManager.setProductsOutputFile(new File(fileManager.getFolderPath() + "/../../order_products_out.txt"));

        // And create the output files
        if (fileManager.getOrdersOutputFile().createNewFile()) {
            // Created file
        } else {
            // Exists, delete and recreate
            fileManager.getOrdersOutputFile().delete();
            fileManager.getOrdersOutputFile().createNewFile();
        }

        if (fileManager.getProductsOutputFile().createNewFile()) {
            // Created file
        } else {
            // Exists, delete and recreate
            fileManager.getProductsOutputFile().delete();
            fileManager.getProductsOutputFile().createNewFile();
        }

        // Create writers
        fileManager.setOrdersWriter(new FileWriter(fileManager.getOrdersOutputFile(), true));
        fileManager.setProductsWriter(new FileWriter(fileManager.getProductsOutputFile(), true));


        // Thread manager
        threadManager.setNumberOfThreads(Integer.parseInt(args[1]));

        // File progress manager
        fileProgressManager.setRemainingFileSize(fileManager.getOrdersFile().length());
        fileProgressManager.setChunk_Size(fileManager.getOrdersFile().length() / threadManager.getNumberOfThreads());

        // Thread pools
        ordersReaderThreadPool = Executors.newFixedThreadPool(threadManager.getNumberOfThreads());
        productsScannerThreadPool = Executors.newFixedThreadPool(threadManager.getNumberOfThreads());

        long nrOfBytesToSkip = 0;
        while (fileProgressManager.getRemainingFileSize() >= fileProgressManager.getChunk_Size()) {
            ordersReaderThreadPool.execute(new Tema2(nrOfBytesToSkip));
            fileProgressManager.setRemainingFileSize(fileProgressManager.getRemainingFileSize() - fileProgressManager.getChunk_Size());
            nrOfBytesToSkip = nrOfBytesToSkip + fileProgressManager.getChunk_Size();
        }

        // Load the remaining size
        ordersReaderThreadPool.execute(new Tema2(nrOfBytesToSkip));

        // Stop threads
        ordersReaderThreadPool.shutdown();
        ordersReaderThreadPool.awaitTermination(1000, TimeUnit.SECONDS);
        productsScannerThreadPool.shutdown();
    }
}