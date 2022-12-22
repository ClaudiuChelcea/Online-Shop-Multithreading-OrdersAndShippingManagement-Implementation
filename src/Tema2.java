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
    private long _nrOfBytesToSkip;

    // For making sure we have unique items
    static HashMap<String, Integer > myMap = new HashMap <> ();;

    // Constructor
    public Tema2(long nrOfBytesToSkip) {
        _nrOfBytesToSkip = nrOfBytesToSkip;
    }

    @Override
    public void run() {
        try {
            InputStream is = new FileInputStream(fileManager.getOrdersFile());
            is.skip(_nrOfBytesToSkip);

            // do a computation using the input stream,
            // checking that we don't read more than (end-start) bytes
            int end_location = (int)(_nrOfBytesToSkip + fileProgressManager.getChunk_Size());
            if (end_location - _nrOfBytesToSkip <= 1)
                return;

            Scanner lineScanner = new Scanner(is);
            String line;
            while (_nrOfBytesToSkip < end_location) {
                if (lineScanner.hasNextLine()) {
                    line = lineScanner.nextLine();
                    String[] split = line.split(",");
                    if (split[0].length() <= 0)
                        continue;;
                    if (split[0].charAt(0) != 'o') {
                        // Incomplete line
                        continue;
                    }
                    if (line.length() <= 1)
                        continue;

                    synchronized (myMap) {
                        if (myMap.get(split[0]) != null) { // don't queue the same order more times
                            continue;
                        } else {
                            // Thread pool for each order
                            // Send all the products for the order
                            for (int i = 1; i <= Integer.parseInt(split[1]); ++i) {
                                productsScannerThreadPool.submit(new Products(split[0], i, Integer.parseInt(split[1])));
                            }


                            if (split[0].charAt(1) == '_')
                                myMap.put(split[0], Integer.parseInt(split[1]));

                            _nrOfBytesToSkip += line.length();
                        }
                    }
                } else {
                    break;
                }
            }


            is.close();

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

    class Products implements Runnable {
        String _order;
        int _product_number;
        int const_num_products;

        public Products(String order, int product_number, int max_products) {
            _order = order;
            _product_number = product_number;
            const_num_products = max_products;
        }

        @Override
        public void run() {
            File fopen = new File(fileManager.getProductsString());

            if (fopen.exists() == false) {
                System.out.println("No products file");
                throw new RuntimeException();
            }

            Scanner newScanner;
            try {
                newScanner = new Scanner(fopen);
                if(newScanner.hasNextLine() == false) {
                    System.out.println("wrong");
                }
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            FileWriter myWriter1;
            try {
                myWriter1 = new FileWriter(fileManager.getProductsOutputFile(), true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            FileWriter myWriter;
            try {
                myWriter = new FileWriter(fileManager.getOrdersOutputFile(), true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            while (newScanner.hasNextLine()) {
                String line;
                line = newScanner.nextLine();

                String[] split = line.split(",");
                //
                if (split[0].equals(_order)) {
                    --_product_number;
                }

                if (_product_number == 0) {

                    String output1 = _order + "," + split[1] + ",shipped\n";
                    try {
                        myWriter1.write(output1);
                        myWriter1.flush();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    synchronized (myMap) {
                        Tema2.myMap.put(split[0], Tema2.myMap.get(split[0]) - 1); // decrement cauze we found a product

                        if (Tema2.myMap.get(split[0]) == 0) {
                            try {
                                myWriter.write(_order + "," + const_num_products + ",shipped\n");
                                myWriter.flush();
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
}