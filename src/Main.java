import java.io.*;
import static java.lang.Math.toIntExact;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main implements Runnable
{
    static File file;
    private long _startLocation;
    private int _size;
    int _sequence_number;
    static int shipped_orders = 0;
    int chunk_size;
    int _num_threads;
    static int shippeditems = 0;
    static int line = 0;
    ExecutorService _products;
    static ConcurrentHashMap
            <String, Integer> myMap;

    static {
        myMap = new ConcurrentHashMap
                <>();
    }

    public Main(long loc, int size, int sequence, int num_threads, ExecutorService products)
    {
        _startLocation = loc;
        _size = size;
        _sequence_number = sequence;
        _num_threads = num_threads;
        _products = products;
    }

    @Override
    public void run()
    {
        try
        {

            InputStream is = new FileInputStream(file);
            is.skip(_startLocation);

            // do a computation using the input stream,
            // checking that we don't read more than (end-start) bytes
            int end_location = (int) (_startLocation + _size);
            if(end_location -_startLocation <= 1)
                return;
          //  System.out.println("Computing the part from " + _startLocation + " to " + end_location);

            Scanner lineScanner = new Scanner(is);
            String line;
            while(_startLocation < end_location)
            {
                if(lineScanner.hasNextLine()) {
                    line = lineScanner.nextLine();
                    String[] split = line.split(",");
                    if(split[0].length() <= 0)
                        continue;;
                    if(split[0].charAt(0) != 'o') {
                        // Incomplete line
                        continue;
                    }
                    if(line.length() <= 0)
                        continue;

                    synchronized (this.getClass()) {
                        if (myMap.get(split[0]) != null) { // dont queue the same order more times
                            //  System.out.println("ALREADY IN!!!! " + split[0]);
                            continue;
                        } else {
                            // Thread pool for each order
                            // Send all the products for the order
                            for(int i = 1; i <= Integer.parseInt(split[1]); ++i) {
                                _products.submit(new Products(split[0], i, "checker\\input\\input_0\\order_products.txt", _sequence_number));
                            }

                         //   System.out.println("Thread: " + _sequence_number + " Line: " + line + " from " + _startLocation + " to " + end_location);
                            Main.line++;
                            if (split[0].charAt(0) == 'o')
                                myMap.put(split[0], Integer.parseInt(split[1]));
                            _startLocation += line.length();
                        }
                    }
                } else {
                    break;
                }
            }

           // System.out.println("Finished the part from " + _startLocation + " to " + end_location);

            is.close();

        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    //args[0] is path to read file
//args[1] is the size of thread pool; Need to try different values to fing sweet spot
    public static void main(String[] args) throws Exception
    {
        file = new File(args[0]);
        long remaining_size = file.length(); //get the total number of bytes in the file
        long chunk_size = remaining_size / Integer.parseInt(args[1]); //file_size/threads
        chunk_size = chunk_size;

        //Max allocation size allowed is ~2GB
        if (chunk_size > (Integer.MAX_VALUE - 5))
        {
            chunk_size = (Integer.MAX_VALUE - 5);
        }

        //thread pool
        ExecutorService executor = Executors.newFixedThreadPool(Integer.parseInt(args[1]));
        ExecutorService products = Executors.newFixedThreadPool(1);

        long start_loc = 0;//file pointer
        int i = 0; //loop counter
        while (remaining_size >= chunk_size)
        {
            //launches a new thread
            executor.execute(new Main(start_loc, toIntExact(chunk_size), i, Integer.parseInt(args[1]), products));
            remaining_size = remaining_size - chunk_size;
            start_loc = start_loc + chunk_size;
            i++;
        }

        //load the last remaining piece
        executor.execute(new Main(start_loc, toIntExact(remaining_size), i, Integer.parseInt(args[1]), products));

        //Tear Down
        executor.shutdown();

        //Wait for all threads to finish
        while (!executor.isTerminated())
        {
            //wait for infinity time
        }

        products.shutdown();

        //Wait for all threads to finish
        while (!products.isTerminated())
        {
            //wait for infinity time
        }

        // shipped sau nu mai scriu mortii mei

        //System.out.println("Finished all threads");
        System.out.println(Main.line);
        System.out.println(myMap.size());
        System.out.println("Shipped items count: " + shippeditems);
        System.out.println("Shipped orders: " + shipped_orders);

        int j = 0;
        for(var element : myMap.values()) {
            System.out.println("Element " + myMap.keySet().toArray()[j++] + " left: " + element);
        }
    }

    class Products implements Runnable
    {
        String _order;
        int _product_number;
        String _inputFilePath;
        int sizeScanned = 0;
        int _thread_id;

        public Products(String order, int product_number, String inputFilePath, int thread_id)
        {
            _order = order;
            _product_number = product_number;
            _inputFilePath = inputFilePath;
            _thread_id = thread_id;
        }

        @Override
        public void run() {
            File fopen = new File(_inputFilePath);
            if(fopen.exists() == false) {
                System.out.println("No products file");
                throw new RuntimeException();
            }
            Scanner newScanner;
            try {
                newScanner = new Scanner(fopen);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            while(newScanner.hasNextLine()) {
                String line;
                line = newScanner.nextLine();
                // System.out.println("Checking line " + line + " with thread " + _thread_id + " for order " + _order);

                String[] split = line.split(",");
                //
                if(split[0].equals(_order)) {
                    --_product_number;
                }

                if(_product_number == 0) {
                    // we reached our element
                    // shipped
                    // System.out.println("Shipped " + split[1] + " for order " + split[0] + " by thread " + _thread_id);

                    Main.shippeditems++;

                    Main.myMap.put(split[0], Main.myMap.get(split[0]) -1); // decrement cauze we found a product

                    if(Main.myMap.get(split[0]) == 0) {
                    //   System.out.println("Shipped order " + _order);
                        Main.shipped_orders++;
                    }
                    break;
                }

                // add shipped
                //System.out.println("Shipped: " + split[1]);
            }

//        System.out.println("Order: " + _order + " by thread: " + _thread_id);
        }
    }
}

