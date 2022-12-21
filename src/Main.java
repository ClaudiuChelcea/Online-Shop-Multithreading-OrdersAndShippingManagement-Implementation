import java.io.*;
import static java.lang.Math.toIntExact;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main implements Runnable
{
    static File file;
    private long _startLocation;
    private int _size;
    int _sequence_number;
    int chunk_size;
    static int line = 0;
    static HashMap<String, Integer> myMap = new HashMap<>();

    public Main(long loc, int size, int sequence)
    {
        _startLocation = loc;
        _size = size;
        _sequence_number = sequence;
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
            System.out.println("Computing the part from " + _startLocation + " to " + end_location);

            Scanner lineScanner = new Scanner(is);
            while(_startLocation < end_location)
            {
                if(lineScanner.hasNextLine()) {
                    String line = lineScanner.nextLine();
                    String[] split = line.split(",");
                    if(split[0].length() <= 0)
                        continue;;
                    if(split[0].charAt(0) != 'o') {
                        // Incomplete line
                        continue;
                    }
                    if(line.length() <= 0)
                        continue;

                  if(myMap.get(split[0]) != null) { // dont queue the same order more times
                      continue;
                  }
                    System.out.println("Thread: " + _sequence_number + " Line: " + line + " from " + _startLocation + " to " + end_location);
                    Main.line++;
                  // }
                    myMap.put(split[0],1);

                    _startLocation += line.length();
                } else {
                    break;
                }
            }

            System.out.println("Finished the part from " + _startLocation + " to " + end_location);

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

        long start_loc = 0;//file pointer
        int i = 0; //loop counter
        while (remaining_size >= chunk_size)
        {
            //launches a new thread
            executor.execute(new Main(start_loc, toIntExact(chunk_size), i));
            remaining_size = remaining_size - chunk_size;
            start_loc = start_loc + chunk_size;
            i++;
        }

        //load the last remaining piece
        executor.execute(new Main(start_loc, toIntExact(remaining_size), i));

        //Tear Down
        executor.shutdown();

        //Wait for all threads to finish
        while (!executor.isTerminated())
        {
            //wait for infinity time
        }
        System.out.println("Finished all threads");
        System.out.println(Main.line);
    }
}