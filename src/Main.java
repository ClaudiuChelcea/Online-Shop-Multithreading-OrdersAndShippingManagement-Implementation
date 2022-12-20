import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    private File file;

    public Main(File _file) {
        // Check if the file exists
        if(_file.exists() == false) {
            System.out.println("File doesn't exist");
            return;
        }
        this.file = _file;
    }

    // Processes the given portion of the file.
    // Called simultaneously from several threads.
    // Use your custom return type as needed, I used String just to give an example.
    public String processPart(long start, long end)
            throws Exception
    {
        InputStream is = new FileInputStream(file);
        is.skip(start);

        // do a computation using the input stream,
        // checking that we don't read more than (end-start) bytes
        System.out.println("Computing the part from " + start + " to " + end);

        // Thread.sleep(1000);
        System.out.println("Finished the part from " + start + " to " + end);

        is.close();
        return "Some result";
    }

    // Creates a task that will process the given portion of the file,
    // when executed.
    public Callable<String> processPartTask(final long start, final long end) {
        return new Callable<String>() {
            public String call()
                    throws Exception
            {
                return processPart(start, end);
            }
        };
    }

    // Splits the computation into chunks of the given size,
    // creates appropriate tasks and runs them using a
    // given number of threads.
    public void processAll(int noOfThreads, int chunkSize)
            throws Exception
    {
        int count = (int)((file.length() + chunkSize - 1) / chunkSize);
        java.util.List<Callable<String>> tasks = new ArrayList<Callable<String>>(count);
        for(int i = 0; i < count; i++)
            tasks.add(processPartTask(i * chunkSize, Math.min(file.length(), (i+1) * chunkSize)));
        ExecutorService es = Executors.newFixedThreadPool(noOfThreads);

        java.util.List<Future<String>> results = es.invokeAll(tasks);
        es.shutdown();

        // use the results for something
        for(Future<String> result : results)
            System.out.println(result.get());
    }

    public static void main(String argv[])
            throws Exception
    {
        // Orders input file
        File ordersFile = new File(argv[0].toString());

        // Max number of threads
        int numThreads = Integer.parseInt(argv[1]);
        int cores = Runtime.getRuntime().availableProcessors(); // max number of cores on my PC

        // Start reading (if possible)
        if(ordersFile.exists() == false || numThreads <= 0 || numThreads > cores) {
            System.out.println("File doesn't exist or the number of threads is impossible!");
            return;
        } else {
            Main s = new Main(ordersFile);
            long chunk_size = ordersFile.length() / numThreads;
            s.processAll(numThreads, (int) chunk_size);
        }
    }
}