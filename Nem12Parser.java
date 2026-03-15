import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class Nem12Parser {

    private static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMdd");
    // This task is CPU-intensive, number of threads ≈ number of CPU cores.
    private static final int THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    // max tasks waiting in the queue,can adjust accordingly
    private static final int QUEUE_SIZE = 10_000;

    public static void main(String[] args) throws Exception {
        Instant start = Instant.now();
        List<Future<?>> futures = new ArrayList<>();
        Set<String> endCodes = Set.of("100","200","300","400","500","900");
        int interval = 0;
        String currentNmi = null;

        File file = new File("C:\\D\\flo_energy\\input.csv");
        if (!file.exists()) {
            System.err.println("Input file not found: " + file.getAbsolutePath());
            return;
        }
        // Create thread pool. set a reasonable queue size to avoid OOM, and use CallerRunsPolicy to handle rejected tasks when the queue is full.
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        ThreadFactory threadFactory = new ThreadFactory() {
            private int count = 1;
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "Nem12ParserThread-" + count++);
                t.setDaemon(false);
                return t;
            }
        };
        RejectedExecutionHandler handler = new ThreadPoolExecutor.CallerRunsPolicy();
        // If the queue is full, run task in caller thread instead of dropping
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                THREAD_COUNT,          // core pool size
                THREAD_COUNT,          // max pool size
                60L, TimeUnit.SECONDS, // idle thread timeout
                workQueue,
                threadFactory,
                handler
        );
        // store line that belongs to next record
        String nextLineCache = null;
        //the sample input is a CSV file with two types of records: 200 and 300. The 200 records contain metadata about the NMI and interval, while the 300 records contain the actual meter readings for specific dates.
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line = "";
            //The nextlinecache is used to store the line that belongs to the next record.
            //logic is to read line by line util the first field matches endCodes, the Set<String> endCodes = Set.of("100","200","300","400","500","900");
            while ((line = (nextLineCache != null ? nextLineCache : reader.readLine())) != null) {
                // reset cache
                nextLineCache = null;
                //200 record: A row in the sample input that starts with 200
                if (line.startsWith("200")){
                    String[] fields = line.split(",");
                    //the NMI (second value in the `200 record` ‒ NEM1201009 in this example);
                    currentNmi = fields[1];
                    //the interval length (ninth value in the 200 records ‒ 30 in this example);
                    interval = Integer.parseInt(fields[8]);
                //300 record: A row in the sample input that starts with 300.
                } else if (line.startsWith("300")) {
                    //There is an issue that 1.
                    //271. 1.271 is a valid value, but in the example data is split to two lines.
                    //So need to match until next code such as 100,200,300,400,500,900.
                    StringBuilder record = new StringBuilder(line);
                    while (true) {
                        //if did not find it, read the next line and append to current line
                        String nextLine = reader.readLine();
                        if (nextLine == null) break;
                        //get the first field of the next line.
                        String firstField = nextLine.split(",", 2)[0].trim();
                        boolean isInt = firstField.matches("\\d+");
                        // check if it's in our known codes
                        if (isInt && endCodes.contains(firstField)) {
                            // found the start of the next record
                            nextLineCache = nextLine; // push it to be processed next
                            break; // stop appending to current record
                        }

                        record.append(nextLine.trim());
                    }
                    String finalCurrentNmi = currentNmi;
                    int finalInterval = interval;
                    String recordString = record.toString();
                    // submit record processing to thread pool
                    Future<?> future = executor.submit(() -> process300Record(finalCurrentNmi, finalInterval, recordString));
                    futures.add(future);
                }
            }
        } catch (IOException e) {
            // Handle exceptions
            System.err.println(">>>Error reading file:");
            e.printStackTrace();
        } catch (Exception e) {
            // Handle any other exceptions
            System.err.println(">>>An unexpected error occurred:");
            e.printStackTrace();
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
        }

        // Wait for all tasks to complete
        for (Future<?> f : futures) {
            try {
                // blocks until this task is done
                f.get();
            } catch (InterruptedException e) {
                // restore interrupt status
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                System.err.println(">>>Error reading future:");
                e.printStackTrace(); // handle task exceptions
            }
        }
        Instant end = Instant.now();
        System.out.println("Processing completed.");
        System.out.println("Time taken: " + Duration.between(start, end).toMillis() + " ms");
    }

    private static void process300Record(String currentNmi, int interval, String recordLine) {
        try {
            String[] fields = recordLine.split(",");
            LocalDate date = LocalDate.parse(fields[1], DATE_FORMAT);

            //1440 is the total minutes in a day.
            int intervalCount = 1440 / interval;
            StringBuilder values = new StringBuilder();
            values.append("INSERT INTO meter_readings (nmi, timestamp, consumption) VALUES ");
            boolean first = true;

            for (int i = 0; i < intervalCount; i++) {
                //process the consumption from the third filed.
                int index = i + 2;
                if (index >= fields.length) break;
                if (fields[index] == null || fields[index].isEmpty()) continue;
                try {
                    double consumption = Double.parseDouble(fields[index]);
                    LocalDateTime timestamp = date.atStartOfDay().plusMinutes(i * interval);
                    if (!first) values.append(",");
                    values.append("('")
                            .append(currentNmi)
                            .append("','")
                            .append(timestamp)
                            .append("',")
                            .append(consumption)
                            .append(")");
                    first = false;

                } catch (NumberFormatException e) {
                    System.err.println("Invalid consumption value: " + fields[index]);
                }
            }
            values.append(";");
            System.out.println(values.toString());
        } catch (Exception e) {
            System.err.println("Error processing 300 record for NMI: " + currentNmi);
            e.printStackTrace();
        }
    }
}
