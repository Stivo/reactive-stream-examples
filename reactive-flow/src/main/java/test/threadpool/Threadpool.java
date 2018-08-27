package test.threadpool;

import test.utils.Compressors;
import test.utils.Indexed;
import test.utils.Parameters;
import test.utils.TimeMeasure;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class Threadpool {
    long millis = System.currentTimeMillis();

    public static void main(String[] args) throws Exception {
        TimeMeasure.measure("Threadpool", () -> {
            Threadpool threadpool = new Threadpool();
//        threadpool.withoutBackpressure();
            threadpool.withBackpressure();

        });

    }

    String name = Parameters.fileToBackup;

    AtomicInteger running = new AtomicInteger();
    AtomicInteger submitted = new AtomicInteger();
    AtomicInteger compressedBytes = new AtomicInteger();


    public void withoutBackpressure() throws Exception  {
        FileOutputStream data = new FileOutputStream("data");
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        long totalRead = 0;
        File file = new File(name);
        FileInputStream fileInputStream = new FileInputStream(name);
        TreeMap<Integer, Future<byte[]>> futures = new TreeMap<>();
        int index = 0;
        while (fileInputStream.available() > 0) {
            byte[] buffer = new byte[Parameters.blockSize];
            int read = fileInputStream.read(buffer);
            totalRead += read;
            System.out.println("Read and compressed " + totalRead + " of " + file.length() + " after " +
                    (System.currentTimeMillis() - millis)+", futures open " + running.get()+" submitted " +submitted.get());
            Future<byte[]> submit = executorService.submit(() -> {
                byte[] done = Compressors.compressLzma(buffer, read);
                compressedBytes.addAndGet(done.length);
                running.decrementAndGet();
                return done;
            });
            futures.put(index, submit);
            running.incrementAndGet();
            submitted.incrementAndGet();
            while (futures.firstEntry().getValue().isDone()) {
                Map.Entry<Integer, Future<byte[]>> integerFutureEntry = futures.firstEntry();
                Integer key = integerFutureEntry.getKey();
                System.out.println("Future " + key+" is done");
                data.write(integerFutureEntry.getValue().get());
                futures.remove(key);
            }
            index += 1;
        }
        System.out.println("Submitted after " + (System.currentTimeMillis() - millis));
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(1000);
            System.out.println("Read and compressed " + totalRead + " of " + file.length() + " after " +
                    (System.currentTimeMillis() - millis)+", futures open " + running.get()+" submitted " +submitted.get());
            while (!futures.isEmpty() && futures.firstEntry().getValue().isDone()) {
                Map.Entry<Integer, Future<byte[]>> integerFutureEntry = futures.firstEntry();
                Integer key = integerFutureEntry.getKey();
                data.write(integerFutureEntry.getValue().get());
                System.out.println("Future " + key+" is done");
                futures.remove(key);
            }
        }
    }

    public void withBackpressure() throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        long totalRead = 0;
        int index = 0;
        File file = new File(name);
        FileInputStream fileInputStream = new FileInputStream(name);
        FileOutputStream data = new FileOutputStream("data");
        TreeMap<Integer, Future<Indexed<byte[]>>> futures = new TreeMap<>();
        while (fileInputStream.available() > 0) {
            byte[] buffer = new byte[Parameters.blockSize];
            int read = fileInputStream.read(buffer);
            totalRead += read;
            System.out.println("Read and compressed " + totalRead + " of " + file.length() + " after " +
                    (System.currentTimeMillis() - millis)+", futures open " + running.get()+" submitted " +submitted.get());
            Indexed<byte[]> indexed = new Indexed<>(buffer, index);
            Future<Indexed<byte[]>> future = executorService.submit(() -> {
                Indexed<byte[]> done = Compressors.compressLzmaIndexed(indexed, read);
                compressedBytes.addAndGet(done.getValue().length);
                running.decrementAndGet();
                return done;
            });
            running.incrementAndGet();
            submitted.incrementAndGet();
            futures.put(index, future);
            if (futures.containsKey(index - 20)) {
                data.write(futures.remove(index - 20).get().getValue());
            }
            index += 1;
        }
        System.out.println("Submitted after " + (System.currentTimeMillis() - millis));
        while (!futures.isEmpty()) {
            Map.Entry<Integer, Future<Indexed<byte[]>>> integerFutureEntry = futures.firstEntry();
            data.write(integerFutureEntry.getValue().get().getValue());
            futures.remove(integerFutureEntry.getKey());
        }
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(1000);
            System.out.println("Read and compressed " + totalRead + " of " + file.length() + " after " +
                    (System.currentTimeMillis() - millis)+", futures open " + running.get()+" submitted " +submitted.get());
        }
        data.close();
    }

}
