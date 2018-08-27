package test.threadpool;

import test.utils.Compressors;
import test.utils.Indexed;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class Threadpool {
    long millis = System.currentTimeMillis();

    static int blocksize = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        Threadpool threadpool = new Threadpool();
        threadpool.withoutBackpressure();
//        threadpool.withBackpressure();

        System.out.println("Finished after " + (System.currentTimeMillis() - threadpool.millis));
        System.out.println("Total compression time " + Compressors.getLzmaTotalMs());
        System.out.println("Total compressed bytes " + threadpool.compressedBytes.get());
    }

    //            String name = "C:\\backup\\backup-demo\\shakespeare.txt";
    String name = "C:\\backup\\dest1\\volume\\volume_000000.json";
//    String name = "C:\\Riot Games\\League of Legends\\RADS\\projects\\lol_game_client\\filearchives\\0.0.1.169\\Archive_1.raf.dat";

    AtomicInteger running = new AtomicInteger();
    AtomicInteger submitted = new AtomicInteger();
    AtomicInteger compressedBytes = new AtomicInteger();


    public void withoutBackpressure() throws Exception  {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        long totalRead = 0;
        File file = new File(name);
        FileInputStream fileInputStream = new FileInputStream(name);
        TreeMap<Integer, Future<byte[]>> futures = new TreeMap<>();
        int index = 0;
        while (fileInputStream.available() > 0) {
            byte[] buffer = new byte[blocksize];
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
                Integer key = futures.firstEntry().getKey();
                System.out.println("Future " + key+" is done");
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
                Integer key = futures.firstEntry().getKey();
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
        HashMap<Integer, Future<Indexed<byte[]>>> futures = new HashMap<>();
        while (fileInputStream.available() > 0) {
            byte[] buffer = new byte[blocksize];
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
            index += 1;
            if (futures.containsKey(index - 20)) {
                futures.remove(index - 20).get();
            }
        }
        System.out.println("Submitted after " + (System.currentTimeMillis() - millis));
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(1000);
            System.out.println("Read and compressed " + totalRead + " of " + file.length() + " after " +
                    (System.currentTimeMillis() - millis)+", futures open " + running.get()+" submitted " +submitted.get());
        }
    }

}
