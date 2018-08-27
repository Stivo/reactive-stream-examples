package test;

import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.processors.UnicastProcessor;
import io.reactivex.schedulers.Schedulers;
import org.springframework.util.StreamUtils;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;

public class TestRxJava {
    public static void main(String[] args) {
        new TestRxJava().withBackpressureRxJava();
    }

    //            String name = "C:\\backup\\backup-demo\\shakespeare.txt";
    String name = "C:\\backup\\dest1\\volume\\volume_000000.json";

    public void withBackpressureRxJava() {
        Iterable<Integer> naturals = IntStream.iterate(0, i -> i + 1)::iterator;

        long start = System.currentTimeMillis();

        AtomicInteger maximumQueued = new AtomicInteger();

        AtomicInteger running = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        UnicastProcessor<byte[]> unicastProcessor = UnicastProcessor.create();
        Scheduler from = Schedulers.from(executor);
//        Flowable<Indexed<byte[]>> sequential = Bytes.from(new File(name), 100 * 1024)
        Flowable<Indexed<byte[]>> sequential = unicastProcessor
                .zipWith(naturals, Indexed::new)
                .parallel(8)
                .runOn(from)
                .map(this::compressLzma)
                .sequential();

        Thread t = createReadingThread(running, unicastProcessor);
        t.start();

        sequential
                .compose(RxUtils.createReorderingTransformer())
                .blockingSubscribe(e -> {
                    running.decrementAndGet();
                    System.out.println("Got byte[] index " + e.index + " with size " + e.value.length);
                });


        System.out.println("Done, maximum elements in state " + maximumQueued.get());
        executor.shutdown();

        System.out.println("Took " + (System.currentTimeMillis() - start) + " milliSeconds");
        System.out.println("Total lambda time " + this.atomicInteger.get() + " milliSeconds");
    }

    private Thread createReadingThread(AtomicInteger running, UnicastProcessor<byte[]> unicastProcessor) {
        return new Thread(() -> {
                try (FileInputStream fileInputStream = new FileInputStream(name)) {
                    while (fileInputStream.available() > 0) {
                        byte[] bytes = new byte[1024 * 1024];
                        int i = fileInputStream.readNBytes(bytes, 0, bytes.length);
                        if (i != bytes.length) {
                            byte[] bytes1 = new byte[i];
                            System.arraycopy(bytes, 0, bytes1, 0, i);
                            bytes = bytes1;
                        }
                        while (running.get() > 20) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        unicastProcessor.onNext(bytes);
                        running.incrementAndGet();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, "reader");
    }

    AtomicLong atomicInteger = new AtomicLong();

    private Indexed<byte[]> compress(Indexed<byte[]> bytes) {
        long lambdaStart = System.currentTimeMillis();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes.value);
        System.out.println("Compressing " + bytes.index + " on thread " + Thread.currentThread().getName());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(out)) {

            StreamUtils.copy(byteArrayInputStream, gzipOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] bytes1 = out.toByteArray();
        long duration = System.currentTimeMillis() - lambdaStart;
        atomicInteger.addAndGet(duration);
        return new Indexed(bytes1, bytes.index);
    }

    private Indexed<byte[]> compressLzma(Indexed<byte[]> bytes) {
        long lambdaStart = System.currentTimeMillis();
        System.out.println("Compressing " + bytes.index + " on thread " + Thread.currentThread().getName());

        byte[] value = bytes.value;
        byte[] bytes1 = Compressors.compressLzma(value, value.length);
        long duration = System.currentTimeMillis() - lambdaStart;
        atomicInteger.addAndGet(duration);
        return new Indexed(bytes1, bytes.index);
    }

}
