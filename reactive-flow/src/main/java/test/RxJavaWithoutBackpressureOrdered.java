package test;

import com.github.davidmoten.rx2.Bytes;
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

public class RxJavaWithoutBackpressureOrdered {
    public static void main(String[] args) {
        new RxJavaWithoutBackpressureOrdered().withBackpressureRxJava();
    }

    //            String name = "C:\\backup\\backup-demo\\shakespeare.txt";
    String name = "C:\\backup\\dest1\\volume\\volume_000000.json";

    public void withBackpressureRxJava() {
        Iterable<Integer> naturals = IntStream.iterate(0, i -> i + 1)::iterator;

        long start = System.currentTimeMillis();

        AtomicInteger maximumQueued = new AtomicInteger();

        AtomicInteger running = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        Scheduler from = Schedulers.from(executor);
        Flowable<Indexed<byte[]>> sequential = Bytes.from(new File(name), 1024 * 1024)
                .zipWith(naturals, Indexed::new)
                .parallel(8)
                .runOn(from)
                .map(e -> Compressors.compressLzmaIndexed(e, e.value.length))
                .sequential();

        sequential
                .compose(RxUtils.createReorderingTransformer())
                .blockingSubscribe(e -> {
                    running.decrementAndGet();
                    System.out.println("Got byte[] index " + e.index + " with size " + e.value.length);
                });


        System.out.println("Done, maximum elements in state " + maximumQueued.get());
        executor.shutdown();

        System.out.println("Took " + (System.currentTimeMillis() - start) + " milliSeconds");
        System.out.println("Total lambda time " + Compressors.getLzmaTotalMs() + " milliSeconds");
    }

}
