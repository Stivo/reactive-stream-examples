package test.rxjava;

import com.github.davidmoten.rx2.Bytes;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;
import test.utils.Compressors;
import test.utils.Indexed;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

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
                .map(e -> Compressors.compressLzmaIndexed(e, e.getValue().length))
                .sequential();

        sequential
                .compose(RxUtils.createReorderingTransformer())
                .blockingSubscribe(e -> {
                    running.decrementAndGet();
                    System.out.println("Got byte[] index " + e.getIndex() + " with size " + e.getValue().length);
                });


        System.out.println("Done, maximum elements in state " + maximumQueued.get());
        executor.shutdown();

        System.out.println("Took " + (System.currentTimeMillis() - start) + " milliSeconds");
        System.out.println("Total lambda time " + Compressors.getLzmaTotalMs() + " milliSeconds");
    }

}
