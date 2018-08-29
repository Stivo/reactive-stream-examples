package test.rxjava;

import com.github.davidmoten.rx2.flowable.Transformers;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.Scheduler;
import io.reactivex.processors.UnicastProcessor;
import io.reactivex.schedulers.Schedulers;
import test.utils.Compressors;
import test.utils.Indexed;
import test.utils.Parameters;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class RxJavaWithBackpressureOrdered {
    public static void main(String[] args) {
        new RxJavaWithBackpressureOrdered().withBackpressureRxJava();
    }

    String name = Parameters.fileToBackup;

    public void withBackpressureRxJava() {
        Iterable<Integer> naturals = IntStream.iterate(0, i -> i + 1)::iterator;

        long start = System.currentTimeMillis();

        AtomicInteger maximumQueued = new AtomicInteger();

        AtomicInteger running = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(8);
        UnicastProcessor<byte[]> unicastProcessor = UnicastProcessor.create();
        Scheduler from = Schedulers.from(executor);
        Flowable<Indexed<byte[]>> sequential = unicastProcessor
                .zipWith(naturals, Indexed::new)
                .parallel(8)
                .runOn(from)
                .map(e -> Compressors.compressLzmaIndexed(e, e.getValue().length))
                .sequential();

        Thread t = createReadingThread(running, unicastProcessor);
        t.start();

        sequential
                .compose(createReorderingTransformer())
                .blockingSubscribe(e -> {
                    running.decrementAndGet();
                    System.out.println("Got byte[] index " + e.getIndex() + " with size " + e.getValue().length);
                });


        System.out.println("Done, maximum elements in state " + maximumQueued.get());
        executor.shutdown();

        System.out.println("Took " + (System.currentTimeMillis() - start) + " milliSeconds");

        System.out.println("Total lambda time " + Compressors.getLzmaTotalMs() + " milliSeconds");
    }

    private Thread createReadingThread(AtomicInteger running, UnicastProcessor<byte[]> unicastProcessor) {
        return new Thread(() -> {
                try (FileInputStream fileInputStream = new FileInputStream(name)) {
                    while (fileInputStream.available() > 0) {
                        byte[] bytes = new byte[Parameters.blockSize];
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


    public static FlowableTransformer<Indexed<byte[]>, Indexed<byte[]>> createReorderingTransformer() {

        return Transformers.stateMachine()
                .initialStateFactory(() -> new State(new TreeMap<>(), 0))
                .<Indexed<byte[]>, Indexed<byte[]>>transition((state, element, subscriber) -> {
                    System.out.println("State transition for element " + element + " on thread " + Thread.currentThread().getName());
                    int current = state.current;
                    TreeMap<Integer, Indexed<byte[]>> elements = state.getElements();

                    if (element.getIndex() == state.getCurrent()) {
                        subscriber.onNext(element);
                        current += 1;
                        while (elements.containsKey(current)) {
                            subscriber.onNext(elements.remove(current));
                            current += 1;
                        }
                        return new State(elements, current);
                    } else {
                        elements.put(element.getIndex(), element);
                        return new State(elements, current);
                    }
                }).build();
    }

    public static class State {
        private TreeMap<Integer, Indexed<byte[]>> elements;
        private int current;

        public State(TreeMap<Integer, Indexed<byte[]>> elements, int current) {
            this.elements = new TreeMap<>(elements);
            this.current = current;
        }

        public TreeMap<Integer, Indexed<byte[]>> getElements() {
            return new TreeMap<>(elements);
        }

        public int getCurrent() {
            return current;
        }
    }

}
