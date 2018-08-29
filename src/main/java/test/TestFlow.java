package test;

import com.github.davidmoten.rx2.Bytes;
import com.github.davidmoten.rx2.flowable.Transformers;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import io.reactivex.Scheduler;
import org.springframework.core.io.buffer.*;
import org.springframework.util.StreamUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;
import test.utils.Indexed;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;

public class TestFlow {
    public static void main(String[] args) {
        new TestFlow().try3();
    }
//            String name = "C:\\backup\\backup-demo\\shakespeare.txt";
    String name = "C:\\backup\\dest1\\volume\\volume_000000.json";

    private static List<String> words = Arrays.asList(
            "the",
            "quick",
            "brown",
            "fox",
            "jumped",
            "over",
            "the",
            "lazy",
            "dog"
    );

    public void simpleCreation() {
        DefaultDataBufferFactory dataBufferFactory = new DefaultDataBufferFactory();
        Flux<DataBuffer> dataBufferFlux = DataBufferUtils.readInputStream(() -> new FileInputStream(name),
                dataBufferFactory, 1000 * 1024);

//        dataBufferFlux.subscribe(e -> System.out.println("Got part with " + e.readableByteCount()));

        long l = System.currentTimeMillis();
        AtomicLong atomicInteger = new AtomicLong();
        dataBufferFlux
                .onBackpressureBuffer(100)
                .parallel(10)
                .flatMap(e -> Flux.defer(() -> {
                    long lambdaStart = System.currentTimeMillis();

                    InputStream inputStream = e.asInputStream();
                    DefaultDataBuffer defaultDataBuffer = dataBufferFactory.allocateBuffer(e.readableByteCount() + 1000);
                    try {
                        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(defaultDataBuffer.asOutputStream());
                        StreamUtils.copy(inputStream, gzipOutputStream);
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    long duration = System.currentTimeMillis() - lambdaStart;
                    atomicInteger.addAndGet(duration);
                    System.out.println("Compressing on thread " + Thread.currentThread().getName());
                    return Flux.just(defaultDataBuffer);
                }), false, 10, 100)
                .runOn(Schedulers.newParallel("Test", 20))
                .subscribe(e -> System.out.println("Got part with " + e.readableByteCount()+" on thread "+Thread.currentThread().getName()));

        System.out.println("Took " + (System.currentTimeMillis() - l) + " milliSeconds");
        System.out.println("Total lambda time " + atomicInteger.get() + " milliSeconds");
    }

    public void try2() {
        DefaultDataBufferFactory dataBufferFactory = new DefaultDataBufferFactory();
        Flux<DataBuffer> dataBufferFlux = DataBufferUtils.readInputStream(() -> new FileInputStream(name),
                dataBufferFactory, 1000 * 1024);

//        dataBufferFlux.subscribe(e -> System.out.println("Got part with " + e.readableByteCount()));

        long l = System.currentTimeMillis();
        AtomicLong atomicInteger = new AtomicLong();
        dataBufferFlux
                .onBackpressureBuffer(100)
                .parallel(10)
                .flatMap(e -> Flux.defer(() -> {
                    long lambdaStart = System.currentTimeMillis();

                    InputStream inputStream = e.asInputStream();
                    DefaultDataBuffer defaultDataBuffer = dataBufferFactory.allocateBuffer(e.readableByteCount() + 1000);
                    try {
                        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(defaultDataBuffer.asOutputStream());
                        StreamUtils.copy(inputStream, gzipOutputStream);
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    long duration = System.currentTimeMillis() - lambdaStart;
                    atomicInteger.addAndGet(duration);
                    System.out.println("Compressing on thread " + Thread.currentThread().getName());
                    return Flux.just(defaultDataBuffer);
                }), false, 10, 100)
                .runOn(Schedulers.newParallel("Test", 20))
                .subscribe(e -> System.out.println("Got part with " + e.readableByteCount()+" on thread "+Thread.currentThread().getName()));

        UnicastProcessor<DefaultDataBuffer> processor = UnicastProcessor.create();
//        processor.subscribe(e -> System.out.println("Got part with " + e.readableByteCount()+" on thread "+Thread.currentThread().getName()));
//        dataBufferFlux.subscribe(processor);

        System.out.println("Took " + (System.currentTimeMillis() - l) + " milliSeconds");
        System.out.println("Total lambda time " + atomicInteger.get() + " milliSeconds");
    }

    String asyncLoadBy (String id) {
//        return Flowable.fromCallable(() -> {
            System.out.println("Compressing on thread " + Thread.currentThread().getName());
            return id;
//        });
    }

    public void try3() {
        Iterable<Integer> naturals = IntStream.iterate(0, i -> i + 1)::iterator;

        long l = System.currentTimeMillis();

//        Flowable<String> people = Flowable.fromArray("Asdf", "Basdfs")
//                .parallel(10)
//                .runOn(io())
//                .map(this::asyncLoadBy)
//                .sequential();

        AtomicInteger atomicInteger = new AtomicInteger();
        FlowableTransformer<Indexed<byte[]>, Indexed<byte[]>> build = Transformers.stateMachine()
                .initialStateFactory(State::new)
                .<Indexed<byte[]>, Indexed<byte[]>>transition((state, element, subscriber) -> {
                    if (element.getIndex() == state.current) {
                        state.current += 1;
                        subscriber.onNext(element);
                        while (state.elements.containsKey(state.current)) {
                            subscriber.onNext(state.elements.remove(state.current));
                            state.current += 1;
                        }
                        return state;
                    } else {
                        state.elements.put(element.getIndex(), element);
                        atomicInteger.set(Math.max(atomicInteger.get(), state.elements.size()));
                        return state;
                    }
                }).build();

        ExecutorService executor = Executors.newFixedThreadPool(8);
        Scheduler from = io.reactivex.schedulers.Schedulers.from(executor);
        Flowable<Indexed<byte[]>> sequential = Bytes.from(new File(name), 100 * 1024)
                .zipWith(naturals, (s, i) -> new Indexed<>(s, i))
                .parallel(8)
                .runOn(from)
                .map(this::compress)
                .sequential();
        sequential
                .compose(build)
                .blockingSubscribe(e -> System.out.println("Got byte[] index "+e.getIndex()+" with size "+e.getValue().length));



        System.out.println("Done, maximum elements in state "+atomicInteger.get());
        executor.shutdown();
//        Disposable subscribe = people.subscribe(System.out::println);
//        people.blockingSubscribe(System.out::println);
//        System.out.println(subscribe.isDisposed());

//        Flowable<String> people = Flowable.fromArray("Asdf", "Basdfs")
//                .subscribeOn(io())
//                .flatMap(id -> asyncLoadBy(id)); //BROKEN
        System.out.println("Took " + (System.currentTimeMillis() - l) + " milliSeconds");
        System.out.println("Total lambda time " + this.atomicInteger.get() + " milliSeconds");
    }

    AtomicLong atomicInteger = new AtomicLong();

    private Indexed<byte[]> compress(Indexed<byte[]> bytes) {
        long lambdaStart = System.currentTimeMillis();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes.getValue());
        System.out.println("Compressing "+bytes.getIndex()+" on thread " + Thread.currentThread().getName());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            GZIPOutputStream gzipOutputStream = new GZIPOutputStream(out);
            StreamUtils.copy(byteArrayInputStream, gzipOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] bytes1 = out.toByteArray();
        long duration = System.currentTimeMillis() - lambdaStart;
        atomicInteger.addAndGet(duration);
        return new Indexed(bytes1, bytes.getIndex());
    }

    static class State {
        public TreeMap<Integer, Indexed<byte[]>> elements = new TreeMap<>();
        public int current = 0;
    }

}
