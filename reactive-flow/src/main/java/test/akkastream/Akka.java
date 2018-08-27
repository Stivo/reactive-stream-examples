package test.akkastream;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import test.utils.Compressors;
import test.utils.Indexed;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class Akka {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        AtomicInteger compressedBytes = new AtomicInteger();

        final ActorSystem system = ActorSystem.create("compression");
        final Materializer mat = ActorMaterializer.create(system);
        String name = "C:\\backup\\dest1\\volume\\volume_000000.json";

        Source<ByteString, CompletionStage<IOResult>> source = FileIO.fromPath(new File(name).toPath(), 1024 * 1024);
        CompletionStage<Done> future = source
                .zipWithIndex()
                .mapAsync(8, (pair) -> CompletableFuture.supplyAsync(() -> {
                    byte[] bytes = pair.first().toArray();
                    byte[] bytes1 = Compressors.compressLzma(bytes, bytes.length);
                    return new Indexed<>(bytes1, pair.second().intValue());
                })).runForeach((e) -> {
                    compressedBytes.addAndGet(e.getValue().length);
                    System.out.println("Compressed index " + e.getIndex() + " down to " + e.getValue().length);
                }, mat);

        future.thenAccept((e) -> {
            ((ActorMaterializer) mat).shutdown();
            system.terminate();
            System.out.println("Took " + (System.currentTimeMillis() - start) + " milliSeconds");

            System.out.println("Total lambda time " + Compressors.getLzmaTotalMs() + " milliSeconds");
            System.out.println("Total compressed size " + compressedBytes + " bytes");
        });
    }
}
