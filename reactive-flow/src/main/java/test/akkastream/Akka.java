package test.akkastream;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import test.utils.Compressors;
import test.utils.Indexed;
import test.utils.Parameters;
import test.utils.TimeMeasure;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class Akka {

    public static String name = Parameters.fileToBackup;

    public static void main(String[] args) throws Exception {
        TimeMeasure.measure("Akka Stream", () -> {
            AtomicInteger compressedBytes = new AtomicInteger();

            final ActorSystem system = ActorSystem.create("compression");
            final ActorMaterializer mat = ActorMaterializer.create(system);

            try (FileOutputStream data = new FileOutputStream("data")) {
                Path inputFile = new File(name).toPath();
                Source<ByteString, CompletionStage<IOResult>> source = FileIO.fromPath(inputFile, Parameters.blockSize);
                CompletionStage<Done> future = source
                        .zipWithIndex()
                        .buffer(20, OverflowStrategy.backpressure())
                        .mapAsync(12, (pair) -> CompletableFuture.supplyAsync(() -> {
                            byte[] bytes = pair.first().toArray();
                            byte[] bytes1 = Compressors.compressLzma(bytes, bytes.length);
                            return new Indexed<>(bytes1, pair.second().intValue());
                        })).runForeach((e) -> {
                            compressedBytes.addAndGet(e.getValue().length);
                            System.out.println("Compressed index " + e.getIndex() + " down to " + e.getValue().length);
                            data.write(e.getValue());
                        }, mat);

                future.toCompletableFuture().get();
            }
            mat.shutdown();
            system.terminate();
        });
    }
}
