Reactive Streams Examples
===========================

The example backup program does (inspired use case by [DeScaBaTo](https://github.com/Stivo/Descabato)):
* split one file into 1mb blocks
* compress each block with LZMA (the 7zip algorithm)
* concatenate the compressed content into file "data"
* Write a file called "metadata" with one block number per line with its length

From this data it is easy to reconstruct the backupped file.

Implementations should be parallel and use backpressure.
The program should not run into a OutOfMemoryException when it is backing up
a file larger than the -Xmx setting of the JVM.

Up to date implementations (the rest are abandoned experiments):
- akkastream.AkkaStream
- actors.AkkaStreamScala (Scala!)
- singlethreaded.SingleThreaded
- rxjava.RxJavaWithBackpressureOrdered
- actors.Actors (Scala!)

