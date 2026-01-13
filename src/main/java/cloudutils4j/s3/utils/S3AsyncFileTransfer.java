package cloudutils4j.s3.utils;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;

/**
 * Utility class responsible for creating asynchronous S3 transformers for upload and download operations.
 *
 * <p>
 * Provides:
 * <ul>
 *     <li>{@link #downloadToFile(Path)}: transforms S3 async streams into local file writes with progress updates.</li>
 *     <li>{@link #uploadFromFile(Path)}: transforms local file content into an async stream suitable for S3 uploads with progress updates.</li>
 * </ul>
 *
 * <p>
 * Progress is reported in increments of 5% by default. Both transformers return a {@link CompletableFuture} for
 * monitoring completion or handling exceptions asynchronously.
 *
 * <p>
 * This class centralizes streaming logic for large file operations, simplifying S3 client usage.
 *
 * @author Gabriel Venturini, Matheus Farias
 */
public class S3AsyncFileTransfer {
    private LongConsumer progressConsumer;
    private long progress = -1;

    public S3AsyncFileTransfer() {}

    // --- Utility Helpers ---

    /**
     * Sets a consumer that will receive progress updates during upload or download.
     *
     * @param consumer A {@link LongConsumer} that receives the progress percentage (0-100).
     */
    public void setProgressConsumer(LongConsumer consumer) {
        this.progressConsumer = consumer;
    }

    /**
     * Updates the progress during upload or download.
     *
     * @param percent The actual progress in percentage of the operation (0-100).
     */
    private void publishProgress(long percent) {
        this.progress = percent;
        if (progressConsumer != null) {
            progressConsumer.accept(percent);
        }
    }

    /**
     * Computes an updated progress percentage based on the current number of processed bytes.
     * <p>
     * The method calculates the percentage of {@code currentBytes} relative to {@code totalBytes}
     * and updates the progress only when at least a 5% increase has occurred since
     * {@code lastPercent}. Returned values are quantized to the nearest lower multiple of 5
     * (e.g., 0, 5, 10, ...).
     *
     * @param currentBytes the number of bytes processed so far
     * @param totalBytes   the total number of bytes to process
     * @param lastPercent  the previously reported progress percentage
     * @return the updated progress percentage, quantized to 5% steps; or {@code lastPercent} if
     *         the progress has not advanced by at least 5%
     */
    private static long updateProgress(long currentBytes, long totalBytes, long lastPercent) {
        if (totalBytes <= 0) return lastPercent; // avoid division by zero

        long percent = (currentBytes * 100) / totalBytes;

        if (percent >= lastPercent + 5) {
            return percent - (percent % 5); // quantize (0,5,10,...)
        }
        return lastPercent;
    }

    /**
     * Closes the given {@link Closeable} resource, suppressing any {@link IOException}
     * that may occur during the close operation.
     * <p>
     * This is useful when a failure to close a resource is non-critical or should not
     * interrupt the calling code—such as in cleanup blocks or best-effort shutdown paths.
     * </p>
     *
     * @param c the closeable resource to close; may be {@code null}, in which case
     *          no action is taken
     */
    private static void closeQuietly(Closeable c) {
        try {
            if (c != null) c.close();
        } catch (IOException ignored) {}
    }

    // --- Transformers ---

    /**
     * Creates an {@link AsyncResponseTransformer} for downloading files from S3 asynchronously.
     * <p>
     * The transformer writes the stream from S3 to the specified local file, reporting progress in increments of 5%.
     * Any exceptions encountered during streaming are propagated via the returned {@link CompletableFuture}.
     *
     * @param outputPath The local {@link Path} where the downloaded file will be saved.
     * @return An {@link AsyncResponseTransformer} that can be passed to S3 async getObject calls.
     */
    public AsyncResponseTransformer<GetObjectResponse, Path> downloadToFile(Path outputPath) {
        return new AsyncResponseTransformer<GetObjectResponse, Path>() {

            private CompletableFuture<Path> future;
            private FileOutputStream out;
            private long contentLength = -1;
            private long downloadedBytes = 0;
            private long lastPercent = -1;

            @Override
            public CompletableFuture<Path> prepare() {
                future = new CompletableFuture<>();
                return future;
            }

            @Override
            public void onResponse(GetObjectResponse response) {
                try {
                    contentLength = response.contentLength();
                    out = new FileOutputStream(outputPath.toFile());
                } catch (FileNotFoundException e) {
                    future.completeExceptionally(e);
                }
            }

            @Override
            public void onStream(SdkPublisher<ByteBuffer> publisher) {
                publisher.subscribe(new Subscriber<ByteBuffer>() {

                    private Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.subscription = s;
                        subscription.request(1);
                    }

                    @Override
                    public void onNext(ByteBuffer buffer) {
                        try {
                            byte[] bytes = new byte[buffer.remaining()];
                            buffer.get(bytes);
                            out.write(bytes);

                            downloadedBytes += bytes.length;

                            if (contentLength > 0) {
                                lastPercent = updateProgress(downloadedBytes, contentLength, lastPercent);
                                publishProgress(lastPercent);
                            }

                            subscription.request(1);
                        } catch (IOException e) {
                            subscription.cancel();
                            closeQuietly(out);
                            future.completeExceptionally(e);
                            return;
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        closeQuietly(out);
                        future.completeExceptionally(throwable);
                    }

                    @Override
                    public void onComplete() {
                        try {
                            out.flush();
                            out.close();
                            publishProgress(100);
                            future.complete(outputPath);
                        } catch (Exception e) {
                            future.completeExceptionally(e);
                        }
                    }
                });
            }

            @Override
            public void exceptionOccurred(Throwable throwable) {
                closeQuietly(out);
                future.completeExceptionally(throwable);
            }
        };
    }

    /**
     * Creates an {@link AsyncRequestBody} for uploading local files to S3 asynchronously.
     * <p>
     * The transformer reads the file in chunks (default 10MB) and emits them to the subscriber,
     * reporting progress in increments of 5%. Uploads can be canceled and exceptions are propagated
     * through the subscriber.
     *
     * @param localPath The local {@link Path} of the file to upload.
     * @return An {@link AsyncRequestBody} suitable for S3 async putObject calls.
     */
    public AsyncRequestBody uploadFromFile(Path localPath) {

        final File file = localPath.toFile();
        final long fileSize = file.length();
        final int CHUNK = 10 * 1024 * 1024; // 10MB

        return new AsyncRequestBody() {

            @Override
            public Optional<Long> contentLength() {
                return Optional.of(fileSize);
            }

            @Override
            public void subscribe(Subscriber<? super ByteBuffer> subscriber) {

                subscriber.onSubscribe(new Subscription() {

                    private FileInputStream fis;
                    private volatile boolean cancelled = false;

                    private long uploadedBytes = 0;
                    private long lastPercent = 0;

                    @Override
                    public void request(long n) {
                        if (cancelled) return;

                        try {
                            if (fis == null)
                                fis = new FileInputStream(file);

                            for (int i = 0; i < n; i++) {

                                byte[] buffer = new byte[CHUNK];
                                int read = fis.read(buffer);

                                if (read == -1) {
                                    subscriber.onComplete();
                                    publishProgress(100);
                                    closeQuietly(fis);
                                    return;
                                }

                                uploadedBytes += read;

                                ByteBuffer buf = ByteBuffer.wrap(buffer, 0, read);
                                subscriber.onNext(buf);

                                lastPercent = updateProgress(uploadedBytes, fileSize, lastPercent);
                                publishProgress(lastPercent);
                            }

                        } catch (Exception e) {
                            subscriber.onError(e);
                            closeQuietly(fis);
                        }
                    }

                    @Override
                    public void cancel() {
                        cancelled = true;
                        closeQuietly(fis);
                    }
                });
            }
        };
    }
}
