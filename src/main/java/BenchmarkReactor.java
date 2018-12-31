import io.netty.buffer.ByteBuf;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jianghlu on 7/20/17.
 */
public abstract class BenchmarkReactor {
    // Storage account name
    private static final String ACCOUNT_NAME = System.getenv("ACCOUNT_NAME");
    // blob container name
    private static final String CONTAINER_NAME = System.getenv("CONTAINER_NAME");
    // SAS token, starting with "?"
    private static final String SAS_TOKEN = System.getenv("SAS_TOKEN");
    // The folder where large files can be downloaded and uploaded. For VHD benchmarks, this should have at least 130G free space
    private static final String FILE_ROOT = System.getenv("FILE_ROOT");

    public static final String URL = String.format("https://%s.blob.core.windows.net/%s/", ACCOUNT_NAME, CONTAINER_NAME);
    public abstract FileClientReactor client(String url, String sasToken);

    private void prepare() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        client(URL, SAS_TOKEN).streamAsync("1k.dat", 1024, 1)
                .doOnNext(ByteBuf::release)
                .doOnComplete(latch::countDown)
                .subscribe();

        latch.await();
    }

    public void run() throws Exception {
//        uploadTxt();
//        downloadTxt();
        uploadVhd();
//        downloadVhd();
//        prepare();
//        logDownloadTitle();
//        downloadOne1KB();
//        downloadHundred1KB();
//        downloadThousand1KB();
//        downloadOne1MB();
//        downloadHundred1MB();
//        downloadThousand1MB();
//        downloadOne1GB();
//        downloadOne10GB();
//        downloadTwenty1GB();
//        logUploadTitle();
//        uploadOne1KB();
//        uploadOne1MB();
//        uploadOne1GB();
//        uploadOne1GBBigChunks();
//        uploadOne10GB();
//        uploadOne10GBBigChunks();
//        uploadOne100GB();
//        uploadOne100GBBigChunks();
//        upload20One1GB();
    }

    public void downloadVhd() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long fileSize = 136365212160L;

        Map<Long, Long> tracker = new HashMap<>();
        tracker.put(DateTime.now().getMillis(), 0L);

      client(URL, SAS_TOKEN)
                .downloadAsync("test.vhd", FILE_ROOT + "test-downloaded.vhd", fileSize, 4096 * 1024)

                .reduce((a, b) -> {
                    long downloaded = a + b;
                    trackProgress(downloaded, fileSize, b, tracker);
                    return downloaded;
                })
                .doOnNext(c -> {
                    logTime("127G", 1, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void downloadOne1GB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long fileSize = 1073741824L;

        Map<Long, Long> tracker = new HashMap<>();
        tracker.put(DateTime.now().getMillis(), 0L);

        client(URL, SAS_TOKEN)
                .downloadAsync("1g.dat", FILE_ROOT + "1g.dat", fileSize, 4096 * 1024)
                .reduce((a, b) -> {
                    long downloaded = a + b;
                    trackProgress(downloaded, fileSize, b, tracker);
                    return downloaded;
                })
                .doOnNext(c -> {
                    logTime("1G", 1, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void downloadOne10GB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long fileSize = 10737418240L;

        Map<Long, Long> tracker = new HashMap<>();
        tracker.put(DateTime.now().getMillis(), 0L);

      client(URL, SAS_TOKEN)
                .downloadAsync("10g.dat", FILE_ROOT + "10g.dat", fileSize, 4096 * 1024)
                .reduce((a, b) -> {
                    long downloaded = a + b;
                    trackProgress(downloaded, fileSize, b, tracker);
                    return downloaded;
                })
                .doOnNext(c -> {
                    logTime("10G", 1, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void downloadIncremental() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long fileSize = 136365212160L;

        Map<Long, Long> tracker = new HashMap<>();
        tracker.put(DateTime.now().getMillis(), 0L);

        client(URL, SAS_TOKEN)
                .downloadAsync("incremental.txt", FILE_ROOT + "increment.txt", fileSize, 4096 * 1024)
                .reduce((a, b) -> {
                    long downloaded = a + b;
                    trackProgress(downloaded, fileSize, b, tracker);
                    return downloaded;
                })
                .doOnNext(c -> {
                    logTime("127G", 1, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadVhd() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 4096L * 1024;
        final long fileSize = 136365212160L;

        Map<Long, Long> tracker = new HashMap<>();
        tracker.put(DateTime.now().getMillis(), 0L);

        client(URL, SAS_TOKEN).uploadPageBlobAsync("test-uploaded.vhd", "D:\\test-downloaded.vhd", fileSize, blockSize)
                .reduce((a, b) -> {
                    long downloaded = a + b;
                    trackProgress(downloaded, fileSize, b, tracker);
                    return downloaded;
                })
                .doOnNext(c -> {
                    logTime("127G", (int) (fileSize/blockSize), start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadOne1GB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 4096L * 1024;
        final long fileSize = 1073741824L;

        Map<Long, Long> tracker = new HashMap<>();
        tracker.put(DateTime.now().getMillis(), 0L);

        client(URL, SAS_TOKEN).uploadPageBlobAsync("1g.dat", "D:\\1g.dat", fileSize, blockSize)
                .reduce((a, b) -> {
                    long downloaded = a + b;
                    trackProgress(downloaded, fileSize, b, tracker);
                    return downloaded;
                })
                .doOnNext(c -> {
                    logTime("1G", (int) (fileSize/blockSize), start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadOne10GB() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 4096L * 1024;
        final long fileSize = 10737418240L;

        Map<Long, Long> tracker = new HashMap<>();
        tracker.put(DateTime.now().getMillis(), 0L);

        client(URL, SAS_TOKEN).uploadPageBlobAsync("10g.dat", "D:\\10gblob", fileSize, blockSize)
                .reduce((a, b) -> {
                    long downloaded = a + b;
                    trackProgress(downloaded, fileSize, b, tracker);
                    return downloaded;
                })
                .doOnNext(c -> {
                    logTime("10G", (int) (fileSize/blockSize), start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void uploadTxt() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long blockSize = 4096L * 1024;
        final long fileSize = 38888960L;

        Map<Long, Long> tracker = new HashMap<>();
        tracker.put(DateTime.now().getMillis(), 0L);

        client(URL, SAS_TOKEN).uploadPageBlobAsync("inc.txt", "D:\\inc.txt", fileSize, blockSize)
                .reduce((a, b) -> {
                    long downloaded = a + b;
                    trackProgress(downloaded, fileSize, b, tracker);
                    return downloaded;
                })
                .doOnNext(c -> {
                    logTime("38888960L", (int) (fileSize/blockSize), start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

    public void downloadTxt() throws Exception {
        final DateTime start = DateTime.now();
        final CountDownLatch latch = new CountDownLatch(1);
        final long fileSize = 38888960L;

        Map<Long, Long> tracker = new HashMap<>();
        tracker.put(DateTime.now().getMillis(), 0L);

        client(URL, SAS_TOKEN)
                .downloadAsync("inc.txt", FILE_ROOT + "inc-downloaded.txt", fileSize, 4096 * 1024)
                .reduce((a, b) -> {
                    long downloaded = a + b;
                    trackProgress(downloaded, fileSize, b, tracker);
                    return downloaded;
                })
                .doOnNext(c -> {
                    System.out.println();
                    logTime("38888960L", 1, start, c);
                    latch.countDown();
                })
                .subscribe();

        latch.await();
    }

//
//    public void downloadOne1KB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        MathObservable.sumLong(downloadAsync(URL, "1k", 1))
//                .doOnNext(s -> {
//                    logTime("1k", 1, start, s);
//                    latch.countDown();
//                }).subscribe();
//
//        latch.await();
//    }
//
//    public void downloadHundred1KB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        MathObservable.sumLong(downloadAsync(URL, "1k", 100))
//                .doOnNext(s -> {
//                    logTime("1k", 100, start, s);
//                    latch.countDown();
//                }).subscribe();
//
//        latch.await();
//    }
//
//    public void downloadThousand1KB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        MathObservable.sumLong(downloadAsync(URL, "1k", 1024))
//                .doOnNext(s -> {
//                    logTime("1k", 1024, start, s);
//                    latch.countDown();
//                }).subscribe();
//
//        latch.await();
//    }
//
//    public void downloadOne1MB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        MathObservable.sumLong(downloadAsync(URL, "1m", 1))
//                .doOnNext(s -> {
//                    logTime("1m", 1, start, s);
//                    latch.countDown();
//                }).subscribe();
//
//        latch.await();
//    }
//
//    public void downloadOne1GB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        MathObservable.sumLong(downloadAsync(URL, "1g", 1))
//                .doOnNext(s -> {
//                    logTime("1g", 1, start, s);
//                    latch.countDown();
//                }).subscribe();
//
//        latch.await();
//    }
//
//    public void downloadTwenty1GB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        MathObservable.sumLong(downloadAsync(URL, "1g", 20))
//                .doOnNext(s -> {
//                    logTime("1g", 20, start, s);
//                    latch.countDown();
//                }).subscribe();
//
//        latch.await();
//    }
//
//    public void downloadHundred1MB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        MathObservable.sumLong(downloadAsync(URL, "1m", 100))
//                .doOnNext(s -> {
//                    logTime("1m", 100, start, s);
//                    latch.countDown();
//                }).subscribe();
//
//        latch.await();
//    }
//
//    public void downloadThousand1MB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        MathObservable.sumLong(downloadAsync(URL, "1m", 1024))
//                .doOnNext(s -> {
//                    logTime("1m", 1024, start, s);
//                    latch.countDown();
//                }).subscribe();
//
//        latch.await();
//    }
//
//    public void downOne10GB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//
//        MathObservable.sumLong(downloadAsync(URL, "10G", 1))
//                .doOnNext(s -> {
//                    logTime("1m", 1024, start, s);
//                    latch.countDown();
//                }).subscribe();
//
//        latch.await();
//    }
//
//    public void uploadOne1KB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//        final long fileSize = 1024L;
//
//        uploadAsync(URL, "some1kblob", FILE_ROOT + "1k.dat", fileSize, 4096L * 1024)
//                .reduce((a, b) -> a + b)
//                .last()
//                .doOnNext(c -> {
//                    logTime("1k", 1, start, c);
//                    latch.countDown();
//                })
//                .subscribe();
//
//        latch.await();
//    }
//
//    public void uploadOne1MB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//        final long fileSize = 1024L * 1024;
//
//        uploadAsync(URL, "some1mblob", FILE_ROOT + "1m.dat", fileSize, 4096L * 1024)
//                .reduce((a, b) -> a + b)
//                .last()
//                .doOnNext(c -> {
//                    logTime("1m", 1, start, c);
//                    latch.countDown();
//                })
//                .subscribe();
//
//        latch.await();
//    }
//
//    public void uploadOne1GB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//        final long blockSize = 4096L * 1024;
//        final long fileSize = 1024L * 1024 * 1024;
//
//        uploadAsync(URL, "some1gblob", FILE_ROOT + "1g.dat", fileSize, blockSize)
//                .reduce((a, b) -> a + b)
//                .last()
//                .doOnNext(c -> {
//                    logTime("1g", 256, start, c);
//                    latch.countDown();
//                })
//                .subscribe();
//
//        latch.await();
//    }
//
//    public void uploadOne1GBBigChunks() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//        final long blockSize = 100L * 1024 * 1024;
//        final long fileSize = 1024L * 1024 * 1024;
//
//        uploadAsync(URL, "some1gblob", FILE_ROOT + "1g.dat", fileSize, blockSize)
//                .reduce((a, b) -> a + b)
//                .last()
//                .doOnNext(c -> {
//                    logTime("1g", (int) (fileSize / blockSize), start, c);
//                    latch.countDown();
//                })
//                .subscribe();
//
//        latch.await();
//    }
//
//    public void uploadOne10GB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//        final long blockSize = 4096L * 1024;
//        final long fileSize = 10240L * 1024 * 1024;
//
//        uploadAsync(URL, "some10gblob", FILE_ROOT + "10g.dat", fileSize, blockSize)
//                .reduce((a, b) -> a + b)
//                .last()
//                .doOnNext(c -> {
//                    logTime("10g", 2560, start, c);
//                    latch.countDown();
//                })
//                .subscribe();
//
//        latch.await();
//    }
//
//    public void uploadOne10GBBigChunks() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//        final long blockSize = 100L * 1024 * 1024;
//        final long fileSize = 10240L * 1024 * 1024;
//
//        uploadAsync(URL, "some10gblob", FILE_ROOT + "10g.dat", fileSize, blockSize)
//                .reduce((a, b) -> a + b)
//                .last()
//                .doOnNext(c -> {
//                    logTime("10g", (int) (fileSize / blockSize), start, c);
//                    latch.countDown();
//                })
//                .subscribe();
//
//        latch.await();
//    }
//
//    public void upload20One1GB() throws Exception {
//        final DateTime start = DateTime.now();
//        final CountDownLatch latch = new CountDownLatch(1);
//        final long blockSize = 4096L * 1024;
//        final long fileSize = 1024L * 1024 * 1024;
//
//        Observable.range(1, 20)
//                .flatMap(i -> {
//                    return uploadAsync(URL, "some1gblob" + i, FILE_ROOT + "1g.dat", fileSize, blockSize)
//                            .reduce((a, b) -> a + b)
//                            .last();
//                })
//                .last()
//                .doOnNext(c -> {
//                    logTime("20x1g", (int) (fileSize / blockSize) * 20, start, c);
//                    latch.countDown();
//                })
//                .subscribe();
//
//        latch.await();
//    }

    private void logDownloadTitle() {
        System.out.println("Size\tTimes\tSeconds\t\tDownloaded\t\tSpeed(MB/s)");
        System.out.println("--------------------------------------------------------");
    }

    private void logUploadTitle() {
        System.out.println("Size\tChunks\tSeconds\t\tUploaded\t\tSpeed(MB/s)");
        System.out.println("--------------------------------------------------------");
    }

    private void logTime(String size, int chunks, DateTime start, long downloaded) {
        float seconds = (DateTime.now().getMillis() - start.getMillis()) / (float) 1000;
        System.out.println(size + "\t\t" + chunks + "\t\t" + seconds +
                "\t\t" + downloaded + (downloaded >= 10000000L ? "\t\t" : "\t\t\t") +
                (downloaded / seconds / 1024 / 1024));
    }

    private long exhaustStream(InputStream inputStream) throws IOException {
        long i = inputStream.skip(1000);
        long count = i;
        while (i > 0) {
            i = inputStream.skip(1000);
            count += i;
        }
        inputStream.close();
        return count;
    }

    private void trackProgress(long transferred, long total, long chunkSize, Map<Long, Long> tracker) {
        int percentage = (int) (100 * ((double) transferred) / total);
        StringBuilder builder = new StringBuilder("[");
        int chunk = percentage / 2;
        for (int i = 0; i < chunk; i++) {
            builder.append("#");
        }
        for (int i = chunk; i < 50; i++) {
            builder.append(" ");
        }
        builder.append("] ").append(percentage).append("%");
        if (tracker != null) {
            String[] units = {"B/s", "KB/s", "MB/s", "GB/s"};
            long lastReported = tracker.keySet().iterator().next();
            long now = DateTime.now().getMillis();
            long aggregated = chunkSize + tracker.get(lastReported);
            if (now - lastReported < 1000) {
                tracker.put(lastReported, aggregated);
            } else {
                tracker.remove(lastReported);
                tracker.put(now, 0L);
                float bytesPerSecond = (((float) aggregated) / (now - lastReported)) * 1000;
                int multitude = 0;
                while (bytesPerSecond > 1024) {
                    bytesPerSecond = bytesPerSecond / 1024;
                    multitude++;
                }
                builder.append("\t ").append(String.format("%.2f", bytesPerSecond)).append(units[multitude]);
            }
        }
        System.out.print("\b\b\b\r");
        System.out.print(builder.toString());
    }
}
