import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jianghlu on 7/20/17.
 */
public class S3Tests {

    private static Map<String, S3FileClient> clients;
    private static Benchmark benchmark;

    @BeforeClass
    public static void setup() throws Exception {
        clients = new HashMap<>();

        benchmark = new Benchmark() {
            @Override
            public FileClient client(String url, String sasToken) {
                if (!clients.containsKey(url)) {
                    clients.put(url, new S3FileClient(new DefaultAWSCredentialsProviderChain(), "javabenchmark", Regions.US_WEST_2));
                }
                return clients.get(url);
            }
        };
    }

    @Test
    public void run() throws Exception {
        System.out.println("--------S3---------");
        for (int i = 0 ; i < 1; i++) {
            benchmark.run();
        }
        for (S3FileClient client : clients.values()) {
            client.shutdown();
        }
    }
}
