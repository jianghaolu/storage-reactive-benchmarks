import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jianghlu on 7/20/17.
 */
public class NettyTests {

    private static Map<String, NettyFileClient> clients;
    private static Benchmark benchmark;

    @BeforeClass
    public static void setup() throws Exception {
        clients = new HashMap<>();

        benchmark = new Benchmark() {
            @Override
            public FileClient client(String url, String sasToken) {
                if (!clients.containsKey(url)) {
                    clients.put(url, new NettyFileClient(url).withSasToken(sasToken));
                }
                return clients.get(url);
            }
        };
    }

    @Test
    public void run() throws Exception {
        System.out.println("--------NETTY---------");
        for (int i = 0 ; i < 1; i++) {
            benchmark.run();
        }
    }
}
