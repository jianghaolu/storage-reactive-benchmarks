import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jianghlu on 7/20/17.
 */
public class ReactorNettyTests {

    private static Map<String, ReactorNettyFileClient> clients;
    private static BenchmarkReactor benchmark;

    @BeforeClass
    public static void setup() throws Exception {
        clients = new HashMap<>();

        benchmark = new BenchmarkReactor() {
            @Override
            public FileClientReactor client(String url, String sasToken) {
                if (!clients.containsKey(url)) {
                    clients.put(url, new ReactorNettyFileClient(url).withSasToken(sasToken));
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
