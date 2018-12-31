import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by jianghlu on 9/5/2017.
 */
public class MemoryBookkeeper {
    private final AtomicLong cap;

    public MemoryBookkeeper(long cap) {
        this.cap = new AtomicLong(cap);
    }

    public static final MemoryBookkeeper DEFAULT = new MemoryBookkeeper(1024 * 1024 * 1024L);

    public boolean isAvailable(long capacity) {
        return cap.get() >= capacity;
    }

    public void waitForAvailable(long capacity) {
//        while (cap.get() < capacity) {
//            synchronized (cap) {
//                try {
//                    cap.wait();
//                } catch (InterruptedException e) {
//                    Thread.currentThread().interrupt();
//                }
//            }
//        }
    }

    public void reportUse(long capacity) {
        cap.addAndGet(-capacity);
    }

    public void release(long released) {
//        cap.addAndGet(released);
//        synchronized (cap) {
//            cap.notifyAll();
//        }
    }
}
