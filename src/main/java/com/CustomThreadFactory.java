package com;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class CustomThreadFactory implements ThreadFactory {
    private final String poolName;
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    public CustomThreadFactory(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public Thread newThread(Runnable runnable) {
        String threadName = poolName + "-worker-" + threadNumber.getAndIncrement();
        System.out.println("[ThreadFactory] Creating new thread: " + threadName);
        return new Thread(() -> {
            try {
                runnable.run();
            } finally {
                System.out.println("[ThreadFactory] Thread finished: " + threadName);
            }
        }, threadName);
    }
}
