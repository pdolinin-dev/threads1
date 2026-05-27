package com;

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        CustomThreadPool pool = new CustomThreadPool(
                "MyPool",
                2, // corePoolSize
                4, // maxPoolSize
                5, // keepAliveTime
                TimeUnit.SECONDS,
                5, // queueSize
                1 //minSpareThreads
        );

        System.out.println("=== Normal load and submit() demo ===");
        for (int i = 1; i <= 8; i++) {
            pool.execute(new DemoTask("normal-task-" + i, 1_200));
        }

        Future<String> future = pool.submit(() -> {
            System.out.println("[Task] callable-task started on " + Thread.currentThread().getName());
            Thread.sleep(800);
            return "callable-result";
        });
        System.out.println("[Main] submit() returned: " + future.get());

        System.out.println("=== Idle timeout demo ===");
        Thread.sleep(6_000);

        System.out.println("=== Overload demo ===");
        for (int i = 1; i <= 40; i++) {
            try {
                pool.execute(new DemoTask("overload-task-" + i, 2_500));
            } catch (RejectedExecutionException exception) {
                System.out.println("[Main] rejected overload-task-" + i + ": " + exception.getMessage());
            }
        }

        Thread.sleep(7_000);

        System.out.println("=== Graceful shutdown demo ===");
        pool.shutdown();
        boolean terminated = pool.awaitTermination(20, TimeUnit.SECONDS);
        System.out.println("[Main] terminated = " + terminated);
    }
}
