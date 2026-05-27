package com;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class CustomThreadPool implements CustomExecutor {
    private final String poolName;
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveNanos;
    private final int queueSize;
    private final int minSpareThreads;
    private final CustomThreadFactory threadFactory;
    private final List<Worker> workers = new ArrayList<>();
    private final Object workersLock = new Object();
    private final AtomicInteger workerIds = new AtomicInteger(1);
    private final AtomicInteger roundRobinCursor = new AtomicInteger();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean shutdownNow = new AtomicBoolean(false);

    public CustomThreadPool(
            String poolName,
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            TimeUnit timeUnit,
            int queueSize,
            int minSpareThreads
    ) {
        validate(corePoolSize, maxPoolSize, keepAliveTime, timeUnit, queueSize, minSpareThreads);
        this.poolName = poolName;
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveNanos = timeUnit.toNanos(keepAliveTime);
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        this.threadFactory = new CustomThreadFactory(poolName);

        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
    }

    @Override
    public void execute(Runnable command) {
        Objects.requireNonNull(command, "command");
        if (shutdown.get()) {
            reject(command, "pool is shutting down");
        }

        keepEnoughSpareThreads();

        Worker target = chooseWorker();
        if (target != null && target.enqueue(command)) {
            System.out.println("[Pool] Task accepted into queue #" + target.id() + ": " + command);
            keepEnoughSpareThreads();
            return;
        }

        Worker newWorker = addWorkerIfPossible();
        if (newWorker != null && newWorker.enqueue(command)) {
            System.out.println("[Pool] Task accepted into queue #" + newWorker.id() + ": " + command);
            keepEnoughSpareThreads();
            return;
        }

        reject(command, "all queues are full and all workers are busy");
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        Objects.requireNonNull(callable, "callable");
        FutureTask<T> task = new FutureTask<>(callable) {
            @Override
            public String toString() {
                return callable.toString();
            }
        };
        execute(task);
        return task;
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            System.out.println("[Pool] Graceful shutdown requested.");
        }
    }

    @Override
    public void shutdownNow() {
        shutdown.set(true);
        if (shutdownNow.compareAndSet(false, true)) {
            System.out.println("[Pool] Immediate shutdown requested.");
            synchronized (workersLock) {
                workers.forEach(Worker::stopNow);
            }
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            synchronized (workersLock) {
                if (workers.isEmpty()) {
                    return true;
                }
            }
            Thread.sleep(50);
        }
        return false;
    }

    private void keepEnoughSpareThreads() {
        while (!shutdown.get() && spareWorkersCount() < minSpareThreads) {
            if (addWorkerIfPossible() == null) {
                return;
            }
        }
    }

    private int spareWorkersCount() {
        synchronized (workersLock) {
            int spare = 0;
            for (Worker worker : workers) {
                if (worker.isSpare()) {
                    spare++;
                }
            }
            return spare;
        }
    }

    private Worker chooseWorker() {
        synchronized (workersLock) {
            if (workers.isEmpty()) {
                return null;
            }

            int size = workers.size();
            int start = Math.floorMod(roundRobinCursor.getAndIncrement(), size);
            for (int offset = 0; offset < size; offset++) {
                Worker worker = workers.get((start + offset) % size);
                if (worker.hasQueueCapacity()) {
                    return worker;
                }
            }
            return workers.stream()
                    .min(Comparator.comparingInt(Worker::queueSize))
                    .orElse(null);
        }
    }

    private Worker addWorkerIfPossible() {
        synchronized (workersLock) {
            if (workers.size() >= maxPoolSize) {
                return null;
            }
            return addWorker();
        }
    }

    private Worker addWorker() {
        Worker worker;
        synchronized (workersLock) {
            int id = workerIds.getAndIncrement();
            worker = new Worker(id, new LinkedBlockingQueue<>(queueSize));
            workers.add(worker);
        }
        Thread thread = threadFactory.newThread(worker);
        worker.setThread(thread);
        thread.start();
        return worker;
    }

    private void removeWorker(Worker worker) {
        synchronized (workersLock) {
            workers.remove(worker);
        }
    }

    private void reject(Runnable command, String reason) {
        System.out.println("[Rejected] Task " + command + " was rejected due to overload: " + reason + "!");
        throw new RejectedExecutionException(reason + ": " + command);
    }

    private static void validate(
            int corePoolSize,
            int maxPoolSize,
            long keepAliveTime,
            TimeUnit timeUnit,
            int queueSize,
            int minSpareThreads
    ) {
        Objects.requireNonNull(timeUnit, "timeUnit");
        if (corePoolSize < 0) {
            throw new IllegalArgumentException("corePoolSize must be >= 0");
        }
        if (maxPoolSize <= 0 || maxPoolSize < corePoolSize) {
            throw new IllegalArgumentException("maxPoolSize must be >= corePoolSize and > 0");
        }
        if (keepAliveTime <= 0) {
            throw new IllegalArgumentException("keepAliveTime must be > 0");
        }
        if (queueSize <= 0) {
            throw new IllegalArgumentException("queueSize must be > 0");
        }
        if (minSpareThreads < 0 || minSpareThreads > maxPoolSize) {
            throw new IllegalArgumentException("minSpareThreads must be in range [0, maxPoolSize]");
        }
    }

    private final class Worker implements Runnable {
        private final int id;
        private final BlockingQueue<Runnable> queue;
        private final AtomicBoolean runningTask = new AtomicBoolean(false);
        private volatile Thread thread;

        private Worker(int id, BlockingQueue<Runnable> queue) {
            this.id = id;
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                while (!shutdownNow.get()) {
                    if (shutdown.get() && queue.isEmpty()) {
                        break;
                    }

                    Runnable task = queue.poll(keepAliveNanos, TimeUnit.NANOSECONDS);
                    if (task == null) {
                        if (removeAfterIdleTimeout()) {
                            System.out.println("[Worker] " + thread.getName() + " idle timeout, stopping.");
                            break;
                        }
                        continue;
                    }

                    if (shutdownNow.get()) {
                        break;
                    }

                    runningTask.set(true);
                    try {
                        System.out.println("[Worker] " + thread.getName() + " executes " + task);
                        task.run();
                    } catch (RuntimeException exception) {
                        System.out.println("[Worker] " + thread.getName() + " failed task " + task + ": " + exception);
                    } finally {
                        runningTask.set(false);
                    }
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            } finally {
                removeWorker(this);
                System.out.println("[Worker] " + thread.getName() + " terminated.");
            }
        }

        private boolean enqueue(Runnable command) {
            return queue.offer(command);
        }

        private boolean isSpare() {
            return !runningTask.get() && queue.isEmpty();
        }

        private boolean hasQueueCapacity() {
            return queue.remainingCapacity() > 0;
        }

        private int queueSize() {
            return queue.size();
        }

        private int id() {
            return id;
        }

        private void setThread(Thread thread) {
            this.thread = thread;
        }

        private void wakeUp() {
            if (thread != null) {
                thread.interrupt();
            }
        }

        private void stopNow() {
            queue.clear();
            wakeUp();
        }

        private boolean removeAfterIdleTimeout() {
            synchronized (workersLock) {
                if (workers.size() <= corePoolSize) {
                    return false;
                }
                workers.remove(this);
                return true;
            }
        }
    }
}
