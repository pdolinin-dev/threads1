package com;

final class DemoTask implements Runnable {
    private final String name;
    private final long durationMillis;

    DemoTask(String name, long durationMillis) {
        this.name = name;
        this.durationMillis = durationMillis;
    }

    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();
        System.out.println("[Task] " + name + " started on " + threadName);
        try {
            Thread.sleep(durationMillis);
            System.out.println("[Task] " + name + " finished on " + threadName);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            System.out.println("[Task] " + name + " interrupted on " + threadName);
        }
    }

    @Override
    public String toString() {
        return name + "(" + durationMillis + "ms)";
    }
}
