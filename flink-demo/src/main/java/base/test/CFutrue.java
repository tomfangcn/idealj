package base.test;

import lombok.SneakyThrows;

import java.util.concurrent.*;

public class CFutrue {


    public static void main(String[] args) {

//        testDouble();
//
//        testCancel();

//        testSupply();

//        testChain();

//        testChainAsync();

        testHandle();

    }

    @SneakyThrows
    private static void testHandle() {
        CompletableFuture.<String>supplyAsync(() -> {

//                    throw new RuntimeException();
                    return "RIGHT";
                })
                .handle((r, e) -> {

                    if (e != null) {
                        return "ERROR";
                    } else {
                        return r;
                    }
                })
                .thenAccept(System.out::println);

    }

    @SneakyThrows
    private static void testChainAsync() {
        CompletableFuture<String> completableFuture =
                CompletableFuture.supplyAsync(() -> {
                            System.out.println("supplyAsync = " + Thread.currentThread());
                            return "Java";
                        }, Executors.newCachedThreadPool())
                        .thenApplyAsync(e -> {
                            System.out.println("thenApply = " + Thread.currentThread());
                            return "enhance-" + e;
                        });

        System.out.println(completableFuture.get());
    }

    @SneakyThrows
    private static void testChain() {
        CompletableFuture<String> completableFuture =
                CompletableFuture.supplyAsync(() -> {
                            System.out.println("supplyAsync = " + Thread.currentThread());
                            return "Java";
                        }, Executors.newCachedThreadPool())
                        .thenApply(e -> {
                            System.out.println("thenApply = " + Thread.currentThread());
                            return "enhance-" + e;
                        });

        System.out.println(completableFuture.get());
    }

    private static void testSupply() {
        CompletableFuture<Double> completableFuture = CompletableFuture.supplyAsync(() -> 3.14D, Executors.newCachedThreadPool());

        try {
            System.out.println(completableFuture.get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    private static void testCancel() {
        CompletableFuture<Double> completableFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            sleep(5L);
            completableFuture.cancel(false);
        });

        try {
            completableFuture.get();
        } catch (Exception e) {
            assert e instanceof CancellationException;
        }
    }


    private static void testDouble() {
        CompletableFuture<Double> completableFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            sleep(5L);
            completableFuture.complete(3.14D);
        });

        assert completableFuture.getNow(0.0D) == 0.0D;


        try {
            assert completableFuture.get() == 3.14D;
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void sleep(long timeout) {

        try {
            TimeUnit.SECONDS.sleep(timeout);
        } catch (InterruptedException e) {
            // 忽略
        }

    }

}
