package ec.edu.utpl.carreras.computacion.s7;

import ec.edu.utpl.carreras.computacion.s7.model.ClimateSummary;
import ec.edu.utpl.carreras.computacion.s7.tasks.TaskSummarize;

import java.util.concurrent.*;

public class App {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        var task = new TaskSummarize("C:\\Users\\manager\\Desktop\\PROGRAMACION AVANZADA\\weatherengine\\weatherHistory_1_copia.csv");
        // var thread = new Thread(task);
        // executor.execute(task);
        Future<ClimateSummary> future = executor.submit(task);
        // thread.start();
        // thread.join();

        var result = future.get();
        System.out.println(result);

        executor.shutdown();
//        if(executor.awaitTermination(10, TimeUnit.SECONDS)) {
//            System.out.println(task.getResult());
//        }else{
//            System.out.println("Tiempo de espera agotado");
//        }

        executor.shutdown();
    }
}
