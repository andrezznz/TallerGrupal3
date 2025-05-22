package model;

import ec.edu.utpl.carreras.computacion.s7.model.ClimateSummary;
import ec.edu.utpl.carreras.computacion.s7.tasks.TaskSummarize;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class App {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        var task = new TaskSummarize("C:\\Users\\andre\\OneDrive\\Escritorio\\YO\\UNIVERSIDAD ANDY\\4to Ciclo\\Programacion Avanzada\\TallerGrupal3\\weatherengine\\weatherHistory - copia.csv");
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
