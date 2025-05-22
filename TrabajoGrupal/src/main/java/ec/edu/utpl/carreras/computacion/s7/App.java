package ec.edu.utpl.carreras.computacion.s7;

import ec.edu.utpl.carreras.computacion.s7.model.ClimateSummary;
import ec.edu.utpl.carreras.computacion.s7.tasks.TaskSummarize;

import java.util.concurrent.*;

public class App {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        var task = new TaskSummarize("C:\\Users\\andre\\OneDrive\\Escritorio\\YO\\UNIVERSIDAD ANDY\\4to Ciclo\\Programacion Avanzada\\TallerGrupal3\\TrabajoGrupal\\weatherHistory - copia.csv");

        Future<ClimateSummary> future = executor.submit(task);

        var result = future.get();
        System.out.println(result);

        executor.shutdown();

        task.printExtremes();
    }
}
