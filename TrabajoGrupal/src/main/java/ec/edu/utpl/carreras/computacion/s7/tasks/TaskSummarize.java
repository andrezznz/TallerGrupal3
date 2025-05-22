package ec.edu.utpl.carreras.computacion.s7.tasks;

import ec.edu.utpl.carreras.computacion.s7.model.ClimateRecord;
import ec.edu.utpl.carreras.computacion.s7.model.ClimateSummary;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class TaskSummarize implements Callable<ClimateSummary> {
    private final String path2Data;
    // private ClimateSummary result;

    public TaskSummarize(String path2Data) {
        this.path2Data = path2Data;
    }

    @Override
    public ClimateSummary call() {
        try {
            var data = getDataAsList(path2Data);
            var tempAvg = data.stream().mapToDouble(ClimateRecord::temp).average().orElse(0.0);
            var humidityAvg = data.stream().mapToDouble(ClimateRecord::humidity).average().orElse(0.0);
            var windSpeedAvg = data.stream().mapToDouble(ClimateRecord::windSpeed).average().orElse(0.0);
            var visibilityAvg = data.stream().mapToDouble(ClimateRecord::visibility).average().orElse(0.0);
            var pressureAvg =data.stream().mapToDouble(ClimateRecord::pressure).average().orElse(0.0);

            return new ClimateSummary(tempAvg, humidityAvg, windSpeedAvg, visibilityAvg, pressureAvg);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return new ClimateSummary(-1,-1,-1,-1,-1);
    }

    //Read csv using Apache Commons CSV
    private List<ClimateRecord> getDataAsList(String path2Data) throws IOException {
        List<ClimateRecord> output = new ArrayList<>();
        var csvFormat = CSVFormat
                .RFC4180
                .builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .get();
        try(Reader reader = Files.newBufferedReader(Paths.get(path2Data));
            CSVParser parser = CSVParser.parse(reader, csvFormat)) {

            for(var csvRecord : parser) {
                var temp = Double.parseDouble(csvRecord.get("Temperature (C)"));
                var humidity = Double.parseDouble(csvRecord.get("Humidity"));
                var windSpeed = Double.parseDouble(csvRecord.get("Wind Speed (km/h)"));
                var visibility = Double.parseDouble(csvRecord.get("Visibility (km)"));
                var pressure = Double.parseDouble(csvRecord.get("Pressure (millibars)"));

                output.add(new ClimateRecord(temp, humidity, windSpeed, visibility, pressure));
            }
        }

        return output;
    }

    /*
        IDEA:
       Se planeo utilizar un hilo por cada anio, cada hilo tiene el proceso diferente para sacar el promedio de
       cada anio, asu vez se reutiliza los hilos dados en la iteracion para obtener el promedio con el objetivo de tener
       la fecha y hora

     */


    // public ClimateSummary getResult() {return result;}
}