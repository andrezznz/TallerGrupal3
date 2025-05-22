package ec.edu.utpl.carreras.computacion.s7.tasks;

import ec.edu.utpl.carreras.computacion.s7.model.ClimateRecord;
import ec.edu.utpl.carreras.computacion.s7.model.ClimateSummary;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class TaskSummarize implements Callable<ClimateSummary> {
    private final String path2Data;

    // EXTREMOS
    private ClimateRecord minTempRec, maxTempRec;
    private ClimateRecord minHumRec, maxHumRec;
    private ClimateRecord minWindRec, maxWindRec;
    private ClimateRecord minVisRec, maxVisRec;

    private int getYear(String dateStr) {
        var formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS Z");
        var date = java.time.ZonedDateTime.parse(dateStr, formatter);
        return date.getYear();
    }

    public TaskSummarize(String path2Data) {
        this.path2Data = path2Data;
    }

    @Override
    public ClimateSummary call() {
        try {
            var data = getDataAsList(path2Data);
            var groupedByYear = data.stream().collect(Collectors.groupingBy(record -> getYear(record.Date())));

            List<Future<ClimateSummary>> futures = new ArrayList<>();
            ExecutorService yearExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

            for (var entry : groupedByYear.entrySet()) {
                int year = entry.getKey();
                List<ClimateRecord> records = entry.getValue();

                Callable<ClimateSummary> yearTask = () -> {
                    double tempAvg = records.stream().mapToDouble(ClimateRecord::temp).average().orElse(0.0);
                    double humidityAvg = records.stream().mapToDouble(ClimateRecord::humidity).average().orElse(0.0);
                    double windSpeedAvg = records.stream().mapToDouble(ClimateRecord::windSpeed).average().orElse(0.0);
                    double visibilityAvg = records.stream().mapToDouble(ClimateRecord::visibility).average().orElse(0.0);
                    double pressureAvg = records.stream().mapToDouble(ClimateRecord::pressure).average().orElse(0.0);
                    return new ClimateSummary(String.valueOf(year), tempAvg, humidityAvg, windSpeedAvg, visibilityAvg, pressureAvg);
                };

                futures.add(yearExecutor.submit(yearTask));
            }

            yearExecutor.shutdown();
            yearExecutor.awaitTermination(1, TimeUnit.MINUTES);

            for (Future<ClimateSummary> f : futures) {
                System.out.println(f.get());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    // EXTREMOS: Método para imprimir los valores extremos
    public void printExtremes() {
        System.out.println("\n📊 Valores extremos en todo el dataset:");
        System.out.printf("🌡  Temp Mínima: %.2f °C (%s)%n", minTempRec.temp(), minTempRec.Date());
        System.out.printf("🌡  Temp Máxima: %.2f °C (%s)%n", maxTempRec.temp(), maxTempRec.Date());

        System.out.printf("💧 Humedad Mínima: %.2f %% (%s)%n", minHumRec.humidity(), minHumRec.Date());
        System.out.printf("💧 Humedad Máxima: %.2f %% (%s)%n", maxHumRec.humidity(), maxHumRec.Date());

        System.out.printf("🌬  Viento Mínimo: %.2f km/h (%s)%n", minWindRec.windSpeed(), minWindRec.Date());
        System.out.printf("🌬  Viento Máximo: %.2f km/h (%s)%n", maxWindRec.windSpeed(), maxWindRec.Date());

        System.out.printf("🌫  Visib. Mínima: %.2f km (%s)%n", minVisRec.visibility(), minVisRec.Date());
        System.out.printf("🌫  Visib. Máxima: %.2f km (%s)%n", maxVisRec.visibility(), maxVisRec.Date());
    }

    // CSV reader y cálculo de extremos
    private List<ClimateRecord> getDataAsList(String path2Data) throws IOException {
        List<ClimateRecord> output = new ArrayList<>();
        var csvFormat = CSVFormat.RFC4180.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .get();

        try (Reader reader = Files.newBufferedReader(Paths.get(path2Data));
             CSVParser parser = CSVParser.parse(reader, csvFormat)) {

            for (var csvRecord : parser) {
                var date = String.valueOf(csvRecord.get("Formatted Date"));
                var temp = Double.parseDouble(csvRecord.get("Temperature (C)"));
                var humidity = Double.parseDouble(csvRecord.get("Humidity"));
                var windSpeed = Double.parseDouble(csvRecord.get("Wind Speed (km/h)"));
                var visibility = Double.parseDouble(csvRecord.get("Visibility (km)"));
                var pressure = Double.parseDouble(csvRecord.get("Pressure (millibars)"));

                ClimateRecord rec = new ClimateRecord(date, temp, humidity, windSpeed, visibility, pressure);
                output.add(rec);

                // EXTREMOS
                if (minTempRec == null || temp < minTempRec.temp()) minTempRec = rec;
                if (maxTempRec == null || temp > maxTempRec.temp()) maxTempRec = rec;

                if (minHumRec == null || humidity < minHumRec.humidity()) minHumRec = rec;
                if (maxHumRec == null || humidity > maxHumRec.humidity()) maxHumRec = rec;

                if (minWindRec == null || windSpeed < minWindRec.windSpeed()) minWindRec = rec;
                if (maxWindRec == null || windSpeed > maxWindRec.windSpeed()) maxWindRec = rec;

                if (minVisRec == null || visibility < minVisRec.visibility()) minVisRec = rec;
                if (maxVisRec == null || visibility > maxVisRec.visibility()) maxVisRec = rec;
            }
        }

        return output;
    }
}