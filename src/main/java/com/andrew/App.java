package com.andrew;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;


/**
 * Hello world!
 *
 */
public class App
{
    private static final Duration WINDOW_DURATION = Duration.ofMinutes(5);

    public static void main( String[] args ) throws Exception {
        if( args.length != 1 ){
            System.err.println("Usage: TelemetryApp <inputFilePath>");
            System.exit(1);
        }

        String inputFile = args[0];

        //Read telemetry records from the file
        List<TelemetryRecord> records = readTelemetryRecords(inputFile);

        //Process the records to generate alerts
        List<Alert> alerts = processTelemetry(records);

        outputAlertAsJSON(alerts);

        System.out.println(alerts);
    }

    private static void outputAlertAsJSON(List<Alert> alerts) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        String json = mapper.writeValueAsString(alerts);
        System.out.println(json);

    }



    private static List<TelemetryRecord> readTelemetryRecords(String filePath) throws IOException {
        List<TelemetryRecord> records = new ArrayList<>();
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().length() == 0) continue;
                TelemetryRecord record = TelemetryRecord.parse(line);
                // Add the parsed record to the list:
                records.add(record);
            }
        }
        return records;
    }


    private static List<Alert> processTelemetry(List<TelemetryRecord> records) {
        //Group violation records by satelite and component
        Map<String,List<TelemetryRecord>> violationMap = new HashMap<>();
        for (TelemetryRecord record : records) {
            if(record.isViolation()){
                String key = record.sateliteId + "_" + record.component;
                violationMap.computeIfAbsent(key, k -> new ArrayList<>()).add(record);

            }
        }

        List<Alert> alerts = new ArrayList<>();
        //For each group, check the window
        for (Map.Entry<String, List<TelemetryRecord>> entry : violationMap.entrySet()) {
            List<TelemetryRecord> violations = entry.getValue();
            // Sort the records by time.
            violations.sort(Comparator.comparing(r -> r.timestamp));
            int n = violations.size();
            int windowStart = 0;
            for (int windowEnd = 0; windowEnd < n; windowEnd++) {
                // Adjust the window so that its total duration is within 5 minutes.
                while (windowStart < windowEnd &&
                        Duration.between(violations.get(windowStart).timestamp, violations.get(windowEnd).timestamp)
                                .compareTo(WINDOW_DURATION) > 0) {
                    windowStart++;
                }
                // If three or more violations exist in the current window, create an alert.
                if ((windowEnd - windowStart + 1) >= 3) {
                    TelemetryRecord alertRecord = violations.get(windowStart);
                    String severity = ("BATT".equals(alertRecord.component)) ? "RED LOW" : "RED HIGH";
                    alerts.add(new Alert(alertRecord.sateliteId, severity, alertRecord.component, alertRecord.timestamp));
                    // Only one alert per group is needed.
                    break;
                }
            }
        }
        return alerts;
    }

    private static class TelemetryRecord {
        Instant timestamp;
        int sateliteId;
        double redHighLimit;
        double yellowHighLimit;
        double yellowLowLimit;
        double redLowLimit;
        double rawValue;
        String component;

        // Formatter for input timestamps (e.g., "20180101 23:01:05.001")
        private static final DateTimeFormatter INPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS");


        static TelemetryRecord parse(String line) {
            String[] parts = line.split("\\|");
            if (parts.length != 8) {
                throw new IllegalArgumentException("Invalid telemetry record: " + line);
            }
            TelemetryRecord record = new TelemetryRecord();
            // Parse the timestamp using the correct formatter.
            LocalDateTime ldt = LocalDateTime.parse(parts[0].trim(), INPUT_FORMATTER);
            record.timestamp = ldt.atZone(ZoneOffset.UTC).toInstant();
            record.sateliteId = Integer.parseInt(parts[1].trim());
            record.redHighLimit = Double.parseDouble(parts[2].trim());
            record.yellowHighLimit = Double.parseDouble(parts[3].trim());
            record.yellowLowLimit = Double.parseDouble(parts[4].trim());
            record.redLowLimit = Double.parseDouble(parts[5].trim());
            record.rawValue = Double.parseDouble(parts[6].trim());
            record.component = parts[7].trim();
            return record;
        }

        //check if record is bad
        //BATT : rawValue must be less than redLowLimit
        //TSTAT: rawValue must be greater than redHighLimit
        boolean isViolation() {
            if ("BATT".equals(component)) {
                return rawValue < redLowLimit;
            }else if ("TSTAT".equals(component)) {
                return rawValue > redHighLimit;
            }
            return false;
        }


    }

    private static class Alert {
        int sateliteId;
        String severity;
        String component;
        String timestamp;

        public Alert(int sateliteId, String severity, String component, Instant timestamp) {
            this.sateliteId = sateliteId;
            this.severity = severity;
            this.component = component;
            this.timestamp = timestamp.toString();
        }

        public int getSateliteId() {
            return sateliteId;
        }

        public String getSeverity() {
            return severity;
        }

        public String getComponent() {
            return component;
        }

        public String getTimestamp() {
            return timestamp;
        }
    }
}

