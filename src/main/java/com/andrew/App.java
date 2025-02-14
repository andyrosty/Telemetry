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
        // Checks to see if you put one argument (the file path).
        if( args.length != 1 ){
            System.err.println("Usage: TelemetryApp <inputFilePath>");
            System.exit(1);
        }

        // The first argument is assumed to be the path to our input file.
        String inputFile = args[0];

        //Read telemetry records from the file specified
        List<TelemetryRecord> records = readTelemetryRecords(inputFile);

        // Process these telemetry records to find any alerts that need to be generated.
        List<Alert> alerts = processTelemetry(records);

        // Converts the list of alerts to JSON format and print it out.
        outputAlertAsJSON(alerts);

        //prints out the alert to console
        System.out.println(alerts);
    }

    /**
     * This method takes a list of alerts and prints them to the console in JSON format.
     * JSON (JavaScript Object Notation) is a format that looks like:
     * [
     *   {
     *     "sateliteId": 1,
     *     "severity": "RED HIGH",
     *     ...
     *   },
     *   ...
     * ]
     */
    private static void outputAlertAsJSON(List<Alert> alerts) throws Exception {
        // Create an object that can convert Java objects to JSON strings.
        ObjectMapper mapper = new ObjectMapper();

        // Tell the mapper to format the JSON output in a pretty, easy-to-read way.
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        // Convert our list of alerts to a JSON string.
        String json = mapper.writeValueAsString(alerts);

        // Print the JSON string to the console.
        System.out.println(json);

    }



    /**
     * This method reads telemetry records from a file.
     * Each line in the file is a single "telemetry record," containing data about satellites.
     *
     * @param filePath The path to the file containing telemetry data.
     * @return A list of TelemetryRecord objects that were parsed from the file.
     */
    private static List<TelemetryRecord> readTelemetryRecords(String filePath) throws IOException {
        // We'll store each line we read from the file as a TelemetryRecord object in this list.
        List<TelemetryRecord> records = new ArrayList<>();

        // Try to open the file using a BufferedReader, which allows us to read it line by line.
        try (BufferedReader br = Files.newBufferedReader(Paths.get(filePath))) {
            String line;
            // Keep reading until we reach the end of the file (null means end of file).
            while ((line = br.readLine()) != null) {
                // If a line is blank (empty), skip it.
                if (line.trim().length() == 0) continue;
                TelemetryRecord record = TelemetryRecord.parse(line);
                // Add the parsed record to the list:
                records.add(record);
            }
        }
        return records;
    }


    /**
     * This method processes all the telemetry records and finds any "alerts" that need to be created.
     * An "alert" occurs when we see three or more "violation" records within a 5-minute window for the same satellite and component.
     *
     * @param records A list of TelemetryRecords (data from the file).
     * @return A list of Alert objects indicating serious issues that were found.
     */
    private static List<Alert> processTelemetry(List<TelemetryRecord> records) {
        // We will group any "violation" records by a combination of:
        //   1) which satellite it belongs to
        //   2) which component is affected
        //
        // For example, if we have satellite 1000 with component BATT,
        // all violations for that pair go into the same group.
        Map<String,List<TelemetryRecord>> violationMap = new HashMap<>();
        // We loop through all records and check if they are violations.
        for (TelemetryRecord record : records) {
            if(record.isViolation()){
                // Create a string like "1000_BATT" or "1000_TSTAT" to group them.
                String key = record.sateliteId + "_" + record.component;
                // If we don't already have a list for this (satellite, component), create one.
                // Then add the current record to that list.
                violationMap.computeIfAbsent(key, k -> new ArrayList<>()).add(record);

            }
        }

        // We'll store any alerts we find in this list.
        List<Alert> alerts = new ArrayList<>();
        // Now that we've grouped all the violations, we need to check each group.
        // We'll see if there are three or more violations within 5 minutes in each group.
        for (Map.Entry<String, List<TelemetryRecord>> entry : violationMap.entrySet()) {
            // Get the list of violations for this satellite + component.

            List<TelemetryRecord> violations = entry.getValue();
            // Sort the violations by the time they occurred (earliest first).
            violations.sort(Comparator.comparing(r -> r.timestamp));
            int n = violations.size();

            // We'll use two indices, windowStart and windowEnd, to define
            // the time window of violations we're looking at.
            int windowStart = 0;

            // Move windowEnd through the whole list of violations.
            for (int windowEnd = 0; windowEnd < n; windowEnd++) {
                // If the window (from the record at windowStart to the record at windowEnd)
                // is bigger than 5 minutes, we move the windowStart forward until it's within 5 minutes.
                while (windowStart < windowEnd &&
                        Duration.between(violations.get(windowStart).timestamp, violations.get(windowEnd).timestamp)
                                .compareTo(WINDOW_DURATION) > 0) {
                    windowStart++;
                }

                // Now we check how many violations are in the window:
                // (windowEnd - windowStart + 1) is the count.
                if ((windowEnd - windowStart + 1) >= 3) {
                    // We found at least three violations in 5 minutes!
                    // The first record in this windowStart gives us the satellite ID, component, timestamp, etc.
                    TelemetryRecord alertRecord = violations.get(windowStart);

                    // The severity depends on which component had the violation.
                    // If it's BATT, we call it "RED LOW"; if it's something else (TSTAT here), it's "RED HIGH".
                    String severity = ("BATT".equals(alertRecord.component)) ? "RED LOW" : "RED HIGH";

                    // Create an Alert object and add it to our list of alerts.
                    alerts.add(new Alert(alertRecord.sateliteId, severity, alertRecord.component, alertRecord.timestamp));
                    // Only one alert per group is needed.
                    break;
                }
            }
        }
        // After checking every group, return the list of alerts we found.
        return alerts;
    }

    /**
     * This class represents one line of telemetry data from the file.
     * It has information about:
     *   - when it was recorded (timestamp)
     *   - which satellite it came from (sateliteId)
     *   - various limit values (red/yellow high and low)
     *   - the actual measured value (rawValue)
     *   - the component name (e.g., BATT or TSTAT)
     */
    private static class TelemetryRecord {
        Instant timestamp;  // The exact moment this telemetry reading was recorded.
        int sateliteId;// The ID of the satellite this data is for (e.g., "1000")
        double redHighLimit;// The "red high" limit. If rawValue exceeds this, it's a serious (red) high violation.
        double yellowHighLimit;// The "yellow high" limit (less severe than red).
        double yellowLowLimit;// The "yellow low" limit.
        double redLowLimit;// The "red low" limit. If rawValue falls below this, it's a serious (red) low violation.
        double rawValue;// The actual measured value at this time.
        String component;// The component name, e.g., "BATT" or "TSTAT".

        // Formatter for input timestamps (e.g., "20180101 23:01:05.001")
        private static final DateTimeFormatter INPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss.SSS");


        /**
         * This method takes a single line of text from the file and creates a TelemetryRecord object from it.
         * The line is expected to be in the format:
         *   timestamp|satelliteId|redHighLimit|yellowHighLimit|yellowLowLimit|redLowLimit|rawValue|component
         */
        static TelemetryRecord parse(String line) {
            // Split the line into parts, separated by the '|' character.
            String[] parts = line.split("\\|");
            // We expect exactly 8 parts. If we don't see 8, it's an error
            if (parts.length != 8) {
                throw new IllegalArgumentException("Invalid telemetry record: " + line);
            }

            // Create a new TelemetryRecord to fill in.
            TelemetryRecord record = new TelemetryRecord();
            // Convert the timestamp (e.g., "20180101 23:01:05.001") to a Java Instant.
            // We first parse it into a LocalDateTime, then convert that to an Instant in UTC (Coordinated Universal Time).
            LocalDateTime ldt = LocalDateTime.parse(parts[0].trim(), INPUT_FORMATTER);
            record.timestamp = ldt.atZone(ZoneOffset.UTC).toInstant();

            // Convert each remaining piece of text into the proper data type (numbers, strings, etc.).
            record.sateliteId = Integer.parseInt(parts[1].trim());
            record.redHighLimit = Double.parseDouble(parts[2].trim());
            record.yellowHighLimit = Double.parseDouble(parts[3].trim());
            record.yellowLowLimit = Double.parseDouble(parts[4].trim());
            record.redLowLimit = Double.parseDouble(parts[5].trim());
            record.rawValue = Double.parseDouble(parts[6].trim());
            record.component = parts[7].trim();
            return record;
        }

        /**
         * This method determines whether this record is a "violation."
         * For BATT (battery), it's a violation if the rawValue is less than the redLowLimit.
         * For TSTAT (thermostat), it's a violation if the rawValue is greater than the redHighLimit.
         */
        boolean isViolation() {
            if ("BATT".equals(component)) {
                // For battery, check if rawValue is below the redLowLimit.
                return rawValue < redLowLimit;
            }else if ("TSTAT".equals(component)) {
                // For thermostat, check if rawValue is above the redHighLimit.
                return rawValue > redHighLimit;
            }
            return false;
        }


    }

    /**
     * This class represents an "Alert" – a serious event found in the telemetry data.
     * For example, if the battery voltage was too low 3 times in 5 minutes, we create an Alert.
     */
    private static class Alert {
        int sateliteId;
        String severity;
        String component;
        String timestamp;

        /**
         * Construct an Alert with the key information.
         *
         * @param sateliteId The satellite ID.
         * @param severity   The severity level ("RED LOW" or "RED HIGH").
         * @param component  The component name (e.g., "BATT").
         * @param timestamp  The time the alert occurred (as an Instant).
         */
        public Alert(int sateliteId, String severity, String component, Instant timestamp) {
            this.sateliteId = sateliteId;
            this.severity = severity;
            this.component = component;
            // Convert the Instant to a string, e.g., "2023-01-01T12:00:00Z".
            this.timestamp = timestamp.toString();
        }

        // Getter methods allow other parts of the code or JSON serialization to
        // retrieve these values.
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

