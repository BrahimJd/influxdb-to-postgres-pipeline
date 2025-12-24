import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class InfluxToPostgres {

    private static final int BATCH_SIZE = 5000; // Execute batch every 5000 rows

    public static void main(String[] args) {

        // InfluxDB config
        String influxUrl = System.getenv("INFLUX_URL");
        String influxToken = System.getenv("INFLUX_TOKEN");
        String influxOrg = System.getenv("INFLUX_ORG");
        String influxBucket = System.getenv("INFLUX_BUCKET");

        // PostgreSQL config
        String postgresUrl = System.getenv("POSTGRES_URL");
        String postgresUser = System.getenv("POSTGRES_USER");
        String postgresPassword = System.getenv("POSTGRES_PASSWORD");

        // Flux query - limit time range if needed
        String flux = "from(bucket: \"" + influxBucket + "\")\n" +
                " |> range(start: -24h)\n" +  // Adjust time range as needed
                " |> filter(fn: (r) => r._measurement == \"events\")\n" +
                " |> filter(fn: (r) => r._field == \"value\")\n" +  // Only numeric field
                " |> group(columns: [\"company\", \"project\", \"cohort\", \"user\", \"stage\", \"java\"])\n" +
                " |> sum()\n" +  // Use sum for value field
                " |> yield(name: \"aggregated_by_dimensions\")";

        InfluxDBClient influxClient = null;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            // 1. Query InfluxDB
            System.out.println("Connecting to InfluxDB...");
            influxClient = InfluxDBClientFactory.create(influxUrl, influxToken.toCharArray(), influxOrg, influxBucket);

            System.out.println("Querying data from InfluxDB...");
            List<FluxTable> tables = influxClient.getQueryApi().query(flux);

            // 2. Connect to PostgreSQL with optimizations
            System.out.println("Connecting to PostgreSQL...");
            conn = DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);

            // PostgreSQL performance optimizations
            conn.setAutoCommit(false);

            // Use COPY or bulk insert (PostgreSQL specific optimization)
            String insertSQL = "INSERT INTO aggregated_data (company, project, cohort, \"user\", stage, java, value) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT DO NOTHING";  // Avoid duplicates if running multiple times

            pstmt = conn.prepareStatement(insertSQL);

            // 3. Process and insert with batching
            int totalRecords = 0;
            int batchCount = 0;

            System.out.println("Processing records...");

            for (FluxTable table : tables) {
                List<FluxRecord> records = table.getRecords();

                for (FluxRecord record : records) {
                    try {
                        pstmt.setString(1, (String) record.getValueByKey("company"));
                        pstmt.setString(2, (String) record.getValueByKey("project"));
                        pstmt.setString(3, (String) record.getValueByKey("cohort"));
                        pstmt.setString(4, (String) record.getValueByKey("user"));
                        pstmt.setString(5, (String) record.getValueByKey("stage"));
                        pstmt.setString(6, (String) record.getValueByKey("java"));

                        // Handle different value types
                        Object value = record.getValue();
                        if (value instanceof Number) {
                            pstmt.setDouble(7, ((Number) value).doubleValue());
                        } else {
                            pstmt.setDouble(7, 0.0);
                        }

                        pstmt.addBatch();
                        batchCount++;
                        totalRecords++;

                        // Execute batch every BATCH_SIZE rows
                        if (batchCount >= BATCH_SIZE) {
                            pstmt.executeBatch();
                            conn.commit();
                            System.out.println("Processed " + totalRecords + " records...");
                            batchCount = 0;
                        }

                    } catch (Exception e) {
                        System.err.println("Error processing record: " + e.getMessage());
                    }
                }
            }

            // Execute remaining batch
            if (batchCount > 0) {
                pstmt.executeBatch();
                conn.commit();
            }


            System.out.println("\nData transferred successfully!");
            System.out.println("Total records: " + totalRecords);

        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
            e.printStackTrace();

            // Rollback on error
            try {
                if (conn != null) {
                    conn.rollback();
                    System.err.println("Transaction rolled back.");
                }
            } catch (SQLException rollbackEx) {
                System.err.println("Rollback failed: " + rollbackEx.getMessage());
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();

        } finally {
            // Clean up resources
            try {
                if (pstmt != null) pstmt.close();
                if (conn != null) conn.close();
                if (influxClient != null) influxClient.close();
            } catch (Exception e) {
                System.err.println("Error closing resources: " + e.getMessage());
            }
        }
    }
}