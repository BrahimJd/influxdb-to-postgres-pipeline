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

        // Flux query
        String flux = "from(bucket: \"" + influxBucket + "\")\n" +
                " |> range(start: -1h)\n" +
                " |> filter(fn: (r) => r._measurement == \"events\")\n" +
                " |> group(columns: [\"company\", \"project\", \"cohort\", \"user\", \"stage\", \"java\"])\n" +
                " |> sum()\n" +
                " |> yield(name: \"aggregated_by_dimensions\")";


        // 1. Query InfluxDB
        InfluxDBClient influxClient = InfluxDBClientFactory.create(influxUrl, influxToken.toCharArray(), influxOrg, influxBucket);
        List<FluxTable> tables = influxClient.getQueryApi().query(flux);

        // 2. Connect to PostgreSQL
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(postgresUrl, postgresUser, postgresPassword);

            conn.setAutoCommit(false); // IMPROVEMENT: Transaction start
            String insertSQL = "INSERT INTO aggregated_data (company, project, cohort, \"user\", stage, java, value) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(insertSQL);


            // 3. Process and insert each record
            tables.forEach(table -> {
                List<FluxRecord> records = table.getRecords();
                records.forEach(record -> {
                    try {
                        pstmt.setString(1, (String) record.getValueByKey("company"));
                        pstmt.setString(2, (String) record.getValueByKey("project"));
                        pstmt.setString(3, (String) record.getValueByKey("cohort"));
                        pstmt.setString(4, (String) record.getValueByKey("user"));
                        pstmt.setString(5, (String) record.getValueByKey("stage"));
                        pstmt.setString(6, (String) record.getValueByKey("java"));
                        pstmt.setDouble(7, (Double) record.getValue());
                        pstmt.addBatch(); // IMPROVEMENT: Batch instead of executeUpdate()
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            });

            pstmt.executeBatch(); // IMPROVEMENT: Execute all at once
            conn.commit(); // IMPROVEMENT: Commit transaction

            System.out.println("Data transferred successfully!");

        } catch (SQLException e) {
        System.err.println("Database error: " + e.getMessage());
        // rollback if something went wrong
        try {
            if (conn != null) {
                conn.rollback();
                System.err.println("Transaction rolled back.");
            }
        } catch (SQLException rollbackEx) {
            System.err.println("Rollback failed: " + rollbackEx.getMessage());
        }
        } finally {
            influxClient.close();
        }
    }
}

