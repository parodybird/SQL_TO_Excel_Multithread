import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ParallelSQLToExcelExporter {
    // Number of threads to run in parallel (adjust based on your system)
    private static final int NUM_THREADS = 4;

    // Custom object to hold query details
    private static class QueryDetails {
        String query;
        String tableName;
        String sheetName;
        int startRow;
        int startColumn;

        public QueryDetails(String query, String tableName, String sheetName, int startRow, int startColumn) {
            this.query = query;
            this.tableName = tableName;
            this.sheetName = sheetName;
            this.startRow = startRow;
            this.startColumn = startColumn;
        }
    }

    public static void main(String[] args) {
        // Database connection parameters
        String dbUrl = "jdbc:mysql://localhost:3306/your_database";
        String dbUser = "your_username";
        String dbPassword = "your_password";

        // Excel file information
        String outputLocation = "C:/output_folder/";
        String fileName = "Morning Stats - " + new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + ".xlsx";

        // List of queries along with their details
        List<QueryDetails> queriesList = new ArrayList<>();
        queriesList.add(new QueryDetails(
            "SELECT column1, column2 FROM table1;",
            "Table1",
            "Sheet1",
            2,
            0
        ));
        queriesList.add(new QueryDetails(
            "SELECT column1, column2, column3 FROM table2;",
            "Table2",
            "Sheet2",
            1,
            1
        ));
        // Add more queries if needed

        try {
            // Establish the database connection
            Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);

            // Create Excel workbook
            Workbook workbook = new XSSFWorkbook();

            // Start the ExecutorService
            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

            // Execute each query in parallel using a separate thread
            for (QueryDetails queryDetails : queriesList) {
                executor.submit(() -> {
                    try {
                        // Execute the SQL query
                        Statement statement = connection.createStatement();
                        ResultSet resultSet = statement.executeQuery(queryDetails.query);

                        // Create or get the sheet based on the sheet name
                        Sheet sheet = workbook.getSheet(queryDetails.sheetName);
                        if (sheet == null) {
                            sheet = workbook.createSheet(queryDetails.sheetName);
                        }

                        // Write the results to Excel
                        writeResultSetToExcel(resultSet, sheet, queryDetails.startRow, queryDetails.startColumn);

                        resultSet.close();
                        statement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                });
            }

            // Shutdown the ExecutorService to ensure all threads finish
            executor.shutdown();
            while (!executor.isTerminated()) {
                // Wait for all threads to finish
            }

            // Save the Excel file
            String outputFile = outputLocation + fileName;
            FileOutputStream fileOut = new FileOutputStream(outputFile);
            workbook.write(fileOut);
            fileOut.close();
            workbook.close();

            System.out.println("Data exported to Excel successfully.");
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    private static synchronized void writeResultSetToExcel(ResultSet resultSet, Sheet sheet, int startRow, int startColumn)
            throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int numColumns = metaData.getColumnCount();
        int currentRow = startRow;

        // Write headers if needed (only once)
        if (currentRow == startRow) {
            Row headerRow = sheet.createRow(startRow);
            for (int col = startColumn; col < startColumn + numColumns; col++) {
                Cell headerCell = headerRow.createCell(col);
                headerCell.setCellValue(metaData.getColumnLabel(col + 1));
            }
            currentRow++;
        }

        // Write data
        while (resultSet.next()) {
            Row dataRow = sheet.createRow(currentRow);
            for (int col = startColumn; col < startColumn + numColumns; col++) {
                Cell dataCell = dataRow.createCell(col);
                dataCell.setCellValue(resultSet.getString(col + 1));
            }
            currentRow++;
        }
    }
}
