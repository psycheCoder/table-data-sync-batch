package com.table.writer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.table.solution.provider.DynamicQueryGenerator;

@Component
public class ReplaceDataWriter implements org.springframework.batch.item.ItemWriter<Map<String, Object>>{

    
    private final DataSource targetDataSource;
    private final String targetTable;
    private final String targetSchema;
    @Autowired
    private DynamicQueryGenerator queryGenerator;
    private static final Logger log = LoggerFactory.getLogger(ReplaceDataWriter.class);

    public ReplaceDataWriter(DataSource targetDataSource, String targetTable, String targetSchema) {
        this.targetDataSource = targetDataSource;
        this.targetTable = targetTable;
        this.targetSchema = targetSchema;
    }
    
    public void write(List<? extends Map<String, Object>> items) throws Exception {
    	  

        String insertQuery = queryGenerator.generateInsertQuery(targetTable, targetDataSource, targetSchema);
        String[] columnNames = insertQuery.substring(insertQuery.indexOf("(") + 1, insertQuery.indexOf(") VALUES")).split(",\\s*");
        try (Connection connection = targetDataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(insertQuery)) {
        	
            for (Map<String, Object> item : items) {
                int index = 1;
                for (String columnName : columnNames) {
                    Object value = item.get(columnName);
                    if (value == null) {
                        ps.setObject(index++, ""); // Handle null values
                    } else if (value instanceof String) {
                        String strValue = (String) value;
                        if (strValue.startsWith("\"") && strValue.endsWith("\"")) {
                            strValue = strValue.substring(1, strValue.length() - 1);
                        }
                        ps.setString(index++, strValue);
                    } else {
                        ps.setObject(index++, value);
                    }
                   
                }
                ps.addBatch();
            }
            ps.executeBatch();
            log.info("Successfully wrote {} items to the target table.", items.size());
        } catch (SQLException e) {
            log.error("Error occurred while writing items to the target table", e);
            throw new RuntimeException("Error during batch insert", e);
        }
    }

}
