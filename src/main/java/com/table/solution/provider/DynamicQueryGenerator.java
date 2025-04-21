package com.table.solution.provider;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.stereotype.Component;

@Component
public class DynamicQueryGenerator {

    public String generateSelectQuery(String tableName, DataSource dataSource,String sourceSchema,int limit, int offset) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, tableName, null);
            StringBuilder columnList = new StringBuilder();

            while (columns.next()) {
                if (columnList.length() > 0) {
                    columnList.append(", ");
                }
                columnList.append(columns.getString("COLUMN_NAME"));
            }
            StringBuilder sql = new StringBuilder("SELECT * FROM ").append(sourceSchema).append(".").append(tableName);

            if (limit > 0) {
                sql.append(" LIMIT ").append(limit);
                if (offset > 0) {
                    sql.append(" OFFSET ").append(offset);
                }
            }

            return sql.toString();
            
        }
    }

    public String generateInsertQuery(String tableName, DataSource dataSource,String targetSchema) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(null, null, tableName, null);
            StringBuilder columnList = new StringBuilder();
            StringBuilder valuePlaceholders = new StringBuilder();

            while (columns.next()) {
                if (columnList.length() > 0) {
                    columnList.append(", ");
                    valuePlaceholders.append(", ");
                }
                columnList.append(columns.getString("COLUMN_NAME"));
                valuePlaceholders.append("?");
            }

            return "INSERT INTO " +targetSchema +"."+tableName + " (" + columnList + ") VALUES (" + valuePlaceholders + ")";
        }
    }
}
