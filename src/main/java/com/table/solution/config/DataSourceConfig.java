package com.table.solution.config;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;



@Configuration
@EnableBatchProcessing
public class DataSourceConfig {

    // Configuring the source database data source
    @Bean(name = "sourceDataSource")
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource.source")
    public DataSource sourceDataSource() {
        return DataSourceBuilder.create().build();
    }

    // Configuring the target database data source
    @Bean(name = "targetDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.target")
    public DataSource targetDataSource() {
        return DataSourceBuilder.create().build();
    }

    // Configuring the transaction manager for the source database
    @Bean(name = "sourceTransactionManager")
    public DataSourceTransactionManager sourceTransactionManager(@Qualifier("sourceDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    // Configuring the transaction manager for the target database (optional, if you need separate transactions)
    @Bean(name = "targetTransactionManager")
    public DataSourceTransactionManager targetTransactionManager(@Qualifier("targetDataSource") DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }
}
