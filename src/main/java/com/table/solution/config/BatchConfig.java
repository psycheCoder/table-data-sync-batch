package com.table.solution.config;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.table.solution.provider.DynamicQueryGenerator;
import com.table.writer.ReplaceDataWriter;

@Configuration
@EnableBatchProcessing
public class BatchConfig  {

    private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);
    @Value("${spring.datasource.source.schema}")
    private String sourceSchema;
    
    @Value("${spring.datasource.source.table}")
    private String sourceTable;

    @Value("${spring.datasource.target.schema}")
    private String targetSchema;
    
    @Value("${spring.datasource.target.table}")
    private String targetTable;
    
    @Value("${data.fetch.limit:5000}")
    private int limit;
    
    @Value("${data.fetch.offset:0}")
    private int offset;

    @Autowired
    @Qualifier("sourceDataSource") // Specify the data source for Spring Batch metadata
    private DataSource sourceDataSource;

    @Autowired
    @Qualifier("targetDataSource") // Specify the data source for the target database
    private DataSource targetDataSource;

    @Autowired
    private DynamicQueryGenerator queryGenerator;
    
    @Value("${batch.chunk-size:5000}")
    private int chunkSize;
    private final DataSource sourceDataSourceX;

    // Constructor Injection for DataSource
    public BatchConfig(DataSource sourceDataSource) {
        this.sourceDataSourceX = sourceDataSource;
    }

    // Configuring the JobRepository with auto schema creation
    @Bean
    public JobRepository jobRepository() throws Exception {
        JobRepositoryFactoryBean factoryBean = new JobRepositoryFactoryBean();
        factoryBean.setDataSource(sourceDataSource);  // Use your sourceDataSource
        factoryBean.setTransactionManager(new DataSourceTransactionManager(sourceDataSource));
        factoryBean.setDatabaseType("MYSQL");  // Specify the database type for schema creation
//        factoryBean.setInitializeSchema(JobRepositoryFactoryBean.SchemaMode.ALWAYS);  // Create the schema if it doesn't exist
        return factoryBean.getObject();
    }
    @Bean
    public JobLauncher jobLauncher(JobRepository jobRepository) {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        return jobLauncher;
    }
    /**
     * Configure the ItemReader to read data from the source database dynamically.
     */
    @Bean
    @StepScope
	public JdbcCursorItemReader<Map<String, Object>> reader() throws SQLException {
		log.info("Initializing JdbcCursorItemReader to read data from table: {}", sourceTable);

		JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(sourceDataSource);

		// Make sure query generation works as expected
		String sql = queryGenerator.generateSelectQuery(sourceTable, sourceDataSource, sourceSchema,limit,offset);
		offset+=limit;
		reader.setSql(sql);

		reader.setRowMapper((rs, rowNum) -> {
			Map<String, Object> rowMap = new HashMap<>();
			try {
				// Get metadata once, outside the loop
				ResultSetMetaData metaData = rs.getMetaData();
				int columnCount = metaData.getColumnCount();

				// Iterate over columns and populate rowMap
				for (int i = 1; i <= columnCount; i++) {
					String columnName = metaData.getColumnName(i);
					Object columnValue = rs.getString(i);

					if (columnValue != null) {
						rowMap.put(columnName, columnValue);
					} else {
						rowMap.put(columnName, ""); // Or null, depending on your needs
					}
				}
			} catch (SQLException e) {
				log.error("Error mapping row {}", rowNum, e); // Log rowNum for debugging
				throw new RuntimeException("Error mapping row", e); // Wrap exception for clarity
			}
			return rowMap;
		});

		return reader;
	}


    /**
     * Configure a pass-through ItemProcessor that processes data without modifications.
     */
	/*
	 * @StepScope
	 * 
	 * @Bean(name = "itemProcessor") public ItemProcessor<Map<String, Object>,
	 * Map<String, Object>> itemProcessor() { return item -> {
	 * log.info("Processing item: {}", item); return item; // Pass-through processor
	 * }; }
	 */

    /**
     * Configure the ItemWriter to write data to the target database dynamically.
     */
    @Bean
    public ReplaceDataWriter writer() {
        return new ReplaceDataWriter(targetDataSource, targetTable, targetSchema);
    }

    /**
     * Configure the Step for migration using reader, processor, and writer.
     */
    @Bean
	public Step migrationStep(StepBuilderFactory stepBuilderFactory,
			ItemReader<Map<String, Object>> itemReader, /*
														 * @Qualifier("itemProcessor") ItemProcessor<Map<String,
														 * Object>, Map<String, Object>> itemProcessor,
														 */
                              @Qualifier("writer") ItemWriter<Map<String, Object>> itemWriter) {

        return stepBuilderFactory.get("migrationStep")
                .<Map<String, Object>, Map<String, Object>>chunk(chunkSize)
                .reader(itemReader)
				/* .processor(itemProcessor) */
                .writer(itemWriter)
                .build();
    }

    /**
     * Configure the Job for migration using the defined step.
     */
    @Bean
    public Job migrationJob(JobBuilderFactory jobBuilderFactory, @Qualifier("migrationStep") Step migrationStep) {
        return jobBuilderFactory.get("migrationJob")
                .start(migrationStep)
                .build();
    }
}
