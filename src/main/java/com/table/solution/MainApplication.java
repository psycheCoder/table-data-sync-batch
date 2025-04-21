package com.table.solution;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;


@SpringBootApplication
@EnableConfigurationProperties
public class MainApplication /* implements CommandLineRunner */{

	public static void main(String[] args) {
		System.exit(SpringApplication.exit(new SpringApplicationBuilder()
				.sources(MainApplication.class).web(false).run(args)));
	}

	/*
	 * //Uncomment For Local testing only
	 * 
	 * @Autowired JobLauncher jobLauncher;
	 * 
	 * @Autowired
	 * 
	 * @Qualifier("migrationJob") Job job;
	 * 
	 * @Override public void run(String... arg0) throws Exception { JobParameters
	 * jobParameters = new JobParametersBuilder() .addLong("startAt",
	 * System.currentTimeMillis()).addString("JobID", "migrationJob")
	 * .addString("JOB_NAME", "migrationJob").toJobParameters();
	 * 
	 * 
	 * jobLauncher.run(job,jobParameters); }
	 */
}
