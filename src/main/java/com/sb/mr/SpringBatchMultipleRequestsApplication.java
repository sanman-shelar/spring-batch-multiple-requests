package com.sb.mr;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchMultipleRequestsApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchMultipleRequestsApplication.class, args);
	}

}
