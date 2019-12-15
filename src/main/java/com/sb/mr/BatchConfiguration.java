package com.sb.mr;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldExtractor;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

@Configuration
public class BatchConfiguration {

	@Autowired
	JobBuilderFactory jobBuilderFactory; 
	
	@Autowired
	StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public Job sampleJobConfig() {
		return jobBuilderFactory.get("sampleJob")
					.start(step1())		
					.build();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1")
					.<Person, Person>chunk(2)
					.reader(reader())
					.processor(processor())
					.writer(writer())
					.build();
	}
	
	
	@Bean
	public FlatFileItemReader<Person> reader() {

		FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
		reader.setResource(new FileSystemResource("data/data1.txt"));
		reader.setLinesToSkip(1);

		reader.setLineMapper(lineMapper());
		return reader;
	}

	@Bean
    public LineMapper<Person> lineMapper() {
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<Person>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter("|");
        BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<Person>();

        lineTokenizer.setNames(new String[]{"name", "age"});
        lineTokenizer.setIncludedFields(new int[]{0, 1});
        fieldSetMapper.setTargetType(Person.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }
	
	@Bean
    public ItemProcessor<Person, Person> processor() {
        return new PersonProcessor();
    }
	
	@Bean
	ItemWriter<Person> writer() {
        FlatFileItemWriter<Person> csvFileWriter = new FlatFileItemWriter<>(); 
        String exportFilePath = "data/person.csv";
        csvFileWriter.setShouldDeleteIfExists(true);
        csvFileWriter.setResource(new FileSystemResource(exportFilePath)); 
        csvFileWriter.setLineAggregator(personLineAggregator());
        return csvFileWriter;
    }
	
	public LineAggregator<Person> personLineAggregator() {
        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
 
        FieldExtractor<Person> fieldExtractor = personFieldExtractor();
        lineAggregator.setFieldExtractor(fieldExtractor);
 
        return lineAggregator;
    }
 
	public FieldExtractor<Person> personFieldExtractor() {
        BeanWrapperFieldExtractor<Person> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[] {"name", "age"});
        return extractor;
    }
}
