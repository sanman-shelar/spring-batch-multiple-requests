package com.sb.mr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
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
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
public class BatchConfiguration {

	@Autowired
	JobBuilderFactory jobBuilderFactory;

	@Autowired
	StepBuilderFactory stepBuilderFactory;

	List<String> filenames = Arrays.asList("data/data1.txt", "data/data2.txt");

	@Bean
	public Job myJob() {

		List<Step> steps = new ArrayList<>();
		filenames.forEach(filename -> steps.add(createStep(filename)));
		
		return jobBuilderFactory.get("myJob").start(createParallelFlow(steps)).build().build();
		
	}

	private Step createStep(String filename) {
		return stepBuilderFactory.get("convertStepFor" + filename)
				.<Person, Person>chunk(2)
				.reader(createFileReader(filename, lineMapper())).processor(processor())
				.writer(createFileWriter("data/converted_" + filename, personLineAggregator())).build();

	}

	private static Flow createParallelFlow(List<Step> steps) {
		SimpleAsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
		taskExecutor.setConcurrencyLimit(steps.size());

		List<Flow> flows = steps.stream()
				.map(step -> new FlowBuilder<Flow>("flow_" + step.getName()).start(step).build())
				.collect(Collectors.toList());

		return new FlowBuilder<SimpleFlow>("parallelStepsFlow").split(taskExecutor)
				.add(flows.toArray(new Flow[flows.size()])).build();
	}

	public static FlatFileItemReader<Person> createFileReader(String source, LineMapper<Person> lineMapper) {
		FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();

		reader.setEncoding("UTF-8");
		reader.setResource(new FileSystemResource(source));
		reader.setLinesToSkip(1);
		reader.setLineMapper(lineMapper);
		try {
			reader.afterPropertiesSet();
		} catch (Exception e) {
		}

		return reader;
	}

	public static ItemWriter<Person> createFileWriter(String target, LineAggregator<Person> aggregator) {
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<>();

		writer.setEncoding("UTF-8");
		writer.setResource(new FileSystemResource(target));
		writer.setShouldDeleteIfExists(true);
		writer.setLineAggregator(aggregator);

		try {
			writer.afterPropertiesSet();
		} catch (Exception e) {	}
		return writer;
	}

	public ItemProcessor<Person, Person> processor() {
		return new PersonProcessor();
	}

	public LineMapper<Person> lineMapper() {
		DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<Person>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter("|");
		BeanWrapperFieldSetMapper<Person> fieldSetMapper = new BeanWrapperFieldSetMapper<Person>();

		lineTokenizer.setNames(new String[] { "name", "age" });
		lineTokenizer.setIncludedFields(new int[] { 0, 1 });
		fieldSetMapper.setTargetType(Person.class);
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);

		return lineMapper;
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
		extractor.setNames(new String[] { "name", "age" });
		return extractor;
	}
}
