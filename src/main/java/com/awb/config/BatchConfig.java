package com.awb.config;

import com.awb.model.Client;
import com.awb.model.ClientRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.xml.builder.StaxEventItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import java.util.Collections;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private TestTasklet tasklet;

    @Value("${file.input}")
    private String fileInput;

    @Value("${file.output}")
    private String fileOutput;

    @Bean
    public Job clientsJob(@Qualifier("step1WriteInFile") Step step1, @Qualifier("step2WriteToDatabase") Step step2, @Qualifier("step3Tasklet") Step taskletStep,  JobRepository jobRepository) {
        Flow flow = new FlowBuilder<SimpleFlow>("flow")
                .start(taskletStep)
                .next(step1)
                .next(step2)
                .build();

        return jobBuilderFactory
                .get("clientsJob")
                .repository(jobRepository)
                .start(flow)
                .end()
                .build();
    }

    @Bean
    public Step step1WriteInFile(@Qualifier("step1Reader") ItemReader<Client> reader, @Qualifier("step1WriterXML") ItemWriter writer) {
        return stepBuilderFactory
                .get("step1WriteInFile")
                .allowStartIfComplete(true)
                .chunk(10)
                .reader(reader)
                .writer(writer)
                .build();
    }

    @Bean
    public FlatFileItemReader step1Reader() {
        return new FlatFileItemReaderBuilder<>()
                .name("step1Reader")
                .resource(new FileSystemResource(fileInput))
                .delimited()
                .names("id", "nom", "prenom", "adresse", "numeroCompte")
                .fieldSetMapper(new BeanWrapperFieldSetMapper() {{
                    setTargetType(Client.class);
                }})
                .linesToSkip(1)
                .build();
    }

    @Bean
    public ItemWriter step1Writer() {
        return new FlatFileItemWriterBuilder<Client>()
                .name("step1Writer")
                .resource(new FileSystemResource(fileOutput))
                .delimited()
                .delimiter("|")
                .names("id", "nom", "prenom", "numeroCompte")
                .append(true)
                .build();
    }

    @Bean
    public ItemWriter step1WriterXML() {
        XStreamMarshaller studentMarshaller = new XStreamMarshaller();
        studentMarshaller.setAliases(Collections.singletonMap(
                "client",
                Client.class
        ));

        return new StaxEventItemWriterBuilder<Client>().name("step1WriterXML")
                .resource(new FileSystemResource(fileOutput))
                .marshaller(studentMarshaller)
                .rootTagName("clients")
                .build();
    }

    @Bean
    public Step step2WriteToDatabase(@Qualifier("step1Reader") ItemReader<Client> reader, @Qualifier("step2Writer") ItemWriter writer) {
        return stepBuilderFactory
                .get("step2WriteToDatabase")
                .allowStartIfComplete(true)
                .chunk(10)
                .reader(reader)
                .writer(writer)
                .build();
    }

    @Bean
    public RepositoryItemWriter<Client> step2Writer(ClientRepository clientRepository) {
        return new RepositoryItemWriterBuilder<Client>()
                .repository(clientRepository).build();
    }

    @Bean
    public Step step3Tasklet() {
        return stepBuilderFactory.get("step3Tasklet").tasklet(tasklet).allowStartIfComplete(true).build();
    }

}
