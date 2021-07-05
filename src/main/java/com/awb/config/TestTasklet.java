package com.awb.config;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.stream.Stream;

@Component
public class TestTasklet implements Tasklet {

    @Value("${file.input}")
    private String fileInput;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {

        File file = new File(fileInput);
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);

        String line;
        if((line = br.readLine()) != null) {
            String[] columns = line.split(",");
            for (int i = 0; i < columns.length; i++) {
                if (i == 0 && !columns[i].equalsIgnoreCase("xx")) throw new Exception("File not valid");

            }
            Stream.of(columns).forEach(System.out::println);
        }

        fr.close();
        return RepeatStatus.FINISHED;
    }
}
