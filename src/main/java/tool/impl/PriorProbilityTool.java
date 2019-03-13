package tool.impl;

import constant.PropertyConstant;
import exception.BayesException;
import org.apache.commons.lang.BooleanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import tool.ConfiguredTool;

import java.io.IOException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PriorProbilityTool extends ConfiguredTool {

    static class PriorProbilityInputFormat extends FileInputFormat<Text, Text> {

        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new PriorProbilityRecordReader();
        }
    }

    static class PriorProbilityRecordReader extends RecordReader<Text,Text>{
        private boolean processed = false;
        private Path path = null;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) inputSplit;
            path = split.getPath();
            System.out.println(path.toString());
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean haveNext = !processed;
            processed = true;
            return haveNext;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return new Text(Objects.nonNull(path)?path.getName():"");
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            Path parentPath = Objects.nonNull(path)?path.getParent():null;
            return new Text(Objects.nonNull(parentPath)?parentPath.getName():"");
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return processed?1:0;
        }

        @Override
        public void close() throws IOException {
        }

    }

    static class PriorProbilityMapper extends Mapper<Text,Text,Text, IntWritable>{
        private static final IntWritable ONE = new IntWritable(1);
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text(value),ONE);
        }
    }

    static class PriorProbilityReduce extends Reducer<Text,IntWritable,Text,Text>{
        @SuppressWarnings("Duplicates")
        public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value:values){
                sum += value.get();
            }
            context.write(key,new Text(String.valueOf(sum)));
        }
    }

    @Override
    @SuppressWarnings("Duplicates")
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        String inputPathDirectory = configuration.get(PropertyConstant.BAYES_INPUT_PATH);
        String outputPathDirectory = configuration.get(PropertyConstant.BAYES_OUTPUT_PATH);

        Path input = new Path(inputPathDirectory);
        Path output = new Path(outputPathDirectory);
        FileSystem inputFileSystem =input.getFileSystem(configuration);
        if(BooleanUtils.isFalse(inputFileSystem.exists(input))){
            throw new BayesException("输入路径不存在,path={}",inputPathDirectory);
        }

        FileStatus[] inputFileStatuses = inputFileSystem.listStatus(input);
        Path[] inputPathArray = Stream.of(inputFileStatuses).map(FileStatus::getPath).collect(Collectors.toList()).toArray(new Path[0]);
        FileSystem outputFileSystem = output.getFileSystem(configuration);

        if(outputFileSystem.exists(output)){
            outputFileSystem.delete(output,true);
        }

        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());
        job.setMapperClass(PriorProbilityMapper.class);
        job.setReducerClass(PriorProbilityReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(PriorProbilityInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        for(Path path : inputPathArray){
            PriorProbilityInputFormat.addInputPath(job,path);
        }
        TextOutputFormat.setOutputPath(job,output);
        return job.waitForCompletion(true)?0:1;
    }
}
