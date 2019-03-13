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
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import tool.ConfiguredTool;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConditionalProbilityTool extends ConfiguredTool {

    static class ConditionalProbilityInputFormat extends FileInputFormat<Text, Text>{
        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new ConditionalProbilityRecordReader();
        }
    }

    static class ConditionalProbilityRecordReader extends RecordReader<Text,Text>{
        private String clazz = null;
        private LineRecordReader lineRecordReader = null;
        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            FileSplit fileSplit = (FileSplit) genericSplit;
            Path path = fileSplit.getPath();
            clazz  = path.getParent().getName();
            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(genericSplit,context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            return lineRecordReader.nextKeyValue();
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return new Text(clazz+"@"+getCurrentValue().toString());
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return lineRecordReader.getCurrentValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }
    }

    static class ConditionalProbilityMapper extends Mapper<Text,Text,Text,IntWritable>{
        private static final IntWritable ONE = new IntWritable(1);
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, ONE);
        }
    }

    static class ConditionalProbilityCombine extends Reducer<Text,IntWritable,Text,IntWritable>{
        @SuppressWarnings("Duplicates")
        public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable value:values){
                count += value.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    static class ConditionalProbilityReducer extends Reducer<Text,IntWritable,Text,Text>{
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
        job.setMapperClass(ConditionalProbilityMapper.class);
        // 默认是与outputKey，outputValue一致，但此处类型不同，需重新设置
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(ConditionalProbilityCombine.class);
        job.setReducerClass(ConditionalProbilityReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(ConditionalProbilityInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        for(Path path : inputPathArray){
            PriorProbilityTool.PriorProbilityInputFormat.addInputPath(job,path);
        }
        TextOutputFormat.setOutputPath(job,output);
        return job.waitForCompletion(true)?0:1;
    }
}
