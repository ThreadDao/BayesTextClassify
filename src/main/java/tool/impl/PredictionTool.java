package tool.impl;

import constant.PropertyConstant;
import exception.BayesException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import tool.ConfiguredTool;
import util.ClassifyUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PredictionTool extends ConfiguredTool {

    static class PredictionInputFormat extends FileInputFormat<Text, Text>{

        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            return new PredictionRecordReader();
        }
    }

    static class PredictionRecordReader extends RecordReader<Text, Text>{

        private LineRecordReader lineRecordReader = null;
        private boolean processed = false;
        private Text value = null;
        private Text key = null;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) inputSplit;
            Path path = fileSplit.getPath();
            Path parentPath = path.getParent();
            String classnamePrefix = Objects.nonNull(parentPath)?parentPath.getName()+"@":"";
            key = new Text(classnamePrefix+path.getName());
            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(inputSplit,taskAttemptContext);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if(processed){
                return false;
            }
            StringBuffer stringBuffer = new StringBuffer();
            while(lineRecordReader.nextKeyValue()){
                stringBuffer.append(lineRecordReader.getCurrentValue());
                stringBuffer.append("\n");
            }
            value = new Text(stringBuffer.toString());
            processed = true;
            return true;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
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

    static class PredictionMapper extends Mapper<Text, Text, Text, Text>{

        private Hashtable<String,Double> priorProbilityMap = null;
        private Hashtable<String,Double> conditionalProbilityMap = null;
        private Double defaultConditionalProbility = null;

        public void setup(Context context)throws IOException, InterruptedException{
            Configuration configuration = context.getConfiguration();
            // 以key value形式读取文件每行值
            //<class,docNum>
            Hashtable<String,Long> priorResultMap = ClassifyUtils.mapKeyValueFromHDFS(configuration, PropertyConstant.BAYES_OUTPUT_PRIOR_PROBILITY_COUNT);
            //<class@word,wordNum>
            Hashtable<String,Long> conditionalResultMap = ClassifyUtils.mapKeyValueFromHDFS(configuration, PropertyConstant.BAYES_OUTPUT_CONDITIONAL_PROBILITY_COUNT);

            // class-log 先验概率 <class,priorProb>
            priorProbilityMap = new Hashtable<>();
            final long totalDocCount = priorResultMap.values().stream().reduce(Long::sum).orElse(-1l);
            priorResultMap.forEach((key,value)->priorProbilityMap.put(key,Math.log(Double.valueOf(value)/totalDocCount)));

            // class@word-log 条件概率
            Set<String> wordset = new HashSet<>();// 训练集的单词集合
            // 训练集，每个类别的单词总数
            Map<String,Long> wordCountEveryClassToMap = new HashMap<>();
            String classname = null;
            String word = null;
            String[] elements = null;
            for(Map.Entry<String,Long> entry:conditionalResultMap.entrySet()){
                elements = StringUtils.splitByWholeSeparatorPreserveAllTokens(entry.getKey(),"@");
                classname = elements[0];
                word = elements[1];
                wordset.add(word);
                //<class,totalWordCount>
                wordCountEveryClassToMap.put(classname,MapUtils.getLongValue(wordCountEveryClassToMap,classname,0L)+entry.getValue());
            }
            conditionalProbilityMap = new Hashtable<>();
            for(Map.Entry<String,Long> entry:conditionalResultMap.entrySet()){
                elements = StringUtils.splitByWholeSeparatorPreserveAllTokens(entry.getKey(),"@");
                classname = elements[0];
                conditionalProbilityMap.put(
                        entry.getKey(),
                        Math.log(
                                Double.valueOf(entry.getValue()+1)/
                                        (wordCountEveryClassToMap.get(classname)+wordset.size())
                        )
                );
            }
            // 默认条件概率
            defaultConditionalProbility = Math.log(1d/wordset.size());
        }

        public Double bayesProbabilityForClass(String content,String classname){
            // 大字符串，少遍历
            if(Objects.isNull(content)||StringUtils.isBlank(classname)){
                return 0d;
            }
            Double probility = priorProbilityMap.get(classname);
            // filter blank
            String[] words = StringUtils.splitByWholeSeparator(content,"\n");
            for(String word : words){
                probility += MapUtils.getDouble(conditionalProbilityMap,classname+"@"+word,defaultConditionalProbility);
            }
            return probility;
        }

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Double probility = null;
            for(String classname : priorProbilityMap.keySet()){
                probility = bayesProbabilityForClass(value.toString(),classname);
                context.write(key, new Text(classname+"@"+probility));
            }
        }
    }

    static class PredictionReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
            Double maxProbility = -Double.MAX_VALUE;
            Double compareProbility = 0d;
            String classname = null;
            String[] elements = null;
            for(Text value:values){
                elements = StringUtils.splitByWholeSeparatorPreserveAllTokens(value.toString(),"@");
                compareProbility = Double.valueOf(elements[1]);
                if(compareProbility>maxProbility){
                    classname = elements[0];
                    maxProbility = compareProbility;
                }
            }
            System.out.println(key+"@"+classname);
            context.write(key,new Text(classname));
        }
    }

    @Override
    @SuppressWarnings("Duplicate")
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
        job.setMapperClass(PredictionMapper.class);
        job.setReducerClass(PredictionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(PredictionInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        for(Path path : inputPathArray){
            PredictionInputFormat.addInputPath(job,path);
        }
        TextOutputFormat.setOutputPath(job,output);
        return job.waitForCompletion(true)?0:1;
    }
}
