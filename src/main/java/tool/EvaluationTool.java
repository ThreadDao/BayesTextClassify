package tool;

import constant.PropertyConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import util.ClassifyUtils;

import java.io.IOException;
import java.util.*;

public class EvaluationTool {

    private Hashtable<String,String> bayesClassifyMap = null;

    public EvaluationTool(Configuration configuration, String pathKey) throws IOException {
        bayesClassifyMap = ClassifyUtils.mapKeyStringValueFromHDFS(configuration, pathKey);
    }
    /*
    * 精度计算，需要按类计算
    * 所以可以遍历bayesClassifyMap.keySet获取所有的类别
    * 然后对每个类别根据bayesClassifyMap数据进行统计，可以抽象出方法evaluation(String classname)
    * 如果直接从控制台输出，只要system.out.println就可以了，如果写到文件，可以抽象一个结果对象，作为上面方法返回值
    * */
    public String evaluation(){
        // 求出不同的类别
        Set<String> classSet = new HashSet<>();
        List<PredictEntry> predictEntries = new ArrayList<>();
        String classname = null;
        // classname@filename-classname
        for(Map.Entry<String,String> entry:bayesClassifyMap.entrySet()){
            classname = StringUtils.splitByWholeSeparatorPreserveAllTokens(entry.getKey(),"@")[0];
            predictEntries.add(new PredictEntry(classname,entry.getValue()));
            classSet.add(classname);
        }
        // 对每个类别进行评分
        ClazzEvaluation evaluation = null;
        StringBuilder stringBuilder = new StringBuilder();
        for(String clazz:classSet) {
            evaluation = evaluation(clazz,predictEntries);
            System.out.println(evaluation.toString());
            stringBuilder.append(evaluation.toString());
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

    private ClazzEvaluation evaluation(String classname,List<PredictEntry> predictEntryList){
        int TP=0,FP=0,FN=0;
        for(PredictEntry entry:predictEntryList){
            if(entry.predictTrue() && entry.trueClass.equals(classname)){
                    TP++;
            }
            else{
                if(entry.trueClass.equals(classname)){
                    FP++;
                }else if(entry.predictClass.equals(classname)){
                    FN++;
                }
            }
        }
        ClazzEvaluation clazzEvaluation = new ClazzEvaluation(classname,TP,FP,FN);
        clazzEvaluation.evaluation();
        return clazzEvaluation;
    }

    class PredictEntry{
       String trueClass;
       String predictClass;
       public PredictEntry(String trueClass,String predictClass){
           this.trueClass=trueClass;
           this.predictClass=predictClass;
       }
       public boolean predictTrue(){
           return trueClass.equals(predictClass);
       }
    }
    class ClazzEvaluation{
        private String clazz;
        private int TP=0;//key=value=c的数目
        private int FP=0;//value为c的记录数
        private int FN=0;//key为c的数目
        private double precision;
        private double recall;
        private double F1;
        public ClazzEvaluation(String clazz,int TP,int FP,int FN){
            this.clazz=clazz;
            this.TP=TP;
            this.FP=FP;
            this.FN=FN;
            /*this.TP=TP;
            this.TPandFP=TPandFP;
            this.TPandFN=TPandFN;*/
        }
        public void evaluation(){
            this.precision=Double.valueOf(this.TP)/(this.TP+this.FP);
            this.recall=Double.valueOf(this.TP)/(this.TP+this.FN);
            this.F1=2*this.recall*this.precision/(this.precision+this.recall);
        }
        @Override
        public String toString(){
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(clazz);
            stringBuilder.append(":");
            stringBuilder.append("TP=");
            stringBuilder.append(TP);
            stringBuilder.append(",");
            stringBuilder.append("FP=");
            stringBuilder.append(FP);
            stringBuilder.append(",");
            stringBuilder.append("FN=");
            stringBuilder.append(FN);
            stringBuilder.append(",");
            stringBuilder.append("precision=");
            stringBuilder.append(precision);
            stringBuilder.append(",");
            stringBuilder.append("recall=");
            stringBuilder.append(recall);
            stringBuilder.append(",");
            stringBuilder.append("F1=");
            stringBuilder.append(F1);
            return stringBuilder.toString();
        }

    }
}
