package task.impl;

import constant.PropertyConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import task.Task;
import tool.EvaluationTool;
import tool.impl.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class BayesTextClassifyTask implements Task {

    private String profile;
    private Properties properties;

    @Override
    public String getProfile(){
        return this.profile;
    }

    @Override
    public void setProfile(String profile){
        this.profile = profile;
    }

    @Override
    public String readProperty(String propertyKey){
        String key = propertyKey.startsWith("bayes")?properties.getProperty(PropertyConstant.BAYES_HDFS_PATH):"";
        return key+properties.getProperty(propertyKey);
    }

    private void loadProperties() throws IOException {
        properties = new Properties();
        String profile = getProfile();
        String propertyName = (StringUtils.isNotBlank(profile)?profile+".":"")+ PropertyConstant.ROOT_PROPERTIES_NAME;
        InputStream in = this.getClass().getResourceAsStream("/"+ propertyName);
        properties.load(in);
    }

    @Override
    @SuppressWarnings("Duplicates")
    public void process(String[] args) throws Exception {
        loadProperties();

        Configuration priorProbilityConf = new Configuration();
        priorProbilityConf.set(PropertyConstant.BAYES_INPUT_PATH,readProperty(PropertyConstant.BAYES_INPUT_PATH));
        priorProbilityConf.set(PropertyConstant.BAYES_OUTPUT_PATH,readProperty(PropertyConstant.BAYES_OUTPUT_PRIOR_PROBILITY_COUNT));
        PriorProbilityTool priorProbilityTool = new PriorProbilityTool();
        ToolRunner.run(priorProbilityConf,priorProbilityTool,args);


        Configuration conditionalProbilityConf = new Configuration();
        conditionalProbilityConf.set(PropertyConstant.BAYES_INPUT_PATH,readProperty(PropertyConstant.BAYES_INPUT_PATH));
        conditionalProbilityConf.set(PropertyConstant.BAYES_OUTPUT_PATH,readProperty(PropertyConstant.BAYES_OUTPUT_CONDITIONAL_PROBILITY_COUNT));
        ConditionalProbilityTool conditionalProbilityTool = new ConditionalProbilityTool();
        ToolRunner.run(conditionalProbilityConf,conditionalProbilityTool,args);
//
        Configuration predictionConf = new Configuration();
        predictionConf.set(PropertyConstant.BAYES_INPUT_PATH,readProperty(PropertyConstant.BAYES_INPUT_TEST_PATH));
        predictionConf.set(PropertyConstant.BAYES_OUTPUT_PATH,readProperty(PropertyConstant.BAYES_OUTPUT_PREDICTION_RESULT));
        predictionConf.set(PropertyConstant.BAYES_OUTPUT_PRIOR_PROBILITY_COUNT,readProperty(PropertyConstant.BAYES_OUTPUT_PRIOR_PROBILITY_COUNT));
        predictionConf.set(PropertyConstant.BAYES_OUTPUT_CONDITIONAL_PROBILITY_COUNT,readProperty(PropertyConstant.BAYES_OUTPUT_CONDITIONAL_PROBILITY_COUNT));
        PredictionTool predictionTool = new PredictionTool();
        ToolRunner.run(predictionConf,predictionTool,args);

        EvaluationTool evaluationTool = new EvaluationTool(predictionConf,PropertyConstant.BAYES_OUTPUT_PATH);
        evaluationTool.evaluation();
    }
}
