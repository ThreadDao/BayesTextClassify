package constant;

public interface PropertyConstant {
    final String ROOT_PROPERTIES_NAME = "application.properties"; // 从项目打包的根目录读取，否则是相对路径
    final String BAYES_HDFS_PATH = "bayes.HDFS.path";
    final String BAYES_INPUT_PATH = "bayes.input.path"; // 每个Tool的对应输入
    final String BAYES_INPUT_TEST_PATH = "bayes.input.test.path";
    final String BAYES_OUTPUT_PATH = "bayes.output.path";
    final String BAYES_OUTPUT_PRIOR_PROBILITY_COUNT = "bayes.output.priorProbilityCount";
    final String BAYES_OUTPUT_CONDITIONAL_PROBILITY_COUNT = "bayes.output.conditionalProbilityCount";
    final String BAYES_OUTPUT_PREDICTION_RESULT = "bayes.output.predictionResult";
    final String BAYES_OUTPUT_EVALUATION_RESULT = "bayes.output.evaluationResult";
}