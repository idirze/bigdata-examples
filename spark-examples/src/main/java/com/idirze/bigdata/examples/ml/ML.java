package com.idirze.bigdata.examples.ml;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.ml.feature.RFormulaModel;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.util.Arrays;

public class ML {

    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("Stocks - FileStructuredStreaming")
                //.config("spark.sql.autoBroadcastJoinThreshold", 1)
                .config("spark.sql.shuffle.partitions", "2")// reduce number of tasks (shuffle): creating too many shuffle partitions:
                .getOrCreate();

        // Apart from JSON, there are some specific data formats commonly used for supervised learning, including LIBSVM.
        // 1- Collect the data
        Dataset<Row> df = sparkSession
                .read()
                .json("data/simple-ml");
                df
                .orderBy("value2")
                .show();

        //2- Convert the data into feature vectors (double), feature engineering
        RFormula rFormula = new RFormula()
                .setFormula("lab ~ . + color:value1 + color:value2");

        RFormulaModel fittedRF = rFormula.fit(df);
        Dataset<Row> preparedDF = fittedRF.transform(df);
        preparedDF.show(false);

        //3- Split the dataset into training and validation sets
        Dataset<Row>[] rows = preparedDF.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = rows[0];
        Dataset<Row> testing = rows[1];

        // 4- Create the model
        LogisticRegression lr = new
                LogisticRegression()
                .setLabelCol("label")
                .setFeaturesCol("features");


        // 5- Create the pipeline
        PipelineStage[] stages = new PipelineStage[]{rFormula, lr};
        Pipeline pipeline = new Pipeline()
                .setStages(stages);

        //6- Training and Evaluation
        // Train several variations of the model by specifying different combinations of -> hyperparameters
        ParamMap[] params = new ParamGridBuilder()
                // Two different versions of the RFormula
                .addGrid(rFormula.formula(), JavaConverters
                        .iterableAsScalaIterableConverter(Arrays.asList("lab ~ . + color:value1",
                                "lab ~ . + color:value1 + color:value2"))
                        .asScala())
                // Three different options for the ElasticNet parameter
                .addGrid(lr.elasticNetParam(), new double[]{0.0, 0.5, 1.0})
                // Two different options for the regularization parameter
                .addGrid(lr.regParam(), new double[]{0.1, 2.0})
                .build();

        //7- Evaluation
        // We will then select the best model using an Evaluator that compares their predictions on our validation data.
        Evaluator evaluator = new BinaryClassificationEvaluator()
                .setMetricName("areaUnderROC")
                .setRawPredictionCol("prediction")
                .setLabelCol("label");

        TrainValidationSplit tvs = new TrainValidationSplit()
                .setTrainRatio(0.75)
                .setEstimatorParamMaps(params)
                .setEstimator(pipeline)
                .setEvaluator(evaluator);

        //8- Train every version of the model
        TrainValidationSplitModel tvsFitted = tvs.fit(training);

        //9- Evaluate against the testing set
        evaluator.evaluate(tvsFitted.transform(testing));

        //10- Persist the model (k/v pair database)
        // Good for recomandation
        // Not for classification or regression
        tvsFitted
                .write()
                .overwrite()
                .save("/tmp/modelLocation");

        // 11- Model later use
        TrainValidationSplitModel model = TrainValidationSplitModel.load("/tmp/modelLocation");
        model.transform(testing);

    }

}
