package com.lavapm.model

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import scala.collection.mutable.ListBuffer

/**
  * Cisco
  * Created by dalei on 1/25/19.
  */
object Cisco {

  val originRddCol = Array("mobile_md5", "mobile_brand", "mobile_type", "mobile_price", "mobile_os", "mobile_lang", "mobile_operator",
    "province", "city", "age", "gender", "consume_lever", "richman", "household", "marriage", "looking_for_lover", "married", "single_women",
    "occupation", "apply_job", "job_hunting", "job_hopping", "teacher", "programmer", "doctor", "truck_driver", "cab_driver", "industry",
    "enterprise_owner", "sexual_orientation", "residential_area", "foreign_personnel", "city_level", "house_moving", "domestic_tourism",
    "outbound_travel", "has_car", "has_estate", "pre_pregnant", "pregnant", "has_child_0_2", "has_child_3_6", "has_child_0_6", "has_child_3_14",
    "has_child_0_14", "is_mother", "mother_baby", "has_pupil", "has_middle", "has_second_child", "has_bady", "graduate",	"stock",
    "invest", "keep_account", "bank", "credit", "lottey", "p2p", "online_shopping", "offshore_shopping",
    "express", "wine", "group_buying", "pre_school_education", "pri_sed_education", "adult_education", "english_train", "go_aboard_train",
    "exam", "language_learn", "online_education", "business_trip", "leisure_travel", "hotel", "outdoor", "around_travel",
    "rent_car", "buy_car", "driving_test", "like_car", "driving_service", "peccancy", "car_maintain", "second_car",
    "news", "magazine", "novel", "listen_story", "cartoon", "funny", "pictorial", "sports_news", "kara", "music_player", "radio", "ring",
    "video_player", "telecast", "online_video", "online_music", "chat", "friends", "dating", "weibo", "community", "restaurant",
    "take_out", "fresh_delivery", "cleaner", "washing", "cooking", "spa", "movie", "pet", "rent_house", "buy_house", "decorate", "wedding",
    "fitness", "reduce_weight", "health_care",  "chronic", "beautify", "pic_sharing", "video_shooting", "beauty",
    "parenting", "menstrual", "taxi",	"menu", "map_nav", "whether", "cloud_disk", "office", "note", "email", "intelligence", "child_edu",
    "speed", "sport", "shooting", "cheese", "cosplay", "tactics", "network_game")

  val numericCol = Array("richman", "household", "looking_for_lover", "married", "single_women", "apply_job", "job_hunting", "job_hopping",
    "teacher", "programmer", "doctor", "truck_driver", "cab_driver", "industry", "foreign_personnel", "house_moving", "domestic_tourism",
    "has_car", "has_estate", "pregnant", "has_child_0_2", "has_child_3_6", "has_child_0_6", "has_child_3_14",
    "has_child_0_14", "is_mother", "mother_baby", "has_pupil", "has_middle", "has_second_child", "has_bady", "graduate",
    "cleaner", "rent_house", "wedding", "intelligence", "child_edu",
    "speed", "sport", "shooting", "cheese", "cosplay", "tactics", "network_game")

  val keyCols = Array("mobile_md5")

//  val oneHotCols = Array("mobile_brand", "mobile_type", "mobile_price", "mobile_os", "mobile_lang", "mobile_operator",
//    "province", "city", "age", "gender", "consume_lever", "marriage", "occupation", "enterprise_owner", "sexual_orientation", "residential_area",
//    "city_level", "outbound_travel", "pre_pregnant", "stock",
//    "invest", "keep_account", "bank", "credit", "lottey", "p2p", "online_shopping", "offshore_shopping",
//    "express", "wine", "group_buying", "pre_school_education", "pri_sed_education", "adult_education", "english_train", "go_aboard_train",
//    "exam", "language_learn", "online_education", "business_trip", "leisure_travel", "hotel", "outdoor", "around_travel",
//    "rent_car", "buy_car", "driving_test", "like_car", "driving_service", "peccancy", "car_maintain", "second_car",
//    "news", "magazine", "novel", "listen_story", "cartoon", "funny", "pictorial", "sports_news", "kara", "music_player", "radio", "ring",
//    "video_player", "telecast", "online_video", "online_music", "chat", "friends", "dating", "weibo", "community", "restaurant",
//    "take_out", "fresh_delivery", "washing", "cooking", "movie", "pet",  "buy_house", "decorate",
//    "fitness", "reduce_weight", "health_care",  "chronic", "beautify", "pic_sharing", "video_shooting", "beauty",
//    "parenting", "menstrual", "taxi",	"menu", "map_nav", "whether", "cloud_disk", "office", "note", "email")

  val oneHotCols = Array("mobile_brand", "mobile_type", "mobile_price", "mobile_os", "mobile_lang", "mobile_operator",
    "province", "city", "age", "gender", "consume_lever", "richman", "household", "marriage", "looking_for_lover", "married", "single_women",
    "occupation", "apply_job", "job_hunting", "job_hopping", "teacher", "programmer", "doctor", "truck_driver", "cab_driver", "industry",
    "enterprise_owner", "sexual_orientation", "residential_area", "foreign_personnel", "city_level", "house_moving", "domestic_tourism",
    "outbound_travel", "has_car", "pre_pregnant", "pregnant", "has_child_0_2", "has_child_3_6", "has_child_0_6", "has_child_3_14",
    "has_child_0_14", "is_mother", "mother_baby", "has_pupil", "has_middle", "has_second_child", "graduate",	"stock",
    "invest", "keep_account", "bank", "credit", "lottey", "p2p", "online_shopping", "offshore_shopping",
    "express", "wine", "group_buying", "pre_school_education", "pri_sed_education", "adult_education", "english_train", "go_aboard_train",
    "exam", "language_learn", "online_education", "business_trip", "leisure_travel", "hotel", "outdoor", "around_travel",
    "rent_car", "buy_car", "driving_test", "like_car", "driving_service", "peccancy", "car_maintain", "second_car",
    "news", "magazine", "novel", "listen_story", "cartoon", "funny", "pictorial", "sports_news", "kara", "music_player", "radio", "ring",
    "video_player", "telecast", "online_video", "online_music", "chat", "friends", "dating", "weibo", "community", "restaurant",
    "take_out", "fresh_delivery", "cleaner", "washing", "cooking", "movie", "pet", "rent_house", "buy_house", "decorate", "wedding",
    "fitness", "reduce_weight", "health_care",  "chronic", "beautify", "pic_sharing", "video_shooting", "beauty",
    "parenting", "menstrual", "taxi",	"menu", "map_nav", "whether", "cloud_disk", "office", "note", "email", "intelligence", "child_edu",
    "speed", "sport", "shooting", "cheese", "cosplay", "tactics", "network_game")


  val schema = StructType(originRddCol.map(fieldName => StructField(fieldName, StringType, true)))



  def main(args: Array[String]): Unit = {

//    val posPath = args(0)
//    val poolPath = args(1)
//    val resPath = args(2)
//    val scale = args(3).toInt

    val posPath = "/user/dmp_app_user/original_data/getui_data/cisco_pos"
    val poolPath = "/user/dmp_app_user/original_data/getui_data/cisco_neg"
    val resPath = "/user/dmp_app_user/lookalike/cisco"

    val scale = 10000

    val spark = SparkSession
                .builder()
                .appName("Lookalike for Cisco")
                .getOrCreate()

    println("**************** before trainModel ")
    trainModel(spark, posPath, poolPath, resPath, scale)

    spark.stop()
  }


  /**
    * create Word2Vec model by specific corpus
    * @param spark        spark session
    * @param posPath      positive audience path in hdfs
    * @param poolPath     pool audience path in hdfs
    * @param resPath      result audience path in hdfs
    * @param scale        the scale for enlarging
    */
  def trainModel( spark: SparkSession,
                  posPath: String,
                  poolPath: String,
                  resPath: String,
                  scale: Int) : Unit = {

    // transfer the positive audience from file to data frame
    val posRDD = spark.sparkContext.textFile(posPath).map(_.split(",", -1)).map { p =>
      var valueSeq: Seq[String] = Seq()
      for (i <- 0 to (originRddCol.length - 1)) {
        if(p(i) != null && p(i).trim.length > 0)
          valueSeq :+= p(i)
        else
          valueSeq :+= "0"
      }
      Row.fromSeq(valueSeq)
    }

    val posDF = spark.createDataFrame(posRDD, schema)
    val posNum = posDF.count.toInt
    posDF.createOrReplaceTempView("pos")


    // transfer the pool audience from file to data frame, default remove the positive audience
    val poolRDD = spark.sparkContext.textFile(poolPath).map(_.split(",", -1)).map { p =>
      var valueSeq: Seq[String] = Seq()
      for (i <- 0 to (originRddCol.length - 1)) {
        if(p(i) != null && p(i).trim.length > 0)
          valueSeq :+= p(i)
        else
          valueSeq :+= "0"
      }
      Row.fromSeq(valueSeq)
    }

    val poolDF = spark.createDataFrame(poolRDD, schema).except(posDF).limit(1000000)
    poolDF.createOrReplaceTempView("pool")


    // generate the negative audience from poolDF and posDF
    val negDF = poolDF.limit(posNum)
    negDF.createOrReplaceTempView("neg")


    // prepare the training data frame with label and feature, prepare the pool data frame with mobile_md5 and feature
    var featureColStr = ""
    for(col <- oneHotCols) featureColStr += " , " + col
    //for(col <- numericCol) featureColStr += " , cast(" + col + " as Double) as " + col

    val trainingDF = spark.sql("select cast(1 as Double) as label " + featureColStr + " from pos").union(spark.sql("select cast(0 as Double) as label " + featureColStr + " from neg"))
    val poolDF1 = spark.sql("select mobile_md5 " + featureColStr + " from pool")


    // create the pipeline
    val stagesArray = new ListBuffer[PipelineStage]()

    // create category index by StringIndexer, then create the vector by OneHotEncoder
    for (col <- oneHotCols) {
      val indexer = new StringIndexer().setInputCol(col).setOutputCol(s"${col}_index")
      val encoder = new OneHotEncoderEstimator().setInputCols(Array(indexer.getOutputCol)).setOutputCols(Array(s"${col}_classVec"))
      //val encoder = new OneHotEncoder().setInputCol(indexer.getOutputCol).setOutputCol(s"${col}classVec").setDropLast(false)
      stagesArray.append(indexer, encoder)
    }


    // prepare the features
    val assemblerInputs = oneHotCols.map(_ + "_classVec")

    // convert all features to one vector by VectorAssembler
    val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("features")
    stagesArray.append(assembler)

    val pipeline = new Pipeline()
    pipeline.setStages(stagesArray.toArray)


    // process the one-hot encoding for training and pool data frame
    val pipelineModel = pipeline.fit(poolDF1.union(trainingDF))
    val trainingVecDF = pipelineModel.transform(trainingDF)
    val poolVecDF = pipelineModel.transform(poolDF1)

    println(s"trainingVecDF size=${trainingVecDF.count}, poolVecDF size=${poolVecDF.count}")


    // build the model and fit the training data
    val lrModel = new LogisticRegression()
                      .setLabelCol("label")
                      .setFeaturesCol("features")
                      .setRegParam(0.1)
                      .setElasticNetParam(0.5)
                      .setMaxIter(100)
                      .setTol(1E-6)
                      .setFitIntercept(true).fit(trainingVecDF)

    println(s"  Weights: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")


    // calculate the accuracy
    val predictDF = lrModel.transform(trainingVecDF)
    val predictions = predictDF.select("prediction").rdd.map(_.getDouble(0))
    val labels = predictDF.select("label").rdd.map(_.getDouble(0))
    val metrics = new MulticlassMetrics(predictions.zip(labels))

    println(s"  Accuracy : ${metrics.accuracy}")
    println(s"  Weighted Recall : ${metrics.weightedRecall}")
    println(s"  Weighted FMeasure : ${metrics.weightedFMeasure}")
    println(s"  Weighted False Positive Rate : ${metrics.weightedFalsePositiveRate}")
    println(s"  Weighted True Positive Rate : ${metrics.weightedTruePositiveRate}")


    // make the prediction on the pool
    //val predictDF = lrModel.transform(poolVecDF).select("mobile_md5", "prediction")

    // select the alike audience and write to result folder
    //predictDF.orderBy(predictDF("prediction").desc).limit(scale).rdd.map(x => x.get(0)).saveAsTextFile(resPath)

  }

}
