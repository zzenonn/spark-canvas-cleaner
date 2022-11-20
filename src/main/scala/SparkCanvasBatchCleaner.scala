package edu.ateneo.nrg.spark

import java.sql.Timestamp

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.SparkFiles._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.year
import org.apache.spark.sql.functions.month
import org.apache.spark.sql.functions.dayofmonth



/** Run sample SQL operations on a dataset. */
object SparkCanvasBatchCleaner {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val inputPath = args(0)
        
    // Create a SparkContext using every core of the local machine
    val spark = SparkSession
        .builder
        .appName("SparkCanvasBatchCleaner")
        .master("local[*]")
        .getOrCreate()

    val schema = StructType(Array(
        StructField("body_account_id", StringType),
        StructField("body_all_day", BooleanType),
        StructField("body_all_day_date", StringType),
        StructField("body_asset_id", StringType),
        StructField("body_asset_name", StringType),
        StructField("body_asset_subtype", StringType),
        StructField("body_asset_type", StringType),
        StructField("body_assignment_group_id", StringType),
        StructField("body_assignment_id", StringType),
        StructField("body_assignment_name", StringType),
        StructField("body_assignment_override_id", StringType),
        StructField("body_attachment_id", StringType),
        StructField("body_attachment_ids", ArrayType(StringType)),
        StructField("body_attachment_ids_0", StringType),
        StructField("body_attempt", LongType),
        StructField("body_author_id", StringType),
        StructField("body_body", StringType),
        StructField("body_category", StringType),
        StructField("body_content_type", StringType),
        StructField("body_context_id", StringType),
        StructField("body_context_type", StringType),
        StructField("body_context_uuid", StringType),
        StructField("body_conversation_id", StringType),
        StructField("body_course_id", StringType),
        StructField("body_course_section_id", StringType),
        StructField("body_created_at", StringType),
        StructField("body_created_on_blueprint_sync", BooleanType),
        StructField("body_description", StringType),
        StructField("body_discussion_entry_id", StringType),
        StructField("body_discussion_topic_id", StringType),
        StructField("body_display_name", StringType),
        StructField("body_domain", StringType),
        StructField("body_due_at", StringType),
        StructField("body_enrollment_id", StringType),
        StructField("body_filename", StringType),
        StructField("body_folder_id", StringType),
        StructField("body_grade", StringType),
        StructField("body_graded_at", StringType),
        StructField("body_grader_id", StringType),
        StructField("body_grading_complete", BooleanType),
        StructField("body_group_category_id", StringType),
        StructField("body_group_category_name", StringType),
        StructField("body_group_id", StringType),
        StructField("body_group_membership_id", StringType),
        StructField("body_group_name", StringType),
        StructField("body_group_weight", DoubleType),
        StructField("body_is_announcement", BooleanType),
        StructField("body_late", BooleanType),
        StructField("body_learning_outcome_group_id", StringType),
        StructField("body_level", StringType),
        StructField("body_lock_at", StringType),
        StructField("body_lti_assignment_description", StringType),
        StructField("body_lti_assignment_id", StringType),
        StructField("body_lti_resource_link_id", StringType),
        StructField("body_lti_resource_link_id_duplicated_from", StringType),
        StructField("body_lti_user_id", StringType),
        StructField("body_max_membership", LongType),
        StructField("body_message_id", StringType),
        StructField("body_missing", BooleanType),
        StructField("body_module_id", StringType),
        StructField("body_module_item_id", StringType),
        StructField("body_muted", BooleanType),
        StructField("body_name", StringType),
        StructField("body_old_grade", StringType),
        StructField("body_old_points_possible", DoubleType),
        StructField("body_old_score", DoubleType),
        StructField("body_parent_discussion_entry_id", StringType),
        StructField("body_points_possible", DoubleType),
        StructField("body_position", LongType),
        StructField("body_posted_at", StringType),
        StructField("body_quiz_id", StringType),
        StructField("body_redirect_url", StringType),
        StructField("body_role", StringType),
        StructField("body_rules", StringType),
        StructField("body_score", DoubleType),
        StructField("body_section_id", StringType),
        StructField("body_short_name", StringType),
        StructField("body_student_id", StringType),
        StructField("body_student_sis_id", StringType),
        StructField("body_submission_comment_id", StringType),
        StructField("body_submission_id", StringType),
        StructField("body_submission_type", StringType),
        StructField("body_submission_types", StringType),
        StructField("body_submitted_at", StringType),
        StructField("body_text", StringType),
        StructField("body_title", StringType),
        StructField("body_type", StringType),
        StructField("body_unlock_at", StringType),
        StructField("body_updated_at", StringType),
        StructField("body_url", StringType),
        StructField("body_user_id", StringType),
        StructField("body_user_login", StringType),
        StructField("body_user_sis_id", StringType),
        StructField("body_uuid", StringType),
        StructField("body_workflow_state", StringType),
        StructField("metadata_client_ip", StringType),
        StructField("metadata_context_account_id", StringType),
        StructField("metadata_context_id", StringType),
        StructField("metadata_context_role", StringType),
        StructField("metadata_context_sis_source_id", StringType),
        StructField("metadata_context_type", StringType),
        StructField("metadata_developer_key_id", StringType),
        StructField("metadata_event_name", StringType),
        StructField("metadata_event_time", TimestampType),
        StructField("metadata_hostname", StringType),
        StructField("metadata_http_method", StringType),
        StructField("metadata_producer", StringType),
        StructField("metadata_real_user_id", StringType),
        StructField("metadata_referrer", StringType),
        StructField("metadata_request_id", StringType),
        StructField("metadata_root_account_id", StringType),
        StructField("metadata_root_account_lti_guid", StringType),
        StructField("metadata_root_account_uuid", StringType),
        StructField("metadata_session_id", StringType),
        StructField("metadata_time_zone", StringType),
        StructField("metadata_url", StringType),
        StructField("metadata_user_account_id", StringType),
        StructField("metadata_user_agent", StringType),
        StructField("metadata_user_id", StringType),
        StructField("metadata_user_login", StringType),
        StructField("metadata_user_sis_id", StringType)
    ))

 
  
    // Load each line of the source data into an RDD
    val canvasData = spark.read
        .format("json")
        .schema(schema)
        .load(inputPath)

    val canvasDataExpandedDate = canvasData.withColumn("year", year(col("metadata_event_time")))
                       .withColumn("month", month(col("metadata_event_time")))
                       .withColumn("day", dayofmonth(col("metadata_event_time")))
    
    canvasDataExpandedDate.createOrReplaceTempView("canvasdata")

    // SQL can be run over DataFrames that have been registered as a table.
    val canvasdata = spark.sql("SELECT * FROM canvasdata LIMIT 5")

    canvasDataExpandedDate.printSchema()

    canvasdata.show()

    

    // results.foreach(println)
    spark.stop()
  }
    
}
  
