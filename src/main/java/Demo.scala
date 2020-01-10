import org.apache.spark.sql.SparkSession

object Demo {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder()
        .appName("HiveSupport")
        .master("local[*]")
        //拷贝hdfs-site.xml不用设置，如果使用本地hive，可通过该参数设置metastore_db的位置
        //.config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport() //开启支持hive
        .getOrCreate()

    //spark.sparkContext.setLogLevel("WARN") //设置日志输出级别
    import spark.implicits._
    import spark.sql
    sql("show databases")
    sql("show tables").show()
    sql("select * from cdr limit 20").show()
    spark.stop()
  }

}
