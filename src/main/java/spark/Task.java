package spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class Task {
    private SparkSession spark;

    private static final String destination = "hdfs://internship-hadoop107203:8020/web_logs/result";

    private final String source = "/web_logs/data";

    private void writeToParquet(Dataset<Row> dataset,String path){
        dataset.write().parquet(path);
    }
    private void topDomainByGuid(Dataset<Row> df){
        //Lấy top 5 domain có số lượng GUID nhiều nhất.
        Dataset<Row> res1 = df
                .select(col("domain").cast("String"),col("guid"))
                .groupBy(col("domain"))
                .agg(count("guid"))
                .orderBy(col("count(guid)").desc())
                .limit(5);

        res1=res1.withColumnRenamed("count(guid)","NumberOfGuids");
        res1.write().option("delimiter", ";").option("header", "true").mode(SaveMode.Overwrite).csv(destination + "/topDomainByGuid");
    }
    private void topLocationByGuid(Dataset<Row> df){
        //Lấy top 5 vị trí địa lý có nhiều GUID truy cập nhất. Vị trí địa lý sử dụng trường locid >1.


        Dataset<Row> res2=df.filter("locid>1")
                .select(col("locid"),col("guid"))
                .groupBy(col("locid"))
                .agg(count("guid"))
                .orderBy(col("count(guid)").desc())
                .limit(5);

        res2=res2.withColumnRenamed("count(guid)","NumberOfGuids");
        res2.write().option("delimiter", ";").option("header", "true").mode(SaveMode.Overwrite).csv(destination + "/topLocationByGuid");
    }

    private void GGFBRate(Dataset<Row> df){
        //Tính tỉ lệ pageview phát sinh từ google, fb.

        //@pageViewGoogle: Lưu số bản ghi phát sinh từ Google
        long pageViewGoogle = df.select(col("refer").cast("String"))
                .filter(col("refer")
                        .rlike("google.com|com.google")).count();
        //@pageViewFacebook: Lưu số bản ghi phát sinh từ Facebook
        long pageViewFacebook =  df.select(col("refer").cast("String"))
                .filter(col("refer")
                        .rlike("facebook.com")).count();
        //@total: Lưu tổng số bản ghi
        long total=df.count();
        StructType structType = new StructType();
        structType = structType.add("source", DataTypes.StringType, false);
        structType = structType.add("rate", DataTypes.DoubleType, false);

        List<Row> nums = new ArrayList<Row>();
        nums.add(RowFactory.create("Google", (pageViewGoogle*1.0/total)*100));
        nums.add(RowFactory.create("Facebook", (pageViewFacebook*1.0/total)*100));
        Dataset<Row> res3 = spark.createDataFrame(nums, structType);
        res3.write().option("delimiter", ";").option("header", "true").mode(SaveMode.Overwrite).csv(destination + "/GGFBRate");
    }

    public  void run() {
        this.spark = SparkSession.builder()
                .appName("Task")
                .master("yarn")
                .getOrCreate();
        this.spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read().parquet(source);
//        df.show(false);
        topDomainByGuid(df);
        topLocationByGuid(df);
        GGFBRate(df);
    }

    public static void main(String[] args) {
        Task task = new Task();
        task.run();
    }
}
