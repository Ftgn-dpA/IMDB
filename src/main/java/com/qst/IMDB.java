package com.qst;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IMDB {
    public static class Month_Genres implements WritableComparable<Month_Genres>, DBWritable {
        private String month;
        private String genres;

        public Month_Genres() {
            this.month = "";
            this.genres = "";
        }

        public Month_Genres(String month, String genres) {
            this.month = month;
            this.genres = genres;
        }

        public void setMonth(String month) {
            this.month = month;
        }

        public void setGenres(String genres) {
            this.genres = genres;
        }

        public String getMonth() {
            return month;
        }

        public String getGenres() {
            return genres;
        }

        @Override
        public int compareTo(Month_Genres o) {
            int i = Integer.parseInt(this.getMonth()) - Integer.parseInt(o.getMonth());
            int k = this.getGenres().compareTo(o.getGenres());
            return i == 0 ? k : i;
        }

        @Override
        public String toString() {
            return month + "\t" + genres;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(month);
            dataOutput.writeUTF(genres);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.month = dataInput.readUTF();
            this.genres = dataInput.readUTF();
        }

        @Override
        public void readFields(ResultSet rs) throws SQLException {
            month = rs.getString(1);
            genres = rs.getString(2);
        }

        @Override
        public void write(PreparedStatement ps) throws SQLException {
            ps.setString(1, month);
            ps.setString(2, genres);
        }
    }

    public static class WriteDB implements Writable, DBWritable {
        private String month;
        private String genres;
        private float popularity;

        public WriteDB(String month, String genres, float popularity) {
            this.month = month;
            this.genres = genres;
            this.popularity = popularity;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(month);
            dataOutput.writeUTF(genres);
            dataOutput.writeFloat(popularity);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.month = dataInput.readUTF();
            this.genres = dataInput.readUTF();
            this.popularity = dataInput.readFloat();
        }

        @Override
        public void readFields(ResultSet rs) throws SQLException {
            month = rs.getString(1);
            genres = rs.getString(2);
            popularity = rs.getFloat(3);
        }

        @Override
        public void write(PreparedStatement ps) throws SQLException {
            ps.setString(1, month);
            ps.setString(2, genres);
            ps.setFloat(3, popularity);
        }
    }

    public static class IMDB_Mapper extends Mapper<LongWritable, Text, Month_Genres, FloatWritable> {
        private Month_Genres data = new Month_Genres();
        private FloatWritable popularity = new FloatWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lines = value.toString();
            String[] data_lst = lines.split(",");//切分三列:体裁、受欢迎度、上映时间
            String[] genres_lst = data_lst[0].split("/");//体裁数组

            for (String i : genres_lst) {
                if (data_lst[2].split("/").length != 3)
                    continue;
                data.setMonth(data_lst[2].split("/")[1]);//切分月份
                data.setGenres(i);//体裁
                popularity.set(Float.parseFloat(data_lst[1]));//受欢迎度

                context.write(data, popularity);
            }
        }
    }

    public static class IMDB_Reducer extends Reducer<Month_Genres, FloatWritable, WriteDB, NullWritable> {
        private Month_Genres data = new Month_Genres();
        FloatWritable popularity = new FloatWritable();

        @Override
        protected void reduce(Month_Genres key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            ConcurrentHashMap<Month_Genres, Float> map = new ConcurrentHashMap<>();
            for (FloatWritable i : values) {
                Float hasStatus = map.get(key);
                //如果没有，添加体裁
                if (hasStatus == null) {
                    map.put(key, i.get());
                } else {
                    //增加受欢迎度值
                    map.put(key, hasStatus + i.get());
                }
            }
            for (Map.Entry<Month_Genres, Float> entry : map.entrySet()) {
                data.setMonth(key.getMonth());
                data.setGenres(key.getGenres());
                popularity.set(entry.getValue());

                context.write(new WriteDB(data.getMonth(), data.getGenres(), popularity.get()), NullWritable.get());
            }
            map.clear();
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Upload.upload(conf);

        DBConfiguration.configureDB(conf,
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://localhost:3306/imdb?serverTimezone=GMT",
                "root",
                "pp18001476164");
        Job job = Job.getInstance(conf, "IMDB");

        job.setJarByClass(IMDB.class);
        job.setMapperClass(IMDB_Mapper.class);
        job.setReducerClass(IMDB_Reducer.class);

        job.setMapOutputKeyClass(Month_Genres.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(WriteDB.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job, "output", "month", "genres", "popularity");

        FileInputFormat.addInputPath(job, new Path(args[0]));

        //输出到本地代码
//        Path path = new Path(args[1]);
//        FileSystem fs = FileSystem.get(conf);
//        if (fs.exists(path)) {
//            fs.delete(path, true);
//        }
//        FileOutputFormat.setOutputPath(job, path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
