package cluster_algorithm.DBscan_cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import preprocessing.del_null_sortbytime.user_model;

public class DBscanJob extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.map.cpu.vcores","1");
        conf.set("mapreduce.reduce.cpu.vcores","2");
        conf.set("mapreduce.job.map.memory.mb","4096");
        conf.set("mapreduce.job.reduce.memory.mb","8192");
        conf.set("mapreduce.map.java.opts","-Xmx3480m");
        conf.set("mapreduce.reduce.java.opts","-Xmx6963m");
//        conf.set("mapreduce.job.reduce.memory.mb","8192");
//        conf.set("mapreduce.reduce.java.opts","-Xmx6963m");
        conf.set("mapred.task.timeout","1800000");
        conf.set("intdouble_path",args[0]);
        Job job = Job.getInstance(conf,"DBscanJob");

        job.setJarByClass(DBscanJob.class);
        //job.setJar("/code/hadoopTest/Scheduleanalysis/target/Scheduleanalysis-1.0-SNAPSHOT.jar");
        job.setInputFormatClass(TextInputFormat.class);

        for(int i=1 ; i<args.length-1;i++){
            FileInputFormat.addInputPath(job,new Path(args[i]));
        }
        Path outputpath = new Path(args[args.length-1]);
        FileOutputFormat.setOutputPath(job,outputpath);

        FileSystem fileSystem = outputpath.getFileSystem(conf);
        if(fileSystem.exists(outputpath)){
            fileSystem.delete(outputpath,true);
        }
        job.setMapperClass(DBscanMapper.class);
        job.setReducerClass(DBscanReducer.class);
        //job.setReducerClass(regularReducer.class);

        //设置mapper输出类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(user_model.class);

        //设置reducer输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(6);

        return job.waitForCompletion(true)?0:1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(),new DBscanJob(),args);
    }
}
