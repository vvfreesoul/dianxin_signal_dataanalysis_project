package preprocessing.pingpang_drit;

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

public class dsm_driftjob extends Configured implements Tool {
    public int run(String[] args) throws Exception {

        if (args.length<2){
            System.err.println("Usage: driftJob [optional:config_file] <in> <out>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.reduce.memory.mb","8192");
        conf.set("mapreduce.reduce.java.opts","-Xmx6963m");
        conf.set("intdouble_path",args[0]);
        Job job = Job.getInstance(conf,"dsm_driftJob");

//        job.setJarByClass(driftJob.class);
        job.setJar("/Users/apple/Desktop/dianxin_signal_dataanalysis_project/target/dianxin_signal_dataanalysis_project-1.0-SNAPSHOT.jar");
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
        job.setMapperClass(driftMapper.class);
        job.setReducerClass(driftReducer.class);

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
        ToolRunner.run(new Configuration(),new dsm_driftjob(),args);
    }
}
