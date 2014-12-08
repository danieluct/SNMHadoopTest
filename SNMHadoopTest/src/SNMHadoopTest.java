import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.durda.comparators.ProductSmartComparator;
import com.durda.comparators.UserCatComparator;
import com.durda.mappers.ProductDBMapper;
import com.durda.mappers.ProductLogMapper;
import com.durda.mappers.UserCatMapper;
import com.durda.partitioners.ProductSmartPartitioner;
import com.durda.partitioners.UserCatPartitioner;
import com.durda.reducers.ProductSmartCombiner;
import com.durda.reducers.ProductSmartReducer;
import com.durda.reducers.UserCatReducer;
import com.durda.writables.ProductDBWritable;
import com.durda.writables.ProductSmartKey;
import com.durda.writables.ProductSmartValue;
import com.durda.writables.UserCatKey;
import com.durda.writables.UserCatValue;

import org.postgresql.Driver;

class JobRunner implements Runnable {
	private JobControl control;

	public JobRunner(JobControl _control) {
		this.control = _control;
	}

	public void run() {
		this.control.run();
	}
}

public class SNMHadoopTest {

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] otherArgs1 = new GenericOptionsParser(conf1, args)
				.getRemainingArgs();

		//setup db connections
		DBConfiguration.configureDB(conf1, "org.postgresql.Driver",
				"jdbc:postgresql://10.104.215.2:5432/sanoma", "postgres",
				"1234");
		String[] fields = { "id", "name", "category", "price" };

    
		//create first mapreduce job
		ControlledJob job1 = new ControlledJob(conf1);
		job1.setJobName("sub job 1");
		Job job = job1.getJob();

		job.setJarByClass(SNMHadoopTest.class);
		DBInputFormat.setInput(job, ProductDBWritable.class, "products", null,
				"id", fields);
		//the path is bogus, this Mapper will read from the database
		MultipleInputs.addInputPath(job, new Path(otherArgs1[2]),
				DBInputFormat.class, ProductDBMapper.class);
		//this Mapper will actually read from the logs
		MultipleInputs.addInputPath(job, new Path(otherArgs1[1]),
				TextInputFormat.class, ProductLogMapper.class);
		//shuffle & sort setup
		job.setMapOutputKeyClass(ProductSmartKey.class);
		job.setMapOutputValueClass(ProductSmartValue.class);
		job.setPartitionerClass(ProductSmartPartitioner.class);
		job.setGroupingComparatorClass(ProductSmartComparator.class);
		//job.setCombinerClass(ProductSmartCombiner.class);
    //reduce&output setup
		job.setReducerClass(ProductSmartReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs1[3]));

    
		//configure second job
		Configuration conf2 = new Configuration();
		ControlledJob job2 = new ControlledJob(conf2);
		job2.setJobName("sub job 2");
		//will only start after the first finishes
		job2.addDependingJob(job1);

		job = job2.getJob();
		job.setJarByClass(SNMHadoopTest.class);
		job.setMapperClass(UserCatMapper.class);
		job.setMapOutputKeyClass(UserCatKey.class);
		job.setMapOutputValueClass(UserCatValue.class);
		job.setReducerClass(UserCatReducer.class);
		job.setPartitionerClass(UserCatPartitioner.class);
		job.setGroupingComparatorClass(UserCatComparator.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		//will read from the results of the first job
		FileInputFormat.setInputPaths(job, new Path(otherArgs1[3]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs1[4]));
		JobControl control = new JobControl("SNM Hadoop Test");
		control.addJob(job1);
		control.addJob(job2);
		control.run();

		Thread jobrunning = new Thread(new JobRunner(control)); 
		jobrunning.start();
		while (!control.allFinished()) {
			System.out.println("Running");
			Thread.sleep(5000);
		}
		System.out.println("done");
		control.stop();

		//cleanup
		FileSystem fs = FileSystem.get(conf1);
		fs.delete(new Path(otherArgs1[3]), true);

	}

}
