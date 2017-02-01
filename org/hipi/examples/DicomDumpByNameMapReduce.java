package org.hipi.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.hipi.image.DicomImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.imagebundle.mapreduce.HibInputFormat;

public class DicomDumpByNameMapReduce extends Configured implements Tool {
	
	static String name = null;
	
	public static class DicomDumpMapper extends Mapper<HipiImageHeader, DicomImage, Text, DicomImage> {

		public void map(HipiImageHeader key, DicomImage value, Context context)
				throws IOException, InterruptedException {

			if (value.getDicomInputStream() != null) {

				if ( ((String)key.getValue(HipiImageHeader.DICOM_INDEX_PATIENT_NAME)).contains(name.toUpperCase()) 
						|| ((String)key.getValue(HipiImageHeader.DICOM_INDEX_PATIENT_NAME)).contains(name.toLowerCase()) 
						|| ((String)key.getValue(HipiImageHeader.DICOM_INDEX_PATIENT_NAME)).contains(name))
					context.write(new Text(key.toString()), value);
			} 
		}
	} 

	public static class DicomDumpReducer extends Reducer<Text, DicomImage, Text, Text> {

		public void reduce(Text key, Iterable<DicomImage> values, Context context)
				throws IOException, InterruptedException {

			for (DicomImage val : values) {
				context.write(new Text(key) , new Text(val.toString()));
			}

		}

	} 

	public int run(String[] args) throws Exception {
		// Check input arguments
		if (args.length != 3) {
			System.out.println("Usage: nameFile.jar name <input HIB> <output directory>");
			System.exit(0);
		}
		
		name = args[0];
		
		// Initialize and configure MapReduce job
		Job job = Job.getInstance();
		// Set input format class which parses the input HIB and spawns map tasks
		job.setInputFormatClass(HibInputFormat.class);
		// Set the driver, mapper, and reducer classes which express the computation
		job.setJarByClass(DicomDumpByNameMapReduce.class);
		job.setMapperClass(DicomDumpMapper.class);
		job.setReducerClass(DicomDumpReducer.class);
		// Set the types for the key/value pairs passed to/from map and reduce layers
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DicomImage.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the input and output paths on the HDFS
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// Execute the MapReduce job and block until it complets
		boolean success = job.waitForCompletion(true);

		// Return success or failure
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DicomDumpByNameMapReduce(), args);
		System.exit(0);
	}

}