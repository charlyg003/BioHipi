package org.hipi.examples;

import org.hipi.image.DicomImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TakeIndividualPatientDicomOutOfHIB extends Configured implements Tool {

	public static class TakeIndividualPatientDicomOutOfHIBMapper extends Mapper<HipiImageHeader, DicomImage, Text, DicomImage> {

		public void map(HipiImageHeader key, DicomImage value, Context context)
				throws IOException, InterruptedException {

			// Verify that image was properly decoded, is of sufficient size, and has three color channels (RGB)
			if (value.getDicomInputStream() != null) {

				// Emit record to reducer
				if ( ((String)key.getValue(HipiImageHeader.DICOM_INDEX_PATIENT_NAME)).contains("MOUGE") )
					context.write(new Text(key.toString()), value);
				

			} // If (value != null...

		} // map()

	} 

	public static class TakeIndividualPatientDicomOutOfHIBReducer extends Reducer<Text, DicomImage, Text, Text> {

		public void reduce(Text key, Iterable<DicomImage> values, Context context)
				throws IOException, InterruptedException {

			// Emit output of job which will be written to HDFS
			for (DicomImage val : values)
				context.write(new Text(key) , new Text(val.toString()));

		} // reduce()

	} 

	public int run(String[] args) throws Exception {
		// Check input arguments
		if (args.length != 2) {
			System.out.println("Usage: helloWorld <input HIB> <output directory>");
			System.exit(0);
		}

		// Initialize and configure MapReduce job
		Job job = Job.getInstance();
		// Set input format class which parses the input HIB and spawns map tasks
		job.setInputFormatClass(HibInputFormat.class);
		// Set the driver, mapper, and reducer classes which express the computation
		job.setJarByClass(TakeIndividualPatientDicomOutOfHIB.class);
		job.setMapperClass(TakeIndividualPatientDicomOutOfHIBMapper.class);
		job.setReducerClass(TakeIndividualPatientDicomOutOfHIBReducer.class);
		// Set the types for the key/value pairs passed to/from map and reduce layers
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DicomImage.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the input and output paths on the HDFS
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Execute the MapReduce job and block until it complets
		boolean success = job.waitForCompletion(true);

		// Return success or failure
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new TakeIndividualPatientDicomOutOfHIB(), args);
		System.exit(0);
	}

}