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
import org.hipi.image.FloatImage;
import org.hipi.image.HipiImageHeader;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.util.RGBArray;

public class SearchRGBValuePixelFromDicomHIB extends Configured implements Tool {

	public static class SearchRGBValuePixelFromDicomHIBMapper extends Mapper<HipiImageHeader, DicomImage, Text, FloatImage> {

		public void map(HipiImageHeader key, DicomImage value, Context context)
				throws IOException, InterruptedException {

			// Verify that image was properly decoded, is of sufficient size, and has three color channels (RGB)
			if (value.getDicomInputStream() != null) {
				
				FloatImage floatImage = value.getFloatPngImage();
				
				// Emit record to reducer
				context.write(new Text(floatImage.getHipiImageHeader().toString()), floatImage);

			} // If (value != null...

		} // map()

	} 

	public static class SearchRGBValuePixelFromDicomHIBReducer extends Reducer<Text, FloatImage, Text, Text> {

		public void reduce(Text key, Iterable<FloatImage> values, Context context)
				throws IOException, InterruptedException {

			// Emit output of job which will be written to HDFS
			for (FloatImage val : values) {
				context.write(key , new Text(RGBArray.getRGBFloatArray(val).getRGBValue(10, 10).toString()));
			}

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
		job.setJarByClass(SearchRGBValuePixelFromDicomHIB.class);
		job.setMapperClass(SearchRGBValuePixelFromDicomHIBMapper.class);
		job.setReducerClass(SearchRGBValuePixelFromDicomHIBReducer.class);
		// Set the types for the key/value pairs passed to/from map and reduce layers
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatImage.class);
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
		ToolRunner.run(new SearchRGBValuePixelFromDicomHIB(), args);
		System.exit(0);
	}

}