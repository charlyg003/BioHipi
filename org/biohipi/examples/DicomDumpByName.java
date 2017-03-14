package org.biohipi.examples;

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
import org.biohipi.image.DicomImage;
import org.biohipi.image.BioHipiImage;
import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.BioHipiImageHeader.BioHipiKeyMetaData;
import org.biohipi.imagebundle.mapreduce.BioHibInputFormat;

/**
 * DicomDumpByName is an example of how to manipulate
 * DICOM images in BioHIB.<br>
 * Specifically, it returns a string representation
 * of each DICOM image, owned by a specific patient,
 * such as a list of tags.
 */
public class DicomDumpByName extends Configured implements Tool {

	public static String name = null;
	
	/**
	 * Useful class for the phase map.
	 * 
	 * @see Mapper
	 */
	public static class DicomDumpMapper extends Mapper<BioHipiImageHeader, BioHipiImage, Text, DicomImage> {

		/**
		 * For each image has returned a set of key / value pairs, which respectively 
		 * indicate the BioHipiImageHeader metadata and DicomImage of the patient in question.
		 */
		public void map(BioHipiImageHeader key, BioHipiImage value, Context context)
				throws IOException, InterruptedException {

			if (!(value instanceof DicomImage))
				return;

			if ( (value != null) && ((key.getMetaData(BioHipiKeyMetaData.PATIENT_NAME)).toUpperCase().contains(name.toUpperCase())) )
				context.write(new Text(key.toString()), (DicomImage) value);
		}
	} 

	/**
	 * Useful class for the phase reduce.
	 * 
	 * @see Reducer
	 */
	public static class DicomDumpReducer extends Reducer<Text, DicomImage, Text, Text> {

		/**
		 * Obtaining the information contained in the header of all the 
		 * DICOM images of the patient in question.
		 */
		public void reduce(Text key, Iterable<DicomImage> values, Context context)
				throws IOException, InterruptedException {

			for (DicomImage val : values)
				context.write(new Text(key) , new Text(val.toString()));
		}
	} 

	public int run(String[] args) throws Exception {
		// Check input arguments
		if (args.length != 3) {
			System.out.println("Usage: dicomDump.jar <patient name> <input BioHIB> <output directory>");
			System.exit(0);
		}

		name = args[0];

		// Initialize and configure MapReduce job
		Job job = Job.getInstance();
		
		// Set input format class which parses the input BioHIB and spawns map tasks
		job.setInputFormatClass(BioHibInputFormat.class);
		
		// Set the driver, mapper, and reducer classes which express the computation
		job.setJarByClass(DicomDumpByName.class);
		job.setMapperClass(DicomDumpMapper.class);
		job.setReducerClass(DicomDumpReducer.class);
		
		// Set the types for the key/value pairs passed to/from map and reduce layers
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DicomImage.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(3);

		// Set the input and output paths on the HDFS
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		// Execute the MapReduce job and block until it complets
		boolean success = job.waitForCompletion(true);

		// Return success or failure
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DicomDumpByName(), args);
		System.exit(0);
	}

}