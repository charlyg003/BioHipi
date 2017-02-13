package org.hipi.examples;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.HipiImageHeader.HipiKeyImageInfo;
import org.hipi.image.NiftiImage;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.util.niftijio.NiftiVolume;
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
import java.util.HashMap;
import java.util.Map;
/**
 * 
 * CutNiftiImage is an example of how to manipulate
 * NIfTI images in HIB.<br>
 * Particularly it shows how to use the function 
 * for the extraction of a part of voxels from each NIfTI image.
 * 
 */
public class CutNiftiImage extends Configured implements Tool {

	public static class CutNiftiImageMapper extends Mapper<HipiImageHeader, NiftiImage, Text, NiftiImage> {

		public void map(HipiImageHeader key, NiftiImage value, Context context)
				throws IOException, InterruptedException {

			if (value != null) {
				NiftiVolume niiVol = value.extractAPart(120, 120, 120, 0, 140, 140, 140, 1);

				Map<HipiKeyImageInfo, Object> values = new HashMap<HipiKeyImageInfo, Object>();
				values.put(HipiKeyImageInfo.X_LENGTH, niiVol.header.dim[1]);
				values.put(HipiKeyImageInfo.Y_LENGTH, niiVol.header.dim[2]);
				values.put(HipiKeyImageInfo.Z_LENGTH, niiVol.header.dim[3]);
				values.put(HipiKeyImageInfo.T_LENGTH, niiVol.header.dim[4]);
				
				HipiImageHeader imageHeader = new HipiImageHeader(HipiImageFormat.NIFTI, values, null, null);
				imageHeader.setMetaData(value.getAllMetaData());

				context.write(new Text(imageHeader.toString()), new NiftiImage(niiVol, imageHeader));
			}
		}
	} 

	public static class CutNiftiImageReducer extends Reducer<Text, NiftiImage, Text, Text> {

		public void reduce(Text key, Iterable<NiftiImage> values, Context context)
				throws IOException, InterruptedException {

			context.write(key, new Text(values.iterator().next().toString()));
		}
	} 

	public int run(String[] args) throws Exception {
		// Check input arguments
		if (args.length != 2) {
			System.out.println("Usage: cutNifti.jar <input HIB> <output directory>");
			System.exit(0);
		}

		// Initialize and configure MapReduce job
		Job job = Job.getInstance();
		// Set input format class which parses the input HIB and spawns map tasks
		job.setInputFormatClass(HibInputFormat.class);
		// Set the driver, mapper, and reducer classes which express the computation
		job.setJarByClass(CutNiftiImage.class);
		job.setMapperClass(CutNiftiImageMapper.class);
		job.setReducerClass(CutNiftiImageReducer.class);
		// Set the types for the key/value pairs passed to/from map and reduce layers
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NiftiImage.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the input and output paths on the HDFS
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Execute the MapReduce job and block until it completes
		boolean success = job.waitForCompletion(true);

		// Return success or failure
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CutNiftiImage(), args);
		System.exit(0);
	}

}