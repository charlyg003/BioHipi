package org.biohipi.examples;

import org.biohipi.image.BioHipiImage;
import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.BioHipiImageHeader.BioHipiImageFormat;
import org.biohipi.image.BioHipiImageHeader.BioHipiKeyMetaData;
import org.biohipi.image.NiftiImage;
import org.biohipi.imagebundle.mapreduce.BioHibInputFormat;
import org.biohipi.util.niftijio.NiftiVolume;
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
/**
 * CutNiftiImage is an example of how to manipulate
 * NIfTI images in BioHIB.<br>
 * Particularly it shows how to use the function 
 * for the extraction of a part of voxels from each NIfTI image.
 */
public class CutNiftiImage extends Configured implements Tool {

	static Integer xLength, yLength, zLength, tLength = null;
	static Integer xStart, yStart, zStart, tStart = null;
	
	/**
	 * Useful class for the phase map.
	 * 
	 * @see Mapper
	 */
	public static class CutNiftiImageMapper extends Mapper<BioHipiImageHeader, BioHipiImage, Text, NiftiImage> {

		/**
		 * For each image has returned a set of key / value pairs, 
		 * which respectively indicate the BioHipiImageHeader metadata 
		 * and NiftiImage containing the specific area.
		 */
		public void map(BioHipiImageHeader key, BioHipiImage value, Context context)
				throws IOException, InterruptedException {
			
			if (!(value instanceof NiftiImage))
				return;

			if (value != null) {
				NiftiVolume niiVol = ((NiftiImage) value).cut(xStart, yStart, zStart, tStart, xLength, yLength, zLength, tLength);

				if (niiVol == null)
					return;
					
				BioHipiImageHeader imageHeader = new BioHipiImageHeader(BioHipiImageFormat.NIFTI);
				
				imageHeader.addMetaData(BioHipiKeyMetaData.SOURCE, new String("(Cut) ").concat(value.getMetaData(BioHipiKeyMetaData.SOURCE)));
				imageHeader.addMetaData("Cut X-Axis", String.format("from %d to %d", xStart, xStart+xLength-1));
				imageHeader.addMetaData("Cut Y-Axis", String.format("from %d to %d", yStart, yStart+yLength-1));
				imageHeader.addMetaData("Cut Z-Axis", String.format("from %d to %d", zStart, zStart+zLength-1));
				imageHeader.addMetaData("Cut T-Axis", String.format("from %d to %d", tStart, tStart+tLength-1));
				imageHeader.addMetaData(BioHipiKeyMetaData.X_LENGTH, String.valueOf(niiVol.header.dim[1]));
				imageHeader.addMetaData(BioHipiKeyMetaData.Y_LENGTH, String.valueOf(niiVol.header.dim[2]));
				imageHeader.addMetaData(BioHipiKeyMetaData.Z_LENGTH, String.valueOf(niiVol.header.dim[3]));
				imageHeader.addMetaData(BioHipiKeyMetaData.T_LENGTH, String.valueOf(niiVol.header.dim[4]));

				Text resultKey = new Text(imageHeader.toString());
				NiftiImage resultValue = new NiftiImage(niiVol, imageHeader);
				
				
				context.write(resultKey, resultValue);
			}
		}
	}

	/**
	 * Useful class for the phase reduce.
	 * 
	 * @see Reducer
	 */
	public static class CutNiftiImageReducer extends Reducer<Text, NiftiImage, Text, Text> {

		/**
		 * Generating a list of the values of all voxels that characterize the modified image.
		 */
		public void reduce(Text key, Iterable<NiftiImage> values, Context context)
				throws IOException, InterruptedException {

			context.write(key, new Text(values.iterator().next().toString()));
		}
	} 

	public int run(String[] args) throws Exception {
		// Check input arguments
		if (args.length != 10) {
			System.out.println("Usage: cutNifti.jar <xStart> <yStart> <zStart> <tStart> <xLength> <yLength> <zLength> <tLength> <input BioHIB> <output directory>");
			System.exit(0);
		}
		
		xStart = Integer.parseInt(args[0]);		xLength = Integer.parseInt(args[4]);
		yStart = Integer.parseInt(args[1]);		yLength = Integer.parseInt(args[5]);
		zStart = Integer.parseInt(args[2]);		zLength = Integer.parseInt(args[6]);
		tStart = Integer.parseInt(args[3]);		tLength = Integer.parseInt(args[7]);
		

		// Initialize and configure MapReduce job
		Job job = Job.getInstance();
		// Set input format class which parses the input BioHIB and spawns map tasks
		job.setInputFormatClass(BioHibInputFormat.class);
		// Set the driver, mapper, and reducer classes which express the computation
		job.setJarByClass(CutNiftiImage.class);
		job.setMapperClass(CutNiftiImageMapper.class);
		job.setReducerClass(CutNiftiImageReducer.class);
		// Set the types for the key/value pairs passed to/from map and reduce layers
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NiftiImage.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(3);

		// Set the input and output paths on the HDFS
		FileInputFormat.setInputPaths(job, new Path(args[8]));
		FileOutputFormat.setOutputPath(job, new Path(args[9]));

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