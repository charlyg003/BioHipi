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

import ij.ImageStack;
import ij.util.DicomTools;

/**
 * 
 * VoxelDicomStack is an example of how to manipulate
 * DICOM images in BioHIB.<br>
 * Specifically, it returns voxels of the 3D DICOM images.<br>
 * These are formed by a sequence of 2D DICOM images
 * that are linked together.  
 * 
 */
public class Voxel3dDicom extends Configured implements Tool {
	ImageStack dicomStack = new ImageStack(192, 256);

	/**
	 * Useful class for the phase map.
	 * 
	 * @see Mapper
	 */
	public static class VoxelMapper extends Mapper<BioHipiImageHeader, BioHipiImage, Text, DicomImage> {

		/**
		 * For each image has returned a set of key / value pairs, 
		 * which indicate the patient's name and DicomImage with 
		 * the data to be analyzed.
		 */
		public void map(BioHipiImageHeader key, BioHipiImage value, Context context)
				throws IOException, InterruptedException {
			
			if (!(value instanceof DicomImage))
				return;
			
			if (value != null)
				context.write(new Text(key.getMetaData(BioHipiKeyMetaData.PATIENT_NAME)), (DicomImage) value);
		}
	} 

	/**
	 * Useful class for the phase reduce.
	 * 
	 * @see Reducer
	 */
	public static class VoxelReducer extends Reducer<Text, DicomImage, Text, Text> {

		/**
		 * Generation of 3D DICOM images and obtaining of voxels.
		 */
		public void reduce(Text key, Iterable<DicomImage> values, Context context)
				throws IOException, InterruptedException {

			ImageStack dicomStack = null;

			for (DicomImage val : values) {
				if (dicomStack == null)
					dicomStack = new ImageStack(val.getDICOM().getWidth(), val.getDICOM().getHeight());
				dicomStack.addSlice(val.getDICOM().getProcessor());
			}

			DicomTools.sort(dicomStack);

			for (int x = 0; x < dicomStack.getWidth(); x++)
				for (int y = 0; y < dicomStack.getHeight(); y++)
					for (int z = 0; z < dicomStack.getSize(); z++) {
						Text textKey = new Text(String.format("voxel[%d][%d][%d] -> ", x, y, z));
						Text textVal = new Text(String.valueOf(dicomStack.getVoxel(x, y, z)));
						
						context.write(textKey, textVal);
					}
		}
	} 

	public int run(String[] args) throws Exception {
		// Check input arguments
		if (args.length != 2) {
			System.out.println("Usage: voxelDicom.jar <input BioHIB> <output directory>");
			System.exit(0);
		}

		// Initialize and configure MapReduce job
		Job job = Job.getInstance();

		// Set input format class which parses the input BioHIB and spawns map tasks
		job.setInputFormatClass(BioHibInputFormat.class);

		// Set the driver, mapper, and reducer classes which express the computation
		job.setJarByClass(Voxel3dDicom.class);
		job.setMapperClass(VoxelMapper.class);
		job.setReducerClass(VoxelReducer.class);

		// Set the types for the key/value pairs passed to/from map and reduce layers
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DicomImage.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(3);

		// Set the input and output paths on the HDFS
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// Execute the MapReduce job and block until it completes
		boolean success = job.waitForCompletion(true);

		// Return success or failure
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Voxel3dDicom(), args);
		System.exit(0);
	}

}