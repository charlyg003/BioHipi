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

import ij.ImageStack;
import ij.util.DicomTools;

public class VoxelDicomStack extends Configured implements Tool {
	ImageStack dicomStack = new ImageStack(192, 256);

	public static class VoxelMapper extends Mapper<HipiImageHeader, DicomImage, Text, DicomImage> {

		public void map(HipiImageHeader key, DicomImage value, Context context)
				throws IOException, InterruptedException {

			if (value.getDicomInputStream() != null)
				context.write(new Text((String) key.getValue(HipiImageHeader.DICOM_INDEX_PATIENT_NAME)), value);
		}
	} 

	public static class VoxelReducer extends Reducer<Text, DicomImage, Text, Text> {

		public void reduce(Text key, Iterable<DicomImage> values, Context context)
				throws IOException, InterruptedException {
			
			ImageStack dicomStack = null;
			
			for (DicomImage val : values) {
				if (dicomStack == null)
					dicomStack = new ImageStack(val.getDICOM().getWidth(), val.getDICOM().getHeight());
				dicomStack.addSlice(val.getDICOM().getProcessor());
			}
			
			DicomTools.sort(dicomStack);
			
			for (int x = 0; x < dicomStack.getWidth(); x++) {
				for (int y = 0; y < dicomStack.getHeight(); y++) {
					for (int z = 0; z < dicomStack.getSize(); z++) {
						context.write(new Text(String.format("voxel[%d][%d][%d] -> ", x, y, z)) , new Text(String.valueOf(dicomStack.getVoxel(x, y, z))));						
					}
				}
			}
		}
	} 

	public int run(String[] args) throws Exception {
		// Check input arguments
		if (args.length != 2) {
			System.out.println("Usage: nameFile.jar <input HIB> <output directory>");
			System.exit(0);
		}

		// Initialize and configure MapReduce job
		Job job = Job.getInstance();
		// Set input format class which parses the input HIB and spawns map tasks
		job.setInputFormatClass(HibInputFormat.class);
		// Set the driver, mapper, and reducer classes which express the computation
		job.setJarByClass(VoxelDicomStack.class);
		job.setMapperClass(VoxelMapper.class);
		job.setReducerClass(VoxelReducer.class);
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
		ToolRunner.run(new VoxelDicomStack(), args);
		System.exit(0);
	}

}