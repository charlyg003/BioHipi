package org.biohipi.examples;

import org.biohipi.image.RasterImage;
import org.biohipi.image.BioHipiImageHeader.BioHipiColorSpace;
import org.biohipi.image.BioHipiImageHeader.BioHipiImageFormat;
import org.biohipi.image.BioHipiImageHeader.BioHipiKeyMetaData;
import org.biohipi.image.BioHipiImage;
import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.imagebundle.mapreduce.BioHibInputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * AvgRGBImage is an example of how to manipulate
 * Raster images, such as PNG or JPEG, in BioHIB.<br>
 * Specifically, it returns the average of the primary 
 * colors (RGB) derived from the images present in the bundle.
 */
public class AvgRGBImage extends Configured implements Tool {

	/**
	 * Useful class for the phase map.
	 * 
	 * @see Mapper
	 */
	public static class AvgRGBImageMapper extends Mapper<BioHipiImageHeader, BioHipiImage, IntWritable, RasterImage> {

		/**
		 * For each image has returned a set of key / value pairs, which 
		 * indicate a standard reference value and RasterImage containing 
		 * the average of the RGB colors.
		 *
		 */
		public void map(BioHipiImageHeader key, BioHipiImage value, Context context)
				throws IOException, InterruptedException {

			if (!(value instanceof RasterImage))
				return;

			// Verify that image was properly decoded, is of sufficient size, and has three color channels (RGB)
			if (value != null && Integer.parseInt(value.getMetaData(BioHipiKeyMetaData.WIDTH)) > 1
					&& Integer.parseInt(value.getMetaData(BioHipiKeyMetaData.HEIGHT)) > 1 && Integer.parseInt(value.getMetaData(BioHipiKeyMetaData.BANDS)) == 3) {

				// Get dimensions of image
				int w = Integer.parseInt(value.getMetaData(BioHipiKeyMetaData.WIDTH));
				int h = Integer.parseInt(value.getMetaData(BioHipiKeyMetaData.HEIGHT));

				// Get pointer to image data
				float[] valData = ((RasterImage) value).getData();

				// Initialize 3 element array to hold RGB pixel average
				float[] avgData = {0,0,0};

				// Traverse image pixel data in raster-scan order and update running average
				for (int j = 0; j < h; j++) {
					for (int i = 0; i < w; i++) {
						avgData[0] += valData[(j*w+i)*3+0]; // R
						avgData[1] += valData[(j*w+i)*3+1]; // G
						avgData[2] += valData[(j*w+i)*3+2]; // B
					}
				}

				BioHipiImageHeader header = new BioHipiImageHeader(value.getStorageFormat());
				header.addMetaData(BioHipiKeyMetaData.SOURCE, new String("(Only average RGB pixels) ").concat(value.getMetaData(BioHipiKeyMetaData.SOURCE)));
				header.addMetaData(BioHipiKeyMetaData.COLOR_SPACE, value.getMetaData(BioHipiKeyMetaData.COLOR_SPACE));
				header.addMetaData(BioHipiKeyMetaData.WIDTH, String.valueOf(1));
				header.addMetaData(BioHipiKeyMetaData.HEIGHT, String.valueOf(1));
				header.addMetaData(BioHipiKeyMetaData.BANDS, value.getMetaData(BioHipiKeyMetaData.BANDS));

				// Create a RasterImage to store the average value
				RasterImage avg = new RasterImage(header, avgData);

				// Divide by number of pixels in image
				avg.scale(1.0f/(float)(w*h));

				// Emit record to reducer
				context.write(new IntWritable(1), avg);

			}

		} 

	}

	/**
	 * Useful class for the phase reduce.
	 * 
	 * @see Reducer
	 */
	public static class AvgRGBImageReducer extends Reducer<IntWritable, RasterImage, IntWritable, Text> {

		/**
		 * retrieve the image containing the color average of all Raster Image.
		 */
		public void reduce(IntWritable key, Iterable<RasterImage> values, Context context)
				throws IOException, InterruptedException {

			// Create FloatImage object to hold final result
			BioHipiImageHeader header = new BioHipiImageHeader(BioHipiImageFormat.UNDEFINED);
			header.addMetaData(BioHipiKeyMetaData.COLOR_SPACE, BioHipiColorSpace.RGB.toString());
			header.addMetaData(BioHipiKeyMetaData.WIDTH, String.valueOf(1));
			header.addMetaData(BioHipiKeyMetaData.HEIGHT, String.valueOf(1));
			header.addMetaData(BioHipiKeyMetaData.BANDS, String.valueOf(3));

			RasterImage avg = new RasterImage(header);

			// Initialize a counter and iterate over IntWritable/FloatImage records from mapper
			int total = 0;
			for (RasterImage val : values) {
				avg.add(val);
				total++;
			}

			if (total > 0) {
				// Normalize sum to obtain average
				avg.scale(1.0f / total);
				// Assemble final output as string
				float[] avgData = avg.getData();
				String result = String.format("Average pixel value: %f %f %f", avgData[0], avgData[1], avgData[2]);

				// Emit output of job which will be written to HDFS
				context.write(key, new Text(result));
			}

		} 

	}

	public int run(String[] args) throws Exception {
		// Check input arguments
		if (args.length != 2) {
			System.out.println("Usage: avgRGB.jar <input BioHIB> <output directory>");
			System.exit(0);
		}

		// Initialize and configure MapReduce job
		Job job = Job.getInstance();
		// Set input format class which parses the input BioHIB and spawns map tasks
		job.setInputFormatClass(BioHibInputFormat.class);
		// Set the driver, mapper, and reducer classes which express the computation
		job.setJarByClass(AvgRGBImage.class);
		job.setMapperClass(AvgRGBImageMapper.class);
		job.setReducerClass(AvgRGBImageReducer.class);
		// Set the types for the key/value pairs passed to/from map and reduce layers
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(RasterImage.class);
		job.setOutputKeyClass(IntWritable.class);
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
		ToolRunner.run(new AvgRGBImage(), args);
		System.exit(0);
	}

}