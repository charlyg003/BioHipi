package org.biohipi.examples;

import org.biohipi.image.BioHipiImage;
import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.NiftiImage;
import org.biohipi.imagebundle.mapreduce.BioHibInputFormat;
import org.biohipi.util.niftijio.NiftiVolume;
import org.apache.commons.io.FilenameUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 
 * VoxelNifti is an example of how to manipulate
 * NIfTI images in BioHIB.<br>
 * Particularly identifies the maximum value
 * of a sample of coordinates of each NIfTI image.
 * 
 */
public class VoxelNifti extends Configured implements Tool {

	/**
	 * Useful class for the phase map.
	 * 
	 * @see Mapper
	 */
	public static class VoxelNiftiMapper extends Mapper<BioHipiImageHeader, BioHipiImage, Text, DoubleWritable> {

		public Path path;
		public FileSystem fileSystem;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			fileSystem = FileSystem.get(context.getConfiguration());
			path = new Path(conf.get("voxelNifti.outdir"));
			fileSystem.mkdirs(path);
		}

		/**
		 * For each image has returned a set of key / value pairs,
		 * which indicate the coordinates and the value of the voxel.
		 */
		@Override
		public void map(BioHipiImageHeader header, BioHipiImage image, Context context) throws IOException, InterruptedException {

			// Check for null image (malformed HIB segment of failure to decode header)
			if (header == null || image == null) {
				System.err.println("Failed to decode image, skipping.");
				return;
			}

			if (!(image instanceof NiftiImage))
				return;

			String source = header.getMetaData("source");
			if (source == null) {
				System.err.println("Failed to locate source metadata key/value pair, skipping.");
				return;
			}

			String base = FilenameUtils.getBaseName(source);
			if (base == null) {
				System.err.println("Failed to determine base name of source metadata value, skipping.");
				return;
			}

			NiftiVolume nii = ((NiftiImage) image).getNiftiVolume();
			double[][][][] data = nii.data;

			for(int z=110; z<130; z++)
				for(int y=110; y<130; y++)
					for(int x=110; x<130; x++){
						double val = data[x][y][z][0];
						Text text = new Text(Integer.toString(x) + " " + Integer.toString(y) + " " + Integer.toString(z));
						DoubleWritable dwritable = new DoubleWritable(val);
						context.write(text,dwritable);
					}
		}
	}

	/**
	 * Useful class for the phase reduce.
	 * 
	 * @see Reducer
	 */
	public static class VoxelNiftiReduced extends Reducer<Text, DoubleWritable, Text, Double>{

		/**
		 * Analysis of the lists to obtain the maximum value of the voxel 
		 * for each coordinate x, y, z, t.
		 */
		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double maxValue = Double.MIN_VALUE;
			for(DoubleWritable currD : values){
				if(currD.get() > maxValue)
					maxValue = currD.get();
			}
			context.write(key, maxValue);
		}

	}

	private static void removeDir(String pathToDirectory, Configuration conf) throws IOException {
		Path pathToRemove = new Path(pathToDirectory);
		FileSystem fileSystem = FileSystem.get(conf);
		if (fileSystem.exists(pathToRemove)) {
			fileSystem.delete(pathToRemove, true);
		}
	}

	public int run(String[] args) throws Exception {

		// Check arguments
		if (args.length != 2) {
			System.out.println("Usage: voxelNifti.jar <input BioHIB> <output Path>");
			System.exit(0);
		}

		String inputPath = args[0];
		String outputPath = args[1];

		// Setup job configuration
		Configuration conf = new Configuration();
		conf.setStrings("voxelNifti.outdir", outputPath);
		// Clean up output directory
		removeDir(outputPath, conf);

		// Setup MapReduce classes
		Job job = Job.getInstance(conf, "voxelNifti");
		job.setJarByClass(VoxelNifti.class);
		job.setMapperClass(VoxelNiftiMapper.class);
		job.setReducerClass(VoxelNiftiReduced.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Double.class);

		job.setInputFormatClass(BioHibInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Double.class);

		job.setNumReduceTasks(10);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		BioHibInputFormat.setInputPaths(job, new Path(inputPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new VoxelNifti(), args);
		System.exit(res);
	}

}
