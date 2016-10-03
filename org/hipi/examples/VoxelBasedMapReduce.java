package org.hipi.examples;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.NiftiImage;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.niftijio.NiftiVolume;
import org.hipi.util.MultipleLinearRegression;
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

public class VoxelBasedMapReduce extends Configured implements Tool {

	public static class VoxelMapper extends Mapper<HipiImageHeader, NiftiImage, Text, DoubleWritable> {

		public Path path;
		public FileSystem fileSystem;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			fileSystem = FileSystem.get(context.getConfiguration());
			path = new Path(conf.get("mapReduceExample.outdir"));
			fileSystem.mkdirs(path);
		}

		/* 
		 * Write each image (represented as an encoded byte array) to the
		 * HDFS using the hash of the byte array to generate a unique
		 * filename.
		 */
		@Override
		public void map(HipiImageHeader header, NiftiImage image, Context context) throws IOException, InterruptedException {

			// Check for null image (malformed HIB segment of failure to decode header)
			if (header == null || image == null) {
				System.err.println("Failed to decode image, skipping.");
				return;
			}

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

			NiftiVolume nii = image.getNiftiVolume();
			double[][][][] data = nii.data;

			for(int i=120-10; i<120+10; i++){
				for(int j=120-10; j<120+10; j++){
					for(int k=120-10; k<120+10; k++){
						double val = data[i][j][k][0];
						Text text = new Text(Integer.toString(i) + " " + Integer.toString(j) + " " + Integer.toString(k));
						DoubleWritable dwritable = new DoubleWritable(val);
						context.write(text,dwritable);
					}
				}
			}
		}
	}

	public static class VoxelReduced extends Reducer<Text, DoubleWritable, Text, Double>{

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double maxValue = Double.MIN_VALUE;
			int size = 0;
			for(DoubleWritable currD : values){
				currD.get();
				size++;
			}

			double[] y = new double[size];
			double[][] x = new double[size][2];

			size = 0;
			for(DoubleWritable currD : values){
				y[size] = currD.get();
				size++;
			}
			@SuppressWarnings("unused")
			MultipleLinearRegression mlr = new MultipleLinearRegression(x, y);
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
			System.out.println("Usage: MapReduceExample.jar <input HIB> <output Path>");
			System.exit(0);
		}

		String inputPath = args[0];
		String outputPath = args[1];

		// Setup job configuration
		Configuration conf = new Configuration();
		conf.setStrings("mapReduceExample.outdir", outputPath);
		// Clean up output directory
		removeDir(outputPath, conf);

		// Setup MapReduce classes
		Job job = Job.getInstance(conf, "mapReduceExample");
		job.setJarByClass(VoxelBasedMapReduce.class);
		job.setMapperClass(VoxelMapper.class);
		job.setReducerClass(VoxelReduced.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Double.class);

		job.setInputFormatClass(HibInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Double.class);

		job.setNumReduceTasks(10);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		HibInputFormat.setInputPaths(job, new Path(inputPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new VoxelBasedMapReduce(), args);
		System.exit(res);
	}

}
