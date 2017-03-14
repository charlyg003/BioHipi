package org.biohipi.tools;

import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.image.BioHipiImageHeader.BioHipiImageFormat;
import org.biohipi.image.BioHipiImageHeader.BioHipiKeyMetaData;
import org.biohipi.image.BioHipiImage;
import org.biohipi.image.io.CodecManager;
import org.biohipi.image.io.ImageEncoder;
import org.biohipi.imagebundle.mapreduce.BioHibInputFormat;

import org.apache.commons.io.FilenameUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BioHibExport extends Configured implements Tool {

	public static String formatExport = null;

	public static class HibExportMapper extends Mapper<BioHipiImageHeader, BioHipiImage, Text, Text> {

		public Path path;
		public FileSystem fileSystem;

		@Override
		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			fileSystem = FileSystem.get(context.getConfiguration());
			path = new Path(conf.get("imgfromhib.outdir"));
			fileSystem.mkdirs(path);
		}

		/* 
		 * Write each image to the HDFS.
		 */
		@Override
		public void map(BioHipiImageHeader header, BioHipiImage image, Context context) throws IOException, InterruptedException {

			// Check for null image (malformed HIB segment of failure to decode header)
			if (header == null || image == null) {
				System.err.println("Failed to decode image, skipping.");
				return;
			}

			String source = header.getMetaData(BioHipiKeyMetaData.SOURCE);

			if (source == null) {
				System.err.println("Failed to locate source metadata key/value pair, skipping.");
				return;
			}

			String base = FilenameUtils.getBaseName(source);
			if (base == null) {
				System.err.println("Failed to determine base name of source metadata value, skipping.");
				return;
			}

			Path outpath = null;
			Boolean verified = false;
			StringBuilder sb = new StringBuilder(path + "/" + base + ".");
			BioHipiImageFormat imgFormat = header.getStorageFormat();
			
			switch (formatExport) {
			case "all":
				verified = true;
				switch (imgFormat) {
				case JPEG:
					sb.append("jpg");
					break;
				case PNG:
					sb.append("png");
					break;
				case NIFTI:
					sb.append("nii");
					break;
				case DICOM:
					sb.append("dcm");
					break;
				case UNDEFINED:
				default:
					verified = false;
					break;
				}
				break;
			case "jpg":
				if (imgFormat == BioHipiImageFormat.JPEG) {
					verified = true;
					sb.append("jpg");
				}
				break;
			case "png":
				if (imgFormat == BioHipiImageFormat.PNG) {
					verified = true;
					sb.append("png");
				}
				break;
			case "nii":
				if (imgFormat == BioHipiImageFormat.NIFTI) {
					verified = true;
					sb.append("nii");
				}
				break;
			case "dcm":
				if (imgFormat == BioHipiImageFormat.DICOM) {
					verified = true;
					sb.append("dcm");
				}
				break;
			default:
				break;
			}

			if (verified) {
				// Write image file to HDFS
				outpath = new Path(new String(sb));
				FSDataOutputStream os = fileSystem.create(outpath);
				ImageEncoder encoder = CodecManager.getEncoder(imgFormat);
				encoder.encodeImage(image, os);
				os.flush();
				os.close();

				context.write(new Text(header.getStorageFormat().toString()), new Text(base));
			}
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
		if (args.length != 3) {
			System.out.println("Usage: hibExport.jar <all | nii | dcm | jpg | png> <input HIB> <output directory>");
			System.exit(0);
		}

		formatExport = args[0];
		formatExport = formatExport.toLowerCase();
		
		if (!(formatExport.equals("all") || formatExport.equals("nii") || formatExport.equals("dcm")
				|| formatExport.equals("jpg") || formatExport.equals("png"))) {
			System.out.println(String.format("Accepted export formats [%s, %s, %s, %s, %s], you have entered [%s]",
					"all", "nii", "dcm", "jpg", "png", formatExport));
			System.exit(0);
		}

		String inputPath = args[1];
		String outputPath = args[2];

		// Setup job configuration
		Configuration conf = new Configuration();
		conf.setStrings("imgfromhib.outdir", outputPath);

		// Setup MapReduce classes
		Job job = Job.getInstance(conf, "imgfromhib");
		job.setJarByClass(BioHibExport.class);
		job.setMapperClass(HibExportMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(BioHibInputFormat.class);

		job.setNumReduceTasks(3);

		// Clean up output directory
		removeDir(outputPath, conf);

		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		BioHibInputFormat.setInputPaths(job, new Path(inputPath));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new BioHibExport(), args);
		System.exit(res);
	}

}
