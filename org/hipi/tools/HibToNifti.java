package org.hipi.tools;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.NiftiImage;
import org.hipi.image.io.NiftiCodec;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.apache.commons.io.FilenameUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HibToNifti extends Configured implements Tool {

  public static class HibToNiftiMapper extends Mapper<HipiImageHeader, NiftiImage, BooleanWritable, Text> {

    public Path path;
    public FileSystem fileSystem;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      fileSystem = FileSystem.get(context.getConfiguration());
      path = new Path(conf.get("jpegfromhib.outdir"));
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

      Path outpath = new Path(path + "/" + base + ".nii");

      // Write image file to HDFS
      FSDataOutputStream os = fileSystem.create(outpath);
//      NiftiVolume nii = image.getNifti();
//      nii.write(os);
      
      NiftiCodec.getInstance().encodeImage(image, os);
      os.flush();
      os.close();

      // Report success to reduce task
      context.write(new BooleanWritable(true), new Text(base));
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
      System.out.println("Usage: hibToNIfti.jar <input HIB> <output directory>");
      System.exit(0);
    }

    String inputPath = args[0];
    String outputPath = args[1];

    // Setup job configuration
    Configuration conf = new Configuration();
    conf.setStrings("jpegfromhib.outdir", outputPath);

    // Setup MapReduce classes
    Job job = Job.getInstance(conf, "jpegfromhib");
    job.setJarByClass(HibToNifti.class);
    job.setMapperClass(HibToNiftiMapper.class);
    job.setReducerClass(Reducer.class);
    job.setOutputKeyClass(BooleanWritable.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(HibInputFormat.class);

    job.setNumReduceTasks(1);

    // Clean up output directory
    removeDir(outputPath, conf);

    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    HibInputFormat.setInputPaths(job, new Path(inputPath));

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new HibToNifti(), args);
    System.exit(res);
  }

}
