package org.hipi.test;

import org.hipi.image.HipiImageHeader;
import org.hipi.image.HipiImageHeader.HipiImageFormat;
import org.hipi.image.NiftiImage;
import org.hipi.imagebundle.mapreduce.HibInputFormat;
import org.hipi.niftijio.NiftiVolume;
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

public class TestNiftiWriter extends Configured implements Tool {
    
    public static class TestNiftiWriterMapper extends Mapper<HipiImageHeader, NiftiImage, IntWritable, NiftiImage> {
        
        public void map(HipiImageHeader key, NiftiImage value, Context context)
        throws IOException, InterruptedException {
            
            // Verify that image was properly decoded, is of sufficient size, and has three color channels (RGB)
            if (value != null && value.getNiftiVolume() != null) {
                
                NiftiVolume niiVol = NiftiImage.extractAPart(value.getNiftiVolume(), 120, 120, 120, 0, 140, 140, 140, 0);
                HipiImageHeader imageHeader = new HipiImageHeader(HipiImageFormat.NIFTI, niiVol.header.dim[1], niiVol.header.dim[2], niiVol.header.dim[3], niiVol.header.dim[4], null, null);
                imageHeader.setMetaData(value.getAllMetaData());
                
                // Emit record to reducer
                context.write(new IntWritable(1), new NiftiImage(niiVol, imageHeader));
                
            } // If (value != null...
            
        } // map()
        
    } 
    
    public static class TestNiftiWriterReducer extends Reducer<IntWritable, NiftiImage, IntWritable, Text> {
        
        public void reduce(IntWritable key, NiftiImage value, Context context)
        throws IOException, InterruptedException {

        	
                context.write(key, new Text(value.toString()));
            
        } // reduce()
        
    } 
    
    public int run(String[] args) throws Exception {
        // Check input arguments
        if (args.length != 2) {
            System.out.println("Usage: helloWorld <input HIB> <output directory>");
            System.exit(0);
        }
        
        // Initialize and configure MapReduce job
        Job job = Job.getInstance();
        // Set input format class which parses the input HIB and spawns map tasks
        job.setInputFormatClass(HibInputFormat.class);
        // Set the driver, mapper, and reducer classes which express the computation
        job.setJarByClass(TestNiftiWriter.class);
        job.setMapperClass(TestNiftiWriterMapper.class);
        job.setReducerClass(TestNiftiWriterReducer.class);
        // Set the types for the key/value pairs passed to/from map and reduce layers
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NiftiImage.class);
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
        ToolRunner.run(new TestNiftiWriter(), args);
        System.exit(0);
    }
    
}