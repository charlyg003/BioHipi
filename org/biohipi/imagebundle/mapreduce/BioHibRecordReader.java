package org.biohipi.imagebundle.mapreduce;

import org.biohipi.image.BioHipiImage;
import org.biohipi.image.BioHipiImageHeader;
import org.biohipi.imagebundle.BioHipiImageBundle;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Main MapReduce {@link RecordReader} class for BioHIB files. Utilizes 
 * {@link org.biohipi.imagebundle.BioHipiImageBundle.BioHibReader} to read and decode
 * the individual image records (image meta data + image pixel data) stored in a BioHIB. This class
 * determines the desired image type (the second "value" parameter to the map method in the
 * Mapper class) dynamically using the {@link BioHipiImageFactory} class.
 */
public class BioHibRecordReader extends RecordReader<BioHipiImageHeader, BioHipiImage> {

  private Configuration conf;
  private BioHipiImageBundle.BioHibReader reader;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) 
  throws IOException, IllegalArgumentException {

    FileSplit bundleSplit = (FileSplit)split;
    conf = context.getConfiguration();
    
    Path path = bundleSplit.getPath();
    FileSystem fs = path.getFileSystem(conf);
    
    // Report locations of first and last byte in image segment
    System.out.println("BioHibRecordReader#initialize: Input split starts at byte offset " + bundleSplit.getStart() +
		       " and ends at byte offset " + (bundleSplit.getStart() + bundleSplit.getLength() - 1));
    
    reader = new BioHipiImageBundle.BioHibReader(fs, path, bundleSplit.getStart(), bundleSplit.getStart() + bundleSplit.getLength() - 1);
  }
  
  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public BioHipiImageHeader getCurrentKey() throws IOException, InterruptedException  {
    return reader.getCurrentKey();
  }

  @Override
  public BioHipiImage getCurrentValue() throws IOException, InterruptedException  {
    return reader.getCurrentValue();
  }
  
  @Override
  public float getProgress() throws IOException  {
    return reader.getProgress();
  }
  
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException  {
    return reader.nextKeyValue();
  }
}
