package org.apache.hadoop.hive.contrib.fileformat.netcdf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.InputFormatChecker;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.InvalidInputException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.NetworkTopology;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class NcFileSimpleInputFormat extends FileInputFormat<LongWritable, Text>
  implements InputFormatChecker
{
  private static final double SPLIT_SLOP = 1.1;
  private long minSplitSize = 1;
  private static final PathFilter hiddenFileFilter = new PathFilter() {
    public boolean accept(Path p) {
      String name = p.getName();
      return (!name.startsWith("_")) && (!name.startsWith("."));
    }
  };

  protected boolean isSplitable(FileSystem fs, Path filename)
  {
    return false;
  }

  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
    throws IOException
  {
    reporter.setStatus(split.toString());
    return new NcFileRecordReader(conf, split);
  }

  protected FileStatus[] listStatus(JobConf job, FileStatus file) throws IOException
  {
    Path p = file.getPath();

    List result = new ArrayList();
    List errors = new ArrayList();

    List filters = new ArrayList();
    filters.add(hiddenFileFilter);
    PathFilter jobFilter = getInputPathFilter(job);
    if (jobFilter != null) {
      filters.add(jobFilter);
    }
    PathFilter inputFilter = new MultiPathFilter(filters);
    FileSystem fs = p.getFileSystem(job);
    FileStatus[] matches = fs.globStatus(p, inputFilter);
    if (matches == null)
      errors.add(new IOException("Input path does not exist: " + p));
    else if (matches.length == 0) {
      errors.add(new IOException("Input Pattern " + p + " matches 0 files"));
    }
    else {
      for (FileStatus globStat : matches) {
        if (globStat.isDir())
          for (FileStatus stat : fs.listStatus(globStat.getPath(), inputFilter))
          {
            result.add(stat);
          }
        else {
          result.add(globStat);
        }
      }
    }
    if (!errors.isEmpty()) {
      throw new InvalidInputException(errors);
    }
    return (FileStatus[])result.toArray(new FileStatus[result.size()]);
  }

  public long getLen(JobConf job, FileStatus file)
    throws IOException
  {
    long all = 0L;
    if (file.isDir()) {
      FileStatus[] files = listStatus(job, file);
      for (FileStatus subfile : files)
        all += getLen(job, subfile);
    }
    else {
      all += file.getLen();
    }
    return all;
  }

  public ArrayList<FileSplit> getSplits(JobConf job, FileStatus dirFile, long totalSize, int numSplits)
    throws IOException
  {
    ArrayList fsArr = new ArrayList(numSplits);
    FileStatus[] files = listStatus(job, dirFile);
    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong("mapred.min.split.size", 1), 1);

    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file : files) {
      if (!file.isDir()) {
        Path path = file.getPath();
        FileSystem fs = path.getFileSystem(job);
        long length = file.getLen();
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0L, length);

        if ((length != 0L) && (isSplitable(fs, path))) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          while (bytesRemaining / splitSize > 1.1D) {
            String[] splitHosts = getSplitHosts(blkLocations, length - bytesRemaining, splitSize, clusterMap);

            fsArr.add(new FileSplit(path, length - bytesRemaining, splitSize, splitHosts));

            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0L) {
            fsArr.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, blkLocations[(blkLocations.length - 1)].getHosts()));
          }

        }
        else if (length != 0L) {
          String[] splitHosts = getSplitHosts(blkLocations, 0L, length, clusterMap);

          fsArr.add(new FileSplit(path, 0L, length, splitHosts));
        }
        else {
          fsArr.add(new FileSplit(path, 0L, length, new String[0]));
        }
      } else {
        fsArr.addAll(getSplits(job, file, totalSize, numSplits));
      }

    }

    return fsArr;
  }

  public InputSplit[] getSplits(JobConf job, int numSplits)
    throws IOException
  {
    FileStatus[] files = listStatus(job);

    long totalSize = 0L;
    for (FileStatus file : files) {
      if (file.isDir()) {
        totalSize += getLen(job, file);
      }
      totalSize += file.getLen();
    }

    long goalSize = totalSize / (numSplits == 0 ? 1 : numSplits);
    long minSize = Math.max(job.getLong("mapred.min.split.size", 1), 1);

    ArrayList splits = new ArrayList(numSplits);
    NetworkTopology clusterMap = new NetworkTopology();
    for (FileStatus file : files) {
      if (!file.isDir()) {
        Path path = file.getPath();
        FileSystem fs = path.getFileSystem(job);
        long length = file.getLen();
        BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0L, length);

        if ((length != 0L) && (isSplitable(fs, path))) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(goalSize, minSize, blockSize);

          long bytesRemaining = length;
          while (bytesRemaining / splitSize > 1.1D) {
            String[] splitHosts = getSplitHosts(blkLocations, length - bytesRemaining, splitSize, clusterMap);

            splits.add(new FileSplit(path, length - bytesRemaining, splitSize, splitHosts));

            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0L) {
            splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, blkLocations[(blkLocations.length - 1)].getHosts()));
          }

        }
        else if (length != 0L) {
          String[] splitHosts = getSplitHosts(blkLocations, 0L, length, clusterMap);

          splits.add(new FileSplit(path, 0L, length, splitHosts));
        }
        else {
          splits.add(new FileSplit(path, 0L, length, new String[0]));
        }
      } else {
        splits.addAll(getSplits(job, file, totalSize, numSplits));
      }
    }
    LOG.debug("Total # of splits: " + splits.size());
    return (InputSplit[])splits.toArray(new FileSplit[splits.size()]);
  }

  public boolean validateInput(FileSystem fs, HiveConf conf, ArrayList<FileStatus> files)
    throws IOException
  {
    if (files.size() <= 0) {
      return false;
    }
    for (int fileId = 0; fileId < files.size(); fileId++) {
      try {
        NetcdfFile ncfile = NetcdfFile.open(((FileStatus)files.get(fileId)).getPath().toString());
        ncfile.close();
      } catch (IOException e) {
        return false;
      }
    }
    return true;
  }

  private static class MultiPathFilter
    implements PathFilter
  {
    private final List<PathFilter> filters;

    public MultiPathFilter(List<PathFilter> filters)
    {
      this.filters = filters;
    }

    public boolean accept(Path path) {
      for (PathFilter filter : this.filters) {
        if (!filter.accept(path)) {
          return false;
        }
      }
      return true;
    }
  }

  static class NcFileRecordReader
    implements RecordReader<LongWritable, Text>
  {
    private final long start;
    private final long end;
    private NetcdfFile ncfile = null;
    private boolean isReaded;
    private final String filename;
    private int count;
    private final Index idx;
    private final String seprator;
    private final ArrayList<Array> varArr;
    private final ArrayList<Array> dimArr;
    int[] dimMap;
    int[] types;
    int[] shrinkedTypes;
    int validIDsCount;

    public NcFileRecordReader(Configuration job, InputSplit genericSplit)
      throws IOException
    {
      FileSplit split = (FileSplit)genericSplit;
      this.start = split.getStart();
      this.end = (this.start + split.getLength());
      this.ncfile = NetcdfFile.open(split.getPath().toString());
      this.filename = split.getPath().getName();
      this.isReaded = false;
      this.seprator = "\t";
      ArrayList notSkipIDs = ColumnProjectionUtils.getReadColumnIDs(job);

      ArrayList varList = (ArrayList)this.ncfile.getVariables();

      if (notSkipIDs.size() == 0) {
        for (int i = 0; i < varList.size(); i++) {
          notSkipIDs.add(Integer.valueOf(i));
        }
      }
      Variable mainVar = null;
      int mainVarPos = 0;
      this.varArr = new ArrayList();
      this.dimArr = new ArrayList();
      Variable var;
      for (int i = 0; i < notSkipIDs.size(); i++) {
        var = (Variable)varList.get(((Integer)notSkipIDs.get(i)).intValue());
        if ((var.getDimensions().size() != 1) || (!var.getDimension(0).getName().equals(var.getName())))
        {
          if (mainVar == null) {
            mainVar = var;
            mainVarPos = i;
          } else {
            List<Dimension> varDs = var.getDimensions();
            for (Dimension d : varDs) {
              if (d.getName().equals(mainVar.getName())) {
                mainVar = var;
                mainVarPos = i;
                break;
              }
            }
          }
        }
      }
      if (mainVar == null) {
        mainVar = (Variable)varList.get(((Integer)notSkipIDs.get(0)).intValue());
        mainVarPos = 0;
      }
      this.varArr.add(mainVar.read());
      this.types = new int[notSkipIDs.size()];
      this.dimMap = new int[mainVar.getDimensions().size()];
      this.types[mainVarPos] = 2;

      for (int i = 0; i < notSkipIDs.size(); i++) {
        if (i != mainVarPos)
        {
          var = (Variable)varList.get(((Integer)notSkipIDs.get(i)).intValue());
          boolean isDimension = false;
          int size = mainVar.getDimensions().size();
          for (int j = 0; j < size; j++) {
            if (mainVar.getDimension(j).getName().equals(var.getName())) {
              this.types[i] = 1;
              this.dimArr.add(var.read());

              this.dimMap[(this.dimArr.size() - 1)] = j;
              isDimension = true;
            }
          }
          if ((!isDimension) && 
            (var.getDimensions().size() == mainVar.getDimensions().size())) {
            boolean isSame = true;
            for (int j = 0; j < var.getDimensions().size(); j++) {
              if (!var.getDimension(j).getName().equals(mainVar.getDimension(j).getName())) {
                isSame = false;
                break;
              }
            }
            if (isSame) {
              this.varArr.add(var.read());
              this.types[i] = 2;
            }
          }
        }
      }
      ArrayList validIDs = new ArrayList();
      this.shrinkedTypes = new int[this.types.length];
      int pos = 0;
      for (int i = 0; i < this.types.length; i++) {
        if (this.types[i] != 0) {
          this.shrinkedTypes[pos] = this.types[i];
          validIDs.add(notSkipIDs.get(i));
          pos++;
        }
      }
      org.apache.hadoop.hive.contrib.serde2.NcFileSerDe.notSkipIDsFromInputFormat = validIDs;
      this.validIDsCount = validIDs.size();
      this.idx = mainVar.read().getIndex();
    }

    public synchronized boolean next(LongWritable key, Text value)
      throws IOException
    {
      if (this.count >= this.idx.getSize()) {
        this.isReaded = true;
        key = null;
        value = null;

        return false;
      }

      int[] idxCount = this.idx.getCurrentCounter();

      StringBuilder result = new StringBuilder();
      int dimPos = 0;
      int varPos = 0;
      for (int i = 0; i < this.validIDsCount; i++) {
        switch (this.shrinkedTypes[i]) {
        case 1:
          result.append(((Array)this.dimArr.get(dimPos)).getObject(idxCount[this.dimMap[dimPos]]));
          dimPos++;
          break;
        case 2:
          result.append(((Array)this.varArr.get(varPos)).getObject(this.idx));
          varPos++;
        }

        if (i != this.validIDsCount - 1) {
          result.append(this.seprator);
        }
      }

      key.set(this.count);
      value.set(result.toString());

      this.idx.incr();
      this.count += 1;
      return true;
    }

    public LongWritable createKey() {
      return new LongWritable();
    }

    public Text createValue() {
      return new Text();
    }

    public synchronized long getPos() throws IOException {
      return this.count;
    }

    public void close() throws IOException {
      if (this.ncfile != null)
        this.ncfile.close();
    }

    public float getProgress() throws IOException
    {
      return 1.0F * this.count / (float)this.idx.getSize();
    }
  }
}
