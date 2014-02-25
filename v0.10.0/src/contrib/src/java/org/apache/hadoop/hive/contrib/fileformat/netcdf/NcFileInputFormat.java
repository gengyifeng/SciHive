package org.apache.hadoop.hive.contrib.fileformat.netcdf;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
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
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Section;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

public class NcFileInputFormat2 extends FileInputFormat<LongWritable, Text>
  implements InputFormatChecker
{
  private static final double SPLIT_SLOP = 1.1;
  private  long minSplitSize = 1;
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
          while (bytesRemaining / splitSize > 1.1) {
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
  static class DRange{
  	public int start;
  	public int length;
  	public DRange(){
  		start=0;
  		length=0;
  	}
  	public DRange(int i){
  		start=i;
  		length=1;
  	}
  }
  static class NcFileRecordReader
    implements RecordReader<LongWritable, Text>
  {
    private final long start;
    private final long end;
    private NetcdfFile ncfile = null;
    private final String filename;
    private int count=0;
    private int sectionCount=0;
    private Index idx = null;
    private final String seprator;
    private ArrayList<Array> varArr;
    private ArrayList<Variable> vars;
    private ArrayList<Array> dimArr;
    int[] dimMap;
    int[] types;
    int[] shrinkedTypes;
    int validIDsCount;
    int[] validShape;
    int[] validShapeDivider;
    int totalSize=0;
    int sectionNum=0;
    int dimsSize=0;
    boolean hasSection=false;
    boolean lastOne=false;
    int []curSectionStart=null;
    int []curSectionCount=null;
    int []curSectionDshape=null;
    int innerSectionSize=0;
    int innerSectionIndex=0;
    public HashMap<String,Double> equalValueMap= new HashMap();
    public HashMap<String, Double> greaterValueMap = new HashMap();
    public HashMap<String, Boolean> greaterBoolMap = new HashMap();
    public HashMap<String, Double> lessValueMap = new HashMap();
    public HashMap<String, Boolean> lessBoolMap = new HashMap();
    public HashMap<String, ArrayList<Double>> notEqualMap = new HashMap();
    public HashSet<String> fixs = new HashSet();
    public HashSet<String> cols = new HashSet();
    public HashMap<String,ArrayList<DRange>> dRangeIndexes=new HashMap();
    public ArrayList<ArrayList<DRange>>dRangeIndexArr=new ArrayList();
    boolean noResult=false;
    boolean notEqual(Double input,Double num){
        return !input.equals(num)?true:false;
    }
    boolean greaterEqual(Double input,Double num){
        return input>=num?true:false;
    }
    boolean greater(Double input,Double num){
        return input>num?true:false;
    }
    boolean lessEqual(Double input,Double num){
        return input<=num?true:false;
    }
    boolean less(Double input,Double num){
        return input<num?true:false;
    }
    boolean eval_value(Double val,String col,Double gValue,Double lValue,ArrayList<Double> nValues){
        if(gValue!=null){
            if(greaterBoolMap.get(col)==true){
                if(!greaterEqual(val,gValue)){
                    return false;
                }
            }else{
                if(!greater(val,gValue)){
                    return false;
                }
            }
        }
        if(lValue!=null){
            if(lessBoolMap.get(col)==true){
                if(!lessEqual(val,lValue)){
                    return false;
                }
            }else{
                if(!less(val,lValue)){
                    return false;
                }
            }
        }
        if(nValues!=null){
            for(Double d:nValues){
                if(!notEqual(val,d)){
                    return false;
                }
            }
        }
        return true;
    }
   
    void evalDimensionVariable(Variable v,String col) throws IOException{
        if(v.getDimensions().size()>1)
            return;
        Array arr=v.read();
        long size=arr.getSize();
        ArrayList<Integer> validIndex=new ArrayList<Integer>();
        Double gValue=greaterValueMap.get(col);
        Double lValue=lessValueMap.get(col);
        ArrayList<Double> nValues=notEqualMap.get(col);
        Double val=null;
        boolean hasFirst=false;
        ArrayList<DRange> dRangeIndex=new ArrayList<DRange>();
        DRange dr=null;
        for(int i=0;i<size;i++){
            val=new Double(arr.getObject(i).toString());
            if(!eval_value(val,col,gValue,lValue,nValues)){
                continue;
            }
            if(!hasFirst){
                dr=new DRange(i);
            	hasFirst=true;
            }else{
            	if(i==dr.start+dr.length){
            		dr.length++;
            	}else{
            		dRangeIndex.add(dr);
            		dr=new DRange(i);
            	}
            }
            validIndex.add(i);    
        }
        if(dr!=null){
        	dRangeIndex.add(dr);
        }
        if(validIndex.size()==0)
            noResult=true;
        dRangeIndexes.put(col,dRangeIndex);
    }
    void updateGreaterValueMap(String col,Double value,Boolean equal){
        Double old=greaterValueMap.get(col);
        if(old==null){
            this.greaterValueMap.put(col, value);
            this.greaterBoolMap.put(col, equal);
        }else{
            if(value>old){
                this.greaterValueMap.put(col, value);
                this.greaterBoolMap.put(col, equal);
            }else if(value.equals(old)){
                if(!equal)
                    this.greaterBoolMap.put(col,equal);
            }
        }
    }
    void updateLessValueMap(String col,Double value,Boolean equal){
        Double old=lessValueMap.get(col);
        if(old==null){
            this.lessValueMap.put(col, value);
            this.lessBoolMap.put(col, equal);
        }else{
            if(value<old){
                this.lessValueMap.put(col, value);
                this.lessBoolMap.put(col, equal);
            }else if(value.equals(old)){
                if(!equal)
                    this.lessBoolMap.put(col,equal);
            }
        }
    }
    public void updateConditions(String col, Double value, GenericUDFBaseCompare op, boolean rightOrder) {
      if ((op instanceof GenericUDFOPEqual)) {
        updateGreaterValueMap(col,value,Boolean.valueOf(true));
        updateLessValueMap(col,value,Boolean.valueOf(true));
        if(!equalValueMap.containsKey(col)){
            equalValueMap.put(col, value);
        }else{
            if (value.equals(equalValueMap.get(col))){
                this.noResult=true;
                return;
            }
        }
        this.fixs.add(col);
        this.cols.add(col);
      } else {
        if((op instanceof GenericUDFOPNotEqual)){
          if(notEqualMap.get(col)==null){
            ArrayList<Double> list=new ArrayList<Double>();
            list.add(value);
            notEqualMap.put(col,list);
          }else{
            notEqualMap.get(col).add(value);
          }
          this.cols.add(col); 
          return; 
        }
        if (this.fixs.contains(col)) {
          return;
        }
        this.cols.add(col);
        if (rightOrder) {
          if ((op instanceof GenericUDFOPEqualOrGreaterThan)) {
            updateGreaterValueMap(col,value,Boolean.valueOf(true));
          } else if ((op instanceof GenericUDFOPGreaterThan)) {
            updateGreaterValueMap(col,value,Boolean.valueOf(false));
          } else if ((op instanceof GenericUDFOPEqualOrLessThan)) {
            updateLessValueMap(col,value,Boolean.valueOf(true));
          } else if ((op instanceof GenericUDFOPLessThan)) {
            updateLessValueMap(col,value,Boolean.valueOf(false));
          }
        }
        else if ((op instanceof GenericUDFOPEqualOrGreaterThan)) {
            updateLessValueMap(col,value,Boolean.valueOf(true));
        } else if ((op instanceof GenericUDFOPGreaterThan)) {
            updateLessValueMap(col,value,Boolean.valueOf(false));
        } else if ((op instanceof GenericUDFOPEqualOrLessThan)) {
            updateGreaterValueMap(col,value,Boolean.valueOf(true));
        } else if ((op instanceof GenericUDFOPLessThan)) {
            updateGreaterValueMap(col,value,Boolean.valueOf(false));
        }
      }
    }

    public void parseExprNodeDesc(ExprNodeDesc node)
    {
      GenericUDF udf = ((ExprNodeGenericFuncDesc)node).getGenericUDF();
      if ((udf instanceof GenericUDFOPOr)) {
        return;
      }
      if ((udf instanceof GenericUDFOPAnd)) {
        for (ExprNodeDesc ch : node.getChildren()) {
          parseExprNodeDesc(ch);
        }
      }
      else if (((udf instanceof GenericUDFBaseCompare)) && 
        (!(udf instanceof GenericUDFOPNotEqual))) {
        String col = null; String value = null;
        boolean order = false;
        for (int i = 0; i < node.getChildren().size(); i++) {
          ExprNodeDesc ch2 = (ExprNodeDesc)node.getChildren().get(i);

          if ((ch2 instanceof ExprNodeColumnDesc)) {
            col = ch2.getExprString();
            if (i == 0) {
              order = true;
            }
          }
          if (((ch2 instanceof ExprNodeGenericFuncDesc)) && 
            ((((ExprNodeGenericFuncDesc)ch2).getGenericUDF() instanceof GenericUDFBridge))) {
            value = new StringBuilder().append("-").append(((ExprNodeDesc)ch2.getChildren().get(0)).getExprString()).toString();
          }

          if ((ch2 instanceof ExprNodeConstantDesc)) {
            value = ch2.getExprString();
          }
        }
        if ((col != null) && (value != null))
          try {
            updateConditions(col, new Double(value), (GenericUDFBaseCompare)udf, order);
          } catch (NumberFormatException e) {
            System.err.println(new StringBuilder().append("NumberForamtException in updateRange:").append(node.getExprString()).toString());
          }
      }
    }

    public NcFileRecordReader(Configuration job, InputSplit genericSplit)
      throws IOException
    {
      FileSplit split = (FileSplit)genericSplit;
      this.start = split.getStart();
      this.end = (this.start + split.getLength());
      this.ncfile = NetcdfFile.open(split.getPath().toString());
      this.filename = split.getPath().getName();
      this.seprator = "\t";
      ArrayList notSkipIDs = ColumnProjectionUtils.getReadColumnIDs(job);
      String jobID=Utilities.getHiveJobID(job);
      LinkedHashMap<String, ArrayList<ExprNodeDesc>> pathToNodeDesc = Utilities.getPathToNodeDesc(jobID);
      String pathString;
      if (pathToNodeDesc != null) {
        pathString = split.getPath().toString();
        for (String key : pathToNodeDesc.keySet())
        {
          if (pathString.startsWith(key)) {
            for (ExprNodeDesc node : (ArrayList<ExprNodeDesc>)pathToNodeDesc.get(key))
            {
              parseExprNodeDesc(node);
            }
            break;
          }
        }
      }
      ArrayList varList = (ArrayList)this.ncfile.getVariables();

      if (notSkipIDs.size() == 0) {
        for (int i = 0; i < varList.size(); i++) {
          notSkipIDs.add(Integer.valueOf(i));
        }
      }
      Variable mainVar = null;
      int mainVarPos = 0;
      this.varArr = new ArrayList();
      this.vars = new ArrayList();
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
      this.vars.add(mainVar);

      this.types = new int[notSkipIDs.size()];
      this.dimMap = new int[mainVar.getDimensions().size()];
      this.types[mainVarPos] = 2;

      for (int i = 0; i < notSkipIDs.size(); i++)
        if (i != mainVarPos)
        {
          var = (Variable)varList.get(((Integer)notSkipIDs.get(i)).intValue());
          boolean isDimension = false;
          int size = mainVar.getDimensions().size();
          for (int j = 0; j < size; j++) {
            if (mainVar.getDimension(j).getName().equals(var.getName())) {
              this.types[i] = 1;//Dimension
              this.dimArr.add(var.read());
              
              this.dimMap[(this.dimArr.size() - 1)] = j;
              isDimension = true;
            }
          }
          if (!isDimension)
          {
            if (var.getDimensions().size() == mainVar.getDimensions().size()) {
              boolean isSame = true;
              for (int j = 0; j < var.getDimensions().size(); j++) {
                if (!var.getDimension(j).getName().equals(mainVar.getDimension(j).getName())) {
                  isSame = false;
                  break;
                }
              }
              if (isSame) {
                this.vars.add(var);

                this.types[i] = 2;// Value Variable
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

      for (String key : this.cols) {
          evalDimensionVariable(this.ncfile.findVariable(key),key);
      }
      if(this.noResult)
          return;

      int size = mainVar.getDimensions().size();
      this.dimsSize=size;
      this.validShape=new int[size];
      this.validShapeDivider=new int[size];
      this.curSectionStart=new int[size];
      this.curSectionCount=new int[size];
      this.curSectionDshape=new int[size];
      this.totalSize=1;
      this.sectionNum=1;
      
      for (int j = 0; j < size; j++) {
        Dimension d = (Dimension)mainVar.getDimensions().get(size-1-j);//attention!
        int dRangeLength=0;
        if(this.cols.contains(d.getName())){
            
            dRangeIndexArr.add(dRangeIndexes.get(d.getName()));
            for(DRange r:dRangeIndexes.get(d.getName())){
            	dRangeLength+=r.length;
            }
        }else{
           
            ArrayList<DRange> darr=new ArrayList<DRange>();
            DRange dr=new DRange(0);
            dr.length=d.getLength();
            dRangeLength=dr.length;
            ArrayList<DRange> drlist=new ArrayList();
            drlist.add(dr);
            dRangeIndexArr.add(drlist);
        }

        if(dRangeIndexes.get(d.getName())==null)
            this.validShape[j]=1;
        else
            this.validShape[j]=dRangeIndexes.get(d.getName()).size();

        if(j==0){

            validShapeDivider[j]=1;
        }else{

            validShapeDivider[j]=validShapeDivider[j-1]*validShape[j-1];
        }
        sectionNum*=validShape[j];
        totalSize*=dRangeLength;
      }
      System.err.println("total index size: "+totalSize);

    }
    
    
    public synchronized boolean next(LongWritable key, Text value)
      throws IOException
    {
      boolean getOne=true;
      StringBuilder result = null;
      do{
      if (this.noResult || (this.count >= this.totalSize)) {
          key = null;
          value = null;
          return false;
        }
      if(!hasSection||innerSectionSize==innerSectionIndex){
    	  int secIndex=sectionCount;
    	  DRange dtmp=null;
    	  this.innerSectionSize=1;
    	  this.innerSectionIndex=0;
    	  for(int i=0;i<this.dimsSize;i++){
    		  dtmp=this.dRangeIndexArr.get(this.dimsSize-1-i).get(secIndex/validShapeDivider[this.dimsSize-1-i]);
    		  secIndex=secIndex%validShapeDivider[this.dimsSize-1-i];
    		  this.curSectionStart[i]=dtmp.start;
    		  this.curSectionCount[i]=dtmp.length;
    		  //System.out.println("index "+i+" "+ dtmp.start+" "+dtmp.length);
    		  this.innerSectionSize*=dtmp.length;
    		  
    	  }
    	  for(int i=0;i<dimsSize;i++){
    		  if(i==0){
    			  this.curSectionDshape[dimsSize-1-i]=1;
    		  }else{
    			  this.curSectionDshape[dimsSize-1-i]=this.curSectionDshape[dimsSize-i]*curSectionCount[dimsSize-i];
    		  }
    	  }
    	  sectionCount++;
    	  hasSection=true;
    	  for (int i = 0; i < this.vars.size(); i++){
    		  try {
				Section sc=new Section(curSectionStart,curSectionCount);
				this.varArr=new ArrayList();
				 this.varArr.add(((Variable)this.vars.get(i)).read(sc));
			} catch (InvalidRangeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	  }
      }
      
          getOne=true;
          int [] indexArr=new int[this.dimsSize];
          int remain=innerSectionIndex;
          for(int i=0;i<this.dimsSize;i++){
        	  indexArr[i]=remain/curSectionDshape[i];
             remain=remain%curSectionDshape[i];
          }
          int dimPos = 0;
          int varPos = 0;
          int pos = 0;
          String varName=null;
          Object val=null;
          result = new StringBuilder();
          for (int i = 0; i < this.validIDsCount; i++) {
            if(this.shrinkedTypes[i]==1) {
              pos=this.dimMap[dimPos];
              result.append(((Array)this.dimArr.get(dimPos)).getObject(curSectionStart[pos]+indexArr[pos]));
              dimPos++;
            }else{
              varName=this.vars.get(varPos).getName();
              val=((Array)this.varArr.get(varPos)).getObject(innerSectionIndex);
              //System.out.println("innerSectionIndex "+innerSectionIndex+" "+val);
              if(this.cols.contains(varName)){
                  Double gValue=greaterValueMap.get(varName);
                  Double lValue=lessValueMap.get(varName);
                  ArrayList<Double> nValues=notEqualMap.get(varName);
                  if(!eval_value(new Double(val.toString()),varName,gValue,lValue,nValues)){
                    getOne=false;
                    innerSectionIndex++;
                    this.count++;
                    break;
                  }
                  
              }
              result.append(val.toString());
              varPos++;
            }
            if (getOne && i != this.validIDsCount - 1) {
              result.append(this.seprator);
            }
          }
      }while(!getOne);
      key.set(this.count);
      value.set(result.toString());
      this.count+=1; 
      innerSectionIndex++;
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
      if (this.count >= this.totalSize) {
        return 1.0F;
      }
      return 1.0F* this.count / (float)this.totalSize;
    }
  }
}
