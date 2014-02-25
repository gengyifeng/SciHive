package org.apache.hadoop.hive.contrib.fileformat.netcdf;

import java.io.IOException;
import java.util.HashMap;
import ucar.ma2.Array;
import ucar.ma2.Index;
import ucar.nc2.Variable;

public class VariableSlicer
{
  private boolean hasResult;
  private final HashMap<String, Integer> beginMap;
  private final HashMap<String, Integer> endMap;

  public VariableSlicer()
  {
    this.hasResult = true;
    this.beginMap = new HashMap<String,Integer>();
    this.endMap = new HashMap<String,Integer>();
  }
  public boolean getHasResult() {
    return this.hasResult;
  }
  public int binarySearchAsc(Array arr, double value) {
    int begin = 0;
    int end = (int)(arr.getIndex().getSize() - 1);
    Index idx = arr.getIndex();

    while (end >= begin) {
      int pos = begin + (end - begin >> 1);
      idx.set0(pos);
      double val = new Double(arr.getObject(idx).toString()).doubleValue();
      if (val < value)
        begin = pos + 1;
      else if (val > value)
        end = pos - 1;
      else {
        return pos;
      }
    }
    return begin;
  }
  public int binarySearchDesc(Array arr, double value) {
    int begin = 0;
    int end = (int)(arr.getIndex().getSize() - 1);
    Index idx = arr.getIndex();

    while (end >= begin) {
      int pos = (end + begin) / 2;
      idx.set0(pos);
      double val = new Double(arr.getObject(idx).toString()).doubleValue();
      if (val == value)
        return pos;
      if (val > value)
        begin = pos + 1;
      else {
        end = pos - 1;
      }
    }
    return begin;
  }
  public int lessPos(Array arr, double value, int binaryPos) {
    Index idx = arr.getIndex();
    int pos = binaryPos;
    int bound = (int)(idx.getSize() - 1);
    if ((binaryPos < 0) || (binaryPos > bound)) {
      return binaryPos;
    }
    idx.set0(binaryPos);
    double val = new Double(arr.getObject(idx).toString()).doubleValue();
    if (val != value)
      return binaryPos - 1;
    do
    {
      pos--;
      if (pos < 0) {
        return -1;
      }
      idx.set0(pos);
      val = new Double(arr.getObject(idx).toString()).doubleValue();
    }
    while ((val == value) && (pos >= 0));
    return pos;
  }

  public int greaterPos(Array arr, double value, int binaryPos)
  {
    Index idx = arr.getIndex();
    int pos = binaryPos;
    int bound = (int)(idx.getSize() - 1);
    if ((binaryPos < 0) || (binaryPos > bound)) {
      return binaryPos;
    }
    idx.set0(binaryPos);
    double val = new Double(arr.getObject(idx).toString()).doubleValue();
    if (val != value)
      return binaryPos;
    do
    {
      pos++;
      if (pos > bound) {
        return bound + 1;
      }
      idx.set0(pos);
      val = new Double(arr.getObject(idx).toString()).doubleValue();
    }while ((val == value) && (pos <= bound));
    return pos;
  }

  public int lessEqualPos(Array arr, double value, int binaryPos) {
    Index idx = arr.getIndex();
    int pos = binaryPos;
    int bound = (int)(idx.getSize() - 1);
    if ((binaryPos < 0) || (binaryPos > bound)) {
      return binaryPos;
    }
    idx.set0(binaryPos);
    double val = new Double(arr.getObject(idx).toString()).doubleValue();

    if (val != value)
      return binaryPos - 1;
    do
    {
      pos++;
      if (pos > bound) {
        return bound;
      }
      idx.set0(pos);
      val = new Double(arr.getObject(idx).toString()).doubleValue();
    }
    while ((val == value) && (pos <= bound));
    return pos - 1;
  }

  public int greaterEqualPos(Array arr, double value, int binaryPos)
  {
    Index idx = arr.getIndex();
    int pos = binaryPos;
    int bound = (int)(idx.getSize() - 1);
    if ((binaryPos < 0) || (binaryPos > bound)) {
      return binaryPos;
    }
    idx.set0(binaryPos);
    double val = new Double(arr.getObject(idx).toString()).doubleValue();
    if (val != value)
      return binaryPos;
    do
    {
      pos--;
      if (pos < 0) {
        return 0;
      }
      idx.set0(pos);
      val = new Double(arr.getObject(idx).toString()).doubleValue();
    }while ((val == value) && (pos >= 0));
    return pos + 1;
  }

  public void process(Variable v, Double greaterValue, Boolean greaterEqual, Double lessValue, Boolean lessEqual)
  {
    if(v.getDimensions().size()>1)
        return;
    double minValue =Double.MIN_VALUE;
    boolean minEqual = true;
    double maxValue = Double.MAX_VALUE;
    boolean maxEqual = true;
    if (greaterValue != null) {
      minValue = greaterValue.doubleValue();
      minEqual = greaterEqual.booleanValue();
    }
    if (lessValue != null) {
      maxValue = lessValue.doubleValue();
      maxEqual = lessEqual.booleanValue();
    }
    setMaps(v, minValue, minEqual, maxValue, maxEqual);
  }
  public void setMaps(Variable v, double minValue, boolean minEqual, double maxValue, boolean maxEqual) {
    try {
      Array arr = v.read();
      Index idx = arr.getIndex();
      int beginIndex = 0;
      int endIndex = (int)(idx.getSize() - 1);
      int bound = endIndex;
      idx.set0(beginIndex);
      double first = new Double(arr.getObject(idx).toString()).doubleValue();
      idx.set0(endIndex);
      double last = new Double(arr.getObject(idx).toString()).doubleValue();
      if (first <= last) {
        int binaryPos1 = binarySearchAsc(arr, minValue);
        int binaryPos2 = binarySearchAsc(arr, maxValue);

        if (minEqual)
          beginIndex = greaterEqualPos(arr, minValue, binaryPos1);
        else {
          beginIndex = greaterPos(arr, minValue, binaryPos1);
        }
        if (maxEqual)
          endIndex = lessEqualPos(arr, maxValue, binaryPos2);
        else
          endIndex = lessPos(arr, maxValue, binaryPos2);
      }
      else
      {
        int binaryPos1 = binarySearchDesc(arr, maxValue);
        int binaryPos2 = binarySearchDesc(arr, minValue);

        if (maxEqual)
          beginIndex = greaterEqualPos(arr, maxValue, binaryPos1);
        else {
          beginIndex = greaterPos(arr, maxValue, binaryPos1);
        }
        if (minEqual)
          endIndex = lessEqualPos(arr, minValue, binaryPos2);
        else {
          endIndex = lessPos(arr, minValue, binaryPos2);
        }
      }

      if (beginIndex > endIndex) {
        this.hasResult = false;
        return;
      }if (beginIndex < 0)
        beginIndex = 0;
      else if (endIndex > bound) {
        endIndex = bound;
      }
      this.beginMap.put(v.getName(), Integer.valueOf(beginIndex));
      this.endMap.put(v.getName(), Integer.valueOf(endIndex));
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  public HashMap<String, Integer> getBeginMap() { return this.beginMap; }

  public HashMap<String, Integer> getEndMap() {
    return this.endMap;
  }
}
