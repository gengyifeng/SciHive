package org.apache.hadoop.hive.contrib.serde2;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NcFileSerDe
  implements SerDe
{
  public static final Log LOG = LogFactory.getLog(NcFileSerDe.class.getName());
  StructObjectInspector rowOI;
  ArrayList<Object> row;
  int numColumns;
  List<TypeInfo> columnTypes;
  List<String> columnNames;
  public static List<TypeInfo> colTypes;
  public static List<String> colNames;
  boolean[] fieldsSkip;
  ArrayList<Integer> notSkipIDs;
  ArrayList<Integer> colArray;
  int[] posMap;
  public static ArrayList<Integer> notSkipIDsFromInputFormat;
  Text outputRowText;

  public NcFileSerDe()
    throws SerDeException
  {
  }

  public void initialize(Configuration job, Properties tbl)
    throws SerDeException
  {
    this.notSkipIDs = notSkipIDsFromInputFormat;
    String columnNameProperty = tbl.getProperty("columns");
    String columnTypeProperty = tbl.getProperty("columns.types");

    this.columnNames = Arrays.asList(StringUtils.split(columnNameProperty, ','));
    this.columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

    assert (this.columnNames.size() == this.columnTypes.size());
    this.numColumns = this.columnNames.size();

    if ((this.notSkipIDs == null) || (this.notSkipIDs.size() == 0)) {
      this.notSkipIDs = new ArrayList();
      for (int i = 0; i < this.numColumns; i++) {
        this.notSkipIDs.add(Integer.valueOf(i));
      }
    }

    for (int c = 0; c < this.numColumns; c++) {
      if (((TypeInfo)this.columnTypes.get(c)).getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new SerDeException(new StringBuilder().append(getClass().getName()).append(" only accepts primitive columns, but column[").append(c).append("] named ").append((String)this.columnNames.get(c)).append(" has category ").append(((TypeInfo)this.columnTypes.get(c)).getCategory()).toString());
      }

    }

    List columnOIs = new ArrayList(this.columnNames.size());

    for (int c = 0; c < this.numColumns; c++) {
      columnOIs.add(TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo((TypeInfo)this.columnTypes.get(c)));
    }

    this.rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.columnNames, columnOIs);

    this.row = new ArrayList(this.numColumns);
    for (int c = 0; c < this.numColumns; c++) {
      this.row.add(null);
    }

    this.outputRowText = new Text();
    this.fieldsSkip = new boolean[this.numColumns];
    this.posMap = new int[this.numColumns];
    this.colArray = new ArrayList();
    for (int i = 0; i < this.numColumns; i++)
    {
      for (int j = 0; j < this.notSkipIDs.size(); j++) {
        int id = ((Integer)this.notSkipIDs.get(j)).intValue();
        if (id == i) {
          this.posMap[i] = j;

          this.colArray.add(Integer.valueOf(i));
          break;
        }

      }

    }

    colTypes = this.columnTypes;
    colNames = this.columnNames;
  }

  public Object deserialize(Writable field) throws SerDeException
  {
    Text t = (Text)field;

    String[] list = StringUtils.split(t.toString(), '\t');

    for (Integer i : this.colArray) {
      try {
        this.row.set(i.intValue(), deserializeField((TypeInfo)this.columnTypes.get(i.intValue()), list[this.posMap[i.intValue()]]));
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    }

    return this.row;
  }

  static Object deserializeField(TypeInfo type,
	      String str) throws IOException {
	    if (str==null)
	      return null;

	    switch (type.getCategory()) {
	    case PRIMITIVE: {
	      PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
	      switch (ptype.getPrimitiveCategory()) {

	      case VOID: {
	        return null;
	      }

	      case BOOLEAN: {
	        BooleanWritable r=new BooleanWritable();
	        r.set(new Boolean(str));
	        return r;
	      }
	      case BYTE: {
	        ByteWritable r=new ByteWritable();
	        r.set(new Byte(str));
	        return r;
	      }
	      case SHORT: {
	        ShortWritable r=new ShortWritable();
	        r.set(new Short(str));
	        return r;
	      }
	      case INT: {
	        IntWritable r=new IntWritable();
	        r.set(new Integer(str));
	        return r;
	      }
	      case LONG: {
	        LongWritable r = new LongWritable();
	        r.set(new Long(str));
	        return r;
	      }
	      case FLOAT: {
	        FloatWritable r = new FloatWritable();
	        r.set(new Float(str));
	        return r;
	      }
	      case DOUBLE: {
	        DoubleWritable r = new DoubleWritable();
	        r.set(new Double(str));
	        return r;
	      }
	      case STRING: {
	        Text r = new Text(str);
	        return r;
	      }
	      default: {
	        throw new RuntimeException("Unrecognized type: "
	            + ptype.getPrimitiveCategory());
	      }
	      }
	    }
	      // Currently, deserialization of complex types is not supported
	    case LIST:
	    case MAP:
	    case STRUCT:
	    default: {
	      throw new RuntimeException("Unsupported category: " + type.getCategory());
	    }
	    }
	  }


  public ObjectInspector getObjectInspector()
    throws SerDeException
  {
    return this.rowOI;
  }

  public Class<? extends Writable> getSerializedClass()
  {
    return Text.class;
  }

  public Writable serialize(Object obj, ObjectInspector objInspector)
    throws SerDeException
  {
    StructObjectInspector outputRowOI = (StructObjectInspector)objInspector;
    List outputFieldRefs = outputRowOI.getAllStructFieldRefs();

    if (outputFieldRefs.size() != this.numColumns) {
      throw new SerDeException(new StringBuilder().append("Cannot serialize the object because there are ").append(outputFieldRefs.size()).append(" fields but the output has ").append(this.colArray.size()).append(" columns.").toString());
    }

    StringBuilder outputRowString = new StringBuilder();

    for (int c = 0; c < this.numColumns; c++) {
      if (c > 0) {
        outputRowString.append('\t');
      }

      Object column = outputRowOI.getStructFieldData(obj, (StructField)outputFieldRefs.get(c));

      if (((StructField)outputFieldRefs.get(c)).getFieldObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE)
      {
        outputRowString.append(column == null ? "null" : column.toString());
        System.out.print(new StringBuilder().append(((StructField)outputFieldRefs.get(c)).getFieldObjectInspector().getTypeName()).append(":").append(column.toString()).append("\t").toString());
      }
      else
      {
        outputRowString.append(SerDeUtils.getJSONString(column, ((StructField)outputFieldRefs.get(c)).getFieldObjectInspector()));
      }

    }

    this.outputRowText.set(outputRowString.toString());
    return this.outputRowText;
  }

  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }
}
