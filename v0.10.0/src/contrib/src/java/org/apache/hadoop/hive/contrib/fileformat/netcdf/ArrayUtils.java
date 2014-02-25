package org.apache.hadoop.hive.contrib.fileformat.netcdf;

import java.util.ArrayList;

public class ArrayUtils {
	
	public static void setBoundary(int[] bound,int elementSize,int blockSize, int []shape ,int size){
		int remain=size;
		int []map=new int[size];
		int allSize=1;
		for(int i=0;i<size;i++){
			allSize*=shape[i];
		}
		int blockNum=allSize*elementSize/blockSize;
		if(blockNum==0){
			for(int j=0;j<size;j++){
				if(map[j]==0){
					bound[j]=1;
					map[j]=1;
				}
			}
			return;
		}
		if(allSize*elementSize%blockSize!=0){
			blockNum++;
		}
		for(int i=0;i<size;i++){
			boolean first=false;
			int minPos=0;
			int min=0;
			for(int j=0;j<size;j++){
				if(map[j]==1)
					continue;
				if(!first){
					minPos=j;
					min=shape[j];
					first=true;
					continue;
				}
				if(first&&shape[j]<min){
					minPos=j;
					min=shape[j];
				}
			}
			
			double tmp=Math.pow(blockNum, 1.0/remain);
			//System.out.println("min "+min+ " minPos "+minPos+" blockNum "+blockNum+" tmp "+tmp);
			if(tmp>(int)tmp){
				tmp+=1;
			}
			if((int)tmp<=min){
				for(int j=0;j<size;j++){
					if(map[j]==0){
						bound[j]=(int)tmp;
					}
				}
				return;
			}
			
			bound[minPos]=shape[minPos];
			if(blockNum/shape[minPos]==0){
				for(int j=0;j<size;j++){
					if(map[j]==0){
						bound[j]=1;
						map[j]=1;
					}
				}
				return;
			}
			if(blockNum%shape[minPos]==0){
				blockNum=blockNum/shape[minPos];
			}else{
				blockNum=blockNum/shape[minPos]+1;
			}
			map[minPos]=1;
			remain--;
		}

	}
	public static void getNewShape(int[] newShape,int[]bound,int[]shape,int size){
	    for(int i=0;i<size;i++){
	        if(shape[i]>=bound[i]){
	        	newShape[i]=bound[i];
	        }else{
	        	newShape[i]=shape[i];
	        }
	    }
	}
	public static int getMaxBlockSize(int[]bound, int[]shape,int size){
		int len=1;
	    for(int i=0;i<size;i++){
	        len*=shape[i]-(shape[i]/bound[i])*(bound[i]-1);
	    }
	    return len;
	}
	public static void getStartCount(int[]start,int[]count,int []newidx,int[]newShape,int[]bound,int[]shape,int size){
			int len;
		   for(int i=0;i<size;i++){
		       len=shape[i]/bound[i];
		       start[i]=newidx[i]*len;
		       if(newidx[i]!=newShape[i]-1){
		           count[i]=len;
		       }else{
		           count[i]=shape[i]-newidx[i]*len;
		       }
		   }
	}
	public static void getDshape(int []dShape,int[] shape,int size){
	    int tmp=1;
	    dShape[0]=1;
	    for(int i=1;i<size;i++){
	      dShape[i]=tmp=tmp*shape[size-i];
	    } 
	}
	public static void getIdx(int []idx,int pos,int[]dShape,int size){
		   int tmp=pos;
		   for(int i=0;i<size;i++){
		       idx[i]=tmp/dShape[size-1-i];
		       tmp=tmp-idx[i]*dShape[size-1-i];
		   }
	}
	public static int getIndex(int []idx,int[]dShape,int size){
	   int tmp=idx[size-1];
	   for(int i=1;i<size;i++){
	        tmp+=idx[size-1-i]*dShape[i];
	   }
	   return tmp;
	}
	public static void getBeginCountCountDshape(int[]begin,int []count,int[]countdshape,int id,int[]shape,int[]newdshape,int[]bound,int size ){
		int []idx=new int[size];
	    ArrayUtils.getIdx(idx,id,newdshape,size);
	    int len;
	    for(int i=0;i<size;i++){
	        len=shape[i]/bound[i];
	        begin[i]=idx[i]*len;
	        if(idx[i]!=bound[i]-1){
	            count[i]=len;
	        }else{
	            count[i]=shape[i]-len*(bound[i]-1);
	        }
	    }

	    ArrayUtils.getDshape(countdshape,count,size);
	}
	public static void longArrayToIntArray(int []ibegin,int []icount,long[]begin,long []count,int size){
		for(int i=0;i<size;i++){
			ibegin[i]=(int) begin[i];
			icount[i]=(int)count[i];
		}
	}
	public static void test(){
		int size=3;
		int [] bound=new int[size];
		int elementSize=4;
		//int blockSize=50*100*150*4;
		int blockSize=1024*1024*150;
		//int []shape={100,200,300};
		int []shape={32,1024,1024};
		setBoundary(bound,elementSize,blockSize,shape,3);
		for(int i=0;i<size;i++){
			System.out.println(bound[i]);
		}
	}

	public static void main(String []argv){
		test();
		//System.out.println(a);
	}
}

