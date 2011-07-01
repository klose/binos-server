package com.lyz.test.compiler;
import java.util.Map;

import com.transformer.compiler.Channel;
import com.transformer.compiler.ChannelManager;
import com.transformer.compiler.JobCompiler;
import com.transformer.compiler.JobConfiguration;
import com.transformer.compiler.JobStruct;
import com.transformer.compiler.ParallelLevel;
import com.transformer.compiler.PhaseStruct;
import com.transformer.compiler.TaskStruct;

public class WorkflowTest {
		public static void main(String[] args){
			int mapNum = 20;
			int reduceNum = 4;
			String pathPrefix = JobConfiguration.getPathHDFSPrefix();
			JobStruct job = new JobStruct();
			ParallelLevel pal = new ParallelLevel(ParallelLevel.assignFirstLevel());
			PhaseStruct ps1 = new PhaseStruct(pal);
			PhaseStruct ps2 = new PhaseStruct(pal.nextLevel());
			PhaseStruct ps3 = new PhaseStruct(pal.nextLevel().nextLevel());
			PhaseStruct ps4 = new PhaseStruct(pal.assignEndLevel());
			job.addPhaseStruct(ps1);
			job.addPhaseStruct(ps2);
			job.addPhaseStruct(ps3);
			job.addPhaseStruct(ps4);
			
			TaskStruct ts1 = new TaskStruct();
			TaskStruct ts2 = new TaskStruct();
			TaskStruct ts3 = new TaskStruct();
			TaskStruct ts4 = new TaskStruct();
			
			ts1.setOperationClass(WordCountMap.class);
			ts2.setOperationClass(WordCountPartition.class);
			ts3.setOperationClass(WordCountReduce.class);
			ts4.setOperationClass(WordCountMerge.class);
			
			String[] a = new String[20];
			for(int i = 0;i< mapNum;i++){
				a[i] = pathPrefix + "/testWordCount/data/largdata.txt_"+i+".tmp";
			}
			
			String[] c = {pathPrefix + "/testWordCount/data/output"};
			ps1.addTask(ts1, 20, a);			
			ps2.addTask(ts2, 1);
			ps3.addTask(ts3, 4);		
			ps4.addTask(ts4, 1,c);

			
			Channel[] ch1 = new Channel[ps1.getParallelNum()];
			Channel[] ch12 = new Channel[ps1.getParallelNum()];
			Channel[] ch13 = new Channel[ps1.getParallelNum()];
			Channel[] ch14 = new Channel[ps1.getParallelNum()];
			Channel[] ch2 = new Channel[ps3.getParallelNum()];
			Channel[] ch3 = new Channel[ps3.getParallelNum()];
			int i=0;
			for(;i<ps1.getParallelNum();i++)
			{	
				ch1[i] = new Channel(ps1.getTaskStruct()[i],0,ps2.getTaskStruct()[0],i);
			}
			for(int j=0;j<ps1.getParallelNum();j++){
				ch12[j] = new Channel(ps1.getTaskStruct()[j],1,ps2.getTaskStruct()[0],i++);
			}
			for(int j=0;j<ps1.getParallelNum();j++){
				ch13[j] = new Channel(ps1.getTaskStruct()[j],2,ps2.getTaskStruct()[0],i++);
			}
			for(int j=0;j<ps1.getParallelNum();j++){
				ch14[j] = new Channel(ps1.getTaskStruct()[j],3,ps2.getTaskStruct()[0],i++);
			}
			
			for(int j=0;j<ps3.getParallelNum();j++)
			{	
				ch2[j] = new Channel(ps2.getTaskStruct()[0],j,ps3.getTaskStruct()[j],0);
			}
			for(int j=0;j<ps3.getParallelNum();j++)
			{	
				ch3[j] = new Channel(ps3.getTaskStruct()[j],0,ps4.getTaskStruct()[0],j);
			}
			
			ChannelManager chm = new ChannelManager();
			chm.addChannels(ch1);
			chm.addChannels(ch12);
			chm.addChannels(ch13);
			chm.addChannels(ch14);
			chm.addChannels(ch2);
			chm.addChannels(ch3);
			Map<String, TaskStruct> map = chm.parseDep();
			JobCompiler compiler = new JobCompiler(map, job);
			compiler.compile();
		}
}
