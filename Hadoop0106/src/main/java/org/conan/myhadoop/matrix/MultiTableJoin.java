package org.conan.myhadoop.matrix;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 多表连接在hadoop的具体实现中分为两种不同的情况，一种是reduce side join另外一种则是map side join 之所以存在reduce
 * side join是因为在map阶段不能获取所有的需要的字段，即同一个key对应的字段可能位于多 个不同的map中。但是Reduce side
 * join是非常低效的，因为在shuffle阶段要进行大量的数据传输。 map side
 * join就是针对一下场景进行优化：两个待链接的表中，有一个表非常大，而另一个表非常小 以至于小表能够放在内存中，这样我们可以将小表复制多份，让每个map
 * task内存中存放一份（比如存在 hash table中），然后只 扫描大表，对于大表中的每一条记录key/value,在hash
 * table中查找是否有相同的key的记录，如果由，则连接后输出即可。为了
 * 支持文件的复制，hadoop提供了一个类DistributedCache,使用该类的方法如下：
 * （1）DistributedCache.addCacheFile
 * ()指定要复制的文件，他的参数是文件的URI，JobTracker在作业启动之前会获取这个URI列表
 * ，并且将相应的文件拷贝到各个TaskTracker的本地磁盘中。
 * （2）用户使用DistributedCache.getLocalCacheFile()获取文件目录，并且使用标准的文件读写API读写相应的文件
 * 
 * @author root 登录日志记录了：用户名，性别，登录日期 用户表记录了：用户名和姓名 性别表：性别标识（0，1）和真实的性别（男，女）
 *         要求：输出，姓名，性别，登录次数。如：张三，男，4
 * */
public class MultiTableJoin {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		// 用于缓存sex,user文件中的数据
		private Map<String, String> userMap = new HashMap<String, String>();
		private Map<String, String> sexMap = new HashMap<String, String>();
		private Text oKey = new Text();
		private Text oValue = new Text();
		private String[] kv;

		// 此方法會在map方法执行之前执行
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			BufferedReader in = null;
			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(context
						.getConfiguration());
				String uidNameAddr = null;
				String sidSex = null;
				for (Path path : paths) {
					if (path.toString().contains("user")) {
						in = new BufferedReader(new FileReader(path.toString()));
						while ((uidNameAddr = in.readLine()) != null) {
							userMap.put(uidNameAddr.split("\t", -1)[0],
									uidNameAddr.split("\t", -1)[1]);
						}
					} else if (path.toString().contains("sex")) {
						in = new BufferedReader(new FileReader(path.toString()));
						while ((sidSex = in.readLine()) != null) {
							sexMap.put(sidSex.split("\t", -1)[0],
									sidSex.split("\t", -1)[1]);
						}
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (Exception e2) {
					// TODO: handle exception
				}
			}
		}

		/**
		 * 输入的用户表的格式是：用户名，性别，登录的时间
		 */
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			kv = value.toString().split("\t");
			// map join:在map阶段过滤掉不需要的数据
			// 判断userMap是否包含当前的用户名，并且判断当前的sexMap中是否包含存在的性别
			if (userMap.containsKey(kv[0]) && sexMap.containsKey(kv[1])) {
				oKey.set(userMap.get(kv[0]) + "\t" + sexMap.get(kv[1]));// 將用戶姓名和性別作為key值输出
				// 最好是将用户名这样的唯一标识符也作为map输出key值的一部份
				oValue.set("1");
				context.write(oKey, oValue);
			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		private Text oValue = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum = 0;
			for (Text val : values) {
				sum += Integer.parseInt(val.toString());
			}
			oValue.set(String.valueOf(sum));
			context.write(key, oValue);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(MultiTableJoin.class);
		// 我们将sex和user两个表作为缓存在HDFS上面的文件，由每个运行map任务的机器来读取
		DistributedCache.addCacheFile(
				URI.create("hdfs://192.168.0.16：/join/sex.txt"),
				job.getConfiguration());
		DistributedCache.addCacheFile(
				URI.create("hdfs://192.168.0.16：/join/user.txt"),
				job.getConfiguration());
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(
				"hdfs://192.168.0.16：/login.txt"));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(
				"hdfs://192.168.0.16：/joinresult"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

