package temporary;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.klose.common.*;
import com.klose.common.TransformerIO.ReaderFactory;
import com.klose.common.TransformerIO.VertexReader;
import com.klose.common.TransformerIO.VertexWriter;
import com.klose.common.TransformerIO.WriterFactory;
public class Test {
		public Test(){
			
		}
		public static void main(String[] args) throws IOException {
//			try {
//				ReaderFactory ref = new ReaderFactory(args[0]);
//				VertexReader reader = ref.getReader();
//				String s = reader.readline();
//				System.out.println(s);
//				WriterFactory wrf = new WriterFactory(args[1]);
//				VertexWriter writer = wrf.getWriter();
//				writer.write(s);
//				reader.close();
//				writer.close();
//				System.out.println("this is a small test.");
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
			Path p = new Path("task");
			System.out.println(fs.isDirectory(p));
			fs.copyFromLocalFile(new Path("/tmp/aaa"), p);
			System.out.println(fs.getWorkingDirectory().toString() + "/" + p.toString() + "/"+ "aaa");
		}
}
