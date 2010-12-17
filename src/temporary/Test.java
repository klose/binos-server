package temporary;
import java.io.IOException;

import com.klose.common.*;
public class Test {
		public Test(){
			
		}
		public static void main(String[] args) {
			try {
				ReaderFactory ref = new ReaderFactory(args[0]);
				VertexReader reader = ref.getReader();
				String s = reader.readline();
				System.out.println(s);
				WriterFactory wrf = new WriterFactory(args[1]);
				VertexWriter writer = wrf.getWriter();
				writer.write(s);
				reader.close();
				writer.close();
				System.out.println("this is a small test.");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
}
