import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.net.*;
import java.io.*;
public class test {
	public static void main(String[] a) throws Exception {
		DatagramChannel dc = DatagramChannel.open();
		FileChannel fc = new FileInputStream("large.mp4").getChannel();
		long s = fc.size();
		ByteBuffer buf = ByteBuffer.allocate(991);
		long start = System.nanoTime();
		while (fc.read(buf) != -1) {
			buf.flip();
			dc.send(buf, new InetSocketAddress("localhost", 8284));
			buf.clear();
		}
		System.out.println((System.nanoTime()-start)/1000);
	}
}