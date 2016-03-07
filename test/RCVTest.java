//package UDPFT;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import static java.net.StandardSocketOptions.*;

public class RCVTest {
	public static void main (String[] args) throws Exception {
		DatagramChannel dc = DatagramChannel.open();
		ByteBuffer buf = ByteBuffer.allocate(1000);

		dc.bind(new InetSocketAddress(Integer.parseInt(args[0]))).configureBlocking(true);
		SocketAddress srcAddress = dc.receive((ByteBuffer)buf.clear());
		dc.connect(srcAddress);
		buf.flip();
		dc.write(buf);

		while (true) {
			buf.clear();
			dc.read(buf);
			buf.flip();
			dc.write(buf);
		}

		// log("Initial rcv buffer size in bytes " + dc.getOption(SO_RCVBUF));
		// if (args.length > 1) {
		// 	dc.setOption(SO_RCVBUF, 2*1024*1024);
		// 	log("New rcv buffer size in bytes " + dc.getOption(SO_RCVBUF));
		// }
		// dc.configureBlocking(true);
		// dc.socket().setSoTimeout(2000);
		// dc.socket().bind(new InetSocketAddress("localhost", Integer.parseInt(args[0])));

		// // ByteBuffer pkt = ByteBuffer.allocateDirect(1000);
		// // pkt.limit(1000).position(0);
		// int remaining = 100*1024*8;
		// int count = 0;
		// DatagramSocket sk = dc.socket();
		// DatagramPacket pkt = new DatagramPacket(new byte[128], 128);
		// long startTime = System.nanoTime();
		// boolean delay = false;
		// while (count < remaining) {
		// 	try {
		// 		sk.receive(pkt);
		// 		count++;
		// 	} catch (SocketTimeoutException e) {
		// 		delay = true;
		// 		break;
		// 	}
		// }
		// long elapsed = System.nanoTime() - startTime - 1000000000;
		// if (delay) elapsed -= 2000000000;
		// log("percentage success = " + count/1024/8 + '%');
		// log("Time taken to rcv 100MB? = " + elapsed/1000000 + "ms");
		// log("Throughput = " + count/(elapsed/1000000)/8 + "000 KB/s");
	}
	private static void log(Object s) {System.out.println(s);}
}