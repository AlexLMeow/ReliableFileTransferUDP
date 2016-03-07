//package UDPFT;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import static java.net.StandardSocketOptions.*;
import java.net.InetSocketAddress;


/*
Throughput through unrelinet 0 0 0 and without unrelinet.
RTT through unrelinet 000.
RTT through unrelinet 
*/
class UDPTest {
	public static void main (String[] args) {

	}
	private static void log(Object s) {System.out.println(s);}
}

public class SNDTest {
	private DatagramChannel dc;
	public SNDTest () {

	}
	public static void main (String[] args) throws Exception {
		DatagramChannel dc = DatagramChannel.open();
		ByteBuffer buf = ByteBuffer.allocate(1000);
		byte message = 12;
		dc.bind(null).configureBlocking(true);
		dc.connect(new InetSocketAddress("localhost", Integer.parseInt(args[0])));
		while (true){
			buf.clear();
			log("here we go");
			long start = System.nanoTime();
			dc.write(buf);
			buf.rewind();
			dc.read(buf);
			long elapsed = System.nanoTime() - start;
			log("RTT " + (elapsed/1000000) + "ms " + (elapsed%1000000)/1000 + "us " + (elapsed%1000) + "ns");
			Thread.sleep(1000);
		}
		// log("Initial send buffer size in bytes " + dc.getOption(SO_SNDBUF));
		// if (args.length > 1) {
		// 	dc.setOption(SO_SNDBUF, 2*1024*1024);
		// 	log("New send buffer size in bytes " + dc.getOption(SO_SNDBUF));
		// }
		// dc.configureBlocking(true);
		// dc.connect(new InetSocketAddress("localhost", Integer.parseInt(args[0])));

		// ByteBuffer pkt = ByteBuffer.allocateDirect(128);
		// pkt.limit(128).position(0);
		// int remaining = 100*1024*8;
		// long startTime = System.nanoTime();
		// while (remaining > 0) {
		// 	dc.write(pkt);
		// 	remaining--;
		// 	pkt.rewind();
		// }
		// long elapsed = System.nanoTime() - startTime;
		// log("Time taken to send 100MB = " + elapsed/1000000/8 + "ms");
		// log("Throughput = " + 100*1024/(elapsed/1000000) + "000 KB/s");
	}
	private static void log(Object s) {System.out.println(s);}
}