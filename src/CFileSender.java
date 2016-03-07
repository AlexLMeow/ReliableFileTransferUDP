import java.net.*;
import static java.net.StandardSocketOptions.*;
import java.io.*;
import java.nio.charset.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.ByteOrder;
import java.util.*;
import java.util.zip.CRC32;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class FileSender {

	// misc
	private static final Charset CHARSET_NET = StandardCharsets.UTF_8;
	private final CRC32 CHECKSUM; // ALL CHECKSUMS CAST TO 32 BIT INT
	private static final int CHECKSUM_SIZE = 4;
	public static final int CTRL_INDEX = 4;
	public static final int HEADER_DATA_INDEX = 5;

	// SYN: 4B CRC, 1B 0xFF(syn), 4B NUMPKT
	private static final int SYN_HEADER_SIZE = 9;

	// SACK: 4B CRC, 1B CTRL, 1B RESULT
	private static final int SACK_SIZE = 6;
	private static final int SACK_TIMEOUT = 2;
	public static final int ACK_SIZE = 9;
	public static final int FIN_SIZE = 5;

	// DATA: 4B CRC, 1B 0x0(data), 4B seqnum 
	private static final int HEADER_SIZE = 9;
	private static final int DATA_SIZE = 991;
	private static final int PACKET_SIZE = 1000;
	public static final byte CTRL_FIN = -1;		
	private static final byte CTRL_DAT = 0;			
	public static final byte CTRL_SYN = 1;

	// File IO
	private int PKTS_EXPECTED;
	private final String SRC_FILE_PATH;
	private final String DST_FILE_PATH;
	private final long FILE_SIZE;
	private FileChannel FILECHANNEL;

	// Net IO
	private SocketAddress RCV_ADDR;
	private final DatagramChannel UDP_CHANNEL;
	private final ByteBuffer SYN;

	// others
	private static final int MAX_EXISTING_PACKETS = 4*1024;
	private static final int DATA_BUFFER_SIZE = 2*1024; // number of DATA_SIZE packets to hold
	private static final int NUM_DATA_BUFFERS = 2;
	private static final long ACK_TIMEOUT = 4;
	private final Thread RECEIVER_THREAD;
	private final Thread FILEREADER_THREAD;
	private final Timer SCHEDULER; // daemon
	private final int TOTAL_PACKETS;
	private final Map<Integer, ResendTimerTask> TIMEOUT_CACHE;
	private final Queue<ResendTimerTask> RESEND_Q;
	private final BlockingQueue<ResendTimerTask> FREE_RSTT_Q;
	private final Queue<ByteBuffer[]> FILLED_BUFFER_Q;
	private final BlockingQueue<ByteBuffer[]> EMPTY_BUFFER_Q;
	private int packets_sent;
	private int buffer_pos;
	private int pkts_created;

	public static void main (String[] args) throws Exception {
		if (args.length != 4) {
			log("Format: FileSender <hostname> <port> <source file> <dest file>");
			return;
		}

		FileSender sender = new FileSender(args);
		sender.sync().initialise();
		sender.sendLoop();
	}

	public FileSender (String[] args) throws Exception {

		// Initialise file data
		SRC_FILE_PATH = args[2].trim();
		DST_FILE_PATH = args[3].trim();
		FILECHANNEL = (new FileInputStream(SRC_FILE_PATH)).getChannel();
		FILE_SIZE = FILECHANNEL.size();

		// Initialise connection
		RCV_ADDR = new InetSocketAddress(args[0], Integer.parseInt(args[1]));
		UDP_CHANNEL = DatagramChannel.open().bind(null).connect(RCV_ADDR);
		UDP_CHANNEL.configureBlocking(true);
		UDP_CHANNEL.setOption(SO_RCVBUF, 4*1024*1024);

		// Other misc init
		CHECKSUM = new CRC32();
		int temp = (int)(FILE_SIZE / DATA_SIZE);
		if (temp < 0) {
			throw new Exception("\nFILE TOO BIG\n");
		}
		if (FILE_SIZE % DATA_SIZE > 0) {
			temp++;
		}
		TOTAL_PACKETS = temp;
		packets_sent = 0;
		TIMEOUT_CACHE = new BlockingHashMap<Integer, ResendTimerTask>();
		RESEND_Q = new LinkedList<ResendTimerTask>();
		FREE_RSTT_Q = new LinkedBlockingQueue<ResendTimerTask>();
		FILLED_BUFFER_Q = new LinkedList<ByteBuffer[]>();
		EMPTY_BUFFER_Q = new LinkedBlockingQueue<ByteBuffer[]>();
		SCHEDULER = new Timer(true);
		pkts_created = 0;

		// create unique SYN packet
		byte[] destPath = DST_FILE_PATH.getBytes(CHARSET_NET);
		SYN = ByteBuffer.wrap(new byte[SYN_HEADER_SIZE + destPath.length]);
		SYN.position(CHECKSUM_SIZE);
		SYN.put(CTRL_SYN).putInt(TOTAL_PACKETS).put(destPath);
		CHECKSUM.reset();
		CHECKSUM.update(SYN.array(), CHECKSUM_SIZE, SYN.array().length - CHECKSUM_SIZE);
		SYN.putInt(0, (int)CHECKSUM.getValue()).clear();
		// SYN packet created.

		ResendTimerTask.initialise(RESEND_Q, TIMEOUT_CACHE);
		// create the rstts

		RECEIVER_THREAD = new Thread(new ReceiverThread(
				UDP_CHANNEL,
				TIMEOUT_CACHE,
				FREE_RSTT_Q,
				Thread.currentThread()
			));

		FILEREADER_THREAD = new Thread(new FileReadingThread(
				FILECHANNEL,
				EMPTY_BUFFER_Q,
				FILLED_BUFFER_Q
			));
		FILEREADER_THREAD.start(); // start file io thread

		// create file data buffers
		ByteBuffer[] newDataBuff;
		for (int i = 0; i < NUM_DATA_BUFFERS; i++) {
			newDataBuff = new ByteBuffer[DATA_BUFFER_SIZE];
			for (int j = 0; j < DATA_BUFFER_SIZE; j++) {
				newDataBuff[j] = ByteBuffer.allocate(DATA_SIZE);
			}
			EMPTY_BUFFER_Q.add(newDataBuff);
		}

		log("New FileSender created.\n");
	}

	// ensures the receiver receives valid SYN packet and is set up
	public FileSender sync () throws Exception {
		log("Syncing...");
		// prepare for handshaking
		final DatagramSocket sk = UDP_CHANNEL.socket();
		final byte[] SACK_data = new byte[SACK_SIZE];
		final DatagramPacket SACK = new DatagramPacket(SACK_data, SACK_SIZE);
		sk.setSoTimeout(SACK_TIMEOUT);

		log("Sending SYN...");
		while (true) {
			UDP_CHANNEL.write(SYN);
			SYN.rewind();
			UDP_CHANNEL.write(SYN); // send twice
			SYN.rewind();
			try {
				sk.receive(SACK);
				if (!isCorrupted(SACK) && SACK.getData()[HEADER_DATA_INDEX] == 0) {
					log("SACK success!");
					break; // uncorrupted success SACK received. sync complete.
				}
				log("corrupted/failure SACK, resending SYN...");
			} catch (SocketTimeoutException ste) {
				log("SACK timeout, resend SYN...");
			}
		}
		log("Syncing succeeded!\n");
		return this;
	}

	private boolean isCorrupted (ByteBuffer pkt) {
		final int proposed = pkt.getInt();
		final byte[] dat = new byte[pkt.limit() - pkt.position()];
		pkt.get(dat);
		CHECKSUM.reset();
		CHECKSUM.update(dat);
		final int actual = (int)CHECKSUM.getValue();
		pkt.rewind();
		return proposed != actual;
	}
	private boolean isCorrupted (DatagramPacket pkt) {
		return isCorrupted(ByteBuffer.wrap(pkt.getData()));
	}
	// Sets up send buffers and preloads?
	public FileSender initialise () throws Exception {
		/////////////////////////////////

		RECEIVER_THREAD.start(); // start rcv thread.
		return this;
	}

	public FileSender sendLoop () throws Exception {
		ResendTimerTask nextRSTT;
		ByteBuffer[] dataBuff;
		try { while (true) {

			if (Thread.interrupted()) {
				log("Interrupted by FIN, exiting..");
				break;
			}

			// resend timed out packets first!
			nextRSTT = RESEND_Q.poll();
			if (nextRSTT != null) {
				nextRSTT = new ResendTimerTask(nextRSTT);
				SCHEDULER.schedule(nextRSTT, ACK_TIMEOUT);
				TIMEOUT_CACHE.put(nextRSTT.getSeqN(), nextRSTT);
				UDP_CHANNEL.write(nextRSTT.getPkt());
				nextRSTT.getPkt().rewind();
				continue;
			}

			// if already read whole file, go back and check resend q again
			if (packets_sent >= TOTAL_PACKETS) {
				FILECHANNEL.close();
				if (!UDP_CHANNEL.isOpen()) { // in case no more resends but channel just closed.
					break;
				}
				if (FILEREADER_THREAD.isAlive()) { // kill thread for sure
					FILEREADER_THREAD.interrupt();
				}
				Thread.sleep(1);
				continue;
			}

			// send next 
			dataBuff = FILLED_BUFFER_Q.peek(); // get the current data buffer
			nextRSTT = getFreeRSTT();
			if (nextRSTT == null || dataBuff == null) { // continue if no RSTT or buffer available
				Thread.sleep(1);
				log("out of free RSTTs");
				continue;
			}
			// has available RSTT and buffer
			ByteBuffer data = dataBuff[buffer_pos];
			UDP_CHANNEL.write(prepareDataPacket(nextRSTT, packets_sent, data));
			SCHEDULER.schedule(nextRSTT, ACK_TIMEOUT);
			TIMEOUT_CACHE.put(packets_sent, nextRSTT);
			packets_sent++;
			buffer_pos++;

			if (buffer_pos == DATA_BUFFER_SIZE) { // time to swap out buffer
				EMPTY_BUFFER_Q.add(FILLED_BUFFER_Q.remove());
				buffer_pos = 0;
			}

		} } catch (ClosedChannelException ce) {
			log("DatagramChannel closed by receiver thread because FIN received..");
			ce.printStackTrace();
		} catch (InterruptedException ie) {
			log("Interrupted by FIN, exiting..");
		}
		finish();
		return this;
	}

	private void finish () {

	}

	private ResendTimerTask getFreeRSTT () {
		ResendTimerTask rstt = FREE_RSTT_Q.poll();
		if (rstt != null) { 
			return new ResendTimerTask(rstt);
		}
		if (pkts_created >= MAX_EXISTING_PACKETS) {
			return null;
		}
		rstt = new ResendTimerTask(ByteBuffer.allocateDirect(PACKET_SIZE));
		pkts_created++;
		return rstt;
	}

	private ByteBuffer prepareDataPacket (ResendTimerTask rstt, int seqN, ByteBuffer data) {
		ByteBuffer pkt = rstt.getPkt();
		rstt.setSeqN(seqN);

		pkt.clear().position(CHECKSUM_SIZE);
		pkt.put(CTRL_DAT).putInt(seqN).put(data).flip();

		CHECKSUM.reset();
		byte[] check = new byte[pkt.limit() - CHECKSUM_SIZE];
		pkt.get(check);
		CHECKSUM.update(check);

		pkt.putInt(0, (int)CHECKSUM.getValue());
		pkt.rewind();

		return pkt;
	}

	public static void log (Object s) {System.out.println(s);}
}

class ResendTimerTask extends TimerTask {
	private int seqN;
	private ByteBuffer pkt;
	private static Queue<ResendTimerTask> resendQ = null;
	private static Map<Integer, ResendTimerTask> timerMap = null;
	public ResendTimerTask (ByteBuffer pkt) {
		super();
		this.pkt = pkt;
		seqN = -1;
	}
	public ResendTimerTask (ResendTimerTask old) {
		super();
		pkt = old.getPkt();
		seqN = old.getSeqN();
	}

	@Override
	public void run () {
		timerMap.remove(seqN);
		resendQ.add(this);
	}
	public int getSeqN () {
		return seqN;
	}
	public ByteBuffer getPkt () {
		return pkt;
	}
	public void setSeqN (int sn) {
		seqN = sn;
	}
	public static void initialise (Queue<ResendTimerTask> rsQueue, Map<Integer, ResendTimerTask> timers) {
		resendQ = rsQueue;
		timerMap = timers;
	}
}

class ReceiverThread implements Runnable {

	private final CRC32 CHECKSUM;
	private final DatagramChannel RCV_CHANNEL;
	private final ByteBuffer FACK;
	private final ByteBuffer RESPONSE;
	private final Map<Integer, ResendTimerTask> TIMEOUT_CACHE;
	private final BlockingQueue<ResendTimerTask> FREE_RSTT_Q;
	private final Thread MAINTHREAD;

	public ReceiverThread (
			DatagramChannel dc,
			Map<Integer, ResendTimerTask> timeOutCache,
			BlockingQueue<ResendTimerTask> freeQ,
			Thread main
		) 
	{
		MAINTHREAD = main;
		RCV_CHANNEL = dc;
		CHECKSUM = new CRC32();
		RESPONSE = ByteBuffer.allocate(FileSender.ACK_SIZE);
		TIMEOUT_CACHE = timeOutCache;
		FREE_RSTT_Q = freeQ;

		// prepare F-ACK
		FACK = ByteBuffer.allocateDirect(FileSender.FIN_SIZE);
		CHECKSUM.update((int)FileSender.CTRL_FIN);
		FACK.putInt((int)CHECKSUM.getValue()).put(FileSender.CTRL_FIN).flip();
	}

	public void run () {
		ResendTimerTask timeOutTask;
		try {
			while (true) {
				RESPONSE.clear();
				RCV_CHANNEL.read(RESPONSE); // blocking
				if (RESPONSE.position() < 5) {
					System.out.println("\nRESPONSETOOSHORT === "+RESPONSE);
				}
				RESPONSE.flip();
				if (RESPONSE.limit() < 5) {
					System.out.println("\nRESPONSETOOSHORT === "+RESPONSE+'\n');
				}
				if (isCorrupted(RESPONSE)) {
					continue;
				}
				if (isSACK(RESPONSE)) {
					continue;
				}
				if (isFIN(RESPONSE)) {
					break;
				}
				// is ACK, clear timeout task from resending cache, put into free q
				if (RESPONSE.limit() < 9) {
					continue;
				}
				timeOutTask = TIMEOUT_CACHE.remove(RESPONSE.getInt(FileSender.HEADER_DATA_INDEX));
				log("received ACK: " + RESPONSE.getInt(FileSender.HEADER_DATA_INDEX));
				if (timeOutTask != null) {
					timeOutTask.cancel();
					FREE_RSTT_Q.add(timeOutTask);
				}
			}
			finish();
		} catch (IndexOutOfBoundsException what) {
			what.printStackTrace();
			byte[] x = new byte[RESPONSE.limit()];
			log("length of pack = " + x.length);
			RESPONSE.position(0);
			RESPONSE.get(x);
			for (int i=0;i<x.length;i++){System.out.print(((int)x[i]) +" ");}
			log("");
			System.exit(0);
		}

		catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
	}

	private void finish () throws Exception {

		final ByteBuffer FACK = ByteBuffer.allocateDirect(FileSender.FIN_SIZE);
		CHECKSUM.reset();
		CHECKSUM.update((int)FileSender.CTRL_FIN);
		FACK.putInt((int)CHECKSUM.getValue()).put(FileSender.CTRL_FIN).flip();

		for (int i = 0; i < 8; i++) { // send 8 FACKS
			RCV_CHANNEL.write(FACK);
			FACK.flip();
		}
		MAINTHREAD.interrupt();
		RCV_CHANNEL.close(); // Sender thread will catch the close exception and exit
	}

	private static boolean isSACK (ByteBuffer pkt) {
		return pkt.get(FileSender.CTRL_INDEX) == FileSender.CTRL_SYN;
	}
	private static boolean isFIN (ByteBuffer pkt) {
		return pkt.get(FileSender.CTRL_INDEX) == FileSender.CTRL_FIN;
	}
	private boolean isCorrupted (ByteBuffer pkt) {
		final int proposed = pkt.getInt();
		final byte[] dat = new byte[pkt.limit() - pkt.position()];
		pkt.get(dat);
		CHECKSUM.reset();
		CHECKSUM.update(dat);
		final int actual = (int)CHECKSUM.getValue();
		pkt.rewind();
		return proposed != actual;
	}
	// private boolean isCorrupted (DatagramPacket pkt) {
	// 	return isCorrupted(ByteBuffer.wrap(pkt.getData()));
	// }
	private void log (Object s) {System.out.println(s);}
}

class FileReadingThread implements Runnable {

	private final FileChannel FILECHANNEL;
	private final BlockingQueue<ByteBuffer[]> EMPTY_BUFFER_Q;
	private final Queue<ByteBuffer[]> FILLED_BUFFER_Q;

	public FileReadingThread (
			FileChannel fc, 
			BlockingQueue<ByteBuffer[]> ebq,
			Queue<ByteBuffer[]> fbq
		) 
	{
		FILECHANNEL = fc;
		EMPTY_BUFFER_Q = ebq;
		FILLED_BUFFER_Q = fbq;
	}

	public void run () {
		ByteBuffer[] nextBuf;
		try {
			while (true) {
				nextBuf = EMPTY_BUFFER_Q.take();
				clearBBs(nextBuf);
				if (FILECHANNEL.read(nextBuf) == -1) {
					break;
				}
				flipBBs(nextBuf);
				FILLED_BUFFER_Q.add(nextBuf);
			}
		FILECHANNEL.close();
		} catch (ClosedChannelException ce) {
			FileSender.log("Filechannel is closed, filereading thread terminating!..");
		} catch (InterruptedException ie) {
			FileSender.log("Filereading thread interrupted! terminating!..");
			return;
		} catch (Exception e) {
			FileSender.log(e);
			e.printStackTrace();
		}
	}

	private void clearBBs (ByteBuffer[] bufs) {
		for (int i = 0; i < bufs.length; i++) {
			bufs[i].clear();
		}
	}
	private void flipBBs (ByteBuffer[] bufs) {
		for (int i = 0; i < bufs.length; i++) {
			bufs[i].flip();
		}
	}
}

class BlockingHashMap<K,V> extends HashMap<K,V> {
	public BlockingHashMap () {
		super();
	}
	public BlockingHashMap (int cap) {
		super(cap);
	}
	@Override
	public V put (K key, V value) {
		synchronized (this) {
			return super.put(key, value);
		}
	}
	@Override
	public V remove (Object key) {
		synchronized (this) {
			return super.remove(key);
		}
	}
}