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

public class FileReceiver {

	// misc
	private static final Charset CHARSET_NET = StandardCharsets.UTF_8;
	private final CRC32 CHECKSUM; // ALL CHECKSUMS CAST TO 32 BIT INT
	private static final int CHECKSUM_SIZE = 4;
	private static final int CTRL_INDEX = 4;
	private static final int HEADER_DATA_INDEX = 5;

	// SYN: 4B CRC, 1B 0xFF(syn), 4B NUMPKT
	private static final int SYN_HEADER_SIZE = 9;

	// SACK: 4B CRC, 1B CTRL, 1B RESULT
	private static final int SACK_SIZE = 6;
	private static final int ACK_SIZE = 9;
	private static final int FIN_SIZE = 5;

	// DATA: 4B CRC, 1B 0x0(data), 4B seqnum 
	private static final int HEADER_SIZE = 9;
	private static final int DATA_SIZE = 991;
	private static final int PACKET_SIZE = 1000;
	private static final byte CTRL_FIN = -1;		
	private static final byte CTRL_DAT = 0;			
	private static final byte CTRL_SYN = 1;

	// File IO
	private int PKTS_EXPECTED;
	private String DST_FILE_PATH;
	private FileChannel FILECHANNEL;

	// Net IO
	private SocketAddress SND_ADDR;
	private final int LISTEN_PORT;
	private final DatagramChannel UDP_CHANNEL;
	private final ByteBuffer SACK_SUCCESS; // 
	private final ByteBuffer SACK_FAIL;

	// OTHERS
	// Remember to flip/clear/rewind packets if pooling packets
	private static final int WRITE_BUF_SIZE = 4*1024;
	private static final int CACHE_MAX = 4*1024;
	private final AtomicInteger SEQNUM_FLOOR;
	private final Object SEQN_FLOOR_LOCK; // for SEQNUM_FLOOR/CACHEMAP race condition
	private final Map<Integer, ByteBuffer> CACHEMAP;
	private final Semaphore SEM_WRITE;
	private Thread WRITER_THREAD;
	private int PKTS_RECEIVED;

	public static void main (String[] args) throws Exception {
		if (args.length != 1) {
			log("Format: FileReceiver <listening port> <optional rcv buffer size in kilopkts>");
			return;
		}

		FileReceiver receiver = new FileReceiver(args);
		receiver.sync(); // sync calls initialise.
		receiver.receiveLoop().finish();
	}

	public FileReceiver (String[] args) throws Exception {

		// set up listening channel
		LISTEN_PORT = Integer.parseInt(args[0]);
		UDP_CHANNEL = DatagramChannel.open().setOption(SO_RCVBUF, 40*1024*1024);
		log("RCVBUF: " + UDP_CHANNEL.getOption(SO_RCVBUF));
		UDP_CHANNEL.bind(new InetSocketAddress(LISTEN_PORT));
		UDP_CHANNEL.configureBlocking(true);

		// misc
		CHECKSUM = new CRC32();
		CACHEMAP = new HashMap<>();
		SEM_WRITE = new Semaphore(0);
		SEQNUM_FLOOR = new AtomicInteger(0);
		PKTS_RECEIVED = 0;
		SEQN_FLOOR_LOCK = new Object();

		// prepare SACK_SUCCESS pkt
		SACK_SUCCESS = ByteBuffer.allocateDirect(SACK_SIZE);
		CHECKSUM.reset();
		CHECKSUM.update(CTRL_SYN);
		CHECKSUM.update(0);
		SACK_SUCCESS.putInt((int)CHECKSUM.getValue()).put(CTRL_SYN).flip();
		// SACK_SUCCESS pkt ready

		// prepare SACK_FAIL
		SACK_FAIL = ByteBuffer.allocateDirect(SACK_SIZE);
		CHECKSUM.reset();
		CHECKSUM.update(CTRL_SYN);
		CHECKSUM.update(-1);
		SACK_FAIL.putInt((int)CHECKSUM.getValue()).put(CTRL_SYN).put((byte)-1).flip();
		log("New FileReceiver created.\n");
	}

	// 
	public FileReceiver sync() throws Exception {
		log("Syncing...");

		final ByteBuffer syn = ByteBuffer.allocate(PACKET_SIZE);
		while (true) {
			SND_ADDR = UDP_CHANNEL.receive(syn);
			syn.flip();
			if (!isCorrupted(syn)) {
				parseSYN(syn);
				break;
			} else {
				log("Corrupted SYN received, returning SACK_FAIL");
				UDP_CHANNEL.send(SACK_FAIL, SND_ADDR);
				SACK_FAIL.rewind();
			}
		}
		initialise();
		log("Syncing succeeded!\n");
		UDP_CHANNEL.write(SACK_SUCCESS); // after init 
		SACK_SUCCESS.rewind();
		return this;
	}
	private void parseSYN (ByteBuffer syn) {
		syn.position(5);
		PKTS_EXPECTED = syn.getInt();
		final byte[] pathBytes = new byte[syn.limit()-syn.position()];
		syn.get(pathBytes);
		DST_FILE_PATH = new String(pathBytes, CHARSET_NET);
		syn.rewind();
		log("SYN parsed : Num pkts expected = " + PKTS_EXPECTED + ", path = " + DST_FILE_PATH);
	}
	// init file and connection and buffers
	public FileReceiver initialise () throws Exception {

		// setup remaining IO
		UDP_CHANNEL.connect(SND_ADDR);
		FILECHANNEL = (new FileOutputStream(DST_FILE_PATH, false)).getChannel();
		// start writer thread
		WRITER_THREAD = new Thread(new WriterThread(
				WRITE_BUF_SIZE,
				SEM_WRITE,
				SEQNUM_FLOOR,
				CACHEMAP,
				FILECHANNEL,
				PKTS_EXPECTED,
				SEQN_FLOOR_LOCK
			));
		WRITER_THREAD.start();
		log("Receiver fully initialised!");
		return this;
	}

	public FileReceiver receiveLoop () throws Exception {

		final ByteBuffer head = ByteBuffer.allocate(HEADER_SIZE);
		final ByteBuffer ACK = ByteBuffer.allocateDirect(ACK_SIZE);
		ByteBuffer[] pkt = new ByteBuffer[2];
		pkt[0] = head;

		while (true) {

			pkt[0].clear();
			pkt[1] = ByteBuffer.allocateDirect(DATA_SIZE);
			UDP_CHANNEL.read(pkt);
			flip(pkt);

			if (isCorrupted(pkt)) {
				log("Corrupted packet received!");
				continue;
			}

			if (isSYN(pkt)) {
				UDP_CHANNEL.write(SACK_SUCCESS);
				SACK_SUCCESS.rewind();
				//log("Duplicate SYN received, sending SACK..");
				continue;
			}

			// IS VALID DATA
			int seqN = pkt[0].getInt(5);
			UDP_CHANNEL.write(createACK(seqN, ACK)); // send ack
			ACK.clear();

			// because pkts with seqn smaller than seqnm floor will never be consumed
			synchronized (SEQN_FLOOR_LOCK) { 
				if (seqN < SEQNUM_FLOOR.get()) {
					continue;
				}
				if (CACHEMAP.containsKey(seqN)) {
					//log("Duplicate pkt " + seqN + "received. Discarding.");
					continue;
				}
				// valid data without duplicate
				CACHEMAP.put(seqN, pkt[1]); // put data portion into cachemap
			}

			SEM_WRITE.release(); // signal writer to check
			PKTS_RECEIVED++;

			if (PKTS_RECEIVED == PKTS_EXPECTED) {
				log("ALL PACKETS RECEIVED! finalising...");
				break;
			}
		}
		return this;
	}

	private FileReceiver finish () throws Exception {
		// flush writer
		SEM_WRITE.release();
		WRITER_THREAD.join(); // wait for writer to fin

		final ByteBuffer FIN = ByteBuffer.allocateDirect(FIN_SIZE);
		CHECKSUM.reset();
		CHECKSUM.update((int) CTRL_FIN);
		FIN.putInt((int)CHECKSUM.getValue()).put(CTRL_FIN).flip();

		for (int i = 0; i < 8; i++) { // spam 8 fin
			UDP_CHANNEL.write(FIN);
			FIN.flip();
		}
		// send FIN, wait FACK
		DatagramSocket sk = UDP_CHANNEL.socket();
		byte[] fackdata = new byte[5];
		final DatagramPacket fack = new DatagramPacket(fackdata, 5);
		sk.setSoTimeout(1);
		while (true) {
			try {
				sk.receive(fack);
				if (!isCorrupted(fack)) {
					if (fack.getData()[4] == CTRL_FIN) {
						break;
					}
				}
			} catch (SocketTimeoutException e) {
			} catch (PortUnreachableException pue) {
				break;
			}
			UDP_CHANNEL.write(FIN);
			FIN.flip();
		}
		UDP_CHANNEL.close();
		return this;
	}

	private ByteBuffer createACK (int seqN, ByteBuffer ack) {
		CHECKSUM.reset();
		ack.clear();
		ack.put(CTRL_INDEX, CTRL_DAT).putInt(HEADER_DATA_INDEX, seqN);
		byte[] ba = new byte[5]; // 1 ctrl 4 seqN
		ack.position(CTRL_INDEX);
		ack.get(ba);
		CHECKSUM.update(ba);
		ack.putInt(0, (int)CHECKSUM.getValue());
		ack.clear();
		return ack;
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
	private boolean isCorrupted (ByteBuffer[] pkt) {
		final int proposed = pkt[0].getInt();
		final byte[] dat1 = new byte[pkt[0].limit() - pkt[0].position()];
		final byte[] dat2 = new byte[pkt[1].limit()];
		CHECKSUM.reset();
		pkt[0].get(dat1);
		pkt[1].get(dat2);
		CHECKSUM.update(dat1);
		CHECKSUM.update(dat2);
		final int actual = (int)CHECKSUM.getValue();
		pkt[0].rewind();
		pkt[1].rewind();
		return proposed != actual;
	}
	private boolean isCorrupted (DatagramPacket pkt) {
		return isCorrupted(ByteBuffer.wrap(pkt.getData()));
	}
	private static boolean isSYN (ByteBuffer[] pkt) {
		return pkt[0].get(CTRL_INDEX) == CTRL_SYN;
	}
	private static void flip (ByteBuffer[] pkt) {
		pkt[0].flip();
		pkt[1].flip();
	}
	private static void log (Object s) {System.out.println(s);}
}

class WriterThread implements Runnable {
// remember to clear buffers if buffer pooling

	private final int size;
	private final int expected;
	private final Semaphore sem;
	private final AtomicInteger nextSNtoRead;
	private final Map<Integer, ByteBuffer> cacheMap;
	private final ByteBuffer[] buffer;
	private final FileChannel toFile;
	private final Object lock;
	private int pos;

	public WriterThread (
		int size, 
		Semaphore sem, 
		AtomicInteger floor, 
		Map<Integer, ByteBuffer> cache,
		FileChannel fc,
		int total_pkts,
		Object floorLock
		) 
	{
		lock = floorLock;
		expected = total_pkts;
		this.size = size;
		nextSNtoRead = floor;
		this.sem = sem;
		cacheMap = cache;
		toFile = fc;
		buffer = new ByteBuffer[size];
		pos = 0;
	}

	public void run () {
		while (true) {

			try {
				sem.acquire();
			} catch (InterruptedException e) {
				System.out.println(e);
				e.printStackTrace();
			}

			getFromCache();

			// flush and terminate when complete
			if (nextSNtoRead.get() >= expected) { 
				break;
			}
		}
		finish();
	}
	private void getFromCache () {
		// tries to fill buffer in order from cache
		ByteBuffer data;
		while (true) {

			// syncs SEQNUM_FLOOR and CACHEMAP
			synchronized (lock) {
				data = cacheMap.remove(nextSNtoRead.get());
				if (data == null) {
					break; // next pkt in sequence is not in cache.
				}
				nextSNtoRead.getAndIncrement();
			}

			buffer[pos] = data;
			pos++;
			
			if (pos == size) { // flush check
				try {
					toFile.write(buffer);
				} catch (IOException e) {
					System.out.println("\nERROR WRITING TO FILE!!!\n" + e);
					e.printStackTrace();
				}
				pos = 0;
				System.out.println("buffer flushed before packet " + nextSNtoRead.get());
			}
		}
	}
	private void finish () {
		try {
			if (pos != 0) {
				toFile.write(buffer, 0, pos);
			}
			toFile.close();
		} catch (IOException e) {
				System.out.println("\nERROR WRITING TO FILE!!!\n" + e);
				e.printStackTrace();					
		}
		if (cacheMap.size() > 0) {
			System.out.println("Still has remaining items in cachemap??");
		}
		System.out.println("written " + (nextSNtoRead.get()-1) + " packets. writer thread closing...");
	}
}
