import java.io.*;
import java.net.*;
import java.util.*;
import java.nio.charset.*;
import java.nio.ByteBuffer;
import java.util.zip.*;

public class FileSender {

	private static final Charset CHARSET_NET = StandardCharsets.UTF_8;

	private final String SRC_FILE_PATH;
	private final String DST_FILE_PATH;
	private final SocketAddress RCV_ADDR;

	private final BufferedInputStream FROM_FILE;
	private final DatagramSocket SOCKET;
	private final Checksum CHKSUM;

	private static final int INDEX_CTRL = 4;
	private static final int INDEX_HDR_DATA = 5;
	private static final int INDEX_BODY = 9;
	private static final byte CTRL_SYN = 1;
	private static final byte CTRL_DAT = 0;
	private static final byte CTRL_FIN = -1;

	private static final int PSIZE_SACK = 5;
	private static final int PSIZE_ACK = 9;
	private static final int PSIZE_FIN = 5;
	private static final int PSIZE_MAX = 1000;
	private static final int MAX_BODY_SIZE = PSIZE_MAX - INDEX_BODY;
	private static final int FILE_BUF_SIZE = 8*1024*MAX_BODY_SIZE;

	private static final int SK_TIMEOUT = 8;
	private final int TOTAL_PKTS;
	private final DatagramPacket SYN;
	private final DatagramPacket FIN;

	private int pkts_sent;

	public static void main (String[] args) throws Exception {
		if (args.length != 4) {
			log("Format: FileSender <hostname> <port> <source file> <dest file>");
			return;
		}
		final long start = System.nanoTime();
		FileSender sender = new FileSender(args);
		sender.sync().sndLoop().finish();
		log((System.nanoTime() - start)/1000000000 + "s");
	}

	public FileSender (String[] args) throws Exception {

		// Initialise file IO
		SRC_FILE_PATH = args[2].trim();
		DST_FILE_PATH = args[3].trim();
		final FileInputStream fis = new FileInputStream(SRC_FILE_PATH);
		FROM_FILE = new BufferedInputStream(fis, FILE_BUF_SIZE);

		// get number of packets
		final long size = fis.getChannel().size();
		log("File Size: " + size/1024 + "KB");
		int numpkts = (int) (size / ((long)MAX_BODY_SIZE));
		if (numpkts < 0) {throw new Exception("file too big!");}
		if (size % ((long)MAX_BODY_SIZE) != 0) {
			numpkts++;
		}
		if (numpkts < 0) {throw new Exception("file too big!");}
		TOTAL_PKTS = numpkts;

		// Initialise net IO
		RCV_ADDR = new InetSocketAddress(args[0], Integer.parseInt(args[1]));
		SOCKET = new DatagramSocket();
		SOCKET.connect(RCV_ADDR);
		SOCKET.setSoTimeout(SK_TIMEOUT);

		// Other misc init
		CHKSUM = new CRC32();
		pkts_sent = 0;

		// create unique SYN packet
		byte[] destPath = DST_FILE_PATH.getBytes(CHARSET_NET);
		final byte[] syndat = new byte[destPath.length + INDEX_BODY];
		final ByteBuffer synBB = ByteBuffer.wrap(syndat);
		synBB.position(INDEX_CTRL);
		synBB.put(CTRL_SYN).putInt(TOTAL_PKTS).put(destPath);
		CHKSUM.reset();
		CHKSUM.update(syndat, INDEX_CTRL, syndat.length-INDEX_CTRL);
		synBB.putInt(0, (int)CHKSUM.getValue());
		SYN = new DatagramPacket(syndat, syndat.length);
		// SYN packet created.

		// create FIN packet
		final byte[] findat = new byte[PSIZE_FIN];
		CHKSUM.reset();
		CHKSUM.update((int) CTRL_FIN);
		ByteBuffer.wrap(findat).putInt((int)CHKSUM.getValue()).put(CTRL_FIN);
		FIN = new DatagramPacket(findat, PSIZE_FIN);
		// FIN packet created.

		log("New FileSender created.\n");
	}

	private FileSender sync () throws Exception {
		//log("Syncing...");
		// prepare for handshaking
		final byte[] sackdat = new byte[PSIZE_SACK];
		final DatagramPacket SACK = new DatagramPacket(sackdat, PSIZE_SACK);
		//log("Sending SYN...");
		while (true) {
			SOCKET.send(SYN);
			SOCKET.send(SYN); // send twice
			try {
				SOCKET.receive(SACK);
				if (!isCorrupted(SACK)) {
					//log("SACK success!");
					break; // uncorrupted success SACK received. sync complete.
				}
				//log("corrupted/failure SACK, resending SYN...");
			} catch (SocketTimeoutException ste) {
				//log("SACK timeout, resend SYN...");
			}
		}
		log("Syncing succeeded!\n");
		return this;
	}

	private FileSender sndLoop () throws Exception {

		final byte[] filedat = new byte[PSIZE_MAX];
		final DatagramPacket SNDPKT = new DatagramPacket(filedat, filedat.length);
		final byte[] rcvdat = new byte[PSIZE_ACK+1];
		final DatagramPacket RCVPKT = new DatagramPacket(rcvdat, rcvdat.length);

		while (pkts_sent < TOTAL_PKTS) {

			readDataIntoPkt(SNDPKT);
			SOCKET.send(SNDPKT);

			// This loop ensures the packet is received by the rcver
			while (true) {
				try {

					SOCKET.receive(RCVPKT);

					if (isCorrupted(RCVPKT)) { // resend and re-wait
						SOCKET.send(SNDPKT);
						continue;
					}

					if (isACK(RCVPKT)) {
						if (getSeqNumACK(RCVPKT) != pkts_sent) { // ack not for this packet
							continue; // try to receive another packet
						}
						break; // PKT ACKNOWLEDGED!!!!!!!!
					}
					if (isFIN(RCVPKT)) { // end sndLoop (out of order fin packet)
						log("FIN received out of sequence, short circuiting to finish()!");
						return this; 
					}
					if (isSACK(RCVPKT)) {
						continue; // try to receive another packet
					}

					log("UNKNOWN PACKET: \n" + baToString(RCVPKT.getData(), 0, RCVPKT.getLength()));
					assert(false);
					throw new Exception("WHAT JOKE PACKET??");

				} catch (SocketTimeoutException ste) { // resend and re-wait
					SOCKET.send(SNDPKT);
				}
			}
			// CURRENT PKT ACKNOWLEDGED, INCRM
			if (pkts_sent % 1024 == 0) log(pkts_sent);
			pkts_sent++;
		}
		log("All packets acknowledged! Waiting for FIN!");

		SOCKET.setSoTimeout(0);
		while (true) { // wait till FIN comes
			SOCKET.receive(RCVPKT);
			if (isCorrupted(RCVPKT)) {
				continue;
			}
			if (isFIN(RCVPKT)) {
				return this;
			}
		}
	}
	private FileSender finish () throws Exception {
		for (int i = 0; i < 20; i++) {
			SOCKET.send(FIN);
		}
		FROM_FILE.close();
		SOCKET.close();
		return this;
	}

	private DatagramPacket readDataIntoPkt (DatagramPacket pkt) throws IOException {
		final ByteBuffer datbuf = ByteBuffer.wrap(pkt.getData());

		datbuf.put(INDEX_CTRL, CTRL_DAT).putInt(INDEX_HDR_DATA, pkts_sent);
		final int bread = FROM_FILE.read(pkt.getData(), INDEX_BODY, MAX_BODY_SIZE);
		pkt.setLength(INDEX_BODY + bread);

		CHKSUM.reset();
		CHKSUM.update(pkt.getData(), INDEX_CTRL, pkt.getLength() - INDEX_CTRL);
		datbuf.putInt(0, (int)CHKSUM.getValue());

		//pkts_sent++;
		return pkt;
	}

	private int getSeqNumACK (DatagramPacket pkt) {
		return ByteBuffer.wrap(pkt.getData()).getInt(INDEX_HDR_DATA);
	}
	private boolean isACK (DatagramPacket pkt) {
		return pkt.getData()[INDEX_CTRL] == CTRL_DAT;
	}
	private boolean isSACK (DatagramPacket pkt) {
		return pkt.getData()[INDEX_CTRL] == CTRL_SYN;
	}
	private boolean isFIN (DatagramPacket pkt) {
		return pkt.getData()[INDEX_CTRL] == CTRL_FIN;
	}
	private boolean isCorrupted (DatagramPacket pkt) {
		final int len = pkt.getLength();
		if (len != PSIZE_ACK && len != PSIZE_FIN && len != PSIZE_SACK) {
			return true;
		}
		final ByteBuffer data = ByteBuffer.wrap(pkt.getData());
		final int expected = data.getInt();
		CHKSUM.reset();
		CHKSUM.update(pkt.getData(), INDEX_CTRL, len - INDEX_CTRL);
		return expected != ((int)CHKSUM.getValue());
	}
	private String baToString (byte[] arr, int off, int stop) {
		final StringBuilder sb = new StringBuilder();
		sb.append("[ ");
		for (int i = off; i < stop; i++) {
			sb.append((int) arr[i]);
			sb.append(' ');
		}
		sb.append(']');
		return sb.toString();
	}
	public static void log (Object o) {System.out.println(o);}
}