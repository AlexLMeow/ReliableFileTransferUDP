import java.io.*;
import java.net.*;
import java.util.*;
import java.nio.charset.*;
import java.nio.ByteBuffer;
import java.util.zip.*;

public class FileReceiver {

	private static final Charset CHARSET_NET = StandardCharsets.UTF_8;

	private String DST_FILE_PATH;
	private SocketAddress SND_ADDR;

	private final BufferedOutputStream TO_FILE;
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
	private final DatagramPacket SACK;
	private final DatagramPacket FIN;
	private final int TOTAL_PKTS;

	private int pkts_received;

	public static void main (String[] args) throws Exception {
		if (args.length != 1) {
			log("Format: FileReceiver <listening port> <optional rcv buffer size in kilopkts>");
			return;
		}
		FileReceiver receiver = new FileReceiver(args); // also syncs
		receiver.rcvLoop().finish();
	}

	public FileReceiver (String[] args) throws Exception {

		SOCKET = new DatagramSocket(Integer.parseInt(args[0]));
		CHKSUM = new CRC32();
		pkts_received = 0;

		// create SACK packet
		final byte[] sackdat = new byte[PSIZE_SACK];
		CHKSUM.reset();
		CHKSUM.update((int) CTRL_SYN);
		ByteBuffer.wrap(sackdat).putInt((int)CHKSUM.getValue()).put(CTRL_SYN);
		SACK = new DatagramPacket(sackdat, PSIZE_SACK);
		// SACK created

		// create FIN packet
		final byte[] findat = new byte[PSIZE_FIN];
		CHKSUM.reset();
		CHKSUM.update((int) CTRL_FIN);
		ByteBuffer.wrap(findat).putInt((int)CHKSUM.getValue()).put(CTRL_FIN);
		FIN = new DatagramPacket(findat, PSIZE_FIN);
		// FIN packet created.

		//log("Syncing...");

		final byte[] syndat = new byte[PSIZE_MAX];
		final DatagramPacket SYN = new DatagramPacket(syndat, PSIZE_MAX);
		while (true) {

			SOCKET.receive(SYN);
			if (!isCorrupted(SYN)) {
				// parse SYN
				SND_ADDR = SYN.getSocketAddress();
				final ByteBuffer synbuf = ByteBuffer.wrap(SYN.getData());
				synbuf.position(INDEX_HDR_DATA);
				TOTAL_PKTS = synbuf.getInt();
				final byte[] pathBytes = new byte[SYN.getLength()-synbuf.position()];
				synbuf.get(pathBytes);
				DST_FILE_PATH = new String(pathBytes, CHARSET_NET);
				log("SYN parsed : Num pkts expected = " + TOTAL_PKTS + ", path = " + DST_FILE_PATH);
				// SYN parsed
				break;
			} else {
				//log("Corrupted SYN received, ignoring...");
			}
		}
		log("Syncing succeeded!\n");

		TO_FILE = new BufferedOutputStream(new FileOutputStream(DST_FILE_PATH), FILE_BUF_SIZE);
		SOCKET.connect(SND_ADDR);
		SOCKET.send(SACK); // after init 

		log("New FileReceiver created.\n");
	}

	private FileReceiver rcvLoop () throws IOException {

		final byte[] rcvdat = new byte[PSIZE_MAX+1];
		final DatagramPacket RCVPKT = new DatagramPacket(rcvdat, rcvdat.length);
		final byte[] ackdat = new byte[PSIZE_ACK];
		final DatagramPacket ACK = new DatagramPacket(ackdat, PSIZE_ACK);

		int rcvSeqNum;
		while (pkts_received < TOTAL_PKTS) {

			SOCKET.receive(RCVPKT);

			if (isCorrupted(RCVPKT)) {
				continue;
			}

			if (isDAT(RCVPKT)) {
				rcvSeqNum = getSeqNumDAT(RCVPKT);
				SOCKET.send(prepareACK(rcvSeqNum, ACK)); // hurry up and return the ACK
				if (rcvSeqNum != pkts_received) { // not the pkt we are waiting for
					continue; 
				}
				writeDataFromPkt(RCVPKT);
				//log(pkts_received);
				pkts_received++;
				continue;
			}

			if (isSYN(RCVPKT)) {
				SOCKET.send(SACK);
				continue;
			}

			log("UNKNOWN PACKET: \n" + baToString(RCVPKT.getData(), 0, RCVPKT.getLength()));
			assert false;
		}
		return this;
	}
	private FileReceiver finish () throws IOException {

		TO_FILE.flush();
		TO_FILE.close();
		SOCKET.setSoTimeout(SK_TIMEOUT);

		final byte[] rcvdat = new byte[PSIZE_MAX+1];
		final DatagramPacket RCVPKT = new DatagramPacket(rcvdat, rcvdat.length);
		final byte[] ackdat = new byte[PSIZE_ACK];
		final DatagramPacket ACK = new DatagramPacket(ackdat, PSIZE_ACK);

		while (true) {
			try {
				SOCKET.send(FIN);
				SOCKET.receive(RCVPKT);
				if (isCorrupted(RCVPKT)) {
					continue;
				}
				if (isFIN(RCVPKT)) {
					break;
				}
				if (isDAT(RCVPKT)) { // immediately ACK remaining out of order pkts
					SOCKET.send(prepareACK(getSeqNumDAT(RCVPKT), RCVPKT));
					continue;
				}
			} catch (SocketTimeoutException ste) {
				;
			}
		}
		SOCKET.close();
		return this;
	}

	private DatagramPacket prepareACK (int sN, DatagramPacket pkt) {
		final ByteBuffer ackbuf = ByteBuffer.wrap(pkt.getData()).put(INDEX_CTRL, CTRL_DAT).putInt(INDEX_HDR_DATA, sN);
		CHKSUM.reset();
		CHKSUM.update(pkt.getData(), INDEX_CTRL, PSIZE_ACK - INDEX_CTRL);
		ackbuf.putInt(0, (int)CHKSUM.getValue());
		return pkt;
	}

	private void writeDataFromPkt (DatagramPacket pkt) throws IOException {
		TO_FILE.write(pkt.getData(), INDEX_BODY, pkt.getLength() - INDEX_BODY);
	}
	private int getSeqNumDAT (DatagramPacket pkt) {
		return ByteBuffer.wrap(pkt.getData()).getInt(INDEX_HDR_DATA);
	}
	private boolean isDAT (DatagramPacket pkt) {
		return pkt.getData()[INDEX_CTRL] == CTRL_DAT;
	}
	private boolean isSYN (DatagramPacket pkt) {
		return pkt.getData()[INDEX_CTRL] == CTRL_SYN;
	}
	private boolean isFIN (DatagramPacket pkt) {
		return pkt.getData()[INDEX_CTRL] == CTRL_FIN;
	}
	private boolean isCorrupted (DatagramPacket pkt) {
		final int len = pkt.getLength();
		if (len > PSIZE_MAX || (len != PSIZE_FIN && len < INDEX_BODY)) {
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