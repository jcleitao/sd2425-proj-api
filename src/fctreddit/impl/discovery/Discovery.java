package fctreddit.impl.discovery;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.logging.Logger;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A class to perform service discovery, based on periodic service contact
 * endpoint announcements over multicast communication.
 */
public class Discovery {
    private static Logger Log = Logger.getLogger(Discovery.class.getName());

    static {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("java.util.logging.SimpleFormatter.format", "%4$s: %5$s");
    }

    public static final InetSocketAddress DISCOVERY_ADDR = new InetSocketAddress("226.226.226.226", 2266);
    static final int DISCOVERY_ANNOUNCE_PERIOD = 1000;
    static final int DISCOVERY_RETRY_TIMEOUT = 5000;
    static final int MAX_DATAGRAM_SIZE = 65536;
    private static final String DELIMITER = "\t";

    private final InetSocketAddress addr;
    private final String serviceName;
    private final String serviceURI;
    private final MulticastSocket ms;

    private final ConcurrentMap<String, Set<URI>> discoveredServices = new ConcurrentHashMap<>();
    private final Object lock = new Object(); // Usado para evitar busy waiting

    public Discovery(InetSocketAddress addr, String serviceName, String serviceURI)
            throws IOException {
        this.addr = addr;
        this.serviceName = serviceName;
        this.serviceURI = serviceURI;

        if (this.addr == null) {
            throw new RuntimeException("A multicast address has to be provided.");
        }

        this.ms = new MulticastSocket(addr.getPort());
        this.ms.joinGroup(addr, NetworkInterface.getByInetAddress(InetAddress.getLocalHost()));
    }

    public Discovery(InetSocketAddress addr) throws IOException {
        this(addr, null, null);
    }

    public void start() {
        if (this.serviceName != null && this.serviceURI != null) {
            Log.info(String.format("Starting Discovery announcements on: %s for: %s -> %s",
                    addr, serviceName, serviceURI));

            byte[] announceBytes = String.format("%s%s%s", serviceName, DELIMITER, serviceURI).getBytes();
            DatagramPacket announcePkt = new DatagramPacket(announceBytes, announceBytes.length, addr);

            new Thread(() -> {
                for (;;) {
                    try {
                        ms.send(announcePkt);
                        Thread.sleep(DISCOVERY_ANNOUNCE_PERIOD);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        // Thread para receber anúncios
        new Thread(() -> {
            DatagramPacket pkt = new DatagramPacket(new byte[MAX_DATAGRAM_SIZE], MAX_DATAGRAM_SIZE);
            for (;;) {
                try {
                    pkt.setLength(MAX_DATAGRAM_SIZE);
                    ms.receive(pkt);
                    String msg = new String(pkt.getData(), 0, pkt.getLength());
                    String[] msgElems = msg.split(DELIMITER);

                    if (msgElems.length == 2) {
                        String name = msgElems[0];
                        URI uri = URI.create(msgElems[1]);

                        synchronized (lock) {
                            discoveredServices
                                    .computeIfAbsent(name, k -> ConcurrentHashMap.newKeySet())
                                    .add(uri);
                            lock.notifyAll(); // Notifica quem está à espera
                        }

                        System.out.printf("FROM %s (%s) : %s\n", pkt.getAddress().getHostName(),
                                pkt.getAddress().getHostAddress(), msg);
                    }
                } catch (IOException e) {
                    // do nothing
                }
            }
        }).start();
    }

    public URI[] knownUrisOf(String serviceName, int minReplies) {
        long deadline = System.currentTimeMillis() + DISCOVERY_RETRY_TIMEOUT;

        synchronized (lock) {
            while (System.currentTimeMillis() < deadline) {
                Set<URI> uris = discoveredServices.get(serviceName);
                if (uris != null && uris.size() >= minReplies) {
                    return uris.toArray(new URI[0]);
                }

                long waitTime = deadline - System.currentTimeMillis();
                if (waitTime <= 0) break;

                try {
                    lock.wait(waitTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            Set<URI> uris = discoveredServices.get(serviceName);
            if (uris != null) {
                return uris.toArray(new URI[0]);
            }

            return new URI[0];
        }
    }

    // Apenas para testes
    public static void main(String[] args) throws Exception {
        Discovery discovery = new Discovery(DISCOVERY_ADDR, "test",
                "http://" + InetAddress.getLocalHost().getHostAddress());
        discovery.start();
    }
}
