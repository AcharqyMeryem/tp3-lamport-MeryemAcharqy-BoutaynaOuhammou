import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LamportDistributedMutex {
    private static final int NB_PROCESSUS = 3;

    static class CanalReseau {
        private final Random random = new Random();
        private final AtomicInteger compteurMessages = new AtomicInteger(0);
        private final ScheduledExecutorService facteur = Executors.newSingleThreadScheduledExecutor();
        private final Map<String, Long> derniereLivraison = new HashMap<>();

        public synchronized void envoyerMessage(Processus src, Processus dest, Message msg) {
            compteurMessages.incrementAndGet();
            int delai = 300 + random.nextInt(500);
            String lien = src.id + "->" + dest.id;
            long livraison = System.currentTimeMillis() + delai;
            long precedente = derniereLivraison.getOrDefault(lien, 0L);
            if (livraison <= precedente) livraison = precedente + 1;
            derniereLivraison.put(lien, livraison);
            facteur.schedule(() -> dest.recevoirMessage(msg),
                    livraison - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        public int getNombreMessages() {
            return compteurMessages.get();
        }
    }

    enum TypeMessage {
        REQUEST, REPLY, RELEASE
    }

    static class Message {
        TypeMessage type;
        int horloge;
        int idSource;

        public Message(TypeMessage type, int horloge, int idSource) {
            this.type = type;
            this.horloge = horloge;
            this.idSource = idSource;
        }
    }

    static class Processus implements Runnable {
        int id;
        int horlogeLamport = 0;
        CanalReseau canal;
        List<Processus> autres;
        volatile boolean enPanne = false;

        Queue<Message> fileRequetes = new PriorityBlockingQueue<>(10,
                Comparator.comparingInt((Message m) -> m.horloge).thenComparingInt(m -> m.idSource));
        Set<Integer> reponsesRecues = ConcurrentHashMap.newKeySet();

        public Processus(int id, CanalReseau canal) {
            this.id = id;
            this.canal = canal;
        }

        public void setEnPanne(boolean enPanne) {
            this.enPanne = enPanne;
        }

        private synchronized void incrementerHorloge() {
            horlogeLamport++;
        }

        private void majHorloge(int autreHorloge) {
            horlogeLamport = Math.max(horlogeLamport, autreHorloge) + 1;
        }

        public synchronized Message diffuserMessage(TypeMessage type) {
            incrementerHorloge();
            Message msg = new Message(type, horlogeLamport, id);
            for (Processus p : autres) {
                if (p.id != id && !p.enPanne) {
                    canal.envoyerMessage(this, p, msg);
                }
            }
            return msg;
        }

        public synchronized void recevoirMessage(Message msg) {
            if (enPanne) return;

            majHorloge(msg.horloge);

            switch (msg.type) {
                case REQUEST:
                    fileRequetes.add(msg);
                    canal.envoyerMessage(this, trouverProcessus(msg.idSource), new Message(TypeMessage.REPLY, horlogeLamport, id));
                    break;

                case REPLY:
                    reponsesRecues.add(msg.idSource);
                    break;

                case RELEASE:
                    fileRequetes.removeIf(m -> m.idSource == msg.idSource);
                    break;
            }
        }

        private Processus trouverProcessus(int id) {
            return autres.stream().filter(p -> p.id == id).findFirst().orElse(null);
        }

        public void demanderSectionCritique() throws InterruptedException {
            if (enPanne) return;

            reponsesRecues.clear();
            Message demande = diffuserMessage(TypeMessage.REQUEST);
            fileRequetes.add(demande);
            System.out.println("P" + id + " demande la section critique (t=" + demande.horloge + ")");

            while (reponsesRecues.size() < autres.size() - 1 - nbPannes()) {
                if (enPanne) return;
                Thread.sleep(200);
            }

            while (true) {
                if (enPanne) return;
                fileRequetes.removeIf(m -> trouverProcessus(m.idSource).enPanne);
                Message premier = fileRequetes.peek();
                if (premier != null && premier.idSource == id) break;
                Thread.sleep(100);
            }

            entrerSectionCritique();
            sortirSectionCritique();
        }

        private long nbPannes() {
            return autres.stream().filter(p -> p.enPanne).count();
        }

        private void entrerSectionCritique() throws InterruptedException {
            System.out.println("P" + id + " entre en section critique (horloge=" + horlogeLamport + ")");
            Thread.sleep(1000 + new Random().nextInt(1000));
        }

        private void sortirSectionCritique() {
            System.out.println("P" + id + " quitte la section critique");
            fileRequetes.removeIf(m -> m.idSource == id);
            diffuserMessage(TypeMessage.RELEASE);

            afficherFile();
        }

        private void afficherFile() {
            System.out.print("File de P" + id + " : [");
            fileRequetes.stream()
                    .sorted(Comparator.comparingInt((Message m) -> m.horloge).thenComparingInt(m -> m.idSource))
                    .forEach(m -> System.out.print("P" + m.idSource + "@" + m.horloge + " "));
            System.out.println("]");
        }

        @Override
        public void run() {
            try {
                while (!enPanne) {
                    demanderSectionCritique();
                    Thread.sleep(2000);
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        CanalReseau canal = new CanalReseau();

        Processus p1 = new Processus(1, canal);
        Processus p2 = new Processus(2, canal);
        Processus p3 = new Processus(3, canal);

        List<Processus> tous = Arrays.asList(p1, p2, p3);
        for (Processus p : tous) {
            p.autres = new ArrayList<>(tous);
        }

        new Thread(p1).start();
        new Thread(p2).start();
        new Thread(p3).start();

        Thread.sleep(8000);
        System.out.println("P2 tombe en panne !");
        p2.setEnPanne(true);

        Thread.sleep(10000);

        System.out.println("\n===== STATISTIQUES =====");
        System.out.println("Nombre total de messages échangés : " + canal.getNombreMessages());
        System.out.println("=========================");
        System.exit(0);
    }
}
