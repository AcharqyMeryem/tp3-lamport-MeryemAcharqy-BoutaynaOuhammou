import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LamportDistributedMutex {
    static final int NB_PROCESSUS = 3;
    static final int NB_ENTREES = 3;
    static final double PROBA_PANNE = 0.0;
    static final AtomicInteger entreesSC = new AtomicInteger(0);
    static final AtomicLong tempsAttenteTotal = new AtomicLong(0);
    static List<Processus> tous;

    static class CanalReseau {
        private final Random random = new Random();
        private final AtomicInteger compteurMessages = new AtomicInteger(0);
        static final double PROBA_PERTE = 0.0;
        private final AtomicInteger messagesPerdus = new AtomicInteger(0);
        private final ScheduledExecutorService facteur = Executors.newSingleThreadScheduledExecutor();
        private final Map<String, Long> derniereLivraison = new HashMap<>();

        public synchronized void envoyerMessage(Processus src, Processus dest, Message msg) {
            compteurMessages.incrementAndGet();
            if (random.nextDouble() < PROBA_PERTE) {
                messagesPerdus.incrementAndGet();
                System.out.println("   [PERTE] " + msg.type + " de P" + src.id + " vers P" + dest.id + " perdu !");
                return;
            }
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

        public int getMessagesPerdus() {
            return messagesPerdus.get();
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
        int nbEntrees = 0;
        long attenteProcessus = 0;

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
            long debutAttente = System.currentTimeMillis();

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

            long attente = System.currentTimeMillis() - debutAttente;
            nbEntrees++;
            attenteProcessus += attente;
            tempsAttenteTotal.addAndGet(attente);

            entrerSectionCritique(attente);
            sortirSectionCritique();
        }

        private long nbPannes() {
            return autres.stream().filter(p -> p.enPanne).count();
        }

        private void entrerSectionCritique(long attente) throws InterruptedException {
            entreesSC.incrementAndGet();
            System.out.println("P" + id + " entre en section critique (horloge=" + horlogeLamport + ", attente=" + attente + " ms)");
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
                Random random = new Random();
                while (!enPanne && nbEntrees < NB_ENTREES) {
                    demanderSectionCritique();
                    long vivants = tous.stream().filter(p -> !p.enPanne).count();
                    if (random.nextDouble() < PROBA_PANNE && vivants > 1) {
                        setEnPanne(true);
                        System.out.println("XXX P" + id + " tombe en panne (aléatoire) !");
                        break;
                    }
                    Thread.sleep(2000);
                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        CanalReseau canal = new CanalReseau();

        tous = new ArrayList<>();
        for (int i = 1; i <= NB_PROCESSUS; i++) {
            tous.add(new Processus(i, canal));
        }
        for (Processus p : tous) {
            p.autres = new ArrayList<>(tous);
        }

        System.out.println("Nombre de processus : " + NB_PROCESSUS + " | entrées par processus : " + NB_ENTREES);

        List<Thread> threads = new ArrayList<>();
        for (Processus p : tous) {
            Thread t = new Thread(p);
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }
        Thread.sleep(1000);

        System.out.println("\n===== STATISTIQUES =====");
        System.out.println("Nombre total de messages échangés : " + canal.getNombreMessages());
        System.out.println("Nombre de messages perdus : " + canal.getMessagesPerdus());
        System.out.println("Nombre d'entrées en SC : " + entreesSC.get());
        if (entreesSC.get() > 0) {
            System.out.println("Temps moyen d'attente : " + (tempsAttenteTotal.get() / entreesSC.get()) + " ms");
            System.out.println("Messages par entrée en SC : " + (canal.getNombreMessages() / entreesSC.get())
                    + "  (théorie 3(N-1) = " + (3 * (NB_PROCESSUS - 1)) + ")");
        }
        for (Processus p : tous) {
            String moyenne = p.nbEntrees > 0 ? (p.attenteProcessus / p.nbEntrees) + " ms" : "-";
            System.out.println("P" + p.id + (p.enPanne ? " : EN PANNE" : " : actif")
                    + " | entrées = " + p.nbEntrees + " | attente moyenne = " + moyenne);
        }
        System.out.println("=========================");
        System.exit(0);
    }
}
