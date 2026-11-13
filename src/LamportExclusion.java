import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

class Message {
    public enum Type { REQUEST, REPLY, RELEASE }
    Type type;
    int senderId;
    int timestamp;

    public Message(Type type, int senderId, int timestamp) {
        this.type = type;
        this.senderId = senderId;
        this.timestamp = timestamp;
    }
}

class Processus extends Thread {
    private int id;
    private int clock = 0;
    private List<Processus> autres;
    private Queue<Message> fileMessages = new ConcurrentLinkedQueue<>();
    private boolean enSC = false;
    private Set<Integer> reponsesRecues = new HashSet<>();
    private List<Message> fileDemandes = new ArrayList<>();

    public Processus(int id) {
        this.id = id;
    }

    public void setAutres(List<Processus> autres) {
        this.autres = autres;
    }

    private synchronized void tick() {
        clock++;
    }

    private void majHorloge(int autreHorloge) {
        clock = Math.max(clock, autreHorloge) + 1;
    }

    private void envoyer(Message m, Processus dest) {
        dest.recoitMessage(m);
    }

    public synchronized void recoitMessage(Message m) {
        majHorloge(m.timestamp);
        fileMessages.add(m);
    }

    private Message broadcast(Message.Type type) {
        tick();
        Message m = new Message(type, id, clock);
        for (Processus p : autres) {
            if (p.id != this.id)
                envoyer(m, p);
        }
        return m;
    }

    private void demandeSC() throws InterruptedException {
        reponsesRecues.clear();
        reponsesRecues.add(id);
        Message demande = broadcast(Message.Type.REQUEST);
        fileDemandes.add(demande);
        System.out.println("Processus " + id + " demande SC à t=" + demande.timestamp);

        while (reponsesRecues.size() < autres.size() || !estPremier()) {
            traiterMessages();
            Thread.sleep(100);
        }
        entrerSC();
    }

    private boolean estPremier() {
        fileDemandes.sort(Comparator.comparingInt((Message m) -> m.timestamp)
                                    .thenComparingInt(m -> m.senderId));
        return !fileDemandes.isEmpty() && fileDemandes.get(0).senderId == id;
    }

    private void entrerSC() throws InterruptedException {
        enSC = true;
        System.out.println("==> Processus " + id + " entre en SC (t=" + clock + ")");
        Thread.sleep(1000);
        sortirSC();
    }

    private void sortirSC() {
        enSC = false;
        System.out.println("<== Processus " + id + " quitte SC (t=" + clock + ")");
        fileDemandes.removeIf(m -> m.senderId == id);
        broadcast(Message.Type.RELEASE);
    }

    private void traiterMessages() {
        while (!fileMessages.isEmpty()) {
            Message m = fileMessages.poll();
            switch (m.type) {
                case REQUEST:
                    fileDemandes.add(m);
                    envoyer(new Message(Message.Type.REPLY, id, clock), getProcessus(m.senderId));
                    break;
                case REPLY:
                    reponsesRecues.add(m.senderId);
                    break;
                case RELEASE:
                    fileDemandes.removeIf(x -> x.senderId == m.senderId);
                    break;
            }
        }
    }

    private Processus getProcessus(int id) {
        for (Processus p : autres) {
            if (p.id == id) return p;
        }
        return null;
    }

    @Override
    public void run() {
        try {
            for (int i = 0; i < 3; i++) {
                Thread.sleep(500 + new Random().nextInt(1000));
                demandeSC();
            }
            for (int i = 0; i < 100; i++) {
                traiterMessages();
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

public class LamportExclusion {
    public static void main(String[] args) {
        int N = 3;
        List<Processus> liste = new ArrayList<>();
        for (int i = 0; i < N; i++) liste.add(new Processus(i));

        for (Processus p : liste)
            p.setAutres(liste);

        for (Processus p : liste)
            p.start();
    }
}
