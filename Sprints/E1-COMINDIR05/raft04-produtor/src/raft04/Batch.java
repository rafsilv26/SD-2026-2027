package raft04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** LOTE do protocolo Raft04; os registos incluem o termo em que foram criados. */
public final class Batch {
    private final long term;
    private final int leader;
    private final String address;
    private final int port;
    private final long hb;
    private final long lastSeq;
    private final long lastTerm;
    private final List<SensorRecord> records;

    public Batch(long term, int leader, String address, int port, long hb,
                 long lastSeq, long lastTerm, List<SensorRecord> records) {
        if (term < 0 || leader < 1 || address == null || address.isBlank()
                || address.indexOf(';') >= 0 || port < 1 || port > 65535
                || hb < 1 || lastSeq < 0 || lastTerm < 0) {
            throw new IllegalArgumentException("Cabeçalho LOTE inválido");
        }
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
        long previous = 0;
        for (SensorRecord record : this.records) {
            if (record == null || record.getSeq() <= previous || record.getSeq() > lastSeq) {
                throw new IllegalArgumentException("Sequência inválida no LOTE");
            }
            previous = record.getSeq();
        }
    }

    public String encode() {
        StringBuilder text = new StringBuilder("LOTE;").append(term).append(';')
                .append(leader).append(';').append(address).append(';').append(port)
                .append(';').append(hb).append(';').append(lastSeq).append(';')
                .append(lastTerm).append(';').append(records.size());
        for (SensorRecord record : records) {
            text.append('\n').append(record.toLine());
        }
        return text.toString();
    }

    public static Batch decode(String message, int partitions) {
        String[] lines = message.split("\\R", -1);
        String[] f = lines[0].split(";", -1);
        if (f.length != 9 || !"LOTE".equals(f[0])) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado");
        }
        int count = Integer.parseInt(f[8]);
        if (count < 0 || lines.length != count + 1) {
            throw new IllegalArgumentException("Número de registos LOTE inválido");
        }
        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            SensorRecord record = SensorRecord.fromLine(lines[i]);
            if (record.getVector().size() != partitions) {
                throw new IllegalArgumentException("Vetor do registo tem dimensão inválida");
            }
            records.add(record);
        }
        return new Batch(Long.parseLong(f[1]), Integer.parseInt(f[2]), f[3],
                Integer.parseInt(f[4]), Long.parseLong(f[5]), Long.parseLong(f[6]),
                Long.parseLong(f[7]), records);
    }

    public long getTerm() { return term; }
    public int getLeader() { return leader; }
    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public long getLastTerm() { return lastTerm; }
    public List<SensorRecord> getRecords() { return records; }
}
