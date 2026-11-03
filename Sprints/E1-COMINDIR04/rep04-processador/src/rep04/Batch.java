package rep04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class Batch {
    private final String address;
    private final int port;
    private final long inc;
    private final long hb;
    private final long lastSeq;
    private final List<SensorRecord> records;

    public Batch(String address, int port, long inc, long hb, long lastSeq,
                 List<SensorRecord> records) {
        if (address == null || address.isBlank() || address.indexOf(';') >= 0
                || port < 1 || port > 65535 || inc < 0 || hb < 1 || lastSeq < 0) {
            throw new IllegalArgumentException("Cabeçalho LOTE inválido");
        }
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
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
        StringBuilder message = new StringBuilder("LOTE;")
                .append(address).append(';').append(port).append(';').append(inc)
                .append(';').append(hb).append(';').append(lastSeq).append(';')
                .append(records.size());
        for (SensorRecord record : records) {
            message.append('\n').append(record.toLine());
        }
        return message.toString();
    }

    public static Batch decode(String message, int partitions) {
        String[] lines = message.split("\\R", -1);
        String[] header = lines[0].split(";", -1);
        if (header.length != 7 || !"LOTE".equals(header[0])) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado");
        }
        int count = Integer.parseInt(header[6]);
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
        return new Batch(header[1], Integer.parseInt(header[2]),
                Long.parseLong(header[3]), Long.parseLong(header[4]),
                Long.parseLong(header[5]), records);
    }

    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getInc() { return inc; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public List<SensorRecord> getRecords() { return records; }
}
