package raft04;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * Estado de consenso de um nó. Todos os métodos públicos são sincronizados:
 * term e votedFor persistem, enquanto papel, líder conhecido e votos são voláteis.
 */
public final class RaftState {
    public enum Role { SEGUIDOR, CANDIDATO, LIDER }

    public static final class Vote {
        public final long term;
        public final boolean granted;

        Vote(long term, boolean granted) {
            this.term = term;
            this.granted = granted;
        }
    }

    private final int partition;
    private final int self;
    private final int groupSize;
    private final long minNanos;
    private final long maxNanos;
    private final Path stateFile;
    private final Consumer<String> log;
    private final Set<Integer> votes = new HashSet<>();

    // O voto é guardado antes de publicar mensagens para não votar duas vezes
    // no mesmo termo se o processo falhar imediatamente depois.
    private long term;
    private int votedFor;

    private Role role = Role.SEGUIDOR;
    private int leader;
    private long deadline;

    public RaftState(int partition, int self, int groupSize, long minMs, long maxMs,
                     Path stateFile, Consumer<String> log) throws IOException {
        if (partition < 0 || self < 1 || self > groupSize || groupSize < 1
                || minMs <= 0 || maxMs < minMs) {
            throw new IllegalArgumentException("Parâmetros RaftState inválidos");
        }
        this.partition = partition;
        this.self = self;
        this.groupSize = groupSize;
        this.minNanos = Math.multiplyExact(minMs, 1_000_000L);
        this.maxNanos = Math.multiplyExact(maxMs, 1_000_000L);
        this.stateFile = stateFile;
        this.log = log;

        if (Files.exists(stateFile)) {
            String[] fields = Files.readString(stateFile, StandardCharsets.UTF_8)
                    .trim().split(";", -1);
            if (fields.length != 2) throw new IOException("Estado Raft mal formado: " + stateFile);
            term = Long.parseLong(fields[0]);
            votedFor = Integer.parseInt(fields[1]);
            if (term < 0 || votedFor < 0 || votedFor > groupSize) {
                throw new IOException("Estado Raft fora do intervalo: " + stateFile);
            }
        }
        resetTimer();
    }

    private void resetTimer() {
        long delay = maxNanos == minNanos ? minNanos
                : ThreadLocalRandom.current().nextLong(minNanos, maxNanos + 1);
        deadline = System.nanoTime() + delay;
    }

    /** Escreve um temporário e troca-o atomicamente para não deixar estado parcial. */
    private void persist() {
        try {
            Path tmp = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
            Files.writeString(tmp, term + ";" + votedFor + "\n", StandardCharsets.UTF_8);
            Files.move(tmp, stateFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("Não foi possível persistir estado Raft", e);
        }
    }

    /** Adota um termo superior, limpa o voto anterior e abdica de qualquer papel ativo. */
    private void adoptTerm(long newTerm) {
        if (newTerm <= term) return;
        if (role == Role.LIDER || role == Role.CANDIDATO) {
            log.accept("DEMISSAO;p" + partition + ";termo=" + term);
        }
        long oldTerm = term;
        term = newTerm;
        votedFor = 0;
        role = Role.SEGUIDOR;
        leader = 0;
        votes.clear();
        persist();
        resetTimer();
        log.accept("TERMO;p" + partition + ";termo=" + newTerm + ";anterior=" + oldTerm);
    }

    /** Expira prazos de seguidores/candidatos; o termo e o auto-voto persistem primeiro. */
    public synchronized long tick() {
        if (role == Role.LIDER || System.nanoTime() < deadline) return -1;
        term++;
        votedFor = self;
        persist();
        role = Role.CANDIDATO;
        leader = 0;
        votes.clear();
        votes.add(self);
        resetTimer();
        log.accept("CANDIDATO;p" + partition + ";termo=" + term);
        return term;
    }

    public synchronized Vote onVoteRequest(long candidateTerm, int candidate,
                                           long candidateLastSeq, long candidateLastTerm,
                                           long myLastSeq, long myLastTerm) {
        if (candidate < 1 || candidate > groupSize) {
            return new Vote(term, false);
        }
        if (candidateTerm > term) adoptTerm(candidateTerm);
        boolean candidateIsUpToDate = candidateLastTerm > myLastTerm
                || (candidateLastTerm == myLastTerm && candidateLastSeq >= myLastSeq);
        boolean granted = candidateTerm == term
                && (votedFor == 0 || votedFor == candidate)
                && candidateIsUpToDate;
        if (granted) {
            votedFor = candidate;
            persist();
            // Uma concessão prova que há atividade de eleição: adia nova candidatura.
            resetTimer();
        }
        log.accept("VOTO;p" + partition + ";termo=" + term + ";candidato=n" + candidate
                + ";" + (granted ? "concedido" : "recusado"));
        return new Vote(term, granted);
    }

    /** Aceita votos apenas para a candidatura corrente e só assume com maioria estrita. */
    public synchronized boolean onVote(long voteTerm, int voter, int candidate,
                                       boolean granted) {
        if (voteTerm > term) {
            adoptTerm(voteTerm);
            return false;
        }
        if (role != Role.CANDIDATO || voteTerm != term || candidate != self
                || voter < 1 || voter > groupSize || !granted) return false;
        votes.add(voter);
        log.accept("VOTO_RECEBIDO;p" + partition + ";termo=" + term + ";de=n" + voter
                + ";votos=" + votes.size());
        if (votes.size() <= groupSize / 2) return false;
        role = Role.LIDER;
        leader = self;
        log.accept("LIDER;p" + partition + ";termo=" + term + ";votos=" + votes);
        return true;
    }

    /** Descarta lotes obsoletos e demite candidatos quando reconhecem um líder. */
    public synchronized boolean onBatch(long batchTerm, int batchLeader) {
        if (batchTerm < term || batchLeader < 1 || batchLeader > groupSize) return false;
        if (batchTerm > term) adoptTerm(batchTerm);
        if (role == Role.LIDER) {
            log.accept("ERRO;p" + partition + ";dois lideres no termo " + term);
            return false;
        }
        role = Role.SEGUIDOR;
        if (leader != batchLeader) {
            leader = batchLeader;
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + term + ";lider=n" + batchLeader);
        }
        resetTimer();
        return true;
    }

    public synchronized long termIfLeader() { return role == Role.LIDER ? term : -1; }
    public synchronized long term() { return term; }
    public synchronized Role role() { return role; }
    public synchronized int leader() { return leader; }
}
