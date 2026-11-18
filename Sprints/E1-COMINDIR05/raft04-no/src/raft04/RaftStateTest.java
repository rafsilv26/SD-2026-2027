package raft04;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Test executável, sem dependências externas, das regras essenciais de eleição. */
public final class RaftStateTest {
    private RaftStateTest() { }

    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("raft04-state-test");
        try {
            Path candidateFile = directory.resolve("n1.state");
            List<String> events = new ArrayList<>();
            RaftState candidate = new RaftState(0, 1, 3, 1, 1,
                    candidateFile, events::add);
            Thread.sleep(5);
            long term = candidate.tick();
            require(term == 1, "primeiro timeout deve iniciar o termo 1");
            require(candidate.onVote(term, 2, 1, true),
                    "um voto remoto mais o próprio voto devem eleger o candidato");
            require(candidate.role() == RaftState.Role.LIDER, "candidato deve ser líder");
            require(Files.readString(candidateFile).trim().equals("1;1"),
                    "termo e auto-voto devem estar persistidos");

            Path voterFile = directory.resolve("n2.state");
            RaftState voter = new RaftState(0, 2, 3, 1000, 1000,
                    voterFile, events::add);
            RaftState.Vote first = voter.onVoteRequest(1, 1, 0, 0, 0, 0);
            RaftState.Vote second = voter.onVoteRequest(1, 3, 0, 0, 0, 0);
            require(first.granted, "o primeiro voto elegível deve ser concedido");
            require(!second.granted, "não se pode votar em dois candidatos no mesmo termo");
            RaftState restartedVoter = new RaftState(0, 2, 3, 1000, 1000,
                    voterFile, events::add);
            require(!restartedVoter.onVoteRequest(1, 3, 0, 0, 0, 0).granted,
                    "o voto tem de continuar protegido depois do reinício");

            RaftState lagVoter = new RaftState(0, 3, 3, 1000, 1000,
                    directory.resolve("n3.state"), events::add);
            require(!lagVoter.onVoteRequest(1, 2, 1, 0, 2, 1).granted,
                    "não se deve votar num candidato com registo menos atualizado");
            require(lagVoter.onVoteRequest(1, 2, 2, 1, 2, 1).granted,
                    "candidato igualmente atualizado pode receber o voto");

            VectorClock vector = new VectorClock(3);
            vector.tick(0);
            SensorRecord record = SensorRecord.now(1, 2, "S01", 20.0, 1, vector);
            Batch batch = new Batch(2, 1, "127.0.0.1", 7200, 1, 1, 2,
                    List.of(record));
            Batch decoded = Batch.decode(batch.encode(), 3);
            require(decoded.getTerm() == 2 && decoded.getRecords().get(0).getTerm() == 2,
                    "termo do lote e dos registos deve sobreviver à serialização");

            require(!candidate.onBatch(0, 2), "lote de termo anterior deve ser ignorado");
            require(candidate.onBatch(2, 3), "o lote de termo superior deve ser aceite");
            require(candidate.term() == 2 && candidate.role() == RaftState.Role.SEGUIDOR,
                    "termo superior deve demitir o líder obsoleto");
            System.out.println("RaftStateTest: OK");
        } finally {
            try (var files = Files.list(directory)) {
                for (Path file : (Iterable<Path>) files::iterator) Files.deleteIfExists(file);
            }
            Files.deleteIfExists(directory);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
