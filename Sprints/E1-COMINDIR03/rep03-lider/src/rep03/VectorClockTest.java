package rep03;

import rep03.VectorClock.Order;

public class VectorClockTest {

    public static void main(String[] args) {
        int n = 3;

        VectorClock a1 = VectorClock.parse("0,2,0", n);
        VectorClock b1 = VectorClock.parse("1,2,0", n);
        verifica("antes",       a1.compare(b1), Order.BEFORE);
        verifica("depois",      b1.compare(a1), Order.AFTER);

        VectorClock a2 = VectorClock.parse("3,1,1", n);
        VectorClock b2 = VectorClock.parse("3,1,1", n);
        verifica("igual",       a2.compare(b2), Order.EQUAL);

        VectorClock a3 = VectorClock.parse("0,1,1", n);
        VectorClock b3 = VectorClock.parse("1,0,0", n);
        verifica("concorrente", a3.compare(b3), Order.CONCURRENT);

        try {
            VectorClock.parse("1,2,3,4", n);
            System.out.println("FALHOU: dimensão errada - devia ter lançado exceção");
        } catch (IllegalArgumentException e) {
            System.out.println("OK: dimensão errada - parse recusou (" + e.getMessage() + ")");
        }
    }

    private static void verifica(String caso, Order obtido, Order esperado) {
        String estado = obtido == esperado ? "OK" : "FALHOU";
        System.out.println(estado + ": " + caso + " - esperado " + esperado + ", obtido " + obtido);
    }
}
