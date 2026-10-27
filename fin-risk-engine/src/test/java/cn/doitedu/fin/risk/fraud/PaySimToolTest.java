package cn.doitedu.fin.risk.fraud;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class PaySimToolTest {
    @TempDir Path directory;
    private static final String HEADER = "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud\n";

    @Test void reportsConfusionMatrixAndDoesNotUsePostTransactionBalancesOrLabelsForPrediction() throws Exception {
        Path csv = Files.writeString(directory.resolve("fixture.csv"), HEADER
                + "1,TRANSFER,1000,a,1000,0,b,0,1000,1,0\n"
                + "1,PAYMENT,20,c,100,80,d,0,20,0,0\n"
                + "1,CASH_OUT,200000,e,0,99,f,123,456,0,1\n"
                + "1,PAYMENT,2,g,100,0,h,0,0,1,1\n");
        var metrics = PaySimTool.replay(csv);
        assertEquals(1L, metrics.get("truePositive")); assertEquals(1L, metrics.get("falsePositive"));
        assertEquals(1L, metrics.get("trueNegative")); assertEquals(1L, metrics.get("falseNegative"));
        assertEquals(0.5, metrics.get("precision")); assertEquals(0.5, metrics.get("recall"));
        assertTrue(PaySimTool.historicalBlock("TRANSFER", 100_000, 100_000));
        assertFalse(PaySimTool.historicalBlock("PAYMENT", 100_000_000, 100));
        assertFalse(PaySimTool.historicalBlock("CASH_OUT", 100, 100));
    }

    @Test void refusesSchemaMismatchAndEmptySamplesHaveNoInventedAccuracy() throws Exception {
        Path wrong = Files.writeString(directory.resolve("wrong.csv"), "label,amount\n1,2\n");
        assertThrows(java.io.IOException.class, () -> PaySimTool.replay(wrong));
        var metrics = PaySimTool.replay(Files.writeString(directory.resolve("empty.csv"), HEADER));
        assertNull(metrics.get("precision")); assertNull(metrics.get("recall"));
    }

    @Test void cpuBenchmarkDoesNotLearnFromChangedHoldoutLabels() throws Exception {
        var original = new StringBuilder(HEADER);
        var changed = new StringBuilder(HEADER);
        for (int index = 0; index < 200; index++) {
            boolean fraud = index % 10 == 0;
            String prefix = "1," + (fraud ? "TRANSFER,1000" : "PAYMENT,20") + ",a,1000,0,b,0,0,";
            original.append(prefix).append(fraud ? 1 : 0).append(",0\n");
            changed.append(prefix).append(index < 140 ? (fraud ? 1 : 0) : (fraud ? 0 : 1)).append(",1\n");
        }
        var baseline = PaySimBenchmark.run(Files.writeString(directory.resolve("train.csv"), original));
        var relabelled = PaySimBenchmark.run(Files.writeString(directory.resolve("changed.csv"), changed));
        assertEquals(baseline.get("tree"), relabelled.get("tree"));
        assertNotEquals(baseline.get("truePositive"), relabelled.get("truePositive"));
        assertEquals(140, baseline.get("trainingRows"));
    }

    @Test void publicSourceRowsCanDriveTheActualPaymentWorkflowWithDeclaredSyntheticProfiles() throws Exception {
        Path csv = Files.writeString(directory.resolve("simulation.csv"), HEADER
                + "1,PAYMENT,20,a,1000,980,b,0,20,0,0\n"
                + "1,TRANSFER,181,c,181,0,d,0,181,1,0\n");
        var replay = PaySimScenarioReplay.run(csv);
        var cases = (java.util.List<?>) replay.get("cases");
        assertEquals(2, cases.size());
        assertEquals("PAID", ((java.util.Map<?,?>) cases.get(0)).get("paymentStatus"));
        assertEquals("REVIEW", ((java.util.Map<?,?>) cases.get(1)).get("paymentStatus"));
    }
}
