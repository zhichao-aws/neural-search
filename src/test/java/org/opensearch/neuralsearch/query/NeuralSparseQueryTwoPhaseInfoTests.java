/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.SneakyThrows;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

public class NeuralSparseQueryTwoPhaseInfoTests extends OpenSearchTestCase {
    public void testDefaultConstructor() {
        NeuralSparseQueryTwoPhaseInfo info = new NeuralSparseQueryTwoPhaseInfo();
        assertEquals(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.NOT_ENABLED, info.getStatus());
        assertEquals(0f, info.getTwoPhasePruneRatio(), 0f);
    }

    public void testParameterizedConstructor() {
        NeuralSparseQueryTwoPhaseInfo info = new NeuralSparseQueryTwoPhaseInfo(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PARENT, 0.5F);
        assertEquals(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PARENT, info.getStatus());
        assertEquals(0.5F, info.getTwoPhasePruneRatio(), 0f);
    }

    @SneakyThrows
    public void testStreams() {
        NeuralSparseQueryTwoPhaseInfo original = new NeuralSparseQueryTwoPhaseInfo(
            NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PARENT,
            0.5F
        );

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        original.writeTo(streamOutput);

        NeuralSparseQueryTwoPhaseInfo copy = new NeuralSparseQueryTwoPhaseInfo(streamOutput.bytes().streamInput());
        assertEquals(original.getTwoPhasePruneRatio(), copy.getTwoPhasePruneRatio(), 0f);
        assertEquals(original.getStatus(), copy.getStatus());
    }

    public void testTwoPhaseStatusFromInt() {
        assertEquals(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.NOT_ENABLED, NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.fromInt(0));
        assertEquals(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.PARENT, NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.fromInt(1));
        assertEquals(NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.CHILD, NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.fromInt(2));
    }

    public void testTwoPhaseStatusFromInt_invalidValue_thenFailed() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> NeuralSparseQueryTwoPhaseInfo.TwoPhaseStatus.fromInt(10));
        assertEquals("Invalid two phase status value: 10", exception.getMessage());
    }
}
