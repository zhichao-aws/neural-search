/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.pruning;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PruningUtils {
    public static Map<String, Float> pruningByTopK(Map<String, Float> sparseVector, float lambda) {
        List<Map.Entry<String, Float>> list = new ArrayList<>(sparseVector.entrySet());
        list.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

        // 3. 取出前lambda个元素,构造一个新的Map返回
        Map<String, Float> result = new HashMap<>();
        int k = (int) lambda;
        for (int i = 0; i < k && i < list.size(); i++) {
            Map.Entry<String, Float> entry = list.get(i);
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

    public static Map<String, Float> pruningByMaxRatio(Map<String, Float> sparseVector, float ratio) {
        float maxValue = sparseVector.values().stream().max(Float::compareTo).orElse(0f);

        Map<String, Float> result = new HashMap<>();
        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            float currentValue = entry.getValue();
            float currentRatio = currentValue / maxValue;

            if (currentRatio >= ratio) {
                result.put(entry.getKey(), entry.getValue());
            }
        }

        return result;
    }

    public static Map<String, Float> pruningByValue(Map<String, Float> sparseVector, float lambda) {
        Map<String, Float> result = new HashMap<>(sparseVector);
        for (Map.Entry<String, Float> entry : sparseVector.entrySet()) {
            float currentValue = Math.abs(entry.getValue());
            if (currentValue < lambda) {
                result.remove(entry.getKey());
            }
        }

        return result;
    }

    public static Map<String, Float> pruningByAlphaMass(Map<String, Float> sparseVector, float lambda) {
        List<Map.Entry<String, Float>> sortedEntries = new ArrayList<>(sparseVector.entrySet());
        sortedEntries.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

        float sum = (float) sparseVector.values().stream().mapToDouble(Float::doubleValue).sum();
        float topSum = 0f;

        Map<String, Float> result = new HashMap<>();
        for (Map.Entry<String, Float> entry : sortedEntries) {
            float value = entry.getValue();
            topSum += value;
            result.put(entry.getKey(), value);

            if (topSum / sum >= lambda) {
                break;
            }
        }

        return result;
    }

    public static Map<String, Float> pruningSparseVector(String pruningType, float lambda, Map<String, Float> sparseVector) {
        switch (pruningType) {
            case "top_k":
                return pruningByTopK(sparseVector, lambda);
            case "alpha_mass":
                return pruningByAlphaMass(sparseVector, lambda);
            case "max_ratio":
                return pruningByMaxRatio(sparseVector, lambda);
            case "abs_value":
                return pruningByValue(sparseVector, lambda);
            default:
                return sparseVector;
        }
    }
}
