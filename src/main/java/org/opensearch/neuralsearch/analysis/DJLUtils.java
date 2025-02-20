/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;

/**
 * Utility class for DJL (Deep Java Library) operations related to tokenization and model handling.
 */
public class DJLUtils {
    static Path ML_CACHE_PATH;
    private static final String ML_CACHE_DIR_NAME = "ml_cache";

    /**
     * Builds the DJL cache path based on the OpenSearch data folder.
     * @param opensearchDataFolder The base OpenSearch data folder path
     */
    public static void buildDJLCachePath(Path opensearchDataFolder) {
        // the logic to build cache path is consistent with ml-commons plugin
        // see
        // https://github.com/opensearch-project/ml-commons/blob/14b971214c488aa3f4ab150d1a6cc379df1758be/ml-algorithms/src/main/java/org/opensearch/ml/engine/MLEngine.java#L53
        ML_CACHE_PATH = opensearchDataFolder.resolve(ML_CACHE_DIR_NAME);
    }

    private static <T> T withDJLContext(Callable<T> action) throws PrivilegedActionException {
        return AccessController.doPrivileged((PrivilegedExceptionAction<T>) () -> {
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
                System.setProperty("java.library.path", ML_CACHE_PATH.toAbsolutePath().toString());
                System.setProperty("DJL_CACHE_DIR", ML_CACHE_PATH.toAbsolutePath().toString());
                Thread.currentThread().setContextClassLoader(ai.djl.Model.class.getClassLoader());

                return action.call();
            } finally {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        });
    }

    /**
     * Creates a new HuggingFaceTokenizer instance for the given tokenizer ID.
     * @param resourcePath The resource path of the tokenizer to create
     * @return A new HuggingFaceTokenizer instance
     * @throws RuntimeException if tokenizer initialization fails
     */
    public static HuggingFaceTokenizer buildHuggingFaceTokenizer(String resourcePath) {
        try {
            return withDJLContext(() -> {
                InputStream is = DJLUtils.class.getResourceAsStream(resourcePath);
                if (Objects.isNull(is)) {
                    throw new IllegalArgumentException("Invalid resource path " + resourcePath);
                }
                return HuggingFaceTokenizer.newInstance(is, null);
            });
        } catch (PrivilegedActionException e) {
            throw new RuntimeException("Failed to initialize Hugging Face tokenizer. " + e);
        }
    }

    private static Map<String, Float> parseInputStreamToTokenWeights(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            Map<String, Float> tokenWeights = new HashMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                String[] parts = line.split("\t");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Invalid line in token weights file: " + line);
                }
                String token = parts[0];
                float weight = Float.parseFloat(parts[1]);
                tokenWeights.put(token, weight);
            }
            return tokenWeights;
        }
    }

    /**
     * Fetches token weights from a specified file for a given tokenizer.
     * @param resourcePath The resource path of the tokenizer to create
     * @return A map of token to weight mappings
     * @throws RuntimeException if file fetching or parsing fails
     */
    public static Map<String, Float> fetchTokenWeights(String resourcePath) {
        try (InputStream is = DJLUtils.class.getResourceAsStream(resourcePath)) {
            if (Objects.isNull(is)) {
                throw new IllegalArgumentException("Invalid resource path " + resourcePath);
            }
            return parseInputStreamToTokenWeights(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse token weights file.  " + e);
        }
    }
}
