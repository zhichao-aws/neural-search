/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analyzer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;
import org.opensearch.ml.common.exception.MLException;

import com.google.common.io.CharStreams;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class HFModelTokenizer extends Tokenizer {
    public static String ML_PATH = null;
    public static String NAME = "model_tokenizer";

    private final CharTermAttribute termAtt;

    // payload分数属性
    private final PayloadAttribute payloadAtt;

    private HuggingFaceTokenizer tokenizer;
    private Map<String, Float> idf;

    private Encoding encoding;

    private int tokenIdx = 0;
    private int overflowingIdx = 0;

    public static HuggingFaceTokenizer initializeHFTokenizer(String name) {
        return withDJLContext(() -> HuggingFaceTokenizer.newInstance(name));
    }

    public static float bytesRefToFloat(BytesRef bytesRef) {
        if (bytesRef.length != 4) {
            throw new IllegalArgumentException("BytesRef must have length 4 to represent a float");
        }

        int intBits = 0;
        intBits |= (bytesRef.bytes[bytesRef.offset] & 0xFF) << 24;
        intBits |= (bytesRef.bytes[bytesRef.offset + 1] & 0xFF) << 16;
        intBits |= (bytesRef.bytes[bytesRef.offset + 2] & 0xFF) << 8;
        intBits |= (bytesRef.bytes[bytesRef.offset + 3] & 0xFF);

        return Float.intBitsToFloat(intBits);
    }

    public static HuggingFaceTokenizer initializeHFTokenizerFromConfigString(String configString) {
        return withDJLContext(() -> {
            InputStream inputStream = new ByteArrayInputStream(configString.getBytes(StandardCharsets.UTF_8));
            try {
                return HuggingFaceTokenizer.newInstance(inputStream, null);
            } catch (IOException e) {
                throw new IllegalArgumentException("Fail to create tokenizer. " + e.getMessage());
            }
        });
    }

    public static HuggingFaceTokenizer initializeHFTokenizerFromResources() {
        return withDJLContext(() -> {
            try {
                return HuggingFaceTokenizer.newInstance(HFModelTokenizer.class.getResourceAsStream("tokenizer.json"), null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static Map<String, Float> getIDFFromResources() {
        InputStream stream = HFModelTokenizer.class.getResourceAsStream("idf.json");
        Gson gson = new Gson();
        Type type = new TypeToken<Map<String, Float>>() {
        }.getType();

        try (InputStreamReader reader = new InputStreamReader(stream)) {
            Map<String, Float> idfMap = gson.fromJson(reader, type);
            return idfMap;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static HuggingFaceTokenizer withDJLContext(Supplier<HuggingFaceTokenizer> tokenizerSupplier) {
        try {
            return AccessController.doPrivileged((PrivilegedExceptionAction<HuggingFaceTokenizer>) () -> {
                ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
                try {
                    System.setProperty("java.library.path", ML_PATH);
                    System.setProperty("DJL_CACHE_DIR", ML_PATH);
                    Thread.currentThread().setContextClassLoader(ai.djl.Model.class.getClassLoader());

                    System.out.println("Current classloader: " + Thread.currentThread().getContextClassLoader());
                    System.out.println("java.library.path: " + System.getProperty("java.library.path"));
                    return tokenizerSupplier.get();
                } finally {
                    Thread.currentThread().setContextClassLoader(contextClassLoader);
                }
            });
        } catch (PrivilegedActionException e) {
            throw new MLException("error", e);
        }
    }

    public HFModelTokenizer() {
        this(HFModelTokenizer.initializeHFTokenizer("bert-base-uncased"));
    }

    public HFModelTokenizer(HuggingFaceTokenizer tokenizer) {
        this(tokenizer, null);
    }

    public HFModelTokenizer(HuggingFaceTokenizer tokenizer, Map<String, Float> idf) {
        termAtt = addAttribute(CharTermAttribute.class);
        payloadAtt = addAttribute(PayloadAttribute.class);
        this.tokenizer = tokenizer;
        this.idf = idf;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        tokenIdx = 0;
        overflowingIdx = 0;
        String inputStr = CharStreams.toString(input);
        encoding = tokenizer.encode(inputStr, false, true);
    }

    @Override
    final public boolean incrementToken() throws IOException {
        // todo: 1. overflowing handle 2. max length of index.analyze.max_token_count 3. other attributes
        clearAttributes();
        Encoding curEncoding = encoding;

        while (tokenIdx < curEncoding.getTokens().length || overflowingIdx < encoding.getOverflowing().length) {
            if (tokenIdx >= curEncoding.getTokens().length) {
                tokenIdx = 0;
                curEncoding = encoding.getOverflowing()[overflowingIdx];
                overflowingIdx++;
                continue;
            }
            termAtt.append(curEncoding.getTokens()[tokenIdx]);
            if (idf != null) {
                float weight = idf.getOrDefault(curEncoding.getTokens()[tokenIdx], 1.f);
                int intBits = Float.floatToIntBits(weight);
                payloadAtt.setPayload(
                    new BytesRef(new byte[] { (byte) (intBits >> 24), (byte) (intBits >> 16), (byte) (intBits >> 8), (byte) (intBits) })
                );
            }
            tokenIdx++;
            return true;
        }

        return false;
    }
}
