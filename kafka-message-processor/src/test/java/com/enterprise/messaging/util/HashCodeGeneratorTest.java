package com.enterprise.messaging.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

@DisplayName("Hash Code Generator - Complete Coverage")
class HashCodeGeneratorTest {
    private HashCodeGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new HashCodeGenerator();
    }

    @Test
    void testConsistentHash() {
        String hash1 = generator.generateHashCode("test");
        String hash2 = generator.generateHashCode("test");
        assertThat(hash1).isEqualTo(hash2);
    }

    @Test
    void testDifferentHashes() {
        String hash1 = generator.generateHashCode("test1");
        String hash2 = generator.generateHashCode("test2");
        assertThat(hash1).isNotEqualTo(hash2);
    }

    @Test
    void testHashLength() {
        String hash = generator.generateHashCode("test");
        assertThat(hash).hasSize(64);
    }

    @Test
    void testEmptyString() {
        String hash = generator.generateHashCode("");
        assertThat(hash).isNotEmpty().hasSize(64);
    }

    @Test
    void testNullInput() {
        assertThatThrownBy(() -> generator.generateHashCode(null))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testSpecialCharacters() {
        String hash = generator.generateHashCode("!@#$%^&*()");
        assertThat(hash).hasSize(64);
    }

    @Test
    void testUnicodeCharacters() {
        String hash = generator.generateHashCode("Hello 世界");
        assertThat(hash).hasSize(64);
    }

    @Test
    void testLongString() {
        String longStr = "a".repeat(10000);
        String hash = generator.generateHashCode(longStr);
        assertThat(hash).hasSize(64);
    }
}
