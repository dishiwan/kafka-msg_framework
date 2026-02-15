package com.enterprise.messaging.repository;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RepositoryTestSuite {
    @Test
    void testIncomingMessageRepository() {
        assertThat(IncomingMessageRepository.class).isInterface();
    }

    @Test
    void testOptimizedIncomingMessageRepository() {
        assertThat(OptimizedIncomingMessageRepository.class).isInterface();
    }

    @Test
    void testOutgoingMessageRepository() {
        assertThat(OutgoingMessageRepository.class).isInterface();
    }

    @Test
    void testDuplicateMessageRepository() {
        assertThat(DuplicateMessageRepository.class).isInterface();
    }

    @Test
    void testInstanceRegistryRepository() {
        assertThat(InstanceRegistryRepository.class).isInterface();
    }

    @Test
    void testProcessLockRepository() {
        assertThat(ProcessLockRepository.class).isInterface();
    }
}
