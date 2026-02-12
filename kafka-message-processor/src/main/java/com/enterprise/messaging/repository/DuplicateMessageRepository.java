package com.enterprise.messaging.repository;

import com.enterprise.messaging.model.DuplicateMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;

@Repository
public interface DuplicateMessageRepository extends JpaRepository<DuplicateMessage, BigDecimal> {
}
