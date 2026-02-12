#!/bin/bash

echo "Creating Repository interfaces..."

# Create Repository interfaces
cat > src/main/java/com/enterprise/messaging/repository/IncomingMessageRepository.java << 'EOF'
package com.enterprise.messaging.repository;

import com.enterprise.messaging.model.IncomingMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface IncomingMessageRepository extends JpaRepository<IncomingMessage, BigDecimal> {

    Optional<IncomingMessage> findByXCorrelationIdAndSource(String xCorrelationId, String source);

    List<IncomingMessage> findByFinalStatusAndRetryCountLessThan(String status, Integer maxRetryCount);

    @Query("SELECT im FROM IncomingMessage im WHERE im.internalStatus = :status AND im.insertTimestamp < :threshold")
    List<IncomingMessage> findInProgressMessages(@Param("status") String status, @Param("threshold") LocalDateTime threshold);

    @Query("SELECT im FROM IncomingMessage im WHERE im.finalStatus = 'FAILED' AND im.errorMessage IN :retriableErrors AND im.retryCount < :maxRetries")
    List<IncomingMessage> findRetriableFailedMessages(@Param("retriableErrors") List<String> retriableErrors, @Param("maxRetries") Integer maxRetries);

    @Modifying
    @Query("UPDATE IncomingMessage im SET im.internalStatus = :status, im.updateTimestamp = :timestamp, im.updatedBy = :updatedBy WHERE im.msgId = :msgId")
    int updateInternalStatus(@Param("msgId") BigDecimal msgId, @Param("status") String status, @Param("timestamp") LocalDateTime timestamp, @Param("updatedBy") String updatedBy);

    @Modifying
    @Query("UPDATE IncomingMessage im SET im.finalStatus = :status, im.updateTimestamp = :timestamp WHERE im.msgId = :msgId")
    int updateFinalStatus(@Param("msgId") BigDecimal msgId, @Param("status") String status, @Param("timestamp") LocalDateTime timestamp);

    boolean existsByAttributeKey4AndInsertTimestampAfter(String hashcode, LocalDateTime timestamp);
}
EOF

cat > src/main/java/com/enterprise/messaging/repository/OutgoingMessageRepository.java << 'EOF'
package com.enterprise.messaging.repository;

import com.enterprise.messaging.model.OutgoingMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface OutgoingMessageRepository extends JpaRepository<OutgoingMessage, BigDecimal> {

    List<OutgoingMessage> findByInternalSourceId(BigDecimal internalSourceId);

    @Query("SELECT om FROM OutgoingMessage om WHERE om.finalStatus = 'FAILED' AND om.errorMessage IN :retriableErrors AND om.retryCount < :maxRetries")
    List<OutgoingMessage> findRetriableFailedMessages(@Param("retriableErrors") List<String> retriableErrors, @Param("maxRetries") Integer maxRetries);

    @Modifying
    @Query("UPDATE OutgoingMessage om SET om.phase = :phase, om.updateTimestamp = :timestamp WHERE om.msgId = :msgId")
    int updatePhase(@Param("msgId") BigDecimal msgId, @Param("phase") String phase, @Param("timestamp") LocalDateTime timestamp);

    boolean existsByAttributeKey4AndInsertTimestampAfter(String hashcode, LocalDateTime timestamp);
}
EOF

cat > src/main/java/com/enterprise/messaging/repository/DuplicateMessageRepository.java << 'EOF'
package com.enterprise.messaging.repository;

import com.enterprise.messaging.model.DuplicateMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;

@Repository
public interface DuplicateMessageRepository extends JpaRepository<DuplicateMessage, BigDecimal> {
}
EOF

cat > src/main/java/com/enterprise/messaging/repository/ProcessLockRepository.java << 'EOF'
package com.enterprise.messaging.repository;

import com.enterprise.messaging.model.ProcessLock;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;

@Repository
public interface ProcessLockRepository extends JpaRepository<ProcessLock, String> {

    Optional<ProcessLock> findByLockNameAndLockStatus(String lockName, String lockStatus);

    @Modifying
    @Query("UPDATE ProcessLock pl SET pl.lockStatus = 'RELEASED', pl.lockUpdatedTime = :timestamp WHERE pl.lockName = :lockName AND pl.lockedBy = :lockedBy")
    int releaseLock(@Param("lockName") String lockName, @Param("lockedBy") String lockedBy, @Param("timestamp") LocalDateTime timestamp);
}
EOF

echo "Repository interfaces created!"

echo "Creating Configuration classes..."

cat > src/main/java/com/enterprise/messaging/config/DatabaseConfig.java << 'EOF'
package com.enterprise.messaging.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.util.Properties;

@Slf4j
@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = "com.enterprise.messaging.repository")
public class DatabaseConfig {

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Value("${spring.datasource.username}")
    private String dbUsername;

    @Value("${spring.datasource.password}")
    private String dbPassword;

    @Value("${spring.datasource.hikari.maximumPoolSize:50}")
    private int maxPoolSize;

    @Value("${spring.datasource.hikari.minimumIdle:10}")
    private int minIdle;

    @Primary
    @Bean(destroyMethod = "close")
    public HikariDataSource dataSource() {
        log.info("Configuring HikariCP DataSource");
        
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUsername);
        config.setPassword(dbPassword);
        config.setDriverClassName("oracle.jdbc.OracleDriver");
        
        // Connection Pool Settings
        config.setMaximumPoolSize(maxPoolSize);
        config.setMinimumIdle(minIdle);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(300000);
        config.setMaxLifetime(1800000);
        config.setLeakDetectionThreshold(60000);
        
        // Performance Settings
        config.setAutoCommit(false);
        config.setConnectionTestQuery("SELECT 1 FROM DUAL");
        config.setPoolName("MessageProcessorHikariPool");
        config.setRegisterMbeans(true);
        
        // Oracle Specific Settings
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("oracle.jdbc.implicitStatementCacheSize", "100");
        
        log.info("HikariCP configured with maxPoolSize={}, minIdle={}", maxPoolSize, minIdle);
        
        return new HikariDataSource(config);
    }

    @Primary
    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource);
        em.setPackagesToScan("com.enterprise.messaging.model");
        
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(vendorAdapter);
        
        Properties properties = new Properties();
        properties.setProperty("hibernate.dialect", "org.hibernate.dialect.Oracle12cDialect");
        properties.setProperty("hibernate.hbm2ddl.auto", "validate");
        properties.setProperty("hibernate.jdbc.batch_size", "50");
        properties.setProperty("hibernate.jdbc.fetch_size", "100");
        properties.setProperty("hibernate.order_inserts", "true");
        properties.setProperty("hibernate.order_updates", "true");
        properties.setProperty("hibernate.query.in_clause_parameter_padding", "true");
        
        em.setJpaProperties(properties);
        
        return em;
    }

    @Primary
    @Bean
    public PlatformTransactionManager transactionManager(LocalContainerEntityManagerFactoryBean entityManagerFactory) {
        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(entityManagerFactory.getObject());
        return transactionManager;
    }
}
EOF

cat > src/main/java/com/enterprise/messaging/config/RedisConfig.java << 'EOF'
package com.enterprise.messaging.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;

@Slf4j
@Configuration
@EnableCaching
public class RedisConfig {

    @Value("${spring.data.redis.host}")
    private String redisHost;

    @Value("${spring.data.redis.port}")
    private int redisPort;

    @Value("${spring.data.redis.password:}")
    private String redisPassword;

    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        log.info("Configuring Redis connection to {}:{}", redisHost, redisPort);
        
        RedisStandaloneConfiguration redisConfig = new RedisStandaloneConfiguration();
        redisConfig.setHostName(redisHost);
        redisConfig.setPort(redisPort);
        if (redisPassword != null && !redisPassword.isEmpty()) {
            redisConfig.setPassword(redisPassword);
        }

        LettuceClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(2000))
                .shutdownTimeout(Duration.ofMillis(100))
                .build();

        return new LettuceConnectionFactory(redisConfig, clientConfig);
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        
        StringRedisSerializer stringSerializer = new StringRedisSerializer();
        GenericJackson2JsonRedisSerializer jsonSerializer = new GenericJackson2JsonRedisSerializer();
        
        template.setKeySerializer(stringSerializer);
        template.setValueSerializer(jsonSerializer);
        template.setHashKeySerializer(stringSerializer);
        template.setHashValueSerializer(jsonSerializer);
        
        template.afterPropertiesSet();
        
        log.info("Redis Template configured successfully");
        return template;
    }

    @Bean
    public CacheManager cacheManager(RedisConnectionFactory connectionFactory) {
        RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofHours(24))
                .serializeKeysWith(RedisSerializationContext.SerializationPair.fromSerializer(new StringRedisSerializer()))
                .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(new GenericJackson2JsonRedisSerializer()))
                .disableCachingNullValues();

        return RedisCacheManager.builder(connectionFactory)
                .cacheDefaults(config)
                .build();
    }
}
EOF

echo "Configuration classes created!"

