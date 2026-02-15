# Autoloader Problem & Solution Analysis

## The Problem

When an instance restarts and the Autoloader runs, it needs to pick up messages that were being processed by the crashed instance. However, OTHER healthy instances are ALSO processing messages with "IN_PROGRESS" status. 

**Critical Question:** How do we distinguish between:
1. **Orphaned messages** - Being processed by the now-dead instance
2. **Active messages** - Currently being processed by healthy instances

Without this distinction, the Autoloader would:
- Pick up messages already being processed by other instances (DUPLICATE PROCESSING)
- Cause race conditions and data corruption
- Lead to duplicate message publications

## Solution Options

### Option 1: Heartbeat-based Approach

**Concept:** Each processing thread updates a heartbeat timestamp while processing.

**Implementation:**
- Add `LAST_HEARTBEAT_TIMESTAMP` column to `INCOMING_MESSAGE_TABLE`
- Background thread updates this every 30 seconds during processing
- Autoloader picks messages where heartbeat is older than threshold (e.g., 2 minutes)

**Pros:**
- Simple to implement
- Accurate detection of dead processes

**Cons:**
- Extra database writes (performance impact)
- Requires background heartbeat thread per message
- Heartbeat may fail to update due to network issues

**Recommended:** ❌ Not ideal for high-volume systems

---

### Option 2: Instance ID Tracking with Registry

**Concept:** Track which instance is processing each message and maintain registry of alive instances.

**Implementation:**
- Add `PROCESSING_INSTANCE_ID` column to `INCOMING_MESSAGE_TABLE`
- Create `INSTANCE_REGISTRY_TABLE` with heartbeats
- When processing starts, record instance ID
- Autoloader queries for messages from dead instances

**Pros:**
- Clear ownership tracking
- Easy debugging - can see which instance processed what
- Supports graceful shutdown

**Cons:**
- Requires instance registry management
- More complex than heartbeat approach

**Recommended:** ✅ Good for enterprise systems

---

### Option 3: Timeout-based (Processing Duration)

**Concept:** Pick up messages that have been "IN_PROGRESS" for longer than max expected processing time.

**Implementation:**
- Add `PROCESSING_STARTED_TIMESTAMP` column
- Autoloader picks messages where `SYSDATE - PROCESSING_STARTED_TIMESTAMP > MAX_PROCESSING_TIME`

**Pros:**
- Simplest implementation
- No heartbeats required
- Self-healing

**Cons:**
- May interfere with legitimately slow-processing messages
- Requires accurate max processing time estimate
- No distinction between slow vs. dead

**Recommended:** ⚠️ Simple but risky

---

### Option 4: Lease-based Approach (RECOMMENDED)

**Concept:** Industry-standard lease mechanism with expiry time.

**Implementation:**
- Add `LEASE_EXPIRY_TIMESTAMP` and `PROCESSING_INSTANCE_ID` columns
- When processing starts, set lease expiry (e.g., 15 minutes from now)
- Background thread renews lease every 5 minutes during processing
- Autoloader picks messages with expired leases only

**Pros:**
- Industry standard pattern (used by Kubernetes, Consul, etc.)
- Handles all edge cases (crashes, network failures, slow processing)
- Self-healing without manual intervention
- Clear ownership with expiry safety

**Cons:**
- Requires lease renewal logic
- Slightly more complex than simple timeout

**Recommended:** ✅✅ BEST SOLUTION - Production-grade

---

### Option 5: Hybrid Approach (Ultimate Solution)

**Concept:** Combine instance tracking, lease mechanism, and registry.

**Implementation:**
- `PROCESSING_INSTANCE_ID` - Which instance owns the message
- `LEASE_EXPIRY_TIMESTAMP` - When the lease expires
- `INSTANCE_REGISTRY_TABLE` - Tracks all instances with heartbeats
- Lease renewal service
- Instance registry with auto-cleanup of dead instances

**Query Logic:**
```sql
SELECT * FROM INCOMING_MESSAGE_TABLE
WHERE INTERNAL_STATUS = 'IN_PROGRESS'
  AND (
    -- Lease has expired
    LEASE_EXPIRY_TIMESTAMP < SYSDATE
    OR
    -- Instance is known to be dead
    PROCESSING_INSTANCE_ID IN (
      SELECT INSTANCE_ID FROM INSTANCE_REGISTRY_TABLE
      WHERE LAST_HEARTBEAT < SYSDATE - INTERVAL '5' MINUTE
    )
  )
```

**Recommended:** ✅✅✅ ULTIMATE PRODUCTION SOLUTION

---

## Recommended Implementation

We'll implement **Option 5 (Hybrid Approach)** because it:
1. Handles instance crashes
2. Handles network partitions
3. Handles slow processing
4. Provides clear ownership
5. Enables monitoring and debugging
6. Is self-healing

## Implementation Components

### 1. Database Schema Changes
- Add columns to `INCOMING_MESSAGE_TABLE`:
  - `PROCESSING_INSTANCE_ID VARCHAR2(200)`
  - `LEASE_EXPIRY_TIMESTAMP TIMESTAMP`
  - `PROCESSING_STARTED_TIMESTAMP TIMESTAMP`
  
- Create `INSTANCE_REGISTRY_TABLE`:
  - `INSTANCE_ID VARCHAR2(200) PRIMARY KEY`
  - `INSTANCE_HOSTNAME VARCHAR2(200)`
  - `INSTANCE_IP VARCHAR2(50)`
  - `STATUS VARCHAR2(20)` (ACTIVE, DEAD)
  - `LAST_HEARTBEAT TIMESTAMP`
  - `STARTED_TIMESTAMP TIMESTAMP`
  - `VERSION NUMBER`

### 2. New Services
- `InstanceRegistryService` - Manages instance registration and heartbeats
- `LeaseRenewalService` - Renews leases for in-progress messages
- Enhanced `AutoloaderService` - Uses new columns for orphan detection

### 3. Configuration
```yaml
application:
  instance:
    heartbeat-interval-seconds: 30
    heartbeat-timeout-minutes: 5
  
  lease:
    duration-minutes: 15
    renewal-interval-minutes: 5
  
  autoloader:
    max-processing-time-minutes: 30
```

## Test Scenarios

### Scenario 1: Normal Processing
- Instance A starts processing message M1
- Sets PROCESSING_INSTANCE_ID = A, LEASE_EXPIRY = now + 15min
- Instance B Autoloader runs
- Query returns no messages (lease not expired, instance alive)
- ✅ No duplicate processing

### Scenario 2: Instance Crash
- Instance A processing M1, crashes at T=0
- Lease expires at T=15min
- Instance B Autoloader runs at T=16min
- Query returns M1 (lease expired)
- Instance B picks up M1
- ✅ Message recovered

### Scenario 3: Network Partition
- Instance A processing M1, network partition at T=0
- Instance A continues processing but can't update heartbeat
- Instance A marked DEAD in registry at T=5min
- Lease expires at T=15min
- Instance B Autoloader runs at T=16min
- Query returns M1 (instance dead OR lease expired)
- ✅ Message recovered even if lease renewal failed

### Scenario 4: Slow Processing
- Instance A processing M1, takes 20 minutes (legitimate)
- Lease renewed at T=5, T=10, T=15 minutes
- Instance B Autoloader runs at T=16min
- Lease expiry now at T=20min (renewed)
- Query returns empty (lease not expired)
- ✅ No interference with slow processing

### Scenario 5: Multiple Restarts
- Instance A crashes, Instance B picks up M1
- Instance B crashes, Instance C picks up M1
- Each time lease is reset
- ✅ Message eventually completes

### Scenario 6: Graceful Shutdown
- Instance A processing M1, receives SIGTERM
- Instance A completes processing or releases lease
- Sets status to COMPLETE or releases ownership
- ✅ Clean shutdown, no orphans

## Configurable Retry Implementation

Current issue: Retry count is stored but not enforced during reprocessing.

**Solution:**
- Check `RETRY_COUNT` before reprocessing in Autoloader
- Increment `RETRY_COUNT` on each retry attempt
- Compare with `application.autoloader.max-retry-count`
- Move to FAILED_PERMANENTLY if exceeded

```java
if (message.getRetryCount() >= maxRetryCount) {
    updateStatus(message.getMsgId(), "FAILED_PERMANENTLY");
    return; // Don't retry
}
incrementRetryCount(message.getMsgId());
```

## Testing Strategy

1. **Unit Tests** (20+ test cases)
   - Lease expiry calculation
   - Instance registration
   - Heartbeat updates
   - Orphan detection logic
   
2. **Integration Tests** (15+ test cases)
   - Multiple instances processing
   - Instance crash simulation
   - Network partition simulation
   - Lease renewal during long processing
   
3. **Load Tests**
   - 1000 messages, 5 instances
   - Random instance failures
   - Verify no duplicates, all messages processed

## Summary

The **Hybrid Approach (Option 5)** with:
- Instance ID tracking
- Lease mechanism with expiry
- Instance registry with heartbeats
- Configurable retry limits

Provides the most robust, production-grade solution that handles all edge cases while maintaining high performance.
