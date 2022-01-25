#pragma once
#include <base/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace zkutil
{
const String ReadLockPrefix = "r-";
const String WriteLockPrefix = "w-";
const String LockPathPrefix = "/clickhouse/rwlocks/";
const String TablesDir = "tables/";

class DistributedRWLock;
using DistributedRWLockPtr = std::shared_ptr<DistributedRWLock>;

/// Acquire a distributed read lock or write lock through zookeeper until timeout.
/// Client should use static function tryLock() to acquire a lock.
/// The lock will be released in destructor automically.
///
/// Implement details:
/// 1. Create an ephemeral and sequential node as the number of the "try lock queue".
/// 2. Get all sequential nodes in the "queue", check if trying lock can be successful, if success returns.
/// 3. Otherwise watch a node (the write node for read lock, the previous read node for write lock) and wait for notification, go to step 2 when got notified.
/// Details can be found here: https://zookeeper.apache.org/doc/current/recipes.html#Shared+Locks
class DistributedRWLock
{
public:
    DistributedRWLock(zkutil::ZooKeeperPtr zookeeper_, bool readonly_, const String & path_, std::chrono::seconds timeout);
    ~DistributedRWLock();

    /// Try to get a lock (readonly_ true for read lock, false for write lock) on a specified zookeeper path.
    /// Unlock automically when the returned DistributedRWLockPtr is destructed.
    static DistributedRWLockPtr tryLock(zkutil::ZooKeeperPtr zookeeper_, bool readonly_, const String & database, const String & table = "",
                                        std::chrono::seconds timeout = std::chrono::seconds(60));

    /// TODO other distributed read write lock interface can be added according to the tryLock() which is specially for database and table

    static std::shared_mutex rwlock_node_set_mutex;
    static std::unordered_set<String> rwlock_node_set;

private:
    void run();
    void createRWLockNode();
    void tryLockImpl();

    zkutil::ZooKeeperPtr zk_cli = nullptr;
    /// True for read lock, false for write lock.
    bool readonly;
    /// Parent path of rwlock node, eg: /clickhouse/rwlocks/database01/tables/table01
    String parent_path;
    /// Current lock node, eg: /clickhouse/rwlocks/database01/tables/table01/w-0000000001
    String rwlock_node;

    std::chrono::seconds wait_timeout_seconds;

    Poco::Logger * log;

    enum class LockResult
    {
        INIT = 0,
        SUCCESS = 1,
        RETRY = 2,
        FAILED = 3,
    };
    std::atomic<LockResult> try_lock_result;

    std::mutex watch_cond_mutex;
    std::condition_variable watch_cond;
};
}
