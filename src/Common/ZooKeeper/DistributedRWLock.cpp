#include <base/logger_useful.h>
#include <Common/ZooKeeper/DistributedRWLock.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <chrono>
#include <iostream>
#include <string>
#include <filesystem>

namespace fs = std::filesystem;

namespace zkutil
{
std::unordered_set<String> DistributedRWLock::rwlock_node_set;
std::shared_mutex DistributedRWLock::rwlock_node_set_mutex;

DistributedRWLock::DistributedRWLock(zkutil::ZooKeeperPtr zookeeper_, bool readonly_, const String & path_, std::chrono::seconds timeout)
    : zk_cli(zookeeper_)
    , readonly(readonly_)
    , parent_path(path_)
    , wait_timeout_seconds(timeout)
    , log(&Poco::Logger::get("DistributedRWLock"))
{
    parent_path = fs::path{parent_path}.lexically_normal();

    if (parent_path.ends_with("/"))
        parent_path.resize(parent_path.size() - 1);

    try_lock_result = LockResult::INIT;
}

DistributedRWLock::~DistributedRWLock()
{
    {
        std::unique_lock<std::shared_mutex> lock(rwlock_node_set_mutex);
        rwlock_node_set.erase(rwlock_node);
    }

    if (!rwlock_node.empty())
    {
        zk_cli->remove(rwlock_node);
        LOG_TRACE(log, "{} unlock.", rwlock_node);
    }
}

void DistributedRWLock::createRWLockNode()
{
    if (!zk_cli->exists(parent_path))
    {
        try
        {
            zk_cli->createAncestors(fs::path(parent_path) / "");
        }
        catch (Coordination::Exception & e)
        {
            if (e.code != Coordination::Error::ZNODEEXISTS)
            {
                LOG_WARNING(log, "Create {} got exception: {}.", parent_path, e.message());
                throw e;
            }
        }
    }

    String rwlock_prefix;
    if (readonly)
        rwlock_prefix = fs::path(parent_path) / ReadLockPrefix;
    else
        rwlock_prefix = fs::path(parent_path) / WriteLockPrefix;

    rwlock_node = zk_cli->create(rwlock_prefix, "", 3 /* ephemeral and sequential */);

    LOG_TRACE(log, "{} create node success.", rwlock_node);
}

DistributedRWLockPtr DistributedRWLock::tryLock(zkutil::ZooKeeperPtr zookeeper_, bool readonly_, const String & database,
                                                const String & table, std::chrono::seconds timeout)
{
    auto parent_path = fs::path(LockPathPrefix) / database;
    if (!table.empty())
        parent_path /= (TablesDir + table);
    auto lock = std::make_shared<DistributedRWLock>(zookeeper_, readonly_, parent_path, timeout);
    lock->run();
    return lock;
}

void DistributedRWLock::run()
{
    auto start = std::chrono::system_clock::now();
    createRWLockNode();
    {
        std::unique_lock<std::shared_mutex> lock(rwlock_node_set_mutex);
        rwlock_node_set.insert(rwlock_node);
    }

    tryLockImpl();
    while (true)
    {
        std::unique_lock<std::mutex> lock(watch_cond_mutex);
        if (watch_cond.wait_until(lock, start + wait_timeout_seconds, [&]() { return try_lock_result > LockResult::INIT; }))
        {
            switch (try_lock_result)
            {
                case LockResult::INIT:
                    continue;
                case LockResult::SUCCESS: {
                    LOG_TRACE(log, "{} try lock success.", rwlock_node);
                    return;
                }
                case LockResult::RETRY: {
                    if (std::chrono::system_clock::now() > (start + wait_timeout_seconds))
                        throw Coordination::Exception("Try lock timeout for " + rwlock_node, Coordination::Error::ZOPERATIONTIMEOUT);
                    try_lock_result = LockResult::INIT; /// Reset the flag
                    tryLockImpl();
                    break;
                }
                case LockResult::FAILED:
                    throw Coordination::Exception(
                        "Try lock failed for " + rwlock_node + ", maybe lost connection with zookeeper", Coordination::Error::ZCONNECTIONLOSS);
            }
        }
        else
            throw Coordination::Exception("Try lock timeout for " + rwlock_node, Coordination::Error::ZOPERATIONTIMEOUT);
    }
}

void DistributedRWLock::tryLockImpl()
{
    auto children = zk_cli->getChildren(parent_path);

    /// Only remains the lock items
    std::erase_if(children, [](std::string x) { return (!x.starts_with(WriteLockPrefix) && !x.starts_with(ReadLockPrefix)); });

    auto compare = [](const String & a, const String & b)
    {
        /// WriteLockPrefix and ReadLockPrefix have the same length, so any one is ok
        return std::stoi(a.substr(WriteLockPrefix.size())) < std::stoi(b.substr(WriteLockPrefix.size()));
    };
    std::sort(children.begin(), children.end(), compare);

    String watch_node;
    if (!readonly)
    {
        for (size_t i = 0; i < children.size(); i++)
        {
            if (rwlock_node == (fs::path(parent_path) / children[i]))
            {
                if (i == 0)
                {
                    try_lock_result = LockResult::SUCCESS;
                    return;
                }
                else
                {
                    watch_node = children[i - 1];
                    break;
                }
            }
        }
    }
    else
    {
        /// Used to locate the write node with the next lowest sequence number.
        int write_idx = -1;
        for (size_t i = 0; i < children.size(); i++)
        {
            auto child = children[i];
            if (rwlock_node == (fs::path(parent_path) / child))
            {
                if (write_idx == -1)
                {
                    try_lock_result = LockResult::SUCCESS;
                    return;
                }
                else
                {
                    watch_node = children[write_idx];
                }
            }

            if (child.starts_with(WriteLockPrefix))
            {
                write_idx = i;
            }
        }
    }

    if (watch_node.empty())
        throw Coordination::Exception(
            "Node " + rwlock_node + " not found in parent path: " + parent_path, Coordination::Error::ZINVALIDSTATE);

    LOG_TRACE(log, "{} will watch {}", rwlock_node, watch_node);

    {
        /// The callback must be light weighted.
        /// The 'key' is used to to avoid NULL pointer usage of watch_cond_mutex after DistributedRWLock has been destroyed
        /// And the 'key' variable itself must be value capture.
        auto callback = [&, key = rwlock_node](const Coordination::WatchResponse & resp)
        {
            /// The lock rwlock_node_set_mutex should be hold until finished using on DistributedRWLock's member.
            std::shared_lock<std::shared_mutex> lock(rwlock_node_set_mutex);
            if (!rwlock_node_set.contains(key))
            { /// Maybe the DistributedRWLock object has been destroyed, so just return.
                return;
            }
            LOG_TRACE(log, "{} watch {} got callback event: type: {}, path: {}, state: {}.", rwlock_node, watch_node, resp.type, resp.path, resp.state);
            std::unique_lock<std::mutex> result_lock(watch_cond_mutex);
            try_lock_result = LockResult::RETRY;
            watch_cond.notify_all();
        };

        try
        {
            /// Add watch on the node
            /// Return true if the node exist and add watch success, false if the node does not exist
            /// The callback function will be called when the node was removed
            auto watch_success = zk_cli->existsWatch(fs::path(parent_path) / watch_node, nullptr, callback);
            if (!watch_success)
            {
                /// The node does not exists, but maybe there are some other nodes that still hold the lock, so we should retry
                try_lock_result = LockResult::RETRY;
                return;
            }
        }
        catch (Coordination::Exception & e)
        {
            try_lock_result = LockResult::FAILED;
            throw e;
        }
    }
}
}
