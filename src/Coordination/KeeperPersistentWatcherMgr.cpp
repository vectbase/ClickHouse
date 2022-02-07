#include <atomic>
#include <Coordination/KeeperPersistentWatcherMgr.h>

namespace DB
{
KeeperPersistentWatcherMgr::KeeperPersistentWatcherMgr()
{
    recursiveWatcherCount.store(0);
}
KeeperPersistentWatcherMgr::~KeeperPersistentWatcherMgr()
{
    pWatcherModeMap.clear();
    recursiveWatcherCount.store(0);
}

bool KeeperPersistentWatcherMgr::containsRecursiveWatch()
{
    return std::atomic_load(&(recursiveWatcherCount)) > 0;
}

void KeeperPersistentWatcherMgr::setWatcherMode(int64_t sessionID, String path, WatcherType watcherType, WatcherMode mode)
{
    if (mode == WatcherMode::Standard)
    {
        removeWatcher(sessionID, path, watcherType);
    }
    else
    {
        WatcherMode oldMode = WatcherMode::Standard;
        WatcherKey key(sessionID, path, watcherType);
        auto k = pWatcherModeMap.find(key);
        if (k != pWatcherModeMap.end())
            oldMode = k->second;

        pWatcherModeMap.insert_or_assign(key, mode);
        changeRecursiveCount(oldMode, mode);
    }
}

WatcherMode KeeperPersistentWatcherMgr::getWatcherMode(int64_t sessionID, String path, WatcherType watcherType)
{
    auto k = pWatcherModeMap.find(WatcherKey(sessionID, path, watcherType));
    if (k == pWatcherModeMap.end())
        return WatcherMode::Standard;
    return k->second;
}

void KeeperPersistentWatcherMgr::removeWatcher(int64_t sessionID, String path, WatcherType watcherType)
{
    auto node = pWatcherModeMap.extract(WatcherKey(sessionID, path, watcherType));
    if (!node.empty())
        changeRecursiveCount(node.mapped(), WatcherMode::Standard);
}

void KeeperPersistentWatcherMgr::changeRecursiveCount(WatcherMode oldMode, WatcherMode newMode)
{
    if (oldMode < WatcherMode::Standard || oldMode > WatcherMode::PersistentRecursive)
        oldMode = WatcherMode::Standard;

    if (oldMode != newMode && (oldMode == WatcherMode::PersistentRecursive || newMode == WatcherMode::PersistentRecursive))
    {
        if (newMode == WatcherMode::PersistentRecursive)
            recursiveWatcherCount.fetch_add(1, std::memory_order_relaxed);
        else
            recursiveWatcherCount.fetch_sub(1, std::memory_order_relaxed);
    }
}
}
