#include <atomic>
#include <unordered_map>
#include <base/types.h>

namespace DB
{
using namespace DB;

enum struct WatcherType : int8_t
{
    Children = 1,
    Data = 2,
    Any = 3,
};

struct WatcherKey
{
    int64_t sessionID;
    String path;
    WatcherType watcherType;
    WatcherKey(int64_t sessionID_, String path_, WatcherType watcherType_) : sessionID(sessionID_), path(path_), watcherType(watcherType_) { }
};

struct WatcherKeyHash
{
    std::size_t operator()(const WatcherKey & key) const
    {
        return std::hash<String>()(key.path) + std::hash<int64_t>()(key.sessionID) + std::hash<String>()(key.path)
            + std::size_t(key.watcherType);
    }
};

struct WatcherKeyEqual
{
    bool operator()(const WatcherKey & k1, const WatcherKey & k2) const
    {
        return k1.path == k2.path && k1.path == k2.path && k1.sessionID == k2.sessionID && k1.watcherType == k2.watcherType;
    }
};

/// WatchMode is the mode of a wather, the old watcher (zokeeper<3.6.0) is Standard, Persistent and PersistentRecursive are added in 3.6.0
/// If test by using zookeeper client(eg: zkCli.sh), it's necessary to set zookeeper.disableAutoWatchReset=true on zookeeper client config
/// And test it by: ./zkCli.sh -client-configuration ../conf/zkcli.cfg -server localhost:2181
enum struct WatcherMode : int8_t
{
    Standard = -1, /// not used by the zookeeper raw TCP API, just for inner use
    Persistent = 0,
    PersistentRecursive = 1,
};

/// KeeperPersistentWatcherMgr holds all persistent watchers
/// Because watcher type can be changed, so KeeperPersistentWatcherMgr should also be notified.
class KeeperPersistentWatcherMgr
{
public:
    KeeperPersistentWatcherMgr();
    ~KeeperPersistentWatcherMgr();

    /// Save a watcher and it's mode, watcherType should only be WatcherType::Children or WatcherType::Data
    void setWatcherMode(int64_t sessionID, String path, WatcherType watcherType, WatcherMode mode);

    /// Get a watcher's mode, watcherType should only be WatcherType::Children or WatcherType::Data
    WatcherMode getWatcherMode(int64_t sessionID, String path, WatcherType watcherType);

    /// Delete a watcher's mode, watcherType should only be WatcherType::Children or WatcherType::Data
    void removeWatcher(int64_t sessionID, String path, WatcherType watcherType);

    /// Whether contains any recursive watcher
    bool containsRecursiveWatch();

private:
    std::unordered_map<WatcherKey, WatcherMode, WatcherKeyHash, WatcherKeyEqual> pWatcherModeMap;
    std::atomic<int64_t> recursiveWatcherCount;

    /// Change the recursive watcher count, all mode should be processed (include Standard)
    void changeRecursiveCount(WatcherMode oldMode, WatcherMode newMode);
};
}
