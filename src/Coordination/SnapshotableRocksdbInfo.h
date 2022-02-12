#pragma once
#include <base/StringRef.h>
#include <unordered_map>
#include <list>
#include <atomic>
#include <Common/ZooKeeper/IKeeper.h>
#include <Poco/Logger.h>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/env.h"
#include "clickhouse_grpc.grpc.pb.h"

using GRPCSnapshotableDataInfo = clickhouse::grpc::SnapshotableDataInfo;

namespace DB
{

struct KeeperStorageNode
{
    String data;
    uint64_t acl_id = 0; /// 0 -- no ACL by default
    bool is_sequental = false;
    Coordination::Stat stat{};
    int32_t seq_num = 0;
    std::unordered_set<std::string> children{};
};

struct ListRocksDbNode
{
    std::string key;
    KeeperStorageNode value;
    bool active_in_map;
};

class SnapshotableRocksdbInfo
{
private:
    using List = std::list<ListRocksDbNode>;
    using IndexMap = std::unordered_map<StringRef, typename List::iterator, StringRefHash>;

    List list;
    IndexMap map;
    bool snapshot_mode{false};

    String rocksdb_path;
    bool isopen_rocksdb{false};
    bool isuse_rocksdb{false};
    Poco::Logger * log;
    rocksdb::DB* db;
    rocksdb::Options options;
    size_t snapshot_container_size;
public:
    SnapshotableRocksdbInfo()
    {
        rocksdb_path = "./racksdb";
        snapshot_container_size = 0;
        log = &Poco::Logger::get("SnapshotableRocksdbInfo");
        LOG_DEBUG(log, "SnapshotableRocksdbInfo:SnapshotableRocksdbInfo snapshot_container_size = {} snapshots", snapshot_container_size);
    }

    //RocksDb
    void rocksdbSerializeToString(const KeeperStorageNode & value, std::string &buff)
    {
        GRPCSnapshotableDataInfo rsp;
        rsp.set_data(value.data);
        rsp.set_acl_id(value.acl_id);
        rsp.set_is_sequental(value.is_sequental);
        rsp.set_seq_num(value.seq_num);
        auto stat_info = rsp.mutable_stat();
        stat_info->set_czxid(value.stat.czxid);
        stat_info->set_mzxid(value.stat.mzxid);
        stat_info->set_ctime(value.stat.ctime);
        stat_info->set_mtime(value.stat.mtime);
        stat_info->set_version(value.stat.version);
        stat_info->set_aversion(value.stat.aversion);
        stat_info->set_ephemeralowner(value.stat.ephemeralOwner);
        stat_info->set_datalength(value.stat.dataLength);
        stat_info->set_numchildren(value.stat.numChildren);
        stat_info->set_pzxid(value.stat.pzxid);

        rsp.SerializeToString(&buff);
    }

    bool rocksdbParseFromString(const std::string &buff, const std::string & key, KeeperStorageNode & value) const
    {
        GRPCSnapshotableDataInfo rsp;
        if (!rsp.ParseFromString(buff)) 
        {
            LOG_DEBUG(log, "SnapshotableRocksdbInfo ParseFromString is error!!!!!!!!!!!! key = {}.", key);
            return false;
        }

        value.data = rsp.data();
        value.acl_id = rsp.acl_id();
        value.is_sequental = rsp.is_sequental();
        value.seq_num = rsp.seq_num();
        auto statInfo = rsp.stat();
        value.stat.czxid = statInfo.czxid();
        value.stat.mzxid = statInfo.mzxid();
        value.stat.ctime = statInfo.ctime();
        value.stat.mtime = statInfo.mtime();
        value.stat.version = statInfo.version();
        value.stat.aversion = statInfo.aversion();
        value.stat.ephemeralOwner = statInfo.ephemeralowner();
        value.stat.dataLength = statInfo.datalength();
        value.stat.numChildren = statInfo.numchildren();
        value.stat.pzxid = statInfo.pzxid();
        return true;
    }

    bool rocksdbOpen(const std::string & rocksdbpath_, int isuse_rocksdb_)
    {
        rocksdb_path = rocksdbpath_;
        if (isuse_rocksdb_ > 0)
        {
            isuse_rocksdb = true;
        }
        
        if (isopen_rocksdb)
            return true;
        
        options.create_if_missing = true;
        rocksdb::Status status = rocksdb::DB::Open(options, rocksdb_path, &db);
        if(!status.ok()){
            LOG_ERROR(log, "SnapshotableRocksdbInfo: rocksdbOpen status is fail------path={} .isuse={}",rocksdb_path, isuse_rocksdb);
            return true;
        }
        
        isopen_rocksdb = true;
        LOG_DEBUG(log, "SnapshotableRocksdbInfo: rocksdbOpen is success------path={} .isuse={}",rocksdb_path, isuse_rocksdb);
        return true;
    }

    bool rocksdbPut(const std::string & key, const KeeperStorageNode & value)
    {
        if (!isopen_rocksdb || db == nullptr)
        {
            LOG_ERROR(log, "SnapshotableRocksdbInfo rocksdbPut isopen_rocksdb is error");
            return false;
        }
        
        std::string tempStr;
        rocksdbSerializeToString(value,tempStr);
        rocksdb::Status status = db->Put(rocksdb::WriteOptions(), key, tempStr);
        if(status.ok()){
            return true;
        }
        
        LOG_ERROR(log, "SnapshotableRocksdbInfo rocksdbPut status is error");
        return false;
    }

    ListRocksDbNode rocksdbGet(const std::string & key, bool isneed_children) const
    {
        if (!isopen_rocksdb || db == nullptr)
        {
            LOG_ERROR(log, "SnapshotableRocksdbInfo rocksdbGet isopen_rocksdb is error");
            ListRocksDbNode elem;
            elem.active_in_map = false;
            return elem;
        }

        String get_value;
        rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &get_value);
        if(status.ok())
        {
            KeeperStorageNode tempValue;
            if (rocksdbParseFromString(get_value, key, tempValue))
            {
                if (isneed_children)
                {
                    tempValue.children.clear();
                    auto iter = db->NewIterator(rocksdb::ReadOptions());
                    for (iter->Seek(key); iter->Valid(); iter->Next())
                    {
                        std::string tempStr = iter->key().ToString();
                        if (tempStr == "/" || tempStr == key)
                        {
                            continue;
                        }

                        if (key == parentPath(tempStr))
                        {
                            tempValue.children.insert(getBaseName(tempStr));
                        }
                        else if (tempStr.find(key) == tempStr.npos)
                        {
                            break;
                        }
                        
                    }
                    delete iter;
                }

                ListRocksDbNode elem;
                elem.key = key;
                elem.value = tempValue;
                elem.active_in_map = true;
                return elem;
            }
        }

        LOG_ERROR(log, "SnapshotableRocksdbInfo rocksdbGet fail error -----");
        ListRocksDbNode elem;
        elem.active_in_map = false;
        return elem;
    }

    bool rocksdbContains(const std::string & key) const
    {
        if (!isopen_rocksdb || db == nullptr)
            return false;

        String get_value;
        rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &get_value);
        if(status.ok()){
            return true;
        }

        return false;
    }

    bool rocksdbDelete(const std::string & key)
    {
        if (!isopen_rocksdb || db == nullptr)
            return false;

        rocksdb::Status status = db->Delete(rocksdb::WriteOptions(), key);
        if(status.ok())
        {
            LOG_DEBUG(log, "SnapshotableRocksdbInfo rocksdbDelete!!!{}", key);
            if (!snapshot_mode)
            {
                snapshot_container_size--;
            }
            return true;
        }

        return false;
    }

    bool rocksdbIter(std::vector<std::string>& vecKey)
    {
        if (!isopen_rocksdb || db == nullptr)
        {
            return false;
        }
        
        rocksdb::Iterator* iter = db->NewIterator(rocksdb::ReadOptions());
        for (iter->SeekToFirst();iter->Valid();iter->Next())
        {
            vecKey.push_back(iter->key().ToString());
            KeeperStorageNode tempValue;
            if (rocksdbParseFromString(iter->value().ToString(), iter->key().ToString(), tempValue))
            {
                tempValue.children.clear();
                auto iterSeek = db->NewIterator(rocksdb::ReadOptions());
                for (iterSeek->Seek(iter->key().ToString()); iterSeek->Valid(); iterSeek->Next())
                {
                    std::string tempStr = iterSeek->key().ToString();
                    if (tempStr == "/" || tempStr == iter->key().ToString())
                    {
                        continue;
                    }

                    if (iter->key().ToString() == parentPath(tempStr))
                    {
                        tempValue.children.insert(getBaseName(tempStr));
                    }
                    else if (tempStr.find(iter->key().ToString()) == tempStr.npos)
                    {
                        break;
                    }
                    
                }
                delete iterSeek;
            }
        }
        
        delete iter;
        return true;
    }

    bool rocksdbClose()
    {
        if (isopen_rocksdb)
        {
            db->Close();
            delete db;
            rocksdb::DestroyDB(rocksdb_path, options);
            isopen_rocksdb = false;
            LOG_DEBUG(log, "SnapshotableRocksdbInfo:rocksdbClose!");
        }
        return true;
    }

    static String parentPath(const String & path)
    {
        auto rslash_pos = path.rfind('/');
        if (rslash_pos > 0)
            return path.substr(0, rslash_pos);
        return "/";
    }

    static std::string getBaseName(const String & path)
    {
        size_t basename_start = path.rfind('/');
        return std::string{&path[basename_start + 1], path.length() - basename_start - 1};
    }


    using iterator = typename List::iterator;
    using const_iterator = typename List::const_iterator;
    using reverse_iterator = typename List::reverse_iterator;
    using const_reverse_iterator = typename List::const_reverse_iterator;
    using ValueUpdater = std::function<void(KeeperStorageNode & value)>;

    bool insert(const std::string & key, const KeeperStorageNode & value)
    {
        if (isuse_rocksdb)
        {
            if (rocksdbPut(key,value))
            {
                if (!contains(key))
                {
                    snapshot_container_size++;
                }
                return true;
            }
            return false;
        }
        else
        {
            auto it = map.find(key);
            if (it == map.end())
            {
                ListRocksDbNode elem{key, value, true};
                auto itr = list.insert(list.end(), elem);
                map.emplace(itr->key, itr);
                return true;
            }
        }
        
        return false;
    }


    void insertOrReplace(const std::string & key, const KeeperStorageNode & value)
    {
        if (isuse_rocksdb)
        {
            if (rocksdbPut(key,value))
            {
                if (snapshot_mode || !contains(key))
                {
                    snapshot_container_size++;
                }
            }
        }
        else
        {
            auto it = map.find(key);
            if (it == map.end())
            {
                ListRocksDbNode elem{key, value, true};
                auto itr = list.insert(list.end(), elem);
                map.emplace(itr->key, itr);
            }
            else
            {
                auto list_itr = it->second;
                if (snapshot_mode)
                {
                    ListRocksDbNode elem{key, value, true};
                    list_itr->active_in_map = false;
                    auto new_list_itr = list.insert(list.end(), elem);
                    map.erase(it);
                    map.emplace(new_list_itr->key, new_list_itr);
                }
                else
                {
                    list_itr->value = value;
                }
            }
        }
    }

    bool erase(const std::string & key)
    {
        if (isuse_rocksdb)
        {
            rocksdbDelete(key);
        }
        else
        {
            auto it = map.find(key);
            if (it == map.end())
                return false;

            auto list_itr = it->second;
            if (snapshot_mode)
            {
                list_itr->active_in_map = false;
                map.erase(it);
            }
            else
            {
                map.erase(it);
                list.erase(list_itr);
            }
        }
        return true;
    }

    bool contains(const std::string & key) const
    {
        if (isuse_rocksdb)
        {
            return rocksdbContains(key);
        }
        else
        {
            return map.find(key) != map.end();
        }
    }

    KeeperStorageNode updateValue(const std::string & key, ValueUpdater updater)
    {
        if (isuse_rocksdb)
        {
            KeeperStorageNode tempValue;
            if (!isopen_rocksdb || db == nullptr)
            {
                LOG_ERROR(log, "SnapshotableRocksdbInfo updateValue isopen_rocksdb os error");
                return tempValue;
            }

            String get_value;
            rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &get_value);
            if(status.ok())
            {
                if (rocksdbParseFromString(get_value, key, tempValue))
                {
                    updater(tempValue);
                    rocksdbPut(key,tempValue);
                    if (snapshot_mode)
                    {
                        snapshot_container_size++;
                    }
                    return tempValue;
                }
            }
            else
            {
                LOG_ERROR(log, "SnapshotableRocksdbInfo updateValue status os error");
            }
            return tempValue;
        }
        else
        {
            auto it = map.find(key);
            assert(it != map.end());
            if (snapshot_mode)
            {
                auto list_itr = it->second;
                auto elem_copy = *(list_itr);
                list_itr->active_in_map = false;
                map.erase(it);
                updater(elem_copy.value);
                auto itr = list.insert(list.end(), elem_copy);
                map.emplace(itr->key, itr);
                return itr->value;
            }
            else
            {
                auto list_itr = it->second;
                updater(list_itr->value);
                return list_itr->value;
            }
        }
    }

    ListRocksDbNode find(const std::string & key,bool isneed_children = true) const
    {
        if (isuse_rocksdb)
        {
            return rocksdbGet(key, isneed_children);
        }
        else
        {
            auto map_it = map.find(key);
            if (map_it != map.end())
                return *map_it->second;

            KeeperStorageNode value;
            ListRocksDbNode tempElem{key, value, true};
            tempElem.active_in_map = false;
            return tempElem;
        }
    }

    KeeperStorageNode getValue(const std::string & key) const
    {
        if (isuse_rocksdb)
        {
            KeeperStorageNode tempValue;
            if (!isopen_rocksdb || db == nullptr)
            {
                LOG_ERROR(log, "SnapshotableRocksdbInfo getValue isopen_rocksdb is error");
                return tempValue;
            }

            String get_value;
            rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &get_value);
            if(status.ok())
            {
                if (rocksdbParseFromString(get_value, key, tempValue))
                {
                    tempValue.children.clear();
                    auto iter = db->NewIterator(rocksdb::ReadOptions());
                    for (iter->Seek(key); iter->Valid(); iter->Next())
                    {
                        std::string tempStr = iter->key().ToString();
                        if (tempStr == "/" || tempStr == key)
                        {
                            continue;
                        }

                        if (key == parentPath(tempStr))
                        {
                            tempValue.children.insert(getBaseName(tempStr));
                        }
                        else if (tempStr.find(key) == tempStr.npos)
                        {
                            break;
                        }
                        
                    }
                    delete iter;
                }
                else
                {
                    LOG_ERROR(log, "SnapshotableRocksdbInfo getValue rocksdbParseFromString is error");
                }
                
            }
            return tempValue;
        }
        else
        {
            auto it = map.find(key);
            assert(it != map.end());
            return it->second->value;
        }
    }

    void clearOutdatedNodes()
    {
        if (isuse_rocksdb)
        {
            snapshot_container_size = 0;
        }
        else
        {
            auto start = list.begin();
            auto end = list.end();
            for (auto itr = start; itr != end;)
            {
                if (!itr->active_in_map)
                    itr = list.erase(itr);
                else
                    itr++;
            }
        }
    }

    void clear()
    {
        list.clear();
        map.clear();
        snapshot_container_size = 0;
    }

    void enableSnapshotMode()
    {
        snapshot_mode = true;
    }

    void disableSnapshotMode()
    {
        snapshot_mode = false;
    }

    size_t size() const
    {
        return map.size();
    }

    size_t snapshotSize() const
    {
        if (isuse_rocksdb)
        {
            return snapshot_container_size;
        }
        else
        {
            return list.size();
        }
    }


    iterator begin() { return list.begin(); }
    const_iterator begin() const { return list.cbegin(); }
    iterator end() { return list.end(); }
    const_iterator end() const { return list.cend(); }

    reverse_iterator rbegin() { return list.rbegin(); }
    const_reverse_iterator rbegin() const { return list.crbegin(); }
    reverse_iterator rend() { return list.rend(); }
    const_reverse_iterator rend() const { return list.crend(); }
};


}
