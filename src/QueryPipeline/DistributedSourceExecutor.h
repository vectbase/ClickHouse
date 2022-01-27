#pragma once
#include <Client/GRPCClient.h>
#include <Interpreters/Context.h>
#include <clickhouse_grpc.pb.h>

namespace DB
{
class DistributedSourceExecutor
{
public:
    /// Takes already set connection.
    DistributedSourceExecutor(
        const Block & header_,
        const std::shared_ptr<String> & source_,
        const String & query_id_,
        const String & node_id_,
        int stage_id_,
        int parent_stage_id_);

    ~DistributedSourceExecutor();

    Block read();
    void finish();
    void cancel();

    const Block & getHeader() const { return header; }

    /// Get totals and extremes if any.
    Block getTotals() { return std::move(totals); }
    Block getExtremes() { return std::move(extremes); }

private:
    Block header;
    Block totals;
    Block extremes;
    const std::shared_ptr<String> source;
    String query_id;
    String node_id;
    int stage_id;
    int parent_stage_id;
    GRPCClient client;
    Poco::Logger * log = nullptr;
    std::atomic<bool> finished{false};
    std::atomic<bool> was_cancelled{false};
    std::atomic<bool> got_exception_from_replica{false};
    std::mutex was_cancelled_mutex;

    /// If wasn't sent yet, send request to cancel all connections to replicas
    void tryCancel(const char * reason);

    /// Returns true if query was sent
    bool isQueryPending() const;

    /// Returns true if exception was thrown
    bool hasThrownException() const;
};

}
