#pragma once
//#if USE_GRPC
#include <map>
#include <memory>
#include <shared_mutex>

#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/proto_utils.h>
#include <grpcpp/impl/codegen/sync_stream.h>

#include <Core/Block.h>
#include <base/types.h>
#include <Poco/Net/SocketAddress.h>
#include "clickhouse_grpc.grpc.pb.h"
#include "clickhouse_grpc.pb.h"

using GRPCQueryInfo = clickhouse::grpc::QueryInfo;
using GRPCResult = clickhouse::grpc::Result;
using GRPCTicket = clickhouse::grpc::Ticket;
using GRPCStub = clickhouse::grpc::ClickHouse::Stub;

namespace DB
{
using ReadDataCallback = std::function<void(const Block & block)>;

class GRPCClient
{
public:
    GRPCClient(const String & addr);
    ~GRPCClient();

    /// Send distributed plan to other servers.
    GRPCResult SendDistributedPlanParams(GRPCQueryInfo & g_query_info);

    /// Try to read a block from the remote server by the specified ticket,
    /// If got EOF, an empty Block will be returned, you can use if (!block) to check it.
    Block read(const GRPCTicket & ticket);

private:
    struct InnerContext
    {
        InnerContext(
            std::shared_ptr<grpc::Channel> & ch_,
            std::shared_ptr<grpc::ClientContext> & ctx_,
            std::unique_ptr<GRPCStub> & stub_,
            std::unique_ptr<grpc::ClientReader<GRPCResult>> & reader_)
            : ch(ch_), ctx(ctx_), stub(std::move(stub_)), reader(std::move(reader_))
        {
        }
        ~InnerContext() { }

        std::shared_ptr<grpc::Channel> ch;
        std::shared_ptr<grpc::ClientContext> ctx;
        std::unique_ptr<GRPCStub> stub;
        std::unique_ptr<grpc::ClientReader<GRPCResult>> reader;
    };

    Poco::Logger * log;
    String addr;
    std::map<String, std::shared_ptr<InnerContext>> reader_map;
    mutable std::shared_mutex mu;
};
}
//#endif
