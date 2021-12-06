#include <sstream>
#include <Client/GRPCClient.h>
#include <Formats/FormatFactory.h>
#include <Formats/NativeReader.h>
#include <IO/ReadBufferFromString.h>
#include <grpcpp/grpcpp.h>
#include "Common/ErrorCodes.h"
#include "base/logger_useful.h"
#include "clickhouse_grpc.grpc.pb.h"
#include "clickhouse_grpc.pb.h"

namespace Poco
{
class Logger;
}

using GRPCQueryInfo = clickhouse::grpc::QueryInfo;
using GRPCResult = clickhouse::grpc::Result;
using GRPCException = clickhouse::grpc::Exception;
using GRPCTicket = clickhouse::grpc::Ticket;

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_GRPC_QUERY_INFO;
    extern const int GRPC_READ_ERROR;
}

GRPCClient::GRPCClient(const String & _addr)
{
    addr = _addr;

    log = &Poco::Logger::get("GRPCClient");
}

GRPCClient::~GRPCClient()
{
    reader_map.clear();
}

GRPCResult GRPCClient::SendDistributedPlanParams(GRPCQueryInfo & gQueryInfo)
{
    auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub = clickhouse::grpc::ClickHouse::NewStub(ch);
    grpc::ClientContext ctx;
    GRPCResult result;
    /// Set to native format, cause we decode result by NativeReader in the read function
    gQueryInfo.set_output_format("Native");
    grpc::Status status = stub->SendDistributedPlanParams(&ctx, gQueryInfo, &result);

    if (status.ok())
        return result;
    else
    {
        LOG_ERROR(
            log, "SendDistributedPlanParams to {} failed with code {}, query_id: {}", addr, status.error_code(), gQueryInfo.query_id());
        throw Exception(status.error_message() + ", " + result.exception().display_text(), ErrorCodes::INVALID_GRPC_QUERY_INFO, true);
    }
}

Block GRPCClient::read(const GRPCTicket & ticket)
{
    String key = ticket.query_id() + "/" + std::to_string(ticket.stage_id()) + "/" + ticket.node_id();
    std::shared_ptr<InnerContext> inner_ctx;

    std::shared_lock<std::shared_mutex> lock(mu);
    auto reader_it = reader_map.find(key);
    if (reader_it != reader_map.end())
        inner_ctx = reader_it->second;
    lock.unlock();

    if (reader_it == reader_map.end()) {
        std::unique_lock<std::shared_mutex> wlock(mu);
        /// Check again
        reader_it = reader_map.find(key);
        if (reader_it != reader_map.end())
            inner_ctx = reader_it->second;
        else
        {
            auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
            std::shared_ptr<grpc::ClientContext> ctx = std::make_shared<grpc::ClientContext>();
            auto stub = clickhouse::grpc::ClickHouse::NewStub(ch);
            auto cReader = stub->ExecuteQueryFragmentWithStreamOutput(ctx.get(), ticket);
            reader_map[key] = std::make_shared<InnerContext>(ch, ctx, stub, cReader);
            reader_it = reader_map.find(key);
            inner_ctx = reader_it->second;
        }
        wlock.unlock();
    }

    LOG_DEBUG(log, "Read begin from {}", addr);

    GRPCResult result;
    if (reader_it->second->reader->Read(&result))
    {
        LOG_DEBUG(log, "Read result from {} success, exception.code: {}", addr, result.exception().code());
        if (result.exception().code() != 0)
        {
            LOG_ERROR(log, "GRPC addr: {} result exception: {} {}", addr, result.exception().code(), result.exception().display_text());
            throw Exception(result.exception().display_text(), ErrorCodes::INVALID_GRPC_QUERY_INFO, true);
        }

        LOG_DEBUG(log, "Read from {} success, output size {}", addr, result.output().size());

        if (result.output().size() == 0)
            return {}; /// Read EOF

        ReadBufferFromString b(result.output());
        NativeReader reader(b, 0 /* server_revision_ */);
        Block block = reader.read();
        LOG_DEBUG(log, "Read from {}, block decode success, has {} rows", addr, block.rows());
        return block;
    }

    std::unique_lock<std::shared_mutex> wlock(mu);
    reader_map.erase(reader_it);
    wlock.unlock();
    throw Exception("read from grpc server failed! server: " + addr + ", " + std::to_string(result.exception().code()) + ", " + result.exception().display_text(), ErrorCodes::GRPC_READ_ERROR, true);
}
}
