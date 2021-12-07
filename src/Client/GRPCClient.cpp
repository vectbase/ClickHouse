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

GRPCClient::GRPCClient(const String & addr_)
{
    addr = addr_;
    log = &Poco::Logger::get("GRPCClient(" + addr + ")");
}

GRPCResult GRPCClient::executePlanFragment(GRPCQueryInfo & query_info)
{
    auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub = clickhouse::grpc::ClickHouse::NewStub(ch);
    grpc::ClientContext ctx;
    GRPCResult result;
    /// Set to native format, cause we decode result by NativeReader in the read function
    query_info.set_output_format("Native");
    grpc::Status status = stub->ExecutePlanFragment(&ctx, query_info, &result);

    if (status.ok())
        return result;
    else
    {
        LOG_ERROR(
            log, "Send query info to {} failed, code: {}, plan fragment id: {}.", addr, status.error_code(), query_info.query_id() + toString(query_info.stage_id()) + query_info.node_id());
        throw Exception(status.error_message() + ", " + result.exception().display_text(), ErrorCodes::INVALID_GRPC_QUERY_INFO, true);
    }
}

void GRPCClient::prepareRead(const GRPCTicket & ticket)
{
    auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    std::shared_ptr<grpc::ClientContext> ctx = std::make_shared<grpc::ClientContext>();
    auto stub = clickhouse::grpc::ClickHouse::NewStub(ch);
    auto reader = stub->FetchPlanFragmentResult(ctx.get(), ticket);
    inner_context = std::make_unique<InnerContext>(ch, ctx, stub, reader);
}

Block GRPCClient::read()
{
    assert(inner_context);

    LOG_DEBUG(log, "Start reading result from {}.", addr);
    GRPCResult result;
    if (inner_context->reader->Read(&result))
    {
        if (result.exception().code() != 0)
        {
            LOG_ERROR(log, "Read from {} failed, exception.code: {}, exception.text: {}.", addr, result.exception().code(), result.exception().display_text());
            throw Exception(result.exception().display_text(), ErrorCodes::INVALID_GRPC_QUERY_INFO, true);
        }

        if (result.output().size() == 0)
            return {}; /// Read EOF

        ReadBufferFromString b(result.output());
        NativeReader reader(b, 0 /* server_revision_ */);
        Block block = reader.read();
        LOG_DEBUG(log, "Read from {} success, result size: {}, block rows: {}.", addr, result.output().size(), block.rows());
        return block;
    }

    throw Exception("Read from grpc server " + addr + "failed, " + toString(result.exception().code()) + ", " + result.exception().display_text(), ErrorCodes::GRPC_READ_ERROR, true);
}
}
