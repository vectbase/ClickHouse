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
    extern const int GRPC_CANCEL_ERROR;
}

GRPCClient::GRPCClient(const String & addr_)
{
    addr = addr_;
    log = &Poco::Logger::get("GRPCClient(" + addr + ")");
}

GRPCResult GRPCClient::executePlanFragment(const GRPCQueryInfo & query_info)
{
    auto ch = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    auto stub = clickhouse::grpc::ClickHouse::NewStub(ch);
    grpc::ClientContext ctx;
    GRPCResult result;
    grpc::Status status = stub->ExecutePlanFragment(&ctx, query_info, &result);

    if (status.ok())
        return result;
    else
    {
        LOG_ERROR(
            log,
            "Send query info to {} failed, code: {}, plan fragment id: {}.",
            addr,
            status.error_code(),
            query_info.initial_query_id() + "/" + toString(query_info.stage_id()) + "/" + query_info.node_id());
        throw Exception(status.error_message() + ", " + result.exception().display_text(), ErrorCodes::INVALID_GRPC_QUERY_INFO, true);
    }
}

void GRPCClient::prepareRead(const GRPCTicket & ticket_)
{
    ticket = ticket_;
    grpc::ChannelArguments arg;
    arg.SetMaxReceiveMessageSize(-1);
    auto ch = grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), arg);
    std::shared_ptr<grpc::ClientContext> ctx = std::make_shared<grpc::ClientContext>();
    auto stub = clickhouse::grpc::ClickHouse::NewStub(ch);
    auto reader = stub->FetchPlanFragmentResult(ctx.get(), ticket);
    inner_context = std::make_unique<InnerContext>(ch, ctx, stub, reader);
}

GRPCClient::MessageType GRPCClient::read(Block & block)
{
    assert(inner_context);

    LOG_DEBUG(log, "Start reading result from {}.", addr);
    GRPCResult result;
    if (inner_context->reader->Read(&result))
    {
        if (result.exception().code() != 0)
        {
            LOG_ERROR(
                log,
                "Read from {} failed, exception.code: {}, exception.text: {}.",
                addr,
                result.exception().code(),
                result.exception().display_text());
            throw Exception(result.exception().display_text(), result.exception().code(), true);
        }

        /// Note: totals and extremes are not used and not tested yet.
        if (result.totals().size() > 0)
        {
            ReadBufferFromString b(result.totals());
            NativeReader reader(b, 0);
            block = reader.read();
            LOG_DEBUG(log, "Read totals from {} success, result size: {}, block rows: {}.", addr, result.output().size(), block.rows());
            return MessageType::Totals;
        }

        if (result.extremes().size() > 0)
        {
            ReadBufferFromString b(result.extremes());
            NativeReader reader(b, 0);
            block = reader.read();
            LOG_DEBUG(log, "Read extremes from {} success, result size: {}, block rows: {}.", addr, result.output().size(), block.rows());
            return MessageType::Extremes;
        }

        if (!result.output().empty())
        {
            ReadBufferFromString b(result.output());
            NativeReader reader(b, 0);
            block = reader.read();
            LOG_DEBUG(log, "Read data from {} success, result size: {}, block rows: {}.", addr, result.output().size(), block.rows());
        }
        return MessageType::Data;
    }

    throw Exception(
        "Read from grpc server " + addr + " failed, " + toString(result.exception().code()) + ", " + result.exception().display_text(),
        ErrorCodes::GRPC_READ_ERROR,
        true);
}

void GRPCClient::cancel()
{
    grpc::ClientContext ctx;
    GRPCResult result;

    auto status = inner_context->stub->CancelPlanFragment(&ctx, ticket, &result);

    auto plan_fragment_id = ticket.initial_query_id() + "/" + toString(ticket.stage_id()) + "/" + ticket.node_id();
    if (status.ok())
    {
        if (result.cancelled())
            LOG_INFO(log, "Cancel success from {}, plan fragment id: {}", addr, plan_fragment_id);
        else
        {
            throw Exception("Cancel failed from " + addr + ", plan fragment id: " + plan_fragment_id + ", code: " + toString(result.exception().code()) + ", " + result.exception().display_text(), ErrorCodes::GRPC_CANCEL_ERROR, true);
        }
    }
    else
    {
        LOG_ERROR(
            log, "Cancel failed from {}, code: {}, plan fragment id: {}.", addr, status.error_code(), plan_fragment_id);
        throw Exception(status.error_message() + ", " + result.exception().display_text(), ErrorCodes::GRPC_CANCEL_ERROR, true);
    }
}
}
