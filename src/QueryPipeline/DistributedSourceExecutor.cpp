#include <QueryPipeline/DistributedSourceExecutor.h>
#include <base/logger_useful.h>

namespace DB
{
DistributedSourceExecutor::DistributedSourceExecutor(
    const Block & header_,
    const std::shared_ptr<String> & source_,
    const String & query_id_,
    const String & node_id_,
    int stage_id_,
    int parent_stage_id_)
    : header(header_)
    , totals(header_)
    , extremes(header_)
    , source(source_)
    , query_id(query_id_)
    , node_id(node_id_)
    , stage_id(stage_id_)
    , parent_stage_id(parent_stage_id_)
    , client(*source_, query_id + "/" + toString(stage_id) + "/" + node_id + "<=" + toString(parent_stage_id) + "/" + *source)
    , log(&Poco::Logger::get(
          "DistributedSourceExecutor(" + query_id + "/" + toString(stage_id) + "/" + node_id + "<=" + toString(parent_stage_id) + "/"
          + *source + ")"))
{
    /// Stage A(host is a) read data from Stage B(host is b), The ticket sent from a to b is :
    /// ticket{stage_id = B, node_id = a}
    GRPCTicket ticket;
    ticket.set_initial_query_id(query_id);
    ticket.set_stage_id(parent_stage_id);
    ticket.set_node_id(node_id);
    client.prepareRead(ticket);
}

DistributedSourceExecutor::~DistributedSourceExecutor()
{
}

Block DistributedSourceExecutor::read()
{
    if (was_cancelled)
        return Block();

    try
    {
        Block block;
        auto message_type = client.read(block);
        switch (message_type)
        {
            case GRPCClient::MessageType::Data:
                if (!block)
                    finished = true;
                LOG_DEBUG(log, "Read data block, rows: {}, columns: {}.", block.rows(), block.columns());
                return block;

            case GRPCClient::MessageType::Totals:
                LOG_DEBUG(log, "Read totals block, rows: {}, columns: {}.", block.rows(), block.columns());
                totals = block;
                break;

            case GRPCClient::MessageType::Extremes:
                LOG_DEBUG(log, "Read extremes block, rows: {}, columns: {}.", block.rows(), block.columns());
                extremes = block;
                break;

//            default:
//                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown GRPC block type");
        }
        return {};
    }
    catch (...)
    {
        got_exception_from_replica = true;
        throw;
    }
}

void DistributedSourceExecutor::finish()
{
    if (!isQueryPending() || hasThrownException())
        return;

    LOG_DEBUG(log, "Finish reading.");
    tryCancel("Enough data has been read");

    finished = true;
}

void DistributedSourceExecutor::cancel()
{
    if (!isQueryPending() || hasThrownException())
        return;

    LOG_DEBUG(log, "Cancel reading.");
    client.cancel();
    tryCancel("Query is cancelled");
}

void DistributedSourceExecutor::tryCancel(const char * reason)
{
    /// Flag was_cancelled is atomic because it is checked in read().
    std::lock_guard guard(was_cancelled_mutex);

    if (was_cancelled)
        return;

    was_cancelled = true;

    LOG_TRACE(log, "Reason: {}.", reason);
}

bool DistributedSourceExecutor::isQueryPending() const
{
    return !finished;
}

bool DistributedSourceExecutor::hasThrownException() const
{
    return got_exception_from_replica;
}

}
