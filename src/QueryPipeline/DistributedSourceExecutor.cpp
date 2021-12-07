#include <QueryPipeline/DistributedSourceExecutor.h>
#include <base/logger_useful.h>

namespace DB
{
DistributedSourceExecutor::DistributedSourceExecutor(
    Block header_,
    const std::shared_ptr<String> & source_,
    const String & query_id_,
    const String & node_id_,
    int stage_id_)
    : header(std::move(header_))
    , source(source_)
    , query_id(query_id_)
    , node_id(node_id_)
    , stage_id(stage_id_)
    , client(*source_)
    , log(&Poco::Logger::get("DistributedSourceExecutor(" + query_id + "/" + toString(stage_id) + "/" + node_id + ")"))
{
    GRPCTicket ticket;
    ticket.set_query_id(query_id);
    ticket.set_stage_id(stage_id);
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
        auto block = client.read();
        LOG_DEBUG(log, "Read block, rows: {}, columns: {}.", block.rows(), block.columns());
        return block;
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

    LOG_DEBUG(log, "Finish reading from {}.", *source);
    tryCancel("Cancelling query because enough data has been read");

    finished = true;
}

void DistributedSourceExecutor::cancel()
{
    if (!isQueryPending() || hasThrownException())
        return;

    LOG_DEBUG(log, "Cancel reading from {}.", *source);
    tryCancel("Cancelling query");
}

void DistributedSourceExecutor::tryCancel(const char * reason)
{
    /// Flag was_cancelled is atomic because it is checked in read().
    std::lock_guard guard(was_cancelled_mutex);

    if (was_cancelled)
        return;

    was_cancelled = true;

    LOG_TRACE(log, "({}) {}", *source, reason);
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
