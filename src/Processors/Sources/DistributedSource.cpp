#include <Processors/Sources/DistributedSource.h>
#include <QueryPipeline/DistributedSourceExecutor.h>
#include <base/logger_useful.h>

namespace DB
{
DistributedSource::DistributedSource(DistributedSourceExecutorPtr executor, bool async_read_)
    : SourceWithProgress(executor->getHeader(), false)
    , query_executor(std::move(executor))
    , async_read(async_read_)
    , log(&Poco::Logger::get("DistributedSource"))
{
    LOG_DEBUG(log, "DistributedSource header columns: {}.", getPort().getHeader().columns());
}

DistributedSource::~DistributedSource() = default;

ISource::Status DistributedSource::prepare()
{
    /// Check if query was cancelled before returning Async status. Otherwise it may lead to infinite loop.
    if (was_query_canceled)
    {
        getPort().finish();
        return Status::Finished;
    }

    if (is_async_state)
        return Status::Async;

    Status status = SourceWithProgress::prepare();
    /// To avoid resetting the connection (because of "unfinished" query) in the
    /// RemoteQueryExecutor it should be finished explicitly.
    if (status == Status::Finished)
    {
        query_executor->finish();
        is_async_state = false;
    }
    return status;
}

std::optional<Chunk> DistributedSource::tryGenerate()
{
    /// onCancel() will do the cancel if the query was sent.
    if (was_query_canceled)
        return {};

    Block block;

    if (async_read)
    {
        /// do something if needed
    }
    else
        block = query_executor->read();

    if (!block)
    {
        query_executor->finish();
        return {};
    }

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);

    return std::move(chunk);
}

void DistributedSource::onCancel()
{
    was_query_canceled = true;
    query_executor->cancel();
}

void DistributedSource::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        was_query_canceled = true;
        query_executor->finish();
    }
}

Pipe createDistributedSourcePipe(DistributedSourceExecutorPtr query_executor, bool async_read)
{
    Pipe pipe(std::make_shared<DistributedSource>(query_executor, async_read));
    return pipe;
}

}
