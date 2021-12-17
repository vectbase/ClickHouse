#include <Processors/Sources/DistributedSource.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <QueryPipeline/DistributedSourceExecutor.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <base/logger_useful.h>

namespace DB
{
DistributedSource::DistributedSource(DistributedSourceExecutorPtr executor, bool add_aggregation_info_, bool async_read_)
    : SourceWithProgress(executor->getHeader(), false)
    , query_executor(std::move(executor))
    , add_aggregation_info(add_aggregation_info_)
    , async_read(async_read_)
    , log(&Poco::Logger::get("DistributedSource"))
{
    /// Add AggregatedChunkInfo if we expect DataTypeAggregateFunction as a result.
    const auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            add_aggregation_info = true;
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

    if (add_aggregation_info)
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = block.info.bucket_num;
        info->is_overflows = block.info.is_overflows;
        chunk.setChunkInfo(std::move(info));
    }

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

Pipe createDistributedSourcePipe(DistributedSourceExecutorPtr query_executor, bool add_aggregation_info, bool async_read)
{
    Pipe pipe(std::make_shared<DistributedSource>(query_executor, add_aggregation_info, async_read));
    return pipe;
}

}
