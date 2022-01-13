#pragma once
#include <Processors/Sources/SourceWithProgress.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{
class DistributedSourceExecutor;
using DistributedSourceExecutorPtr = std::shared_ptr<DistributedSourceExecutor>;

class DistributedSource : public SourceWithProgress
{
public:
    DistributedSource(DistributedSourceExecutorPtr executor,
                      bool add_aggregation_info_,
                      bool async_read_);
    ~DistributedSource() override;

    Status prepare() override;
    String getName() const override { return "DistributedSource"; }

    void onUpdatePorts() override;

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() override;

private:
    DistributedSourceExecutorPtr query_executor;
    std::atomic<bool> was_query_canceled = false;
    bool add_aggregation_info = false;
    const bool async_read;
    Poco::Logger * log = nullptr;
    bool is_async_state = false;
};

/// Totals source from RemoteQueryExecutor.
class DistributedTotalsSource : public ISource
{
public:
    explicit DistributedTotalsSource(DistributedSourceExecutorPtr executor_);
    ~DistributedTotalsSource() override;

    String getName() const override { return "DistributedTotals"; }

protected:
    Chunk generate() override;

private:
    DistributedSourceExecutorPtr executor;
};

/// Extremes source from RemoteQueryExecutor.
class DistributedExtremesSource : public ISource
{
public:
    explicit DistributedExtremesSource(DistributedSourceExecutorPtr executor_);
    ~DistributedExtremesSource() override;

    String getName() const override { return "DistributedExtremes"; }

protected:
    Chunk generate() override;

private:
    DistributedSourceExecutorPtr executor;
};

/// Create pipe with distributed sources.
/// Never use add_totals and add_extremes.
Pipe createDistributedSourcePipe(DistributedSourceExecutorPtr query_executor, bool add_aggregation_info, bool add_totals, bool add_extremes, bool async_read);

}
