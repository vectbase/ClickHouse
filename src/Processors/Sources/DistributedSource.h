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
                      bool async_read_);
    ~DistributedSource() override;

    Status prepare() override;
    String getName() const override { return "Distributed"; }

    void onUpdatePorts() override;

protected:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() override;

private:
    DistributedSourceExecutorPtr query_executor;
    std::atomic<bool> was_query_canceled = false;
    const bool async_read;
    Poco::Logger * log = nullptr;
    bool is_async_state = false;
};

/// Create pipe with distributed sources.
Pipe createDistributedSourcePipe(DistributedSourceExecutorPtr query_executor, bool async_read);

}
