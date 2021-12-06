#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{
class DistributedSourceStep final : public ISourceStep
{
public:
    DistributedSourceStep(
        Block header_,
        const std::vector<std::shared_ptr<String>> & sources_,
        const String & query_id_,
        int stage_id_,
        int parent_stage_id_,
        const String & node_id_,
        ContextPtr context_);

    String getName() const override { return "DistributedSourceStep(" + toString(stage_id) + " -> " + toString(parent_stage_id); }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
    const std::vector<std::shared_ptr<String>> & getSources() const { return sources; }

    void setSources(const std::vector<std::shared_ptr<String>> & sources_) { sources = sources_; }

private:
    void addPipe(Pipes & pipes, const std::shared_ptr<String> & source);
    Block header;
    std::vector<std::shared_ptr<String>> sources;
    String query_id;
    int stage_id;
    int parent_stage_id;
    String node_id;
    ContextPtr context;
    Poco::Logger * log;
};
}

