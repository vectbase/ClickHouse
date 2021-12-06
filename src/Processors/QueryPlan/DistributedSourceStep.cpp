#include <Processors/QueryPlan/DistributedSourceStep.h>
#include <Processors/Sources/DistributedSource.h>
#include <QueryPipeline/DistributedSourceExecutor.h>

namespace DB
{

DistributedSourceStep::DistributedSourceStep(
    Block header_,
    const std::vector<std::shared_ptr<String>> & sources_,
    const String & query_id_,
    int stage_id_,
    int parent_stage_id_,
    const String & node_id_,
    ContextPtr context_)
    : ISourceStep(DataStream{.header = std::move(header_)})
    , header(output_stream->header)
    , sources(sources_)
    , query_id(query_id_)
    , stage_id(stage_id_)
    , parent_stage_id(parent_stage_id_)
    , node_id(node_id_)
    , context(std::move(context_))
    , log(&Poco::Logger::get("DistributedSourceStep(" + query_id + "/" + toString(stage_id) + "/" + node_id + ")"))
{
}


void DistributedSourceStep::addPipe(Pipes & pipes, const std::shared_ptr<String> & source)
{
    auto distributed_source_executor = std::make_shared<DistributedSourceExecutor>(header, source, query_id, node_id, parent_stage_id);
    pipes.emplace_back(createDistributedSourcePipe(distributed_source_executor, false));
    pipes.back().addInterpreterContext(context);
}

void DistributedSourceStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipes pipes;
    for (const auto & source : sources)
    {
        addPipe(pipes, source);
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));
    pipeline.init(std::move(pipe));
}

}
