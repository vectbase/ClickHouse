#include <Processors/QueryPlan/DistributedSourceStep.h>
#include <Processors/Sources/DistributedSource.h>
#include <QueryPipeline/DistributedSourceExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{

/// Copy from ReadFromRemote.cpp
static ActionsDAGPtr getConvertingDAG(const Block & block, const Block & header)
{
    /// Convert header structure to expected.
    /// Also we ignore constants from result and replace it with constants from header.
    /// It is needed for functions like `now64()` or `randConstant()` because their values may be different.
    return ActionsDAG::makeConvertingActions(
        block.getColumnsWithTypeAndName(),
        header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        true);
}

/// Copy from ReadFromRemote.cpp
static void addConvertingActions(Pipe & pipe, const Block & header)
{
    if (blocksHaveEqualStructure(pipe.getHeader(), header))
        return;

    auto convert_actions = std::make_shared<ExpressionActions>(getConvertingDAG(pipe.getHeader(), header));
    pipe.addSimpleTransform([&](const Block & cur_header, Pipe::StreamType) -> ProcessorPtr {
        return std::make_shared<ExpressionTransform>(cur_header, convert_actions);
    });
}

DistributedSourceStep::DistributedSourceStep(
    Block header_,
    const std::vector<std::shared_ptr<String>> & sources_,
    const String & query_id_,
    int stage_id_,
    int parent_stage_id_,
    const String & node_id_,
    bool add_aggregation_info_,
    bool add_totals_,
    ContextPtr context_)
    : ISourceStep(DataStream{.header = std::move(header_)})
    , header(output_stream->header)
    , sources(sources_)
    , query_id(query_id_)
    , stage_id(stage_id_)
    , parent_stage_id(parent_stage_id_)
    , node_id(node_id_)
    , add_aggregation_info(add_aggregation_info_)
    , add_totals(add_totals_)
    , context(std::move(context_))
    , log(&Poco::Logger::get("DistributedSourceStep(" + query_id + "/" + toString(stage_id) + "/" + node_id + ")"))
{
}

void DistributedSourceStep::addPipe(Pipes & pipes, const std::shared_ptr<String> & source)
{
    auto distributed_source_executor = std::make_shared<DistributedSourceExecutor>(header, source, query_id, node_id, stage_id, parent_stage_id);
    pipes.emplace_back(createDistributedSourcePipe(
        distributed_source_executor, add_aggregation_info, add_totals, context->getSettingsRef().extremes, false));
    pipes.back().addInterpreterContext(context);
    addConvertingActions(pipes.back(), output_stream->header);
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
