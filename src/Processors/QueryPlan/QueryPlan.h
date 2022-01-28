#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/IColumn.h>
#include <Parsers/ASTSelectQuery.h>

#include <list>
#include <memory>
#include <set>
#include <vector>

namespace DB
{

class DataStream;

class IQueryPlanStep;
using QueryPlanStepPtr = std::unique_ptr<IQueryPlanStep>;

class QueryPipelineBuilder;
using QueryPipelineBuilderPtr = std::unique_ptr<QueryPipelineBuilder>;

class WriteBuffer;

class QueryPlan;
using QueryPlanPtr = std::unique_ptr<QueryPlan>;

class Pipe;

struct QueryPlanOptimizationSettings;
struct BuildQueryPipelineSettings;

namespace JSONBuilder
{
    class IItem;
    using ItemPtr = std::unique_ptr<IItem>;
}

/// TODO: Fields of InterpreterContext will be used to create logical operator in buildQueryPlan().
struct InterpreterParams
{
    InterpreterParams(const ContextPtr & context_, const ASTSelectQuery & query_ast_) : context(context_)
    {
        group_by_with_totals = query_ast_.group_by_with_totals;
        group_by_with_rollup = query_ast_.group_by_with_rollup;
        group_by_with_cube = query_ast_.group_by_with_cube;
    }

    InterpreterParams(const InterpreterParams & interpreter_params)
        : context(interpreter_params.context)
        , group_by_with_totals(interpreter_params.group_by_with_totals)
        , group_by_with_rollup(interpreter_params.group_by_with_rollup)
        , group_by_with_cube(interpreter_params.group_by_with_cube)
    {
    }

    ContextPtr context;
    bool group_by_with_totals;
    bool group_by_with_rollup;
    bool group_by_with_cube;
};
using InterpreterParamsPtr = std::shared_ptr<InterpreterParams>;

/// A tree of query steps.
/// The goal of QueryPlan is to build QueryPipeline.
/// QueryPlan let delay pipeline creation which is helpful for pipeline-level optimizations.
class QueryPlan
{
public:
    QueryPlan();
    ~QueryPlan();
    QueryPlan(QueryPlan &&);
    QueryPlan & operator=(QueryPlan &&);

    void unitePlans(QueryPlanStepPtr step, std::vector<QueryPlanPtr> plans, InterpreterParamsPtr interpreter_params = {});
    void addStep(QueryPlanStepPtr step, InterpreterParamsPtr interpreter_params = {});

    bool isInitialized() const { return root != nullptr; } /// Tree is not empty
    bool isCompleted() const; /// Tree is not empty and root hasOutputStream()
    const DataStream & getCurrentDataStream() const; /// Checks that (isInitialized() && !isCompleted())

    void checkInitialized() const;
    void checkNotCompleted() const;
    void optimize(const QueryPlanOptimizationSettings & optimization_settings);

    void reset();

    void collectCreatingSetPlan(std::vector<std::unique_ptr<QueryPlan>> & creating_set_plans);

    QueryPipelineBuilderPtr buildQueryPipeline(
        const QueryPlanOptimizationSettings & optimization_settings,
        const BuildQueryPipelineSettings & build_pipeline_settings);

    /// If initialized, build pipeline and convert to pipe. Otherwise, return empty pipe.
    Pipe convertToPipe(
        const QueryPlanOptimizationSettings & optimization_settings,
        const BuildQueryPipelineSettings & build_pipeline_settings);

    struct ExplainPlanOptions
    {
        /// Add output header to step.
        bool header = false;
        /// Add description of step.
        bool description = true;
        /// Add detailed information about step actions.
        bool actions = false;
        /// Add information about indexes actions.
        bool indexes = false;
    };

    struct ExplainPipelineOptions
    {
        /// Show header of output ports.
        bool header = false;
    };

    JSONBuilder::ItemPtr explainPlan(const ExplainPlanOptions & options);
    void explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options);
    void explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options);
    void explainEstimate(MutableColumns & columns);

    /// Set upper limit for the recommend number of threads. Will be applied to the newly-created pipelines.
    /// TODO: make it in a better way.
    void setMaxThreads(size_t max_threads_) { max_threads = max_threads_; }
    size_t getMaxThreads() const { return max_threads; }

    void addInterpreterContext(ContextPtr context);

    /// Tree node. Step and it's children.
    struct Node
    {
        QueryPlanStepPtr step;
        std::vector<Node *> children = {};
        Node * parent = nullptr;
        size_t num_parent_stages = 0; /// Number of parent stages whose child is the stage current node belongs to.
        size_t num_leaf_nodes_in_stage = 0; /// Number of leaf nodes(including current node and its descendant nodes) in the same stage.
        InterpreterParamsPtr interpreter_params;
    };

    using Nodes = std::list<Node>;

private:
    friend class DistributedPlanner;

    Nodes nodes;
    Node * root = nullptr;

    /// Those fields are passed to QueryPipeline.
    size_t max_threads = 0;
    std::vector<ContextPtr> interpreter_context;

    Poco::Logger * log;
};

std::string debugExplainStep(const IQueryPlanStep & step);

}
