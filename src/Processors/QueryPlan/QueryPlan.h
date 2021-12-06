#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Columns/IColumn.h>
#include <Processors/QueryPlan/DistributedSourceStep.h>

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

    void unitePlans(QueryPlanStepPtr step, std::vector<QueryPlanPtr> plans);
    void addStep(QueryPlanStepPtr step);

    bool isInitialized() const { return root != nullptr; } /// Tree is not empty
    bool isCompleted() const; /// Tree is not empty and root hasOutputStream()
    const DataStream & getCurrentDataStream() const; /// Checks that (isInitialized() && !isCompleted())

    void optimize(const QueryPlanOptimizationSettings & optimization_settings);

    void reset();
    void buildStages(ContextPtr context);       /// Used by initial node.
    void scheduleStages(ContextPtr context);    /// Used by initial node.
    void buildPlanFragment(ContextPtr context); /// Used by non-initial nodes.
    void buildDistributedPlan(ContextMutablePtr context);

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
    };

    using Nodes = std::list<Node>;

    struct Stage
    {
        int id;
        std::vector<Stage *> parents = {}; /// Previous stages that current stage directly depends on.
        Stage * child = nullptr;
        std::vector<std::shared_ptr<String>> executors; /// Replicas that current stage should be executed on.
        std::vector<std::shared_ptr<String>> sources; /// Parents' executors.
        std::vector<std::shared_ptr<String>> sinks; /// Child's executors.
        Node * node; /// Current stage's root node
    };

    /// Note: do not use vector, otherwise pointers to elements in it will be invalidated when vector increases.
    using Stages = std::list<Stage>;

    struct PlanFragmentInfo
    {
        PlanFragmentInfo(int stage_id_, const String & node_id_, const std::vector<String> & sources_, const std::vector<String> & sinks_)
            : stage_id(stage_id_), node_id(node_id_), sources(sources_), sinks(sinks_) {}
        int stage_id;
        String node_id; /// The replica name of plan fragment receiver, used by DistributedSource.
        std::vector<String> sources; /// Point to the nodes sending data.
        std::vector<String> sinks; /// Point to the nodes receiving data.
    };
    using PlanFragmentInfoPtr = std::shared_ptr<PlanFragmentInfo>;

    String debugLocalPlanFragment(const String & query_id, int stage_id, const String & node_id, const DistributedSourceStep * stage);
    String debugRemotePlanFragment(const String & receiver, const String & query_id, const Stage * stage);

private:
    Nodes nodes;
    Node * root = nullptr;

    void checkInitialized() const;
    void checkNotCompleted() const;

    /// Those fields are passed to QueryPipeline.
    size_t max_threads = 0;
    std::vector<ContextPtr> interpreter_context;

    Stages stages;
    Stage * result_stage = nullptr;

    struct pairHasher {
        template <class T1, class T2>
        size_t operator()(const std::pair<T1, T2> & p) const
        {
            auto hash1 = std::hash<T1>{}(p.first);
            auto hash2 = std::hash<T2>{}(p.second);
            return hash1 ^ hash2;
        }
    };

    /// Key is {stage_id, receiver_address}.
    std::unordered_map<std::pair<int, std::shared_ptr<String>>, PlanFragmentInfoPtr, pairHasher> plan_fragment_infos;
    Poco::Logger * log;
};

std::string debugExplainStep(const IQueryPlanStep & step);

}
