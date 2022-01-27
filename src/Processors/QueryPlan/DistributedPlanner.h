#pragma once

#include <Processors/QueryPlan/QueryPlan.h>
#include <stack>

namespace DB
{

class UnionStep;
class JoinStep;
class AggregatingStep;
class SortingStep;
class LimitStep;
class DistinctStep;
class DistributedSourceStep;

class DistributedPlanner {
public:
    DistributedPlanner(QueryPlan & query_plan_, const ContextMutablePtr & context);

    bool buildDistributedPlan();

private:
    enum class StageSeq
    {
        STAGE0, /// Read data source
        STAGE1, /// Partial processing
        STAGE2  /// Final processing
    };

    String getStageSeqName(const StageSeq & stage_seq)
    {
        switch (stage_seq)
        {
            case StageSeq::STAGE0: return "STAGE0";
            case StageSeq::STAGE1: return "STAGE1";
            case StageSeq::STAGE2: return "STAGE2";
        }
        return "Unknown";
    }

    struct Stage
    {
        int id; /// Current stage id.
        std::vector<Stage *> parents = {}; /// Previous stages that current stage directly depends on.
        Stage * child = nullptr;
        std::vector<std::shared_ptr<String>> workers; /// Replicas that current stage should be executed on.
        std::vector<std::shared_ptr<String>> sinks; /// Child's workers.
        QueryPlan::Node * root_node; /// Current stage's root node.
        std::vector<QueryPlan::Node *> leaf_nodes; /// Store leaf nodes which are from right side to left side.
        bool is_leaf_stage = false; /// Current stage is a leaf stage if it has any leaf node reading data from storage(not from remote).
        bool maybe_has_view_source = false; /// Current stage reads data to trigger materialized view.
        bool has_input_function = false;
        bool empty_result_for_aggregation_by_empty_set = false;
    };

    /// Note: do not use vector, otherwise pointers to elements in it will be invalidated when vector increases.
    using Stages = std::list<Stage>;

    Stages stages;
    Stage * result_stage = nullptr;

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

    struct CheckShuffleResult
    {
        bool is_shuffle = false;
        UnionStep * current_union_step = nullptr;
        JoinStep * current_join_step = nullptr;
        AggregatingStep * child_aggregating_step = nullptr;
        SortingStep * child_sorting_step = nullptr;
        SortingStep * grandchild_sorting_step = nullptr;
        DistinctStep * current_distinct_step = nullptr;
        DistinctStep * child_distinct_step = nullptr;
        LimitStep * current_limit_step = nullptr;
        LimitStep * child_limit_step = nullptr;
        IQueryPlanStep * grandchild_step = nullptr;
    };
    void checkShuffle(
        QueryPlan::Node * current_node,
        QueryPlan::Node * child_node,
        CheckShuffleResult & result,
        StageSeq & stage_seq,
        std::stack<QueryPlan::Node *> & leaf_nodes);

    struct PlanResult
    {
        String initial_query_id;
        int stage_id;
        String node_id;
        std::vector<QueryPlan::Node *> distributed_source_nodes;
        bool is_result_stage_moved_forward = false;
    };
    String debugLocalPlanFragment(PlanResult & plan_result);
    String debugRemotePlanFragment(const String & query, const String & receiver, const String & query_id, const Stage * stage);

    bool isSinglePointDataSource(const String & name);
    void transferInterpreterParams(QueryPlan::Node *current_node, QueryPlan::Node *last_node);
    void buildStages();
    void debugStages();

    void scheduleStages(PlanResult & plan_result);
    void buildPlanFragment(PlanResult & plan_result);
    void uniteCreatingSetSteps(std::vector<std::unique_ptr<QueryPlan>> & creating_set_plans);

private:
    QueryPlan & query_plan;
    ContextMutablePtr context;
    Poco::Logger * log;
};

}
