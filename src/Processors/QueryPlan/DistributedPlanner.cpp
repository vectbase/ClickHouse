#include <Processors/QueryPlan/DistributedPlanner.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/DistributedSourceStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Common/Macros.h>
#include <Interpreters/Cluster.h>
#include <Client/GRPCClient.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TABLE_ALREADY_EXISTS;
}

DistributedPlanner::DistributedPlanner(QueryPlan & query_plan_, const ContextMutablePtr & context_)
    : query_plan(query_plan_)
    , context(context_)
    , log(&Poco::Logger::get("DistributedPlanner"))
{
}

void DistributedPlanner::checkShuffle(
    QueryPlan::Node * current_node,
    QueryPlan::Node * child_node,
    CheckShuffleResult & result,
    StageSeq & stage_seq,
    std::stack<QueryPlan::Node *> & leaf_nodes)
{
    /// Cases requiring special attention:
    /// 1. Distinct:
    ///    distinct => [(shuffle)] => distinct
    ///    distinct => [(shuffle)] => distinct => limit
    ///
    /// 2. Limit(limit should be pushdown):
    ///         => limit => [(shuffle) => limit]
    ///    sort => limit => [(shuffle) => sort => limit]
    ///
    /// 3. Sort:
    ///    distinct => sort => [(shuffle) => sort] => distinct
    ///    distinct => sort => [(shuffle) => sort] => distinct => limit
    ///
    /// Note: The content in "[]" is added to the edge between two stages.
    result.current_union_step = dynamic_cast<UnionStep *>(current_node->step.get());
    if (result.current_union_step)
    {
        result.is_shuffle = true;
        LOG_DEBUG(log, "[{}]Check shuffle: {}, current node is UnionStep", getStageSeqName(stage_seq), result.is_shuffle);
        stage_seq = StageSeq::STAGE1;
        return;
    }

    result.current_join_step = dynamic_cast<JoinStep *>(current_node->step.get());
    if (result.current_join_step)
    {
        assert(current_node->children.size() == 2);
        bool maybe_need_shuffle = false;
        const auto join_kind = result.current_join_step->getJoin()->getTableJoin().kind();
        if (isFull((join_kind)))
        {
            result.is_shuffle = true;
            LOG_DEBUG(log, "[{}]Check shuffle: {}, current node is JoinStep(FULL JOIN)", getStageSeqName(stage_seq), result.is_shuffle);
            return;
        }
        else if (isRight(join_kind))
        {
            if (child_node == current_node->children[1])
            {
                result.is_shuffle = false;
                LOG_DEBUG(log, "[{}]Check shuffle: {}, current node is JoinStep(RIGHT JOIN && right side)", getStageSeqName(stage_seq), result.is_shuffle);
            }
            else
                maybe_need_shuffle = true;
        }
        else if (isLeft(join_kind))
        {
            if (child_node == current_node->children[0])
            {
                result.is_shuffle = false;
                LOG_DEBUG(log, "[{}]Check shuffle: {}, current node is JoinStep(LEFT JOIN && left side)", getStageSeqName(stage_seq), result.is_shuffle);
            }
            else
                maybe_need_shuffle = true;
        }
        else if (child_node == current_node->children[1])
        {
            maybe_need_shuffle = true;
        }

        if (maybe_need_shuffle)
        {
            /// Broadcast one side:
            /// 1. LEFT JOIN && child is right: broadcast right side.
            /// 2. RIGHT JOIN && child is left: broadcast left side.
            /// 3. Other cases: broadcast right side.
            if (child_node->num_parent_stages == 0 && child_node->num_leaf_nodes_in_stage == 1 && !leaf_nodes.empty()
                && isSinglePointDataSource(leaf_nodes.top()->step->getStepDescription()))
            {
                /// 1. Storage name is SystemNumbers: select ... from t1, numbers(10) n
                /// 2. Storage name is Memory: select ... from t1 join (select number from numbers(1))
                /// 3. Storage name is Memory: select ... from t1 join t2
                result.is_shuffle = false;
                LOG_DEBUG(
                    log,
                    "[{}]Check shuffle: {}, current node is JoinStep, child's storage is {}",
                    getStageSeqName(stage_seq),
                    result.is_shuffle,
                    leaf_nodes.top()->step->getStepDescription());
            }
            else
            {
                result.is_shuffle = true;
                LOG_DEBUG(log, "[{}]Check shuffle: {}, current node is JoinStep", getStageSeqName(stage_seq), result.is_shuffle);
            }
        }
        stage_seq = StageSeq::STAGE1;
        return;
    }

    result.child_aggregating_step = dynamic_cast<AggregatingStep *>(child_node->step.get());
    if (result.child_aggregating_step)
    {
        /// From : AggregatingStep =>
        /// To   : AggregatingStep(partial) [=> MergingAggregatedStep(final)] =>
        result.grandchild_step = child_node->children.front()->step.get();
        result.is_shuffle = true;
        LOG_DEBUG(log, "[{}]Check shuffle: {}, child node is AggregatingStep", getStageSeqName(stage_seq), result.is_shuffle);
        stage_seq = StageSeq::STAGE2;
        return;
    }

    result.child_sorting_step = dynamic_cast<SortingStep *>(child_node->step.get());
    if (result.child_sorting_step)
    {
        /// From : SortingStep => Not (LimitStep) =>
        /// To   : SortingStep(partial) [=> SortingStep(final)] => Not (LimitStep) =>
        if (stage_seq == StageSeq::STAGE2)
        {
            result.is_shuffle = false;
            LOG_DEBUG(log, "[{}]Check shuffle: {}, child node is SortingStep", getStageSeqName(stage_seq), result.is_shuffle);
        }
        else if (!(result.current_limit_step = dynamic_cast<LimitStep *>(current_node->step.get())))
        {
            result.is_shuffle = true;
            LOG_DEBUG(log, "[{}]Check shuffle: {}, child node is SortingStep", getStageSeqName(stage_seq), result.is_shuffle);
            stage_seq = StageSeq::STAGE2;
        }
        return;
    }

    if ((result.child_distinct_step = dynamic_cast<DistinctStep *>(child_node->step.get())))
    {
        result.current_distinct_step = dynamic_cast<DistinctStep *>(current_node->step.get());
        if (result.current_distinct_step)
        {
            /// DistinctStep(partial) => (shuffle) => DistinctStep(final) =>
            result.is_shuffle = true;
            LOG_DEBUG(log, "[{}]Check shuffle: {}, both of child and current nodes are DistinctStep", getStageSeqName(stage_seq), result.is_shuffle);
            stage_seq = StageSeq::STAGE2;
        }
        return;
    }

    if ((result.child_limit_step = dynamic_cast<LimitStep *>(child_node->step.get())))
    {
        LOG_DEBUG(log, "[{}]Check shuffle: child node is LimitStep", getStageSeqName(stage_seq));
        if (stage_seq == StageSeq::STAGE2)
        {
            result.is_shuffle = false;
        }
        else
        {
            assert(child_node->children.size() == 1);
            result.grandchild_sorting_step = dynamic_cast<SortingStep *>(child_node->children[0]->step.get());
            /// If grandchild is SortingStep:
            /// From : SortingStep => LimitStep =>
            /// To   : SortingStep(partial) => LimitStep(partial) [=> SortingStep(final) => LimitStep(final)] =>
            result.is_shuffle = true;
            stage_seq = StageSeq::STAGE2;
        }
        return;
    }
}

void DistributedPlanner::transferInterpreterParams(QueryPlan::Node *current_node, QueryPlan::Node *last_node)
{
    if (last_node->interpreter_params)
    {
        if (!current_node->interpreter_params)
        {
            if (current_node->children.size() == 1)
                current_node->interpreter_params = last_node->interpreter_params;
            else
                current_node->interpreter_params = std::make_shared<InterpreterParams>(*last_node->interpreter_params);
            LOG_DEBUG(
                log,
                "Set context({}({}) <= {}({})) to {}",
                current_node->step->getName(),
                current_node->interpreter_params->group_by_with_totals,
                last_node->step->getName(),
                last_node->interpreter_params->group_by_with_totals,
                static_cast<const void *>(current_node->interpreter_params->context.get()));
        }
        else if (current_node->children.size() > 1)
        {
            /// For Join and Union, not for ITransformingStep.
            bool merged_with_totals = current_node->interpreter_params->group_by_with_totals | last_node->interpreter_params->group_by_with_totals;
            LOG_DEBUG(
                log,
                "Merge group_by_with_totals({}({}/{}) <= {}({}/{})) to {}",
                current_node->step->getName(),
                static_cast<void *>(current_node->interpreter_params.get()),
                current_node->interpreter_params->group_by_with_totals,
                last_node->step->getName(),
                static_cast<void *>(last_node->interpreter_params.get()),
                last_node->interpreter_params->group_by_with_totals,
                merged_with_totals);
            current_node->interpreter_params->group_by_with_totals = merged_with_totals;
        }
    }
}

void DistributedPlanner::buildStages()
{
    LOG_DEBUG(log, "===> Build stages.");

    StageSeq stage_seq = StageSeq::STAGE1;
    auto createStage = [this, &stage_seq](
                           int id,
                           std::stack<Stage *> & parent_stages,
                           QueryPlan::Node * root_node,
                           std::stack<QueryPlan::Node *> & leaf_nodes) {
        stages.emplace_back(Stage{.id = id, .root_node = root_node});
        Stage * new_stage = &stages.back();

        if (root_node)
        {
            /// Fill parent stages.
            assert(parent_stages.size() >= root_node->num_parent_stages);
            new_stage->parents.resize(root_node->num_parent_stages);
            for (int i = root_node->num_parent_stages - 1; !parent_stages.empty() && i >= 0; --i)
            {
                new_stage->parents[i] = parent_stages.top();
                new_stage->parents[i]->child = new_stage;
                parent_stages.pop();
            }
            /// Fill leaf nodes.
            assert(leaf_nodes.size() >= root_node->num_leaf_nodes_in_stage);
            new_stage->leaf_nodes.resize(root_node->num_leaf_nodes_in_stage);
            for (int i = root_node->num_leaf_nodes_in_stage - 1; !leaf_nodes.empty() && i >= 0; --i)
            {
                new_stage->leaf_nodes[i] = leaf_nodes.top();
                /// This leaf node is a data source node reading data from storage.
                if (new_stage->leaf_nodes[i]->children.empty())
                    new_stage->is_leaf_stage = true;
                leaf_nodes.pop();
            }
        }
        LOG_DEBUG(log, "[{}]Create stage: id: {}({} parent stages, {} leaf nodes).",getStageSeqName(stage_seq), id, new_stage->parents.size(), new_stage->leaf_nodes.size());
        return new_stage;
    };

    struct Frame
    {
        QueryPlan::Node * node = {};
        int visited_children = 0; /// Number of visited children
    };

    /// Used for visiting the query plan tree.
    std::stack<Frame> stack;
    stack.push(Frame{.node = query_plan.root});
    bool one_child_is_visited = false;

    /// Used for creating stage.
    int stage_id = -1;
    QueryPlan::Node * last_node = nullptr; /// Used for marking the current node's child.
    QueryPlan::Node * leaf_node = nullptr;
    Stage * last_stage = nullptr;
    std::stack<Stage *> parent_stages;
    std::stack<QueryPlan::Node *> leaf_nodes;

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (one_child_is_visited)
        {
            /// This is shuffle, create a new stage for child_node.
            CheckShuffleResult result;
            checkShuffle(frame.node, last_node, result, stage_seq, leaf_nodes);
            if (result.is_shuffle)
            {
                ++stage_id;
                last_stage = createStage(stage_id, parent_stages, last_node, leaf_nodes);
                if (result.child_aggregating_step)
                {
                    last_stage->empty_result_for_aggregation_by_empty_set
                        = result.child_aggregating_step->getParams().empty_result_for_aggregation_by_empty_set;
                }

                /// The new stage is parent of current node's stage.
                parent_stages.push(last_stage);
                frame.node->num_parent_stages += 1;

                /// After creating new stage, current node will be in another stage, so save current node as a candidate leaf node.
                leaf_node = frame.node;
                leaf_nodes.push(leaf_node);
                frame.node->num_leaf_nodes_in_stage += 1;
            }
            else
            {
                frame.node->num_parent_stages += last_node->num_parent_stages;
                frame.node->num_leaf_nodes_in_stage += last_node->num_leaf_nodes_in_stage;
            }

            /// Transfer interpreter params bottom-up.
            transferInterpreterParams(frame.node, last_node);

            ++frame.visited_children;
            one_child_is_visited = false;
        }

        if (frame.node->children.empty())
        {
            if (dynamic_cast<ReadFromRemote *>(frame.node->step.get()))
                throw Exception(
                    "Not support building distributed plan on Distributed table engine, maybe you want to set "
                    "enable_distributed_plan=false",
                    ErrorCodes::LOGICAL_ERROR);
            last_stage = nullptr;
            leaf_node = frame.node;
            leaf_nodes.push(leaf_node);
            frame.node->num_leaf_nodes_in_stage = 1;
        }

        size_t next_child = frame.visited_children;
        if (next_child == frame.node->children.size()
            || (dynamic_cast<CreatingSetsStep *>(frame.node->step.get()) && frame.visited_children == 1))
        {
            LOG_DEBUG(log, "[{}]Visit step: {}({})", getStageSeqName(stage_seq), frame.node->step->getName(), frame.node->step->getStepDescription());
            last_node = frame.node;
            one_child_is_visited = true;
            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child]});
    }

    /// Currently, child_node is the root node of query plan, create stage for it.
    ++stage_id;
    last_stage = createStage(stage_id, parent_stages, last_node, leaf_nodes);

    /// Append result stage for converging data.
    ++stage_id;

    /// Create a virtual node, used in iterating stages.
    /// It will be recreated in sending stages if needed.
    parent_stages.push(last_stage);
    auto step = std::make_unique<DistributedSourceStep>(stage_id, last_stage->id, context);
    query_plan.nodes.emplace_back(QueryPlan::Node{
        .step = std::move(step),
        .children = {last_node},
        .num_parent_stages = 1,
        .interpreter_params = last_node->interpreter_params});
    query_plan.root = &query_plan.nodes.back();
    LOG_DEBUG(
        log,
        "Set context({}({}) <= {}({})) to {}",
        query_plan.root->step->getName(),
        query_plan.root->interpreter_params->group_by_with_totals,
        last_node->step->getName(),
        last_node->interpreter_params->group_by_with_totals,
        static_cast<const void *>(query_plan.root->interpreter_params->context.get()));

    /// Maintain leaf nodes.
    leaf_nodes.push(query_plan.root);
    query_plan.root->num_leaf_nodes_in_stage = 1;

    result_stage = createStage(stage_id, parent_stages, query_plan.root, leaf_nodes);

    debugStages();
}

void DistributedPlanner::debugStages()
{
    WriteBufferFromOwnString buf;
    for (const auto & stage : stages)
    {
        if (stage.is_leaf_stage)
        {
            buf << "stage id (leaf)     : ";
        }
        else
        {
            buf << "stage id (non-leaf) : ";
        }
        buf << stage.id;
        if (stage.child)
        {
            buf << " => " << stage.child->id;
        }
        buf.write('\n');

        buf << "parent stages id    : ";
        for (const auto parent_stage : stage.parents)
        {
            buf << parent_stage->id << " ";
        }
        buf.write('\n');

        if (stage.root_node)
        {
            buf << "root node           : " << stage.root_node->step->getName();
            buf.write('\n');
        }

        buf << "leaf nodes          :\n";
        /// Iterate reversely, because leaf node are stored right to left.
        for (auto it = stage.leaf_nodes.rbegin(); it != stage.leaf_nodes.rend(); ++it)
        {
            buf << "  " << (*it)->step->getName();
            if ((*it)->children.empty())
            {
                buf << " [S]";
                if (const auto * step = dynamic_cast<ReadFromMergeTree *>((*it)->step.get()))
                {
                    const auto & storage_id = step->getStorageID();
                    buf << " (" << storage_id.database_name << "." << storage_id.table_name << ")";
                }
            }
            buf.write('\n');
        }

        buf << "------------------------------\n";
    }
    LOG_DEBUG(log, "===> Print Stages:\n{}", buf.str());
}

bool DistributedPlanner::isSinglePointDataSource(const String & name)
{
    /// They are ReadFromStorageStep with storage name as follows.
    static std::unordered_set<String> single_point_storages{"SystemClusters", "SystemDatabases", "SystemTables", "SystemColumns",
                                                        "SystemDictionaries", "SystemDataSkippingIndices",
                                                        "SystemFunctions", "SystemFormats", "SystemTableEngines",
                                                        "SystemUsers", "SystemRoles", "SystemGrants", "SystemRoleGrants",
                                                        "SystemCurrentRoles", "SystemEnabledRoles", "SystemRowPolicies", "SystemPrivileges",
                                                        "SystemQuotas", "SystemQuotaLimits",
                                                        "SystemSettings", "SystemSettingsProfiles", "SystemSettingsProfileElements",
                                                        "SystemZooKeeper", "SystemProcesses",
                                                        "SystemNumbers", "SystemOne", "SystemZeros",
                                                        "SystemContributors", "SystemLicenses",
                                                        "SystemReplicatedMergeTreeSettings", "SystemMergeTreeSettings",
                                                        "Memory"};
    return single_point_storages.contains(name);
}

void DistributedPlanner::scheduleStages(PlanResult & plan_result)
{
    LOG_DEBUG(log, "===> Schedule stages.");
    /// Use current query id to build the plan fragment id.
    const String & initial_query_id = context->getClientInfo().current_query_id;

    /// Get my replica grpc address
    String my_replica = context->getMacros()->getValue("replica") + ":" + toString(context->getServerPort("grpc_port"));

    /// Retrieve all replicas.
    std::unordered_map<String, ClustersWatcher::ReplicaInfoPtr> replicas = context->getClustersWatcher().getContainer();
    LOG_DEBUG(log, "Schedule stages for query id {} across {} workers.", initial_query_id, replicas.size());
    std::vector<std::shared_ptr<String>> store_replicas, compute_replicas;
    for (const auto & replica : replicas)
    {
        const auto & replica_info = replica.second;
        LOG_DEBUG(
            log,
            "Check worker: {} => ({}/{}/{}, {}).",
            replica.first,
            replica_info->type,
            replica_info->group,
            replica_info->name,
            replica_info->address);

        if (replica_info->type == "store")
        {
            store_replicas.emplace_back(std::make_shared<String>(replica_info->address));
        }
        else
        {
            compute_replicas.emplace_back(std::make_shared<String>(replica_info->address));
        }
    }
    LOG_DEBUG(log, "{} store workers, {} compute workers.", store_replicas.size(), compute_replicas.size());
    if (store_replicas.empty() || compute_replicas.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No enough store workers({}) or compute workers({}).", store_replicas.size(), compute_replicas.size());

    static std::unordered_set<String> special_storages{"HDFS", "S3", "MySQL", "Memory"};

    auto fillStage = [&](Stage * stage)
    {
        /// Leaf stage.
        if (stage->is_leaf_stage)
        {
            bool is_multi_points_data_source = false;
            for (const auto leaf_node : stage->leaf_nodes)
            {
                /// It's a data source.
                if (leaf_node->children.empty())
                {
                    /// It's system table or special storage.
                    if (isSinglePointDataSource(leaf_node->step->getStepDescription()) ||
                        special_storages.contains(leaf_node->step->getStepDescription()))
                    {
                    }
                    /// It's StorageValues.
                    else if (leaf_node->step->getStepDescription() == "Values")
                    {
                        /// StorageValues is used in:
                        /// 1. Trigger materalized view: has view source.
                        /// 2. Execute "SELECT ... FROM values(...)": has no view source.
                        stage->maybe_has_view_source = true;
                    }
                    else if (leaf_node->step->getStepDescription() == "Input")
                    {
                        stage->has_input_function = true;
                    }
                    else
                    {
                        LOG_DEBUG(
                            log,
                            "Leaf node {}({}) is multi-points data source.",
                            leaf_node->step->getName(),
                            leaf_node->step->getStepDescription());
                        is_multi_points_data_source = true;
                        break;
                    }
                }
            }
            /// Fill workers.
            if (is_multi_points_data_source)
            {
                stage->workers.reserve(store_replicas.size());
                stage->workers.insert(stage->workers.end(), store_replicas.begin(), store_replicas.end());
                LOG_DEBUG(log, "Schedule stage {} to {} workers.", stage->id, stage->workers.size());
            }
            else
            {
                stage->workers.emplace_back(std::make_shared<String>(my_replica));
                LOG_DEBUG(log, "Schedule stage {} to 1 worker(local).", stage->id);
            }
            return;
        }

        /// Result stage.
        if (stage == result_stage)
        {
            stage->workers.emplace_back(std::make_shared<String>(my_replica));

            /// Maybe the last stage can be eliminated.
            if (stage->parents.size() == 1)
            {
                auto * parent = stage->parents.front();
                /// Parent stage will be scheduled on the same worker as result stage.
                if (parent->workers.size() == 1 && *(parent->workers.front()) == my_replica)
                {
                    /// Use result stage's parent as result stage.
                    LOG_DEBUG(log, "Move result stage {} forward to stage {}.", result_stage->id, parent->id);
                    assert(result_stage == &stages.back());
                    result_stage = parent;
                    query_plan.root = parent->root_node;
                    stages.pop_back();
                    plan_result.is_result_stage_moved_forward = true;
                    return;
                }
            }
            LOG_DEBUG(log, "Schedule stage {} to 1 worker(local).", stage->id);
            return;
        }

        /// Intermediate stage.
        stage->workers.emplace_back(std::make_shared<String>(my_replica));
        LOG_DEBUG(log, "Schedule stage {} to 1 worker(local).", stage->id);
    };

    struct Frame
    {
        Stage * stage = {};
        int visited_parents = 0; /// Number of visited parents
    };

    assert(result_stage != nullptr);
    std::stack<Frame> stack;
    stack.push(Frame{.stage = result_stage});

    bool one_parent_is_visited = false;
    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (one_parent_is_visited)
        {
            ++frame.visited_parents;
            one_parent_is_visited = false;
        }

        size_t next_parent = frame.visited_parents;
        if (next_parent == frame.stage->parents.size())
        {
            LOG_DEBUG(log, "Visit stage: {}", frame.stage->id);

            fillStage(frame.stage);

            one_parent_is_visited = true;
            stack.pop();
        }
        else
        {
            stack.push(Frame{.stage = frame.stage->parents[next_parent]});
        }
    }

    /// Create query info.
    GRPCQueryInfo query_info;
    {
        /// Fill with data shared among stages.
        query_info.set_database(context->getCurrentDatabase());
        query_info.set_output_format("Native");

        assert(!context->getClientInfo().distributed_query.empty());
        query_info.set_query(context->getClientInfo().distributed_query);
        query_info.set_initial_query_id(initial_query_id);

        /// Fill changed settings.
        for (const auto setting : context->getSettingsRef().allChanged())
        {
            (*query_info.mutable_settings())[setting.getName()] = setting.getValueString();
        }
    }

    /// Send query info.
    LOG_DEBUG(log, "===> Send stages.");
    for (auto & stage : stages)
    {
        /// Don't send result stage.
        if (&stage == result_stage)
        {
            assert(!result_stage->parents.empty());
            if (plan_result.is_result_stage_moved_forward)
            {
                Context::QueryPlanFragmentInfo query_plan_fragment_info{
                    .initial_query_id = initial_query_id,
                    .stage_id = stage.id,
                    .node_id = my_replica
                };
                for (const auto parent : stage.parents)
                {
                    query_plan_fragment_info.parent_sources[parent->id] = parent->workers;
                }
                query_plan_fragment_info.sinks = stage.sinks;
                context->setQueryPlanFragmentInfo(query_plan_fragment_info);

                /// Add external tables to current context.
                assert(stage.root_node->interpreter_params);
                const auto & external_table_holders = stage.root_node->interpreter_params->context->getExternalTableHolders();
                LOG_DEBUG(
                    log,
                    "Add {} external tables using context {} from local.",
                    external_table_holders.size(),
                    static_cast<const void *>(stage.root_node->interpreter_params->context.get()));
                for (const auto & table : external_table_holders)
                {
                    auto table_holder = table.second;
                    String table_name = table.first;

                    LOG_DEBUG(
                        log,
                        "Add external table {} with {} ({}) from local.",
                        table_name,
                        table_holder->getTable()->getStorageID().getFullNameNotQuoted(),
                        table_holder->getTable()->getName());
                    try
                    {
                        context->addExternalTable(table_name, std::move(*table_holder));
                    }
                    catch (Exception & e)
                    {
                        if (e.code() != ErrorCodes::TABLE_ALREADY_EXISTS)
                            throw;
                    }
                }
            }
            else
            {
                /// Clear query plan tree.
                query_plan.root = nullptr;

                /// Note: There will be only one parent stage for the result stage.
                for (const auto parent_stage : result_stage->parents)
                {
                    const QueryPlan::Node * parent_stage_node = parent_stage->root_node;
                    const auto & header = parent_stage_node->step->getOutputStream().header;
                    assert(header);
                    LOG_DEBUG(
                        log,
                        "Take the output stream header of {}: {}, header columns: {}.",
                        parent_stage_node->step->getName(),
                        parent_stage_node->step->getStepDescription(),
                        header.columns());

                    auto distributed_source_step = std::make_unique<DistributedSourceStep>(
                        header,
                        parent_stage->workers,
                        initial_query_id,
                        result_stage->id,
                        parent_stage->id,
                        *result_stage->workers.front(),
                        false,
                        parent_stage_node->interpreter_params->group_by_with_totals,
                        context);
                    query_plan.addStep(std::move(distributed_source_step));
                    plan_result.distributed_source_nodes.emplace_back(query_plan.root);
                }
            }
            continue;
        }

        /// Fill sinks.
        if (!stage.child->workers.empty())
        {
            stage.sinks.reserve(stage.child->workers.size());
            stage.sinks.insert(stage.sinks.end(), stage.child->workers.begin(), stage.child->workers.end());
        }

        LOG_DEBUG(log, "Stage {} has {} workers.", stage.id, stage.workers.size());
        assert(!stage.workers.empty());

        /// Fill with data related to each stage.
        query_info.set_query_id(context->generateQueryId());
        query_info.set_stage_id(stage.id);
        query_info.set_empty_result_for_aggregation_by_empty_set(stage.empty_result_for_aggregation_by_empty_set);

        /// TODO: Not all stages need external tables, so choose the ones that are necessary, at least for leaf stages.
        /// Fill external tables(reference from Connection.cpp: void Connection::sendExternalTablesData(ExternalTablesData & data)):
        if (stage.is_leaf_stage)
        {
            if (!stage.root_node->interpreter_params)
                LOG_DEBUG(log, "No need to prepare external tables data, because interpreter_params is null.");
            else
            {
                assert(stage.root_node->interpreter_params->context);
                /// 1.Construct ExternalTablesData.
                ExternalTablesData external_tables_data;
                {
                    const auto & external_tables = stage.root_node->interpreter_params->context->getExternalTables();
                    LOG_DEBUG(
                        log,
                        "Prepare {} external tables using context {}.",
                        external_tables.size(),
                        static_cast<const void *>(stage.root_node->interpreter_params->context.get()));
                    for (const auto & table : external_tables)
                    {
                        StoragePtr cur = table.second;

                        auto data = std::make_unique<ExternalTableData>();
                        data->table_name = table.first;

                        LOG_DEBUG(
                            log,
                            "Prepare external table {} with {} ({}).",
                            data->table_name,
                            cur->getStorageID().getFullNameNotQuoted(),
                            cur->getName());
                        {
                            SelectQueryInfo select_query_info;
                            auto metadata_snapshot = cur->getInMemoryMetadataPtr();
                            QueryProcessingStage::Enum read_from_table_stage = cur->getQueryProcessingStage(
                                stage.root_node->interpreter_params->context, QueryProcessingStage::Complete, metadata_snapshot, select_query_info);

                            Pipe pipe = cur->read(
                                metadata_snapshot->getColumns().getNamesOfPhysical(),
                                metadata_snapshot,
                                select_query_info,
                                stage.root_node->interpreter_params->context,
                                read_from_table_stage,
                                DEFAULT_BLOCK_SIZE,
                                1);

                            if (pipe.empty())
                            {
                                data->pipe = std::make_unique<Pipe>(
                                    std::make_shared<SourceFromSingleChunk>(metadata_snapshot->getSampleBlock(), Chunk()));
                            }
                            else
                            {
                                data->pipe = std::make_unique<Pipe>(std::move(pipe));
                            }
                        }
                        external_tables_data.emplace_back(std::move(data));
                    }
                }

                /// Fill external tables:
                /// 2.Construct grpc data.
                for (auto & data : external_tables_data)
                {
                    Stopwatch watch;
                    clickhouse::grpc::ExternalTable external_table;
                    external_table.set_name(data->table_name);
                    external_table.set_format("Native");

                    assert(data->pipe);

                    QueryPipelineBuilder pipeline_builder;
                    pipeline_builder.init(std::move(*data->pipe));
                    data->pipe.reset();
                    pipeline_builder.resize(1);
                    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));

                    /// Fill columns name and type.
                    auto header = pipeline.getHeader();
                    for (size_t i = 0; i < header.columns(); ++i)
                    {
                        ColumnWithTypeAndName column = header.safeGetByPosition(i);
                        clickhouse::grpc::NameAndType name_and_type;
                        name_and_type.set_name(column.name);
                        name_and_type.set_type(column.type->getName());
                        external_table.mutable_columns()->Add(std::move(name_and_type));
                    }

                    /// Fill data.
                    std::optional<WriteBufferFromString> write_buffer;
                    write_buffer.emplace(*external_table.mutable_data());
                    std::shared_ptr<IOutputFormat> output_format_processor = context->getOutputFormat("Native", *write_buffer, header);
                    output_format_processor->doWritePrefix();

                    Block block;
                    size_t rows = 0, bytes = 0;
                    auto executor = std::make_shared<PullingAsyncPipelineExecutor>(pipeline);
                    while (executor->pull(block, 100))
                    {
                        if (block)
                        {
                            rows += block.rows();
                            bytes += block.bytes();
                            output_format_processor->write(materializeBlock(block));
                        }
                    }
                    output_format_processor->doWriteSuffix();
                    LOG_DEBUG(
                        log,
                        "Fill external table {} with {} rows, {} bytes in {} sec.",
                        external_table.name(),
                        rows,
                        bytes,
                        watch.elapsedSeconds());

                    query_info.mutable_external_tables()->Add(std::move(external_table));
                }
            }
        }

        /// Fill parents id and sources.
        query_info.clear_parent_sources();
        for (const auto parent : stage.parents)
        {
            clickhouse::grpc::MapEntry entry;
            for (const auto & source : parent->workers)
                entry.add_sources(*source);
            (*query_info.mutable_parent_sources())[parent->id] = entry;
        }

        /// Fill sinks.
        query_info.clear_sinks();
        for (const auto & sink : stage.sinks)
        {
            query_info.add_sinks(*sink);
        }

        /// Send query info to each remote worker.
        for (const auto & worker : stage.workers)
        {
            Stopwatch watch;
            query_info.set_node_id(*worker);
            LOG_DEBUG(log, "Remote plan fragment:\n{}", debugRemotePlanFragment(query_info.query(), *worker, initial_query_id, &stage));

            if (stage.maybe_has_view_source || stage.has_input_function)
            {
                const String & plan_fragment_id
                    = query_info.initial_query_id() + "/" + toString(query_info.stage_id()) + "/" + query_info.node_id();
                if (stage.maybe_has_view_source)
                {
                    const auto & view_source = context->getViewSource();
                    if (view_source)
                    {
                        LOG_DEBUG(
                            log,
                            "Store initial context {} for plan fragment {}, because has view source: {}({}).",
                            static_cast<void *>(context.get()),
                            plan_fragment_id,
                            view_source->getStorageID().getFullNameNotQuoted(),
                            view_source->getName());
                        context->addInitialContext(plan_fragment_id, context);
                    }
                    else
                        stage.maybe_has_view_source = false;
                }
                else
                {
                    LOG_DEBUG(
                        log,
                        "Store initial context {} for plan fragment {}, because has input function.",
                        static_cast<void *>(context->getQueryContext().get()),
                        plan_fragment_id);
                    context->addInitialContext(plan_fragment_id, context->getQueryContext());
                }
            }
            query_info.set_has_view_source(stage.maybe_has_view_source);
            query_info.set_has_input_function(stage.has_input_function);

            GRPCClient cli(*worker, "initiator");
            auto result = cli.executePlanFragment(query_info);
            LOG_DEBUG(
                log,
                "Finish sending GRPC query info in {} sec. {}",
                watch.elapsedSeconds(),
                result.exception().code() == 0
                ? ""
                : "Exception: (code " + toString(result.exception().code()) + ") " + result.exception().display_text());
        }
    }
}

void DistributedPlanner::buildPlanFragment(PlanResult & plan_result)
{
    const auto & query_distributed_plan_info = context->getQueryPlanFragmentInfo();
    int my_stage_id = query_distributed_plan_info.stage_id;
    LOG_DEBUG(
        log,
        "===> [{}]Build plan fragment: {} stage {}({} parent stages).",
        plan_result.is_result_stage_moved_forward ? "From Local" : "From Remote",
        (result_stage ? (my_stage_id == result_stage->id ? "result": "non-result") : "non-result"),
        my_stage_id,
        query_distributed_plan_info.parent_sources.size());

    /// Clean states that may be set in building stages.
    if (plan_result.is_result_stage_moved_forward)
    {
        for (auto & node : query_plan.nodes)
        {
            node.num_parent_stages = 0;
            node.num_leaf_nodes_in_stage = 0;
        }
    }

    /// Get my replica grpc address
    String my_replica = context->getMacros()->getValue("replica") + ":" + toString(context->getServerPort("grpc_port"));

    auto popLeafNodes = [](QueryPlan::Node * root_node, std::stack<QueryPlan::Node *> & leaf_nodes) {
        if (root_node)
        {
            for (size_t i = 0; !leaf_nodes.empty() && i < root_node->num_leaf_nodes_in_stage; ++i)
            {
                leaf_nodes.pop();
            }
        }
    };

    struct Frame
    {
        QueryPlan::Node * node = {};
        int visited_children = 0; /// Number of visited children
    };

    /// Used for visiting the query plan tree.
    std::stack<Frame> stack;
    stack.push(Frame{.node = query_plan.root});
    bool one_child_is_visited = false;
    std::stack<QueryPlan::Node *> leaf_nodes;

    /// Used for locating the plan fragment.
    int stage_id = -1;
    QueryPlan::Node * last_node = nullptr;
    QueryPlan::Node * leaf_node = nullptr;
    StageSeq stage_seq = StageSeq::STAGE1;

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (one_child_is_visited)
        {
            /// Transfer interpreter params bottom-up.
            transferInterpreterParams(frame.node, last_node);

            CheckShuffleResult result;
            checkShuffle(frame.node, last_node, result, stage_seq, leaf_nodes);

            /// This is a shuffle dependency between current node and the last visited child.
            if (result.is_shuffle)
            {
                ++stage_id;
                popLeafNodes(last_node, leaf_nodes);

                /// This is one of my parent stages.
                const auto & it = query_distributed_plan_info.parent_sources.find(stage_id);
                if (it != query_distributed_plan_info.parent_sources.end())
                {
                    assert(last_node == frame.node->children[frame.visited_children]);

                    /// Add steps between current node and child node.
                    auto addStep = [this, &stage_id, &last_node, &stage_seq](QueryPlanStepPtr step, const String & description, QueryPlan::Node * & node)
                    {
                        LOG_DEBUG(log, "[{}]Add step: {}, parent stage id: {}", getStageSeqName(stage_seq), step->getName(), stage_id);
                        step->setStepDescription(description);
                        if (!node)
                            query_plan.nodes.emplace_back(QueryPlan::Node{.step = std::move(step), .interpreter_params = last_node->interpreter_params});
                        else
                        {
                            query_plan.nodes.emplace_back(QueryPlan::Node{.step = std::move(step), .children = {node}, .interpreter_params = last_node->interpreter_params});
                            node->parent = &query_plan.nodes.back();
                        }
                        node = &query_plan.nodes.back();
                        LOG_DEBUG(
                            log,
                            "Set context({}({}) <= {}({})) to {}",
                            node->step->getName(),
                            node->interpreter_params->group_by_with_totals,
                            last_node->step->getName(),
                            last_node->interpreter_params->group_by_with_totals,
                            static_cast<const void *>(node->interpreter_params->context.get()));
                    };

                    bool add_agg_info = false;
                    std::unique_ptr<AggregatingStep> aggregating_step;
                    if (result.child_aggregating_step)
                    {
                        add_agg_info = true;
                        /// Create AggregatingStep, and it should be non-final.
                        aggregating_step = std::make_unique<AggregatingStep>(*result.child_aggregating_step);
                    }
                    /// The aggregating_step header will include aggregate function.
                    const auto & header = result.child_aggregating_step == nullptr
                        ? last_node->step->getOutputStream().header
                        : ((result.child_aggregating_step->getParams().optimize_trivial_count && result.grandchild_step)
                               ? result.grandchild_step->getOutputStream().header
                               : aggregating_step->getOutputStream().header);

                    /// Create DistributedSourceStep.
                    assert(header);
                    const auto & sources = it->second;
                    auto distributed_source_step = std::make_unique<DistributedSourceStep>(
                        header,
                        sources,
                        query_distributed_plan_info.initial_query_id,
                        my_stage_id,
                        stage_id,
                        my_replica,
                        add_agg_info,
                        last_node->interpreter_params->group_by_with_totals,
                        context);
                    QueryPlan::Node * new_node = nullptr;
                    addStep(std::move(distributed_source_step), "", new_node);
                    plan_result.distributed_source_nodes.emplace_back(new_node); /// For debug

                    /// If current step is JoinStep or UnionStep, only add DistributedSourceStep.

                    /// If parent stage has aggregate, add MergingAggregatedStep.
                    if (result.child_aggregating_step)
                    {
                        assert(last_node->interpreter_params);
                        bool aggregate_final = !last_node->interpreter_params->group_by_with_totals
                                               && !last_node->interpreter_params->group_by_with_rollup
                                               && !last_node->interpreter_params->group_by_with_cube;
                        LOG_DEBUG(log, "MergingAggregatedStep final: {}", aggregate_final);

                        auto transform_params = std::make_shared<AggregatingTransformParams>(aggregating_step->getParams(), aggregate_final);
                        transform_params->params.intermediate_header = new_node->step->getOutputStream().header;

                        const auto & settings = context->getSettingsRef();
                        auto merging_aggregated = std::make_unique<MergingAggregatedStep>(
                            new_node->step->getOutputStream(),
                            std::move(transform_params),
                            settings.distributed_aggregation_memory_efficient,
                            settings.max_threads,
                            settings.aggregation_memory_efficient_merge_threads);

                        addStep(std::move(merging_aggregated), "Merge aggregated streams for distributed AGGREGATE", new_node);
                    }
                    /// If parent stage has order by, add SortingStep.
                    else if (result.child_sorting_step)
                    {
                        auto merging_sorted = std::make_unique<SortingStep>(new_node->step->getOutputStream(), *result.child_sorting_step);
                        addStep(std::move(merging_sorted), "Merge sorted streams for distributed ORDER BY", new_node);
                    }
                    /// If parent stage has distinct, do nothing.
                    else if (result.child_distinct_step)
                    {
                        /// Do nothing
                    }
                    /// If parent stage has limit, add LimitStep.
                    else if (result.child_limit_step)
                    {
                        if  (result.grandchild_sorting_step)
                        {
                            auto merging_sorted
                                = std::make_unique<SortingStep>(new_node->step->getOutputStream(), *result.grandchild_sorting_step);
                            addStep(std::move(merging_sorted), "Merge sorted streams for distributed ORDER BY", new_node);
                        }

                        auto limit = std::make_unique<LimitStep>(new_node->step->getOutputStream(), *result.child_limit_step);
                        addStep(std::move(limit), "distributed LIMIT", new_node);
                    }

                    /// Add new child node to current node.
                    frame.node->children[frame.visited_children] = new_node;
                }
                else if (stage_id == my_stage_id)
                {
                    auto replaceStep = [this, &stage_id, &stage_seq](QueryPlanStepPtr step, QueryPlan::Node * & node)
                    {
                        LOG_DEBUG(log, "[{}]Replace step: {}, stage: {}", getStageSeqName(stage_seq), step->getName(), stage_id);
                        node->step = std::move(step);
                    };

                    /// If child is AggregatingStep.
                    if (result.child_aggregating_step)
                    {
                        /// If NOT optimize trivial count, replace AggregatingStep with final=false.
                        if (!result.child_aggregating_step->getParams().optimize_trivial_count)
                        {
                            auto aggregating_step = std::make_unique<AggregatingStep>(*result.child_aggregating_step);
                            if (query_distributed_plan_info.empty_result_for_aggregation_by_empty_set)
                            {
                                LOG_DEBUG(log, "Set empty_result_for_aggregation_by_empty_set to true for AggregatingStep",
                                          query_distributed_plan_info.empty_result_for_aggregation_by_empty_set);
                                aggregating_step->setEmptyResultForAggregationByEmptySet(
                                    query_distributed_plan_info.empty_result_for_aggregation_by_empty_set);
                            }
                            replaceStep(std::move(aggregating_step), last_node);
                        }
                        /// If optimize trivial count, remove AggregatingStep.
                        else
                        {
                            LOG_DEBUG(log, "[{}]Remove step: {}, stage: {}", getStageSeqName(stage_seq), result.child_aggregating_step->getName(), stage_id);
                            last_node = last_node->children[0];
                        }
                    }
                    /// If limit step is pushed down, collect (limit + offset) rows.
                    else if (result.child_limit_step)
                        result.child_limit_step->resetLimitAndOffset();

                    query_plan.root = last_node;

                    return;
                }

                /// Set the number of parent stages and leaf nodes.
                frame.node->num_parent_stages += 1;
                leaf_node = frame.node;
                leaf_nodes.push(leaf_node);
                frame.node->num_leaf_nodes_in_stage += 1;
            }
            else
            {
                frame.node->num_parent_stages += last_node->num_parent_stages;
                frame.node->num_leaf_nodes_in_stage += last_node->num_leaf_nodes_in_stage;
            }

            ++frame.visited_children;
            one_child_is_visited = false;
        }

        /// Set the number of leaf nodes.
        if (frame.node->children.empty())
        {
            leaf_node = frame.node;
            leaf_nodes.push(leaf_node);
            frame.node->num_leaf_nodes_in_stage = 1;
        }

        size_t next_child = frame.visited_children;
        if (next_child == frame.node->children.size()
            || (dynamic_cast<CreatingSetsStep *>(frame.node->step.get()) && frame.visited_children == 1))
        {
            LOG_DEBUG(log, "[{}]Visit step: {}({})", getStageSeqName(stage_seq), frame.node->step->getName(), frame.node->step->getStepDescription());
            last_node = frame.node;
            one_child_is_visited = true;
            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child]});
    }

    /// Check the last stage(in fact it's the parent stage of the result stage).
    ++stage_id;
    if (stage_id == my_stage_id)
        query_plan.root = last_node;
}

bool DistributedPlanner::buildDistributedPlan()
{
    if (!context->getSettingsRef().enable_distributed_plan)
    {
        LOG_DEBUG(log, "Skip building distributed plan, because enable_distributed_plan=false.");
        return false;
    }

    if (context->getInitialQueryId() == "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz")
    {
        LOG_DEBUG(log, "Skip building distributed plan, because reserved initial query id is ignored.");
        return false;
    }

    if (context->getSkipDistributedPlan())
    {
        LOG_DEBUG(log, "Skip building distributed plan, because skip_distributed_plan is true.");
        return false;
    }

    {
        /// Print the original query plan.
        WriteBufferFromOwnString buf;
        buf << "------ Original Query Plan ------\n";
        buf << "SQL: " << context->getClientInfo().distributed_query << "\n";
        QueryPlan::ExplainPlanOptions options{.header = true, .actions = true};
        query_plan.explainPlan(buf, options);
        LOG_DEBUG(log, "[{}] Original query plan:\n{}", static_cast<void*>(context.get()), buf.str());
    }

    /// Query hits directly on the store worker node.
    if (context->isInitialQuery() && context->getRunningMode() == Context::RunningMode::STORE)
    {
        LOG_DEBUG(log, "Skip building distributed plan, because initial query hits directly on store worker.");
        return false;
    }

    std::vector<std::unique_ptr<QueryPlan>> creating_set_plans;
    query_plan.collectCreatingSetPlan(creating_set_plans);

    PlanResult plan_result;
    if (context->isInitialQuery())
    {
        buildStages();

        scheduleStages(plan_result);
        if (plan_result.is_result_stage_moved_forward)
            buildPlanFragment(plan_result);

        if (!creating_set_plans.empty() && !dynamic_cast<CreatingSetsStep *>(query_plan.root->step.get()))
        {
            /// This will change query_plan.root, so save new root.
            uniteCreatingSetSteps(creating_set_plans);
            result_stage->root_node = query_plan.root;
        }

        if (!plan_result.is_result_stage_moved_forward)
        {
            plan_result.initial_query_id = context->getClientInfo().current_query_id;
            plan_result.stage_id = result_stage->id;
            plan_result.node_id = *result_stage->workers.front();
        }
        else
        {
            const auto & query_distributed_plan_info = context->getQueryPlanFragmentInfo();
            plan_result.initial_query_id = query_distributed_plan_info.initial_query_id;
            plan_result.stage_id = query_distributed_plan_info.stage_id;
            plan_result.node_id = query_distributed_plan_info.node_id;
        }
    }
    else
    {
        buildPlanFragment(plan_result);

        if (!creating_set_plans.empty() && !dynamic_cast<CreatingSetsStep *>(query_plan.root->step.get()))
            uniteCreatingSetSteps(creating_set_plans);

        const auto & query_distributed_plan_info = context->getQueryPlanFragmentInfo();
        plan_result.initial_query_id = query_distributed_plan_info.initial_query_id;
        plan_result.stage_id = query_distributed_plan_info.stage_id;
        plan_result.node_id = query_distributed_plan_info.node_id;
    }

    LOG_DEBUG(log, "Local plan fragment:\n{}", debugLocalPlanFragment(plan_result));

    return true;
}

void DistributedPlanner::uniteCreatingSetSteps(std::vector<std::unique_ptr<QueryPlan>> & creating_set_plans)
{
    if (creating_set_plans.empty())
        return;

    LOG_DEBUG(log, "Unite {} CreatingSetStep", creating_set_plans.size());

    InterpreterParamsPtr interpreter_params = query_plan.root->interpreter_params;

    DataStreams input_streams;
    input_streams.emplace_back(query_plan.getCurrentDataStream());

    std::vector<std::unique_ptr<QueryPlan>> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    query_plan = QueryPlan();

    for (auto & creating_set_plan : creating_set_plans)
    {
        input_streams.emplace_back(creating_set_plan->getCurrentDataStream());
        plans.emplace_back(std::move(creating_set_plan));
    }

    auto creating_sets = std::make_unique<CreatingSetsStep>(std::move(input_streams));
    creating_sets->setStepDescription("Create sets before main query execution");
    query_plan.unitePlans(std::move(creating_sets), std::move(plans), std::move(interpreter_params));
}

String DistributedPlanner::debugLocalPlanFragment(PlanResult & plan_result)
{
    WriteBufferFromOwnString buf;
    buf << "------ Local Plan Fragment ------\n";
    buf << "Fragment ID: " << plan_result.initial_query_id << "/" << plan_result.stage_id << "/" << plan_result.node_id;
    buf.write('\n');
    buf << "Distributed Source Nodes: " << plan_result.distributed_source_nodes.size();
    buf.write('\n');
    for (size_t i = 0; i < plan_result.distributed_source_nodes.size(); ++i)
    {
        const QueryPlan::Node * node = plan_result.distributed_source_nodes[i];
        auto distributed_source_step = dynamic_cast<DistributedSourceStep*>(node->step.get());
        buf << "[" << i << "]" << distributed_source_step->getName() << ", sources: ";
        for (const auto & source : distributed_source_step->getSources())
            buf << *source << " ";
        buf.write('\n');
    }
    buf << "\nPlan Fragment:\n";
    QueryPlan::ExplainPlanOptions options{.header = true, .actions = true};
    query_plan.explainPlan(buf, options);
    return buf.str();
}

String DistributedPlanner::debugRemotePlanFragment(
    const String & query,
    const String & worker,
    const String & query_id,
    const Stage * stage)
{
    WriteBufferFromOwnString buf;
    buf << "------ Remote Plan Fragment ------\n";
    buf << "Query: " << query;
    buf.write('\n');
    buf << "Worker: " << worker;
    buf.write('\n');
    buf << "Fragment ID: " << query_id << "/" << stage->id << "/" << worker;
    buf.write('\n');
    buf << "Sources:\n";
    for (const auto parent : stage->parents)
    {
        buf << "  parent stage id: " << parent->id << ", sources: ";
        for (const auto & source : parent->workers)
            buf << *source << " ";
        buf.write('\n');
    }
    buf << "Sinks: ";
    for (const auto & sink : stage->sinks)
    {
        buf << *sink << " ";
    }
    buf.write('\n');
    return buf.str();
}


}
