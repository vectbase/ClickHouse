#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/WriteBuffer.h>
#include <IO/Operators.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>
#include <stack>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/DistributedSourceStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Common/JSONBuilder.h>
#include <Common/Macros.h>
#include <Interpreters/Cluster.h>
#include <Client/GRPCClient.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

QueryPlan::QueryPlan() : log(&Poco::Logger::get("QueryPlan"))
{
}
QueryPlan::~QueryPlan() = default;
QueryPlan::QueryPlan(QueryPlan &&) = default;
QueryPlan & QueryPlan::operator=(QueryPlan &&) = default;

void QueryPlan::checkInitialized() const
{
    if (!isInitialized())
        throw Exception("QueryPlan was not initialized", ErrorCodes::LOGICAL_ERROR);
}

void QueryPlan::checkNotCompleted() const
{
    if (isCompleted())
        throw Exception("QueryPlan was already completed", ErrorCodes::LOGICAL_ERROR);
}

bool QueryPlan::isCompleted() const
{
    return isInitialized() && !root->step->hasOutputStream();
}

const DataStream & QueryPlan::getCurrentDataStream() const
{
    checkInitialized();
    checkNotCompleted();
    return root->step->getOutputStream();
}

void QueryPlan::unitePlans(QueryPlanStepPtr step, std::vector<std::unique_ptr<QueryPlan>> plans, InterpreterParamsPtr interpreter_params)
{
    if (isInitialized())
        throw Exception("Cannot unite plans because current QueryPlan is already initialized",
                        ErrorCodes::LOGICAL_ERROR);

    const auto & inputs = step->getInputStreams();
    size_t num_inputs = step->getInputStreams().size();
    if (num_inputs != plans.size())
    {
        throw Exception("Cannot unite QueryPlans using " + step->getName() +
                        " because step has different number of inputs. "
                        "Has " + std::to_string(plans.size()) + " plans "
                        "and " + std::to_string(num_inputs) + " inputs", ErrorCodes::LOGICAL_ERROR);
    }

    for (size_t i = 0; i < num_inputs; ++i)
    {
        const auto & step_header = inputs[i].header;
        const auto & plan_header = plans[i]->getCurrentDataStream().header;
        if (!blocksHaveEqualStructure(step_header, plan_header))
            throw Exception("Cannot unite QueryPlans using " + step->getName() + " because "
                            "it has incompatible header with plan " + root->step->getName() + " "
                            "plan header: " + plan_header.dumpStructure() +
                            "step header: " + step_header.dumpStructure(), ErrorCodes::LOGICAL_ERROR);
    }

    for (auto & plan : plans)
        nodes.splice(nodes.end(), std::move(plan->nodes));

    nodes.emplace_back(Node{.step = std::move(step), .interpreter_params = std::move(interpreter_params)});
    root = &nodes.back();

    for (auto & plan : plans)
    {
        root->children.emplace_back(plan->root);
        plan->root->parent = root;
    }

    for (auto & plan : plans)
    {
        max_threads = std::max(max_threads, plan->max_threads);
        interpreter_context.insert(interpreter_context.end(),
                                   plan->interpreter_context.begin(), plan->interpreter_context.end());
    }
}

void QueryPlan::addStep(QueryPlanStepPtr step, InterpreterParamsPtr interpreter_params)
{
    checkNotCompleted();

    size_t num_input_streams = step->getInputStreams().size();

    if (num_input_streams == 0)
    {
        if (isInitialized())
            throw Exception("Cannot add step " + step->getName() + " to QueryPlan because "
                            "step has no inputs, but QueryPlan is already initialized", ErrorCodes::LOGICAL_ERROR);
        LOG_DEBUG(log, "Add step {} with context {}\n", step->getName(), interpreter_params ? static_cast<const void*>(interpreter_params->context.get()): nullptr);
        nodes.emplace_back(Node{.step = std::move(step), .interpreter_params = std::move(interpreter_params)});
        root = &nodes.back();
        return;
    }

    if (num_input_streams == 1)
    {
        if (!isInitialized())
            throw Exception("Cannot add step " + step->getName() + " to QueryPlan because "
                            "step has input, but QueryPlan is not initialized", ErrorCodes::LOGICAL_ERROR);

        const auto & root_header = root->step->getOutputStream().header;
        const auto & step_header = step->getInputStreams().front().header;
        if (!blocksHaveEqualStructure(root_header, step_header))
            throw Exception("Cannot add step " + step->getName() + " to QueryPlan because "
                            "it has incompatible header with root step " + root->step->getName() + " "
                            "root header: " + root_header.dumpStructure() +
                            "step header: " + step_header.dumpStructure(), ErrorCodes::LOGICAL_ERROR);

        nodes.emplace_back(Node{.step = std::move(step), .children = {root}, .interpreter_params = std::move(interpreter_params)});
        root->parent = &nodes.back();
        root = root->parent;
        return;
    }

    throw Exception("Cannot add step " + step->getName() + " to QueryPlan because it has " +
                    std::to_string(num_input_streams) + " inputs but " + std::to_string(isInitialized() ? 1 : 0) +
                    " input expected", ErrorCodes::LOGICAL_ERROR);
}

void QueryPlan::reset()
{
    root = nullptr;
    nodes.clear();
}

void QueryPlan::checkShuffle(Node * current_node, Node * child_node, CheckShuffleResult & result)
{
    /// Cases:
    /// 1. child node is aggregate step.
    /// 2. current node is limit step, child node is sort step: no need shuffle.
    /// 3. current node is not limit step, child node is sort step: need shuffle.
    /// 4. child node is limit step: need shuffle.
    result.current_union_step = dynamic_cast<UnionStep *>(current_node->step.get());
    if (result.current_union_step)
    {
        LOG_DEBUG(log, "Check shuffle: child node is UnionStep");
        result.is_shuffle = true;
        return;
    }

    result.current_join_step = dynamic_cast<JoinStep *>(current_node->step.get());
    if (result.current_join_step)
    {
        LOG_DEBUG(log, "Check shuffle: current node is JoinStep(0x{})", static_cast<void*>(result.current_join_step));
        assert(current_node->children.size() == 2);
        /// Only broadcast right side.
        if (child_node == current_node->children[1])
            result.is_shuffle = true;
        return;
    }

    result.child_aggregating_step = dynamic_cast<AggregatingStep *>(child_node->step.get());
    if (result.child_aggregating_step)
    {
        LOG_DEBUG(log, "Check shuffle: child node is AggregatingStep");
        result.is_shuffle = true;
        return;
    }

    result.child_sorting_step = dynamic_cast<SortingStep *>(child_node->step.get());
    if (result.child_sorting_step)
    {
        LOG_DEBUG(log, "Check shuffle: child node is SortingStep");
        result.current_limit_step = dynamic_cast<LimitStep *>(current_node->step.get());
    }
    else
    {
        result.child_limit_step = dynamic_cast<LimitStep *>(child_node->step.get());
        if (result.child_limit_step)
            LOG_DEBUG(log, "Check shuffle: child node is LimitStep");
    }

    if ((result.child_sorting_step && !result.current_limit_step) || result.child_limit_step)
        result.is_shuffle = true;
}

void QueryPlan::buildStages(ContextPtr context)
{
    LOG_DEBUG(log, "===> Build stages.");

    auto createStage = [this](int id, std::stack<Stage *> & parent_stages, Node * root_node, std::stack<Node *> & leaf_nodes) {
        stages.emplace_back(Stage{.id = id, .root_node = root_node});
        Stage * new_stage = &stages.back();

        if (root_node)
        {
            for (int i = 0; !parent_stages.empty() && i < root_node->num_parent_stages; ++i)
            {
                new_stage->parents.emplace_back(parent_stages.top());
                parent_stages.top()->child = new_stage;
                parent_stages.pop();
            }
            for (int i = 0; !leaf_nodes.empty() && i < root_node->num_leaf_nodes_in_stage; ++i)
            {
                new_stage->leaf_nodes.emplace_back(leaf_nodes.top());
                /// This leaf node is a data source node reading data from storage.
                if (leaf_nodes.top()->children.empty())
                    new_stage->is_leaf_stage = true;
                leaf_nodes.pop();
            }
        }
        LOG_DEBUG(log, "Create stage: id: {}, has {} parent stages and {} leaf nodes.", id, new_stage->parents.size(), new_stage->leaf_nodes.size());
        return new_stage;
    };

    struct Frame
    {
        Node * node = {};
        int visited_children = 0; /// Number of visited children
    };

    /// Used for visiting the query plan tree.
    std::stack<Frame> stack;
    stack.push(Frame{.node = root});
    bool one_child_is_visited = false;

    /// Used for creating stage.
    int stage_id = -1;
    Node * last_node = nullptr; /// Used for marking the current node's child.
    Node * leaf_node = nullptr;
    Stage * last_stage = nullptr;
    std::stack<Stage *> parent_stages;
    std::stack<Node *> leaf_nodes;

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (one_child_is_visited)
        {
            /// This is shuffle, create a new stage for child_node.
            CheckShuffleResult result;
            checkShuffle(frame.node, last_node, result);
            if (result.is_shuffle)
            {
                ++stage_id;
                last_stage = createStage(stage_id, parent_stages, last_node, leaf_nodes);

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
            if (!frame.node->interpreter_params && last_node->interpreter_params)
            {
                frame.node->interpreter_params = last_node->interpreter_params;
                LOG_DEBUG(
                    log,
                    "Set context({} <= {}) to {}",
                    frame.node->step->getName(),
                    last_node->step->getName(),
                    static_cast<const void *>(frame.node->interpreter_params->context.get()));
            }

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
        if (next_child == frame.node->children.size())
        {
            LOG_DEBUG(log, "Visit step: {}", frame.node->step->getName());
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
    parent_stages.push(last_stage);
    auto step = std::make_unique<DistributedSourceStep>(stage_id, last_stage->id, context);
    nodes.emplace_back(Node{.step = std::move(step), .children = {last_node}, .num_parent_stages = 1});
    root = &nodes.back();
    leaf_nodes.push(root);
    root->num_leaf_nodes_in_stage = 1;
    result_stage = createStage(stage_id, parent_stages, root, leaf_nodes);

    debugStages();
}

void QueryPlan::debugStages()
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

bool QueryPlan::scheduleStages(ContextMutablePtr context)
{
    LOG_DEBUG(log, "===> Schedule stages.");
    /// Use initial query id to build the plan fragment id.
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

    static std::unordered_set<String> system_tables{"SystemClusters", "SystemDatabases", "SystemTables", "SystemColumns",
                                                    "SystemDictionaries", "SystemDataSkippingIndices",
                                                    "SystemFunctions", "SystemFormats", "SystemTableEngines",
                                                    "SystemUsers", "SystemRoles", "SystemGrants", "SystemRoleGrants",
                                                    "SystemCurrentRoles", "SystemEnabledRoles", "SystemRowPolicies", "SystemPrivileges",
                                                    "SystemQuotas", "SystemQuotaLimits",
                                                    "SystemSettings", "SystemSettingsProfiles", "SystemSettingsProfileElements",
                                                    "SystemZooKeeper",
                                                    "SystemNumbers", "SystemOne", "SystemZeros",
                                                    "SystemContributors", "SystemLicenses",
                                                    "SystemReplicatedMergeTreeSettings", "SystemMergeTreeSettings"};

    static std::unordered_set<String> special_storages{"HDFS", "S3", "MySQL", "Memory"};

    bool is_result_stage_moved_forward = false;
    /// Fill workers of stages with compute and store replicas, stages includes leaf stage, result stage, intermediate stage.
    auto fillStage = [&store_replicas, this, &my_replica, &is_result_stage_moved_forward](Stage * stage)
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
                    if (system_tables.contains(leaf_node->step->getStepDescription()) ||
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
                LOG_DEBUG(log, "Schedule to {} workers.", store_replicas.size());
                stage->workers.reserve(store_replicas.size());
                stage->workers.insert(stage->workers.end(), store_replicas.begin(), store_replicas.end());
            }
            else
            {
                LOG_DEBUG(log, "Schedule to 1 worker.");
                stage->workers.emplace_back(std::make_shared<String>(my_replica));
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
                    LOG_DEBUG(log, "Move result stage {} forward to parent stage {}.", result_stage->id, parent->id);
                    assert(result_stage == &stages.back());
                    result_stage = parent;
                    root = parent->root_node;
                    stages.pop_back();
                    is_result_stage_moved_forward = true;
                }
            }
            return;
        }

        /// Intermediate stage.
        stage->workers.reserve(1);
        stage->workers.emplace_back(std::make_shared<String>(my_replica));
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
            if (is_result_stage_moved_forward)
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
            }
            else
            {
                /// Clear query plan tree.
                root = nullptr;

                for (const auto parent_stage : result_stage->parents)
                {
                    const Node * parent_stage_node = parent_stage->root_node;
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
                        false,
                        context);
                    addStep(std::move(distributed_source_step));
                }
                {
                    /// Only for debug.
                    LOG_DEBUG(
                        log,
                        "Result plan fragment:\n{}",
                        debugLocalPlanFragment(
                            initial_query_id, result_stage->id, *result_stage->workers.front(), std::vector<Node *>{root}));
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

            GRPCClient cli(*worker);
            auto result = cli.executePlanFragment(query_info);
            LOG_DEBUG(log, "Finish sending GRPC query info in {} sec. Exception: (code {}) {}", watch.elapsedSeconds(), result.exception().code(), result.exception().display_text());
        }
    }

    return is_result_stage_moved_forward;
}

void QueryPlan::buildPlanFragment(ContextPtr context)
{
   const auto & query_distributed_plan_info = context->getQueryPlanFragmentInfo();
    int my_stage_id = query_distributed_plan_info.stage_id;
    LOG_DEBUG(
        log,
        "===> Build plan fragment: {} stage {}, has {} parent stages.",
        (result_stage ? (my_stage_id == result_stage->id ? "result": "no-result") : "no-result"),
        my_stage_id,
        query_distributed_plan_info.parent_sources.size());

    /// Get my replica grpc address
    String my_replica = context->getMacros()->getValue("replica") + ":" + toString(context->getServerPort("grpc_port"));

    struct Frame
    {
        Node * node = {};
        int visited_children = 0; /// Number of visited children
    };

    /// Used for visiting the query plan tree.
    std::stack<Frame> stack;
    stack.push(Frame{.node = root});
    bool one_child_is_visited = false;

    /// Used for locating the plan fragment.
    int stage_id = -1;
    Node * last_node = nullptr;
    std::vector<Node *> distributed_source_nodes; /// Only for debug

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (one_child_is_visited)
        {
            /// Transfer interpreter params bottom-up.
            if (!frame.node->interpreter_params && last_node->interpreter_params)
            {
                frame.node->interpreter_params = last_node->interpreter_params;
                LOG_DEBUG(
                    log,
                    "Set context({} <= {}) to {}",
                    frame.node->step->getName(),
                    last_node->step->getName(),
                    static_cast<const void *>(frame.node->interpreter_params->context.get()));
            }

            CheckShuffleResult result;
            checkShuffle(frame.node, last_node, result);

            /// This is a shuffle dependency between current node and the last visited child.
            if (result.is_shuffle)
            {
                ++stage_id;

                /// This is one of my parent stages.
                const auto & it = query_distributed_plan_info.parent_sources.find(stage_id);
                if (it != query_distributed_plan_info.parent_sources.end())
                {
                    assert(last_node == frame.node->children[frame.visited_children]);

                    /// Add steps between current node and child node.
                    auto addStep = [this, &stage_id, &frame](QueryPlanStepPtr step, const String & description, Node * & node)
                    {
                        LOG_DEBUG(log, "Add step: {}, parent stage: {}", step->getName(), stage_id);
                        step->setStepDescription(description);
                        if (!node)
                            nodes.emplace_back(Node{.step = std::move(step), .interpreter_params = frame.node->interpreter_params});
                        else
                        {
                            nodes.emplace_back(Node{.step = std::move(step), .children = {node}, .interpreter_params = frame.node->interpreter_params});
                            node->parent = &nodes.back();
                        }
                        node = &nodes.back();
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
                    const auto & header = result.child_aggregating_step == nullptr ? last_node->step->getOutputStream().header
                                                                                   : aggregating_step->getOutputStream().header;

                    /// Create DistributedSourceStep.
                    assert(header);
                    const auto & sources = it->second;
                    auto distributed_source_step = std::make_unique<DistributedSourceStep>(
                        header, sources, query_distributed_plan_info.initial_query_id, my_stage_id, stage_id, my_replica,
                        add_agg_info, false, context);
                    Node * new_node = nullptr;
                    addStep(std::move(distributed_source_step), "", new_node);
                    distributed_source_nodes.emplace_back(new_node); /// For debug

                    /// If current step is JoinStep or UnionStep, only add DistributedSourceStep.

                    /// If parent stage has aggregate, add MergingAggregatedStep.
                    if (result.child_aggregating_step)
                    {
                        assert(frame.node->interpreter_params);
                        bool aggregate_final = !frame.node->interpreter_params->group_by_with_totals
                            && !frame.node->interpreter_params->group_by_with_rollup
                            && !frame.node->interpreter_params->group_by_with_cube;
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
                    if (result.child_sorting_step)
                    {
                        auto merging_sorted = std::make_unique<SortingStep>(new_node->step->getOutputStream(), *result.child_sorting_step);
                        addStep(std::move(merging_sorted), "Merge sorted streams for distributed ORDER BY", new_node);
                    }

                    /// If parent stage has limit, add LimitStep.
                    if (result.child_limit_step)
                    {
                        assert(last_node->children.size() == 1);
                        const SortingStep * grandchild_sorting_step = dynamic_cast<SortingStep *>(last_node->children[0]->step.get());
                        if  (grandchild_sorting_step)
                        {
                            auto merging_sorted
                                = std::make_unique<SortingStep>(new_node->step->getOutputStream(), *grandchild_sorting_step);
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
                    auto replaceStep = [this, &stage_id](QueryPlanStepPtr step, Node * & node)
                    {
                        LOG_DEBUG(log, "Replace step: {}, stage: {}", step->getName(), stage_id);
                        node->step = std::move(step);
                    };

                    /// If child is AggregatingStep.
                    if (result.child_aggregating_step)
                    {
                        /// If NOT optimize trivial count, replace AggregatingStep with final=false.
                        if (!result.child_aggregating_step->getParams().optimize_trivial_count)
                        {
                            auto aggregating_step = std::make_unique<AggregatingStep>(*result.child_aggregating_step);
                            replaceStep(std::move(aggregating_step), last_node);
                        }
                        /// If optimize trivial count, remove AggregatingStep.
                        else
                        {
                            LOG_DEBUG(log, "Remove step: {}, stage: {}", result.child_aggregating_step->getName(), stage_id);
                            last_node = last_node->children[0];
                        }
                    }
                    /// If limit step is pushed down, collect (limit + offset) rows.
                    else if (result.child_limit_step)
                        result.child_limit_step->resetLimitAndOffset();

                    root = last_node;
                    {
                        /// Only for debug.
                        LOG_DEBUG(
                            log,
                            "Local plan fragment:\n{}",
                            debugLocalPlanFragment(query_distributed_plan_info.initial_query_id, stage_id, my_replica, distributed_source_nodes));
                    }
                    return;
                }
            }

            ++frame.visited_children;
            one_child_is_visited = false;
        }

        size_t next_child = frame.visited_children;
        if (next_child == frame.node->children.size())
        {
            LOG_DEBUG(log, "Visit step: {}", frame.node->step->getName());
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
    {
        root = last_node;
        {
            /// Only for debug.
            LOG_DEBUG(
                log,
                "Local plan fragment:\n{}",
                debugLocalPlanFragment(query_distributed_plan_info.initial_query_id, stage_id, my_replica, distributed_source_nodes));
        }
        return;
    }
}

bool QueryPlan::buildDistributedPlan(ContextMutablePtr context)
{
    if (!context->getSettingsRef().enable_distributed_plan)
    {
        LOG_DEBUG(log, "Skip building distributed plan, because enable_distributed_plan=false.");
        return false;
    }
    /// Query hits directly on the store worker node.
    if (context->isInitialQuery() && context->getRunningMode() == Context::RunningMode::STORE)
    {
        LOG_DEBUG(log, "Skip building distributed plan, because initial query hits directly on store worker.");
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
        explainPlan(buf, options);
        LOG_DEBUG(log, "[{}] Original query plan:\n{}", static_cast<void*>(context.get()), buf.str());
    }

    checkInitialized();
    optimize(QueryPlanOptimizationSettings::fromContext(context));
    if (context->isInitialQuery())
    {
        buildStages(context);
        if (scheduleStages(context))
            buildPlanFragment(context);
    }
    else
    {
        buildPlanFragment(context);
    }
    return true;
}

String QueryPlan::debugLocalPlanFragment(const String & query_id, int stage_id, const String & node_id, const std::vector<Node *> distributed_source_nodes)
{
    WriteBufferFromOwnString buf;
    buf << "------ Local Plan Fragment ------\n";
    buf << "Fragment ID: " << query_id << "/" << stage_id << "/" << node_id;
    buf.write('\n');
    buf << "Distributed Source Nodes: " << distributed_source_nodes.size();
    buf.write('\n');
    for (size_t i = 0; i < distributed_source_nodes.size(); ++i)
    {
        const Node * node = distributed_source_nodes[i];
        auto distributed_source_step = dynamic_cast<DistributedSourceStep*>(node->step.get());
        buf << "[" << i << "]" << distributed_source_step->getName() << ", sources: ";
        for (const auto & source : distributed_source_step->getSources())
            buf << *source << " ";
        buf.write('\n');
    }
    buf << "\nPlan Fragment:\n";
    ExplainPlanOptions options{.header = true, .actions = true};
    explainPlan(buf, options);
    return buf.str();
}

String QueryPlan::debugRemotePlanFragment(const String & query, const String & worker, const String & query_id, const Stage * stage)
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

QueryPipelineBuilderPtr QueryPlan::buildQueryPipeline(
    const QueryPlanOptimizationSettings & optimization_settings,
    const BuildQueryPipelineSettings & build_pipeline_settings)
{
    checkInitialized();
    optimize(optimization_settings);

    struct Frame
    {
        Node * node = {};
        QueryPipelineBuilders pipelines = {};
    };

    QueryPipelineBuilderPtr last_pipeline;

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (last_pipeline)
        {
            frame.pipelines.emplace_back(std::move(last_pipeline));
            last_pipeline = nullptr; //-V1048
        }

        size_t next_child = frame.pipelines.size();
        if (next_child == frame.node->children.size())
        {
            bool limit_max_threads = frame.pipelines.empty();
            last_pipeline = frame.node->step->updatePipeline(std::move(frame.pipelines), build_pipeline_settings);

            if (limit_max_threads && max_threads)
                last_pipeline->limitMaxThreads(max_threads);

            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child]});
    }

    for (auto & context : interpreter_context)
        last_pipeline->addInterpreterContext(std::move(context));

    return last_pipeline;
}

Pipe QueryPlan::convertToPipe(
    const QueryPlanOptimizationSettings & optimization_settings,
    const BuildQueryPipelineSettings & build_pipeline_settings)
{
    if (!isInitialized())
        return {};

    if (isCompleted())
        throw Exception("Cannot convert completed QueryPlan to Pipe", ErrorCodes::LOGICAL_ERROR);

    return QueryPipelineBuilder::getPipe(std::move(*buildQueryPipeline(optimization_settings, build_pipeline_settings)));
}

void QueryPlan::addInterpreterContext(ContextPtr context)
{
    interpreter_context.emplace_back(std::move(context));
}


static void explainStep(const IQueryPlanStep & step, JSONBuilder::JSONMap & map, const QueryPlan::ExplainPlanOptions & options)
{
    map.add("Node Type", step.getName());

    if (options.description)
    {
        const auto & description = step.getStepDescription();
        if (!description.empty())
            map.add("Description", description);
    }

    if (options.header && step.hasOutputStream())
    {
        auto header_array = std::make_unique<JSONBuilder::JSONArray>();

        for (const auto & output_column : step.getOutputStream().header)
        {
            auto column_map = std::make_unique<JSONBuilder::JSONMap>();
            column_map->add("Name", output_column.name);
            if (output_column.type)
                column_map->add("Type", output_column.type->getName());

            header_array->add(std::move(column_map));
        }

        map.add("Header", std::move(header_array));
    }

    if (options.actions)
        step.describeActions(map);

    if (options.indexes)
        step.describeIndexes(map);
}

JSONBuilder::ItemPtr QueryPlan::explainPlan(const ExplainPlanOptions & options)
{
    checkInitialized();

    struct Frame
    {
        Node * node = {};
        size_t next_child = 0;
        std::unique_ptr<JSONBuilder::JSONMap> node_map = {};
        std::unique_ptr<JSONBuilder::JSONArray> children_array = {};
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    std::unique_ptr<JSONBuilder::JSONMap> tree;

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (frame.next_child == 0)
        {
            if (!frame.node->children.empty())
                frame.children_array = std::make_unique<JSONBuilder::JSONArray>();

            frame.node_map = std::make_unique<JSONBuilder::JSONMap>();
            explainStep(*frame.node->step, *frame.node_map, options);
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child]});
            ++frame.next_child;
        }
        else
        {
            if (frame.children_array)
                frame.node_map->add("Plans", std::move(frame.children_array));

            tree.swap(frame.node_map);
            stack.pop();

            if (!stack.empty())
                stack.top().children_array->add(std::move(tree));
        }
    }

    return tree;
}

static void explainStep(
    const IQueryPlanStep & step,
    IQueryPlanStep::FormatSettings & settings,
    const QueryPlan::ExplainPlanOptions & options)
{
    std::string prefix(settings.offset, ' ');
    settings.out << prefix;
    settings.out << step.getName();

    const auto & description = step.getStepDescription();
    if (options.description && !description.empty())
        settings.out <<" (" << description << ')';

    settings.out.write('\n');

    if (options.header)
    {
        settings.out << prefix;

        if (!step.hasOutputStream())
            settings.out << "No header";
        else if (!step.getOutputStream().header)
            settings.out << "Empty header";
        else
        {
            settings.out << "Header: ";
            bool first = true;

            for (const auto & elem : step.getOutputStream().header)
            {
                if (!first)
                    settings.out << "\n" << prefix << "        ";

                first = false;
                elem.dumpNameAndType(settings.out);
            }
        }

        settings.out.write('\n');
    }

    if (options.actions)
        step.describeActions(settings);

    if (options.indexes)
        step.describeIndexes(settings);
}

std::string debugExplainStep(const IQueryPlanStep & step)
{
    WriteBufferFromOwnString out;
    IQueryPlanStep::FormatSettings settings{.out = out};
    QueryPlan::ExplainPlanOptions options{.actions = true};
    explainStep(step, settings, options);
    return out.str();
}

void QueryPlan::explainPlan(WriteBuffer & buffer, const ExplainPlanOptions & options)
{
    checkInitialized();

    IQueryPlanStep::FormatSettings settings{.out = buffer, .write_header = options.header};

    struct Frame
    {
        Node * node = {};
        bool is_description_printed = false;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (!frame.is_description_printed)
        {
            settings.offset = (stack.size() - 1) * settings.indent;
            explainStep(*frame.node->step, settings, options);
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child]});
            ++frame.next_child;
        }
        else
            stack.pop();
    }
}

static void explainPipelineStep(IQueryPlanStep & step, IQueryPlanStep::FormatSettings & settings)
{
    settings.out << String(settings.offset, settings.indent_char) << "(" << step.getName() << ")\n";
    size_t current_offset = settings.offset;
    step.describePipeline(settings);
    if (current_offset == settings.offset)
        settings.offset += settings.indent;
}

void QueryPlan::explainPipeline(WriteBuffer & buffer, const ExplainPipelineOptions & options)
{
    checkInitialized();

    IQueryPlanStep::FormatSettings settings{.out = buffer, .write_header = options.header};

    struct Frame
    {
        Node * node = {};
        size_t offset = 0;
        bool is_description_printed = false;
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (!frame.is_description_printed)
        {
            settings.offset = frame.offset;
            explainPipelineStep(*frame.node->step, settings);
            frame.offset = settings.offset;
            frame.is_description_printed = true;
        }

        if (frame.next_child < frame.node->children.size())
        {
            stack.push(Frame{frame.node->children[frame.next_child], frame.offset});
            ++frame.next_child;
        }
        else
            stack.pop();
    }
}

void QueryPlan::optimize(const QueryPlanOptimizationSettings & optimization_settings)
{
    QueryPlanOptimizations::optimizeTree(optimization_settings, *root, nodes);
}

void QueryPlan::explainEstimate(MutableColumns & columns)
{
    checkInitialized();

    struct EstimateCounters
    {
        std::string database_name;
        std::string table_name;
        UInt64 parts = 0;
        UInt64 rows = 0;
        UInt64 marks = 0;

        EstimateCounters(const std::string & database, const std::string & table) : database_name(database), table_name(table)
        {
        }
    };

    using CountersPtr = std::shared_ptr<EstimateCounters>;
    std::unordered_map<std::string, CountersPtr> counters;
    using processNodeFuncType = std::function<void(const Node * node)>;
    processNodeFuncType process_node = [&counters, &process_node] (const Node * node)
    {
        if (!node)
            return;
        if (const auto * step = dynamic_cast<ReadFromMergeTree*>(node->step.get()))
        {
            const auto & id = step->getStorageID();
            auto key = id.database_name + "." + id.table_name;
            auto it = counters.find(key);
            if (it == counters.end())
            {
                it = counters.insert({key, std::make_shared<EstimateCounters>(id.database_name, id.table_name)}).first;
            }
            it->second->parts += step->getSelectedParts();
            it->second->rows += step->getSelectedRows();
            it->second->marks += step->getSelectedMarks();
        }
        for (const auto * child : node->children)
            process_node(child);
    };
    process_node(root);

    for (const auto & counter : counters)
    {
        size_t index = 0;
        const auto & database_name = counter.second->database_name;
        const auto & table_name = counter.second->table_name;
        columns[index++]->insertData(database_name.c_str(), database_name.size());
        columns[index++]->insertData(table_name.c_str(), table_name.size());
        columns[index++]->insert(counter.second->parts);
        columns[index++]->insert(counter.second->rows);
        columns[index++]->insert(counter.second->marks);
    }
}

}
