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

void QueryPlan::unitePlans(QueryPlanStepPtr step, std::vector<std::unique_ptr<QueryPlan>> plans)
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

    nodes.emplace_back(Node{.step = std::move(step)});
    root = &nodes.back();

    for (auto & plan : plans)
        root->children.emplace_back(plan->root);

    for (auto & plan : plans)
    {
        max_threads = std::max(max_threads, plan->max_threads);
        interpreter_context.insert(interpreter_context.end(),
                                   plan->interpreter_context.begin(), plan->interpreter_context.end());
    }
}

void QueryPlan::addStep(QueryPlanStepPtr step)
{
    checkNotCompleted();

    size_t num_input_streams = step->getInputStreams().size();

    if (num_input_streams == 0)
    {
        if (isInitialized())
            throw Exception("Cannot add step " + step->getName() + " to QueryPlan because "
                            "step has no inputs, but QueryPlan is already initialized", ErrorCodes::LOGICAL_ERROR);

        nodes.emplace_back(Node{.step = std::move(step)});
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

        nodes.emplace_back(Node{.step = std::move(step), .children = {root}});
        root = &nodes.back();
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

void QueryPlan::buildStages(ContextPtr)
{
    LOG_DEBUG(log, "Build stages.");

    auto createStage = [this](int id, Stage * parent_stage, Node * node) {
        stages.emplace_back(Stage{.id = id, .node = node});
        Stage * new_stage = &stages.back();
        if (parent_stage)
        {
            new_stage->parents.push_back(parent_stage);
            parent_stage->child = new_stage;
        }
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
    Node * stage_root_node = nullptr;
    Stage * last_stage = nullptr;

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (one_child_is_visited)
        {
            ++frame.visited_children;
            one_child_is_visited = false;

            /// TODO: This is shuffle, construct a new stage
            //            if (false)
            //            {
            //                stage_id++; /// current stage
            //                last_stage = createStage(stage_id, last_stage);
            //            }
        }

        size_t next_child = frame.visited_children;
        if (next_child == frame.node->children.size())
        {
            LOG_DEBUG(log, "Visited step: {}", frame.node->step->getName());
            stage_root_node = frame.node;
            one_child_is_visited = true;
            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child]});
    }

    /// At last, append a shuffle for converging data.
    ++stage_id;
    last_stage = createStage(stage_id, last_stage, stage_root_node);

    /// Create result stage.
    ++stage_id;
    result_stage = createStage(stage_id, last_stage, nullptr);
}

void QueryPlan::scheduleStages(ContextPtr context)
{
    /// Retrieve all replicas.
    std::unordered_map<String, ClustersWatcher::ReplicaInfoPtr> replicas = context->getClustersWatcher().getContainer();
    LOG_DEBUG(log, "Schedule stages across {} replicas.", replicas.size());
    std::vector<std::shared_ptr<String>> store_replicas, compute_replicas;
    for (const auto & replica : replicas)
    {
        const auto & replica_info = replica.second;
        LOG_DEBUG(
            log,
            "Check replica: {} => ({}/{}/{}, {}).",
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
    LOG_DEBUG(log, "{} store, {} compute.", store_replicas.size(), compute_replicas.size());

    auto fillStage = [&store_replicas, &compute_replicas](Stage * stage)
    {
        if (stage->parents.empty()) /// Leaf stage.
        {
            /// Fill executors.
            stage->executors.reserve(store_replicas.size());
            stage->executors.insert(stage->executors.end(), store_replicas.begin(), store_replicas.end());
            /// Leaf stage's sources should be empty.
        }
        else /// Non-leaf stage.
        {
            /// Fill executors.
            stage->executors.reserve(compute_replicas.size());
            stage->executors.insert(stage->executors.end(), compute_replicas.begin(), compute_replicas.end());
            /// Fill sources.
            int num_sources = 0;
            for (Stage * parent : stage->parents)
            {
                num_sources += parent->executors.size();
            }
            stage->sources.reserve(num_sources);
            for (Stage * parent : stage->parents)
            {
                stage->sources.insert(stage->sources.end(), parent->executors.begin(), parent->executors.end());
            }
        }
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
            LOG_DEBUG(log, "Visited stage: {}", frame.stage->id);

            fillStage(frame.stage);

            one_parent_is_visited = true;
            stack.pop();
        }
        else
        {
            stack.push(Frame{.stage = frame.stage->parents[next_parent]});
        }
    }

    /// Send plan fragment params.
    const String & query_id = context->getClientInfo().initial_query_id;
    for (auto & stage : stages)
    {
        /// Don't send result stage.
        if (&stage == result_stage)
        {
            assert(!result_stage->parents.empty());
            Block header;
            const Stage * parent_stage = result_stage->parents[0];
            const Node * parent_stage_node = result_stage->parents[0]->node;
            if (!parent_stage_node->step)
                LOG_DEBUG(log, "Step of parent stage's node is empty.");
            else if (!parent_stage_node->step->hasOutputStream())
                LOG_DEBUG(log, "Step of parent stage's node doesn't have output stream.");
            else if (!parent_stage_node->step->getOutputStream().header)
                LOG_DEBUG(log, "Step of parent stage's node has empty header.");
            else
            {
                header = parent_stage_node->step->getOutputStream().header;
                LOG_DEBUG(
                    log,
                    "Take the output stream header of {}: {}, header columns: {}.",
                    parent_stage_node->step->getName(),
                    parent_stage_node->step->getStepDescription(),
                    header.columns());
            }

            /// Get my replica grpc address
            String my_replica = context->getMacros()->getValue("replica") + ":" + toString(context->getServerPort("grpc_port"));
            auto distributed_source_step = std::make_unique<DistributedSourceStep>(
                header, parent_stage->executors, query_id, result_stage->id, parent_stage->id, my_replica, context);
            {
                /// Only for debug.
                LOG_DEBUG(
                    log,
                    "Local plan fragment:\n{}",
                    debugLocalPlanFragment(query_id, result_stage->id, my_replica, distributed_source_step.get()));
            }
            reset();
            addStep(std::move(distributed_source_step));
            continue;
        }

        /// Fill sinks.
        if (!stage.child->executors.empty())
        {
            stage.sinks.reserve(stage.child->executors.size());
            stage.sinks.insert(stage.sinks.end(), stage.child->executors.begin(), stage.child->executors.end());
        }

        LOG_DEBUG(log, "Stage {} has {} executors.", stage.id, stage.executors.size());
        /// Send to each remote executor.
        for (const auto & executor : stage.executors)
        {
            LOG_DEBUG(log, "Plan fragment to send:\nquery: {}\n{}", context->getClientInfo().initial_query, debugRemotePlanFragment(*executor, query_id, &stage));

//            const auto & query_distributed_plan_info = Context::QueryPlanFragmentInfo{
//                .query_id = query_id, .stage_id = stage->id, .node_id = *executor, .sources = stage->sources, .sinks = stage->sinks};
            GRPCQueryInfo query_info;
            query_info.set_query(context->getClientInfo().initial_query);
            query_info.set_query_id(query_id);
            query_info.set_stage_id(stage.id);
            if (!stage.parents.empty())
                query_info.set_parent_stage_id(stage.parents[0]->id);
            query_info.set_node_id(*executor);
//            query_info.set_user_name(context->getUserName());
//            query_info.set_password(context->getClientInfo().)
            for (const auto & source : stage.sources)
            {
                query_info.add_sources(*source);
            }
            for (const auto & sink : stage.sinks)
            {
                query_info.add_sinks(*sink);
            }

            GRPCClient cli(*executor);
            auto result = cli.SendDistributedPlanParams(query_info);
            LOG_INFO(log, "GRPCClient got result exception: {} {}.", result.exception().code(), result.exception().display_text());
        }
    }
}

void QueryPlan::buildPlanFragment(ContextPtr context)
{
    LOG_DEBUG(log, "Building plan fragment.");

    /// Get my replica grpc address
    String my_replica = context->getMacros()->getValue("replica") + ":" + toString(context->getServerPort("grpc_port"));
    const auto & query_distributed_plan_info = context->getQueryPlanFragmentInfo();
    int my_stage_id = query_distributed_plan_info.stage_id;
//    int parent_stage_id = query_distributed_plan_info.parent_stage_id;

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
    Node * fragment_root_node = nullptr;
    Node * fragment_leaf_node = nullptr; /// TODO: For join or union, there are more than one leaf nodes.

    while (!stack.empty())
    {
        auto & frame = stack.top();

        if (one_child_is_visited)
        {
            ++frame.visited_children;
            one_child_is_visited = false;

            /// TODO: This is a shuffle relationship between current node and the last visited child(i.e. fragment_root_node).
//            if (true)
//            {
//                stage_id++;
//                if (stage_id == my_stage_id)
//                {
//                    root = fragment_root_node;
//                    DistributedSourceStep * step = nullptr;
//                    if (fragment_leaf_node)
//                    {
//                        /// Set sources for fragment_leaf_node
//                        step = dynamic_cast<DistributedSourceStep *>(fragment_leaf_node->step.get());
//                        step->setSources(query_distributed_plan_info.sources);
//                    }
//
//                    {
//                        /// Only for debug.
//                        LOG_DEBUG(
//                            log,
//                            "Local plan fragment:\n{}",
//                            debugLocalPlanFragment(query_distributed_plan_info.query_id, stage_id, my_replica, step));
//                    }
//                    return;
//                }
//
//                /// Add a DistributedSourceStep between current node and fragment_root_node.
//                /// But this step may NOT be executed in my fragment.
//                frame.node->children.clear();
//                const auto & header = fragment_root_node->step->getOutputStream().header;
//                const String & query_id = context->getClientInfo().initial_query_id;
//                auto distributed_source_step = std::make_unique<DistributedSourceStep>(header, {}, query_id, my_stage_id, parent_stage_id, my_replica, context);
//                insertStep(frame.node->step, distributed_source_step);
//                nodes.emplace_back(Node{.step = std::move(step)});
//                fragment_leaf_node = &nodes.back();
//                frame.node->children.emplace_back(fragment_leaf_node);
//            }
        }

        size_t next_child = frame.visited_children;
        if (next_child == frame.node->children.size())
        {
            LOG_DEBUG(log, "Visited step: {}", frame.node->step->getName());

            fragment_root_node = frame.node;
            one_child_is_visited = true;
            stack.pop();
        }
        else
            stack.push(Frame{.node = frame.node->children[next_child]});
    }

    /// Check the result stage.
    ++stage_id;
    if (stage_id == my_stage_id)
    {
        root = fragment_root_node;
        DistributedSourceStep * step = nullptr;
        if (fragment_leaf_node)
        {
            step = dynamic_cast<DistributedSourceStep *>(fragment_leaf_node->step.get());
            step->setSources(query_distributed_plan_info.sources);
        }
        {
            /// Only for debug.
            LOG_DEBUG(
                log,
                "Local plan fragment:\n{}",
                debugLocalPlanFragment(query_distributed_plan_info.query_id, stage_id, my_replica, step));
        }
        return;
    }
}

void QueryPlan::buildDistributedPlan(ContextMutablePtr context)
{
    if (context->getRunningMode() == Context::RunningMode::STORE)
        return;

    if (context->getInitialQueryId() == "zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz")
    {
        LOG_DEBUG(log, "Special initial query id, skip building distributed plan.");
        return;
    }

    LOG_DEBUG(log, "Initial query id: {}, to be built to distributed plan.", context->getInitialQueryId());

//    /// For hard code debugging
//    if (context->getMacros()->getValue("cluster") == "store")
//    {
//        auto query_plan_fragment_info
//            = Context::QueryPlanFragmentInfo{.query_id = "xxx-yyy-zzz", .stage_id = 0, .node_id = "centos0"};
//        query_plan_fragment_info.sinks = {std::make_shared<String>("ubuntu0")};
//        context->setQueryPlanFragmentInfo(query_plan_fragment_info);
//    }

    checkInitialized();
    if (context->isInitialComputeNode())
    {
        optimize(QueryPlanOptimizationSettings::fromContext(context));
        buildStages(context);
        scheduleStages(context);
    }
    else
    {
        buildPlanFragment(context);
    }
}

String QueryPlan::debugLocalPlanFragment(const String & query_id, int stage_id, const String & node_id, const DistributedSourceStep * step)
{
    WriteBufferFromOwnString buf;
    ExplainPlanOptions options;
    buf << "fragment id: " << query_id << "/" << stage_id << "/" << node_id << "\n";
    if (step)
    {
        buf << "sources: ";
        for (const auto & source : step->getSources())
        {
            buf << *source << " ";
        }
        buf.write('\n');
    }

    explainPlan(buf, options);
    return buf.str();
}

String QueryPlan::debugRemotePlanFragment(const String & receiver, const String & query_id, const Stage * stage)
{
    WriteBufferFromOwnString buf;
    buf << "receiver: " << receiver;
    buf.write('\n');
    buf << "fragment id: " << query_id << "/" << stage->id << "/" << receiver;
    buf.write('\n');
    buf << "sources: ";
    for (const auto & source : stage->sources)
    {
        buf << *source << " ";
    }
    buf.write('\n');
    buf << "sinks: ";
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
