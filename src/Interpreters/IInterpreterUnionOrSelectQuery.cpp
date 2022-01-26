#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/QueryLog.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/DistributedPlanner.h>

namespace DB
{

void IInterpreterUnionOrSelectQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const
{
    elem.query_kind = "Select";
}

void IInterpreterUnionOrSelectQuery::rewriteDistributedQuery(bool is_subquery, [[maybe_unused]] size_t tables_count, bool)
{
    /// Reset these fields to not share them with parent query.
    if (is_subquery)
    {
        getContext()->resetQueryPlanFragmentInfo();
        getContext()->getClientInfo().query_kind = ClientInfo::QueryKind::INITIAL_QUERY;
        getContext()->getClientInfo().current_query_id = getContext()->generateQueryId();
    }
}

QueryPipelineBuilder IInterpreterUnionOrSelectQuery::buildQueryPipeline()
{
    QueryPlan query_plan;

    buildQueryPlan(query_plan);

    query_plan.checkInitialized();
    query_plan.optimize(QueryPlanOptimizationSettings::fromContext(context));

    DistributedPlanner planner(query_plan, context);
    planner.buildDistributedPlan();

    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = false};
    return std::move(*query_plan.buildQueryPipeline(
        optimization_settings, BuildQueryPipelineSettings::fromContext(context)));
}

}
