#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/QueryLog.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

namespace DB
{

void IInterpreterUnionOrSelectQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const
{
    elem.query_kind = "Select";
}


QueryPipelineBuilder IInterpreterUnionOrSelectQuery::buildQueryPipeline()
{
    QueryPlan query_plan;

    buildQueryPlan(query_plan);

    context->setSelectQuery(queryToString(this->query_ptr));
    bool is_built = query_plan.buildDistributedPlan(context);

    QueryPlanOptimizationSettings optimization_settings{.optimize_plan = !is_built};
    return std::move(*query_plan.buildQueryPipeline(
        optimization_settings, BuildQueryPipelineSettings::fromContext(context)));
}

}
