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
    query_plan.buildDistributedPlan(context);

    QueryPlanOptimizationSettings do_not_optimize_plan{.optimize_plan = false};
    return std::move(*query_plan.buildQueryPipeline(
            do_not_optimize_plan, BuildQueryPipelineSettings::fromContext(context)));
}

}
