#pragma once
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Core/SortDescription.h>

namespace DB
{

/// Executes LIMIT. See LimitTransform.
class LimitStep : public ITransformingStep
{
public:
    LimitStep(
        const DataStream & input_stream_,
        size_t limit_, size_t offset_,
        bool always_read_till_end_ = false, /// Read all data even if limit is reached. Needed for totals.
        bool with_ties_ = false, /// Limit with ties.
        SortDescription description_ = {});

    LimitStep(const DataStream & input_stream_, const LimitStep & limit_step)
        : LimitStep(
            input_stream_,
            limit_step.limit,
            limit_step.offset,
            limit_step.always_read_till_end,
            limit_step.with_ties,
            limit_step.description)
    {
    }

    String getName() const override { return "Limit"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    size_t getLimitForSorting() const
    {
        if (limit > std::numeric_limits<UInt64>::max() - offset)
            return 0;

        return limit + offset;
    }

    /// Change input stream when limit is pushed up. TODO: add clone() for steps.
    void updateInputStream(DataStream input_stream);

    bool withTies() const { return with_ties; }

    void resetLimitAndOffset() {
        limit += offset;
        offset = 0;
    }

private:
    size_t limit;
    size_t offset;
    bool always_read_till_end;

    bool with_ties;
    const SortDescription description;
};

}
