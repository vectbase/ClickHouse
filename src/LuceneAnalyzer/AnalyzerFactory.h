#pragma once
#include <lucene++/LuceneHeaders.h>
#include <common/types.h>
#include <unordered_map>

namespace DB
{

using LuceneAnalyzerPair = std::unordered_map<String, Lucene::AnalyzerPtr>;

class AnalyzerFactory
{
public:

    static AnalyzerFactory & instance();

    const char* getDefaultAnalyzer() const;

    /// Validate codecs AST specified by user
    void validate(const String & analyzer_name) const;

public:
    static const LuceneAnalyzerPair analyzers;

private:
    AnalyzerFactory();
};

}

