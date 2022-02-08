#include <LuceneAnalyzer/AnalyzerFactory.h>
#include <Poco/String.h>
#include <Common/Exception.h>

namespace DB{

namespace ErrorCodes
{
    extern const int UNKNOWN_LUCENE_ANLYZER;
}

LuceneAnalyzerPair genLuceneAnalyzers()
{
    static LuceneAnalyzerPair analyzers;
    analyzers["STANDARDANALYZER"] = Lucene::newLucene<Lucene::StandardAnalyzer>(Lucene::LuceneVersion::LUCENE_CURRENT);
    // TODO: stop words
    analyzers["STOPANALYZER"] = Lucene::newLucene<Lucene::StopAnalyzer>(Lucene::LuceneVersion::LUCENE_CURRENT);
    analyzers["WHITESPACEANALYZER"] = Lucene::newLucene<Lucene::WhitespaceAnalyzer>();
    analyzers["SIMPLEANALYZER"] = Lucene::newLucene<Lucene::SimpleAnalyzer>();
    return analyzers;
}

const LuceneAnalyzerPair AnalyzerFactory::analyzers = genLuceneAnalyzers();

void AnalyzerFactory::validate(const String& name) const
{

    auto name_u = Poco::toUpper(name);
    if (analyzers.find(name_u) == analyzers.end())
    {
        throw Exception("Unknown LuceneAnalyzer family analyzer: " + name, ErrorCodes::UNKNOWN_LUCENE_ANLYZER);
    }
}

AnalyzerFactory::AnalyzerFactory()
{
}

AnalyzerFactory& AnalyzerFactory::instance()
{
    static AnalyzerFactory ret;
    return ret;
}

}

