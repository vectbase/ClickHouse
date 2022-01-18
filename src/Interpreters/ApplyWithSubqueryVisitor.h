#pragma once

#include <map>

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{
class ASTFunction;
class ASTSelectQuery;
class ASTSelectWithUnionQuery;
struct ASTTableExpression;

class ApplyWithSubqueryVisitor
{
public:
    struct Data
    {
        std::map<String, ASTPtr> subqueries;
    };

    static void visit(ASTPtr & ast) { visit(ast, {}); }
    static void visit(ASTSelectQuery & select) { visit(select, {}); }
    static void visit(ASTSelectWithUnionQuery & select) { visit(select, {}); }

private:
    static void visit(ASTPtr & ast, const Data & data);
    static void visit(ASTSelectQuery & ast, const Data & data);
    static void visit(ASTSelectWithUnionQuery & ast, const Data & data);
    static void visit(ASTTableExpression & table, const Data & data);
    static void visit(ASTFunction & func, const Data & data);
};

class ReplaceSubqueryMatcher
{
public:
    struct Data
    {
        ASTPtr query;
        bool done = false;
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child, Data & data);
    static void visit(ASTPtr & ast, Data & data);
};

using ReplaceSubqueryVisitor = InDepthNodeVisitor<ReplaceSubqueryMatcher, true, true>;

}
