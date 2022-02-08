#pragma once

#include <Parsers/IAST.h>

namespace DB
{

/** Name, type, default-specifier, default-expression, comment-expression.
 *  The type is optional if default-expression is specified.
 */
class ASTColumnDeclaration : public IAST
{
public:
    String name;
    ASTPtr type;
    std::optional<bool> null_modifier;
    String default_specifier;
    ASTPtr default_expression;
    ASTPtr comment;
    ASTPtr codec;
    ASTPtr ttl;
    std::optional<bool> store_modifier;
    std::optional<bool> index_modifier;
    std::optional<bool> termvector_modifier;
    ASTPtr analyzer;
    ASTPtr search_analyzer;

    String getID(char delim) const override { return "ColumnDeclaration" + (delim + name); }

    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
