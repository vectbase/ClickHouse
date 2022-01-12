#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Access/RolesOrUsersSet.h>

namespace DB
{

class ASTSetRoleQuery;
struct RolesOrUsersSet;
struct User;

class InterpreterSetRoleQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterSetRoleQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static void updateUserSetDefaultRoles(User & user, const RolesOrUsersSet & roles_from_query);

private:
    void setRole(const ASTSetRoleQuery & query);
    void setDefaultRole(const ASTSetRoleQuery & query);

    void uploadCurrentRoles(const std::vector<UUID> & roles);

    ASTPtr query_ptr;
};

}
