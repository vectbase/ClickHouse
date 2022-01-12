#include <Parsers/ASTUseQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Access/AccessFlags.h>
#include <Common/typeid_cast.h>

#include <filesystem>

namespace DB
{

namespace fs = std::filesystem;

BlockIO InterpreterUseQuery::execute()
{
    const String & new_database = query_ptr->as<ASTUseQuery &>().database;
    getContext()->checkAccess(AccessType::SHOW_DATABASES, new_database);
    getContext()->getSessionContext()->setCurrentDatabase(new_database);
    if (!getContext()->getSessionID().empty())
        uploadCurrentDatabase(new_database);
    return {};
}

void InterpreterUseQuery::uploadCurrentDatabase(const String & new_database)
{
    auto zookeeper = getContext()->getZooKeeper();
    auto zookeeper_path = fs::path(DEFAULT_ZOOKEEPER_SESSIONS_PATH) / getContext()->getSessionID();
    zookeeper->createOrUpdate(zookeeper_path / "current_database", new_database, zkutil::CreateMode::Persistent);
}

}
