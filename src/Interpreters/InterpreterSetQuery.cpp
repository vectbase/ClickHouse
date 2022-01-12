#include <Parsers/ASTSetQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>

#include <filesystem>

namespace DB
{

namespace fs = std::filesystem;

BlockIO InterpreterSetQuery::execute()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    getContext()->checkSettingsConstraints(ast.changes);
    getContext()->getSessionContext()->applySettingsChanges(ast.changes);
    if (!getContext()->getSessionID().empty())
        uploadSettingChanges(ast.changes);
    return {};
}


void InterpreterSetQuery::executeForCurrentContext()
{
    const auto & ast = query_ptr->as<ASTSetQuery &>();
    getContext()->checkSettingsConstraints(ast.changes);
    getContext()->applySettingsChanges(ast.changes);
    if (!getContext()->getSessionID().empty())
        uploadSettingChanges(ast.changes);
}

void InterpreterSetQuery::uploadSettingChanges(const SettingsChanges & changes)
{
    auto zookeeper = getContext()->getZooKeeper();
    auto zookeeper_path = fs::path(DEFAULT_ZOOKEEPER_SESSIONS_PATH) / getContext()->getSessionID() / "setting_changes" / "";
    zookeeper->createAncestors(zookeeper_path);
    for (const SettingChange & change : changes)
        zookeeper->createOrUpdate(zookeeper_path / change.name,toString(change.value), zkutil::CreateMode::Persistent);
}

}
