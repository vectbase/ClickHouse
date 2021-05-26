#include <Common/Exception.h>

#include <DataStreams/IBlockInputStream.h>

#include <Storages/StorageTantivy.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryInfo.h>

#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>

#include <IO/WriteBufferFromString.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class TantivySource : public SourceWithProgress
{
public:
    TantivySource(
        Names column_names_,
        const StorageTantivy & storage,
        const StorageMetadataPtr & metadata_snapshot,
        const String & tantivy_arg_,
        const UInt64 limit_,
        TantivySearchIterWrapper *tantivy_iter_)
        : SourceWithProgress(metadata_snapshot->getSampleBlockForColumns(column_names_, storage.getVirtuals(), storage.getStorageID()))
        , column_names(std::move(column_names_))
        , tantivy_arg(std::move(tantivy_arg_))
        , limit(limit_)
        , tantivy_iter(tantivy_iter_)
    {
    }

    String getName() const override { return "Tantivy"; }

protected:
    Chunk generate() override
    {
        if (current_block_idx == 1)
            return {};

        Columns columns;
        columns.reserve(column_names.size());

        auto column_primary = ColumnUInt64::create();
        auto & data_primary = column_primary->getData();

        auto column_secondary = ColumnUInt64::create();
        auto & data_secondary = column_secondary->getData();

        size_t tantivy_size = tantivysearch_iter_count(tantivy_iter);
        if (tantivy_size < limit)
            limit = tantivy_size;
        data_primary.resize(limit);
        data_secondary.resize(limit);

        UInt64 i = 0;
        UInt64 primary_id = 0;
        UInt64 secondary_id = 0;
        int r = tantivysearch_iter_next(tantivy_iter, &primary_id, &secondary_id);
        while (r)
        {
            data_primary[i] = primary_id;
            data_secondary[i] = secondary_id;
            if (i > limit)
                break;
            i++;
            r = tantivysearch_iter_next(tantivy_iter, &primary_id, &secondary_id);
        }
        tantivysearch_iter_free(tantivy_iter);

        for (size_t c=0; c<column_names.size(); c++)
        {
            if (column_names[c]=="primary_id")
                columns.push_back(std::move(column_primary));
            else if (column_names[c]=="secondary_id")
                columns.push_back(std::move(column_secondary));
            else
                throw Exception(
                    "Selecting colum "+column_names[c]+" is not supported",
                    ErrorCodes::NOT_IMPLEMENTED);
        }
        current_block_idx = 1;
        UInt64 num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }

private:
    const Names column_names;
    size_t current_block_idx = 0;
    const String tantivy_arg;
    UInt64 limit;
    TantivySearchIterWrapper *tantivy_iter;
};

class TantivyBlockOutputStream : public IBlockOutputStream
{
public:
    explicit TantivyBlockOutputStream(
        StorageTantivy & storage_,
        const StorageMetadataPtr & metadata_snapshot_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
    {}
    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }
    void write(const Block & block) override
    {
        const auto size_bytes_diff = block.allocatedBytes();
        const auto size_rows_diff = block.rows();
        metadata_snapshot->check(block, true);
        {
            // std::lock_guard lock(storage.mutex);
            // auto new_data = std::make_unique<BlocksList>(*(storage.data.get()));
            // new_data->push_back(block);
            // storage.data.set(std::move(new_data));
            if (block.columns() != 3) {
                throw Exception(
                    "Inserts need all columns",
                    ErrorCodes::NOT_IMPLEMENTED);
            }

            auto & primary_id =  block.getByName("primary_id");
            auto primary_id_col = checkAndGetColumn<ColumnUInt64>(primary_id.column.get());
            auto & secondary_id =  block.getByName("secondary_id");
            auto secondary_id_col = checkAndGetColumn<ColumnUInt64>(secondary_id.column.get());
            auto & body =  block.getByName("body");
            auto body_col = checkAndGetColumn<ColumnString>(body.column.get());

            if (primary_id_col && secondary_id_col && body_col)
            {
                auto & primary_data = primary_id_col->getData();
                auto & secondary_data = secondary_id_col->getData();
                auto & chars = body_col->getChars();
                auto & offsets = body_col->getOffsets();
                const char * char_ptr = reinterpret_cast<const char*>(&chars[0]);

                int res = tantivysearch_index(storage.tantivy_index, &primary_data[0], &secondary_data[0], char_ptr, &offsets[0], primary_data.size());
                std::cerr << "index result: " << res << std::endl;
            } else {
                throw Exception(
                    "Inserts need all columns",
                    ErrorCodes::NOT_IMPLEMENTED);
            }

            storage.total_size_bytes.fetch_add(size_bytes_diff, std::memory_order_relaxed);
            storage.total_size_rows.fetch_add(size_rows_diff, std::memory_order_relaxed);
        }
    }
private:
    StorageTantivy & storage;
    StorageMetadataPtr metadata_snapshot;
};


StorageTantivy::StorageTantivy(const StorageID & table_id_, ColumnsDescription columns_description_, ConstraintsDescription constraints_, const String & index_path_)
    : IStorage(table_id_), index_path(index_path_), log(&Poco::Logger::get("StorageTantivy (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_description_));
    storage_metadata.setConstraints(std::move(constraints_));
    setInMemoryMetadata(storage_metadata);
}


Pipe StorageTantivy::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    const Context & /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    unsigned /*num_streams*/)
{
    metadata_snapshot->check(column_names, getVirtuals(), getStorageID());

    const ASTSelectQuery & select = query_info.query->as<ASTSelectQuery &>();
    const ASTPtr & where = select.where();
    if (!where)
    {
        throw Exception(
                "Missing WHERE clause",
                ErrorCodes::NOT_IMPLEMENTED);
    }
    const auto * function = where->as<ASTFunction>();
    if (function->name != "tantivy")
    {
        throw Exception(
                "WHERE clause should contain only tantivy function",
                ErrorCodes::NOT_IMPLEMENTED);
    }

    UInt64 limit = 1000000UL;

    if (function->arguments->children.size() == 2)
    {
        if (function->arguments->children[1]->as<ASTLiteral>())
        {
                limit = function->arguments->children[1]->as<ASTLiteral &>().value.safeGet<UInt64>();
        }
    }

    String tantivy_text_arg = function->arguments->children[0]->as<ASTLiteral &>().value.safeGet<String>();

    TantivySearchIterWrapper *tantivy_iter = tantivysearch_search(tantivy_index, tantivy_text_arg.c_str(), limit);

    return Pipe(
            std::make_shared<TantivySource>(
                    column_names, *this, metadata_snapshot, tantivy_text_arg, limit, tantivy_iter
            ));
}

BlockOutputStreamPtr StorageTantivy::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<TantivyBlockOutputStream>(*this, metadata_snapshot);
}

bool StorageTantivy::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & /*partition*/,
    bool /*final*/,
    bool /*deduplicate*/,
    const Names & /*deduplicate_by_columns*/,
    const Context & /*context*/)
{
    std::cerr << "Running optimize" << std::endl;
    if (tantivysearch_writer_commit(tantivy_index))
    {
        return true;
    }
    return false;
}

void StorageTantivy::truncate(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /* metadata_snapshot */,
    const Context & /* context */,
    TableExclusiveLockHolder &)
{
    bool res = tantivysearch_index_truncate(tantivy_index);
    LOG_DEBUG(log, "Truncated index with result: {}", res);
}


std::optional<UInt64> StorageTantivy::totalRows(const Settings &) const
{
    /// All modifications of these counters are done under mutex which automatically guarantees synchronization/consistency
    /// When run concurrently we are fine with any value: "before" or "after"
    return total_size_rows.load(std::memory_order_relaxed);
}

std::optional<UInt64> StorageTantivy::totalBytes(const Settings &) const
{
    return total_size_bytes.load(std::memory_order_relaxed);
}

void StorageTantivy::startup()
{
    this->tantivy_index = tantivysearch_open_or_create_index(index_path.c_str());
    return;
}

void StorageTantivy::shutdown()
{
    if (tantivy_index != nullptr)
    {
        tantivysearch_index_free(tantivy_index);
    }
    return;
}

void StorageTantivy::drop() {
    if (tantivy_index != nullptr)
    {
        tantivysearch_index_delete(tantivy_index);
    }
    return;
}

void registerStorageTantivy(StorageFactory & factory)
{
    factory.registerStorage("Tantivy", [](const StorageFactory::Arguments & args)
    {
        if (args.engine_args.size() != 1)
            throw Exception(
                "Engine " + args.engine_name + " needs the data path argument",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        String index_path = args.engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();

        return StorageTantivy::create(args.table_id, args.columns, args.constraints, index_path);
    });
}

}
