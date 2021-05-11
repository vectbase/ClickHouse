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
        const bool ranked_,
        TantivySearchIndexRW *tantivy_index_)
        : SourceWithProgress(metadata_snapshot->getSampleBlockForColumns(column_names_, storage.getVirtuals(), storage.getStorageID()))
        , column_names(std::move(column_names_))
        , tantivy_arg(std::move(tantivy_arg_))
        , limit(limit_)
        , ranked(ranked_)
        , tantivy_index(tantivy_index_)
    {
    }

    String getName() const override { return "Tantivy"; }

protected:
    Chunk generate() override
    {
	if (current_stage == 0) {
            if (ranked) {
                tantivy_iter = tantivysearch_ranked_search(tantivy_index, tantivy_arg.c_str(), limit);
            } else {
                tantivy_iter = tantivysearch_search(tantivy_index, tantivy_arg.c_str(), limit);
            }
	    current_stage = 1;
	} else if (current_stage == 2) {
            return {};
	}

        size_t iter_size = tantivysearch_iter_count(tantivy_iter);
	size_t block_size = std::min(iter_size, 65536UL);

        Columns columns;
        columns.reserve(column_names.size());

	if (column_names.size() == 1) {
	    if (column_names[0] == "processo_id") {
                auto column_proc = ColumnUInt64::create();
                auto & data_proc = column_proc->getData();
                data_proc.resize(block_size);
                size_t written_size = tantivysearch_iter_batch(tantivy_iter, block_size, &data_proc[0], nullptr);

                columns.push_back(std::move(column_proc));
		if (written_size == iter_size) {
                    tantivysearch_iter_free(tantivy_iter);
                    current_stage = 2;
		}
	    } else {
                tantivysearch_iter_free(tantivy_iter);
                throw Exception(
                    "Selecting processo_id column is required",
                    ErrorCodes::NOT_IMPLEMENTED);
	    }
	} else {
            auto column_proc = ColumnUInt64::create();
            auto & data_proc = column_proc->getData();

            auto column_mov = ColumnUInt64::create();
            auto & data_mov = column_mov->getData();

            data_proc.resize(block_size);
            data_mov.resize(block_size);

            size_t written_size = tantivysearch_iter_batch(tantivy_iter, block_size, &data_proc[0], &data_mov[0]);
            // std::cerr << "written: " << written_size << std::endl;

            for (size_t c=0; c<column_names.size(); c++)
            {
                if (column_names[c]=="processo_id") {
                    columns.push_back(std::move(column_proc));
	        } else if (column_names[c]=="movimento_id") {
                    columns.push_back(std::move(column_mov));
		} else {
                    tantivysearch_iter_free(tantivy_iter);
                    throw Exception(
                        "Selecting colum "+column_names[c]+" is not supported",
                        ErrorCodes::NOT_IMPLEMENTED);
		}
            }

            if (written_size == iter_size) {
                tantivysearch_iter_free(tantivy_iter);
                current_stage = 2;
            }
	}

         UInt64 num_rows = columns.at(0)->size();
         return Chunk(std::move(columns), num_rows);
    }

private:
    const Names column_names;
    size_t current_stage = 0;
    const String tantivy_arg;
    UInt64 limit;
    bool ranked;
    TantivySearchIndexRW *tantivy_index;
    TantivySearchIterWrapper *tantivy_iter = nullptr;
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

            auto & processo_id =  block.getByName("processo_id");
            auto processo_id_col = checkAndGetColumn<ColumnUInt64>(processo_id.column.get());
            auto & movimento_id =  block.getByName("movimento_id");
            auto movimento_id_col = checkAndGetColumn<ColumnUInt64>(movimento_id.column.get());
            auto & body =  block.getByName("body");
            auto body_col = checkAndGetColumn<ColumnString>(body.column.get());

            if (processo_id_col && movimento_id_col && body_col)
            {
                auto & processo_data = processo_id_col->getData();
                auto & movimento_data = movimento_id_col->getData();
                auto & chars = body_col->getChars();
                auto & offsets = body_col->getOffsets();
                const char * char_ptr = reinterpret_cast<const char*>(&chars[0]);

                tantivysearch_index(storage.tantivy_index, &processo_data[0], &movimento_data[0], char_ptr, &offsets[0], processo_data.size());
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
    bool ranked = true;

    if (function->arguments->children.size() > 1)
    {
        if (function->arguments->children[1]->as<ASTLiteral>())
        {
                limit = function->arguments->children[1]->as<ASTLiteral &>().value.safeGet<UInt64>();
        }

        if (function->arguments->children.size() > 2)
        {
            if (function->arguments->children[2]->as<ASTLiteral>())
            {
                    ranked = !!function->arguments->children[2]->as<ASTLiteral &>().value.safeGet<UInt64>();
            }
        }
    }

    std::cerr << "ranked: " << ranked << std::endl;

    String tantivy_text_arg = function->arguments->children[0]->as<ASTLiteral &>().value.safeGet<String>();

    return Pipe(
            std::make_shared<TantivySource>(
                    column_names, *this, metadata_snapshot, tantivy_text_arg, limit, ranked, tantivy_index
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
