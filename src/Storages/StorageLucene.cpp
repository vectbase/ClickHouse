#include <Common/Exception.h>

#include <DataStreams/IBlockInputStream.h>

#include <Storages/StorageLucene.h>
#include <Storages/StorageFactory.h>

#include <Interpreters/Context.h>
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
#include <Poco/Path.h>
#include <Poco/File.h>

#include <codecvt>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int INCORRECT_FILE_NAME;
}


class LuceneSource : public SourceWithProgress
{
public:
    LuceneSource(
        Names column_names_,
        const StorageLucene & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const String & query_text_,
        //const UInt64 limit_,
        Lucene::FSDirectoryPtr dir_)
        : SourceWithProgress(metadata_snapshot_->getSampleBlockForColumns(column_names_, storage_.getVirtuals(), storage_.getStorageID())),
        column_names(std::move(column_names_)),
        metadata_snapshot(metadata_snapshot_),
        query_text(std::move(query_text_))
        //limit(limit_),
    {

        this->reader = Lucene::IndexReader::open(dir_, true);
        std::cout << "Opened lucene index path: " << std::endl;

        this->searcher = Lucene::newLucene<Lucene::IndexSearcher>(this->reader);
        Lucene::AnalyzerPtr analyzer = Lucene::newLucene<Lucene::StandardAnalyzer>(Lucene::LuceneVersion::LUCENE_CURRENT);
        Lucene::QueryParserPtr parser = Lucene::newLucene<Lucene::QueryParser>(Lucene::LuceneVersion::LUCENE_CURRENT, L"body", analyzer);
        Lucene::QueryPtr query = parser->parse(Lucene::String(query_text.begin(), query_text.end()));
        std::cout << "Search for: " << query_text << std::endl;
        Lucene::TopScoreDocCollectorPtr collector = Lucene::TopScoreDocCollector::create(500, false);
        searcher->search(query, collector);
        this->hits = collector->topDocs()->scoreDocs;
    }

    ~LuceneSource() override {
        this->searcher->close();
        this->reader->close();
    }

    String getName() const override { return "Lucene"; }

protected:
    Chunk generate() override
    {
        if (current_block_idx == 1)
            return {};

        const auto & sample_block = metadata_snapshot->getSampleBlock();
        auto columns = sample_block.cloneEmptyColumns();
        std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;

        for (int i = 0; i < hits.size(); ++i)
        {
            Lucene::DocumentPtr doc = this->searcher->doc(hits[i]->doc);

            size_t idx = 0;
            for (const auto & elem : sample_block)
            {
                Lucene::String doc_column_name = converter.from_bytes(elem.name);
                Lucene::String doc_column_value = doc->get(doc_column_name);
                String column_value = converter.to_bytes(doc_column_value);
                ReadBufferFromString column_value_buffer(column_value);
                std::cout << "Searched: row[" << i << "]column[" << idx << "]: " << elem.name << "=" << column_value << std::endl;
                elem.type->deserializeAsWholeText(*columns[idx], column_value_buffer, FormatSettings());
                ++idx;
            }
        }

        current_block_idx = 1;
        UInt64 num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }

private:
    const Names column_names;
    const StorageMetadataPtr metadata_snapshot;
    size_t current_block_idx = 0;
    const String query_text;
    //UInt64 limit;
    Lucene::IndexReaderPtr reader;
    Lucene::SearcherPtr searcher;
    Lucene::Collection<Lucene::ScoreDocPtr> hits;
};

class LuceneBlockOutputStream : public IBlockOutputStream
{
public:
    explicit LuceneBlockOutputStream(
        StorageLucene & storage_,
        const StorageMetadataPtr & metadata_snapshot_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
    {
    }
    Block getHeader() const override { return metadata_snapshot->getSampleBlock(); }
    void write(const Block & block) override
    {
        const auto size_bytes_diff = block.allocatedBytes();
        const auto size_rows_diff = block.rows();
        metadata_snapshot->check(block, true);
        {
            if (block.columns() != 3) {
                throw Exception(
                    "Inserts need all columns",
                    ErrorCodes::NOT_IMPLEMENTED);
            }


            std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
            Lucene::String index_path_ws = converter.from_bytes(storage.index_path);
            // create a new index if there is not already an index at the provided path
            // and otherwise open the existing index.
            Lucene::IndexWriterPtr writer = Lucene::newLucene<Lucene::IndexWriter>(
                Lucene::FSDirectory::open(index_path_ws),
                Lucene::newLucene<Lucene::StandardAnalyzer>(Lucene::LuceneVersion::LUCENE_CURRENT),
                Lucene::IndexWriter::MaxFieldLengthLIMITED);

            auto rows = block.rows();

            WriteBufferFromOwnString write_buffer;

            for (size_t i = 0; i < rows; i++)
            {
                std::cout << "Lucene inserting row[" << i << "]" << std::endl;
                Lucene::DocumentPtr doc = Lucene::newLucene<Lucene::Document>();
                size_t idx = 0;
                for (const auto & elem : block)
                {
                    write_buffer.restart();
                    auto column_name = block.safeGetByPosition(idx).name;

                    if (idx < block.columns() - 1)
                    {
                        elem.type->serializeAsText(*elem.column, i, write_buffer, FormatSettings());
                        doc->add(Lucene::newLucene<Lucene::Field>(
                            converter.from_bytes(column_name),
                            converter.from_bytes(write_buffer.str()),
                            Lucene::Field::STORE_YES,
                            Lucene::Field::INDEX_NOT_ANALYZED));
                    }
                    else
                    {
                        elem.type->serializeAsText(*elem.column, i, write_buffer, FormatSettings());
                        doc->add(Lucene::newLucene<Lucene::Field>(
                            converter.from_bytes(column_name),
                            converter.from_bytes(write_buffer.str()),
                            Lucene::Field::STORE_YES,
                            Lucene::Field::INDEX_ANALYZED));
                    }
                    ++idx;
                }
                std::cout << "Lucene inserted row[" << i << "]" << std::endl;
                writer->addDocument(doc);
            }
            if (rows > 0)
            {
                writer->optimize();
            }
            writer->close();

            storage.total_size_bytes.fetch_add(size_bytes_diff, std::memory_order_relaxed);
            storage.total_size_rows.fetch_add(size_rows_diff, std::memory_order_relaxed);
        }
    }
private:
    StorageLucene & storage;
    StorageMetadataPtr metadata_snapshot;
//    size_t primary_id_pos = 0;
};


StorageLucene::StorageLucene(const std::string & relative_table_dir_path, CommonArguments args)
    : StorageLucene(args)
{
    if (relative_table_dir_path.empty())
        throw Exception("Storage " + getName() + " requires data path", ErrorCodes::INCORRECT_FILE_NAME);

    std::cout << "StorageLucene base_path:" << base_path << ", relative_table_dir_path:" << relative_table_dir_path << std::endl;
    this->index_path = base_path + relative_table_dir_path + "/";
    Poco::File(this->index_path).createDirectories();
}

StorageLucene::StorageLucene(CommonArguments args)
    : IStorage(args.table_id)
    , base_path(args.context.getPath())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(args.columns);
    storage_metadata.setConstraints(args.constraints);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageLucene::read(
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
    if (function->name != "lucene")
    {
        throw Exception(
                "WHERE clause should contain only lucene function",
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

    String query_text = function->arguments->children[0]->as<ASTLiteral &>().value.safeGet<String>();

    std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
    Lucene::String index_path_ws = converter.from_bytes(index_path);
    Lucene::FSDirectoryPtr dir = Lucene::FSDirectory::open(index_path_ws);
    if (dir->listAll().empty())
    {
        std::cout << "No files in lucene index path: " << this->index_path << std::endl;
        return {};
    }


    return Pipe(
            std::make_shared<LuceneSource>(
                column_names,
                *this,
                metadata_snapshot,
                query_text,
                //limit,
                dir
            ));
}

BlockOutputStreamPtr StorageLucene::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, const Context & /*context*/)
{
    return std::make_shared<LuceneBlockOutputStream>(*this, metadata_snapshot);
}

bool StorageLucene::optimize(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const ASTPtr & /*partition*/,
    bool /*final*/,
    bool /*deduplicate*/,
    const Names & /*deduplicate_by_columns*/,
    const Context & /*context*/)
{
    std::cerr << "Running optimize" << std::endl;
    return false;
}

void StorageLucene::truncate(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /* metadata_snapshot */,
    const Context & /* context */,
    TableExclusiveLockHolder &)
{
    std::cout << "StorageLucene is truncate" << std::endl;
    Poco::File(this->index_path).remove(true);
    Poco::File(this->index_path).createDirectories();
    // TODO: init lucene index files
}


std::optional<UInt64> StorageLucene::totalRows(const Settings &) const
{
    /// All modifications of these counters are done under mutex which automatically guarantees synchronization/consistency
    /// When run concurrently we are fine with any value: "before" or "after"
    return total_size_rows.load(std::memory_order_relaxed);
}

std::optional<UInt64> StorageLucene::totalBytes(const Settings &) const
{
    return total_size_bytes.load(std::memory_order_relaxed);
}

//void StorageLucene::startup()
//{
//    return;
//}

// when "DROP TABLE" is called, or clickhouse-server is shutdown
//void StorageLucene::shutdown()
//{
//    std::cout << "StorageLucene is shutdown" << std::endl;
//    Poco::File(index_path).remove(true);
//    return;
//}

//void StorageLucene::drop() {
//    std::cout << "StorageLucene is dropped" << std::endl;
//    Poco::File(index_path).remove(true);
//    return;
//}

void registerStorageLucene(StorageFactory & factory)
{
    factory.registerStorage("Lucene", [](const StorageFactory::Arguments & factory_args)
    {
        StorageLucene::CommonArguments storage_args{
            .table_id = factory_args.table_id,
            .columns = factory_args.columns,
            .constraints = factory_args.constraints,
            .context = factory_args.context
        };

        return StorageLucene::create(factory_args.relative_data_path, storage_args);
   });
}

}
