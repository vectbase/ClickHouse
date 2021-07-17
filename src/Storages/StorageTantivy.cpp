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
#include <Poco/Path.h>
#include <Poco/File.h>

#include <codecvt>

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
        const StorageTantivy & storage_,
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

//    ~TantivySource() override {
//        this->reader->close();
//    }
    String getName() const override { return "Tantivy"; }

protected:
    Chunk generate() override
    {
        if (current_block_idx == 1)
            return {};

        const auto & sample_block = metadata_snapshot->getSampleBlock();
        //const auto & key_column = sample_block.getByName("primary_id");
        auto columns = sample_block.cloneEmptyColumns();
        size_t primary_id_pos = sample_block.getPositionByName("primary_id");
        std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;

        for (int i = 0; i < hits.size(); ++i)
        {
            Lucene::DocumentPtr doc = this->searcher->doc(hits[i]->doc);
            Lucene::String primary_id = doc->get(L"primary_id");
            Lucene::String secondary_id = doc->get(L"secondary_id");
            std::wcout << "Lucene searched doc: " << primary_id  << ", " << secondary_id <<std::endl;

            String p_id = converter.to_bytes(primary_id);
            String s_id = converter.to_bytes(secondary_id);
            ReadBufferFromString primary_id_buffer(p_id);
            ReadBufferFromString secondary_id_buffer(s_id);
            std::cout << "Lucene primary_id_buffer=" << p_id << ", secondary_id_buffer=" << s_id << std::endl;

            size_t idx = 0;
            for (const auto & elem : sample_block)
            {
                std::cout << "Lucene elem idx=" << idx << ", primary_id_pos=" << primary_id_pos << std::endl;
                elem.type->deserializeAsWholeText(*columns[idx], idx == primary_id_pos ? primary_id_buffer : secondary_id_buffer, FormatSettings());
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

class TantivyBlockOutputStream : public IBlockOutputStream
{
public:
    explicit TantivyBlockOutputStream(
        StorageTantivy & storage_,
        const StorageMetadataPtr & metadata_snapshot_)
        : storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
    {
        Block sample_block = metadata_snapshot->getSampleBlock();
        for (const auto & elem : sample_block)
        {
            if (elem.name == "primary_id")
                break;
            ++primary_id_pos;
        }
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

            auto & primary_id =  block.getByName("primary_id");
            auto primary_id_col = checkAndGetColumn<ColumnUInt64>(primary_id.column.get());
            auto & secondary_id =  block.getByName("secondary_id");
            auto secondary_id_col = checkAndGetColumn<ColumnUInt64>(secondary_id.column.get());
            auto & body =  block.getByName("body");
            auto body_col = checkAndGetColumn<ColumnString>(body.column.get());

            if (primary_id_col && secondary_id_col && body_col)
            {
                std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
                Lucene::String index_path_ws = converter.from_bytes(storage.index_path);
                Lucene::IndexWriterPtr writer = Lucene::newLucene<Lucene::IndexWriter>(
                    Lucene::FSDirectory::open(index_path_ws),
                    Lucene::newLucene<Lucene::StandardAnalyzer>(Lucene::LuceneVersion::LUCENE_CURRENT),
                        true,
                    Lucene::IndexWriter::MaxFieldLengthLIMITED);

                auto rows = block.rows();

                WriteBufferFromOwnString write_buffer;

                for (size_t i = 0; i < rows; i++)
                {
                    std::cout << "Lucene inserting row[" << i << "]" <<std::endl;
                    Lucene::DocumentPtr doc = Lucene::newLucene<Lucene::Document>();
                    size_t idx = 0;
                    for (const auto & elem : block)
                    {
                        write_buffer.restart();
                        auto column_name = block.safeGetByPosition(idx).name;

                        if (idx < block.columns() - 1)
                        {
                            elem.type->serializeAsText(*elem.column, i, write_buffer, FormatSettings());
                            doc->add(Lucene::newLucene<Lucene::Field>(converter.from_bytes(column_name), converter.from_bytes(write_buffer.str()), Lucene::Field::STORE_YES, Lucene::Field::INDEX_NOT_ANALYZED));
                        } else {
                            elem.type->serializeAsText(*elem.column, i, write_buffer, FormatSettings());
                            doc->add(Lucene::newLucene<Lucene::Field>(converter.from_bytes(column_name), converter.from_bytes(write_buffer.str()), Lucene::Field::STORE_NO, Lucene::Field::INDEX_ANALYZED));
                        }
                        ++idx;
                    }
                    std::cout << "Lucene inserted row[" << i << "]" <<std::endl;
                    writer->addDocument(doc);
                }
                if (rows > 0) {
                    writer->optimize();
                }
                writer->close();
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
    size_t primary_id_pos = 0;
};


StorageTantivy::StorageTantivy(const StorageID & table_id_, ColumnsDescription columns_description_, ConstraintsDescription constraints_, const String & index_path_)
    : IStorage(table_id_), index_path(index_path_), log(&Poco::Logger::get("StorageTantivy (" + table_id_.table_name + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_description_));
    storage_metadata.setConstraints(std::move(constraints_));
    setInMemoryMetadata(storage_metadata);

    Poco::File(index_path + "/").createDirectories();
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

    String query_text = function->arguments->children[0]->as<ASTLiteral &>().value.safeGet<String>();



        //Poco::File(this->index_path)
        std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;
        Lucene::String index_path_ws = converter.from_bytes(index_path);
        Lucene::FSDirectoryPtr dir = Lucene::FSDirectory::open(index_path_ws);
        if(dir->listAll().empty()) {
            std::cout << "No files in lucene index path: " << this->index_path << std::endl;
            return {};
        }


    return Pipe(
            std::make_shared<TantivySource>(
                column_names,
                *this,
                metadata_snapshot,
                query_text,
                //limit,
                dir
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
//    if (this->writer) {
//        this->writer->optimize();
//    }
    return false;
}

void StorageTantivy::truncate(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & /* metadata_snapshot */,
    const Context & /* context */,
    TableExclusiveLockHolder &)
{
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
    return;
}

void StorageTantivy::shutdown()
{
//    if (this->reader) {
//        this->reader->close();
//    }
//    if (this->writer) {
//        this->writer->close();
//    }
    return;
}

void StorageTantivy::drop() {
    Poco::File(index_path).remove(true);
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
