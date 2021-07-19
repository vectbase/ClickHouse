#pragma once

#include <atomic>
#include <optional>
#include <mutex>

#include <ext/shared_ptr_helper.h>

#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>
#include <common/logger_useful.h>

#include <lucene++/LuceneHeaders.h>
#include <lucene++/FileUtils.h>
#include <lucene++/MiscUtils.h>

namespace DB
{

/** Implements storage in the RAM.
  * Suitable for temporary data.
  * It does not support keys.
  * Data is stored as a set of blocks and is not stored anywhere else.
  */
class StorageLucene final : public ext::shared_ptr_helper<StorageLucene>, public IStorage
{

friend struct ext::shared_ptr_helper<StorageLucene>;
friend class LuceneBlockOutputStream;

public:
    String getName() const override { return "Lucene"; }

    size_t getSize() const { return data.size(); }

    bool supportsPrewhere() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(const ASTPtr & /* left_in_operand */, const Context & /* query_context */, const StorageMetadataPtr & /* metadata_snapshot */) const override { return true; }

    Pipe read(
        const Names & column_names,
        const StorageMetadataPtr & /*metadata_snapshot*/,
        SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

//    void startup() override;
//    void shutdown() override;

    bool supportsParallelInsert() const override { return false; }

    BlockOutputStreamPtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, const Context & context) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        const Context & context) override;

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const Context & context,
        TableExclusiveLockHolder &) override;

//    void drop() override;

    bool supportsSampling() const override { return false; }

    std::optional<UInt64> totalRows(const Settings &) const override;
    std::optional<UInt64> totalBytes(const Settings &) const override;

    struct CommonArguments
    {
        StorageID table_id;
        const ColumnsDescription & columns;
        const ConstraintsDescription & constraints;
        const Context & context;
    };

private:
    String base_path;
    /// The data itself. `list` - so that when inserted to the end, the existing iterators are not invalidated.
    BlocksList data;
    String index_path;
    mutable std::mutex mutex;
//    Lucene::IndexReaderPtr reader;
//    Lucene::IndexWriterPtr writer;
    std::atomic<size_t> total_size_bytes = 0;
    std::atomic<size_t> total_size_rows = 0;
    Poco::Logger * log;

protected:
    StorageLucene(const std::string & relative_table_dir_path, CommonArguments args);

private:
    explicit StorageLucene(CommonArguments args);
};

}
