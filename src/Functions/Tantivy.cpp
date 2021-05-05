#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnNullable.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionTantivy : public IFunction
{
public:
    static constexpr auto name = "tantivy";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionTantivy>();
    }

    std::string getName() const override
    {
        return name;
    }

    bool isDeterministic() const override { return false; }
    bool isDeterministicInScopeOfQuery() const override { return true; }
    bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const override { return true; }
    bool useDefaultImplementationForNulls() const override { return false; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
                throw Exception(
                    "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                        + ", should be at least 1.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        WhichDataType which(arguments[0]);

        if (!which.isString())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected String",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size()>1)
        {
            WhichDataType which2(arguments[1]);

            if (arguments.size()>1 && !which2.isNativeUInt())
                throw Exception("Illegal type " + arguments[1]->getName() + " of argument of function " + getName() + ", expected UInt64",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }


        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];
        const IColumn * col = elem.column.get();

        if (!isColumnConst(*col))
            throw Exception("The argument of function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);

        if (arguments.size()>1)
        {
            const ColumnWithTypeAndName & elem2 = arguments[1];
            const IColumn * col2 = elem2.column.get();
            if (!isColumnConst(*col2))
                throw Exception("The argument of function " + getName() + " must be constant.", ErrorCodes::ILLEGAL_COLUMN);
        }


        return DataTypeUInt8().createColumnConst(elem.column->size(), 1u)->convertToFullColumnIfConst();
    }
};

}

void registerFunctionTantivy(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTantivy>();
}

}
