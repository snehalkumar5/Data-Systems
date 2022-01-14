// #include "cursor.h"

// enum IndexingStrategy
// {
//     BTREE,
//     HASH,
//     NOTHING
// };

/**
 * @brief The Matrix class holds all information related to a loaded Matrix. It
 * also implements methods that interact with the parsers, executors, cursors
 * and the buffer manager. There are typically 2 ways a Matrix object gets
 * created through the course of the workflow - the first is by using the LOAD
 * command and the second is to use assignment statements (SELECT, PROJECT,
 * JOIN, SORT, CROSS and DISTINCT). 
 *
 */
class Matrix
{
    vector<unordered_set<int>> distinctValuesInColumns;

public:
    string sourceFileName = "";
    string matrixName = "";
    vector<string> columns;
    uint columnCount = 0;
    long long int rowCount = 0;
    long long int k = 0;
    uint blockCount = 0;
    long long int maxRowsPerBlock = 0;
    long long int maxColumnsPerBlock = 0;
    long long int submatrixcount = 0;
    long long int sparsecount = 0;
    vector<pair<pair<int,int>,int>> sparseMat;
    vector<uint> rowsPerBlockCount;
    vector<uint> colsPerBlockCount;
    bool indexed = false;
    string indexedColumn = "";
    IndexingStrategy indexingStrategy = NOTHING;
    bool sparse = false;
    
    bool extractColumnNames(string firstLine);
    bool blockify();
    Matrix();
    Matrix(string matrixName);
    Matrix(string matrixName, vector<string> columns);
    bool load();
    void print();
    void makePermanent();
    bool isPermanent();
    bool isSparse();
    void getNextPage(Cursor *cursor);
    Cursor getCursor();
    int getColumnIndex(string columnName);
    void unload();
    void transpose();
    int get_pageid(int row, int col);

    /**
 * @brief Static function that takes a vector of valued and prints them out in a
 * comma seperated format.
 *
 * @tparam M current usages include int and string
 * @param row 
 */
template <typename M>
void writeRow(vector<M> row, ostream &fout)
{
    logger.log("Matrix::printRow");
    for (int columnCounter = 0; columnCounter < row.size(); columnCounter++)
    {
        if (columnCounter != 0)
            fout << ",";
        fout << row[columnCounter];
    }
    // fout << endl;
}

/**
 * @brief Static function that takes a vector of valued and prints them out in a
 * comma seperated format.
 *
 * @tparam M current usages include int and string
 * @param row 
 */
template <typename M>
void writeRow(vector<M> row)
{
    logger.log("Matrix::printRow");
    ofstream fout(this->sourceFileName, ios::app);
    this->writeRow(row, fout);
    fout.close();
}
};