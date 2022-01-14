#include "global.h"
/**
 * @brief Construct a new Page object. Never used as part of the code
 *
 */
Page::Page()
{
    this->pageName = "";
    this->tableName = "";
    this->matrixName = "";
    this->pageIndex = -1;
    this->rowCount = 0;
    this->columnCount = 0;
    this->rows.clear();
}

/**
 * @brief Construct a new Page:: Page object given the table name and page
 * index. When tables are loaded they are broken up into blocks of BLOCK_SIZE
 * and each block is stored in a different file named
 * "<tablename>_Page<pageindex>". For example, If the Page being loaded is of
 * table "R" and the pageIndex is 2 then the file name is "R_Page2". The page
 * loads the rows (or tuples) into a vector of rows (where each row is a vector
 * of integers).
 *
 * @param tableName 
 * @param pageIndex 
 */
Page::Page(string tableName, int pageIndex, int sequence)
{
    logger.log("Page::Page");
    this->pageIndex = pageIndex;
    this->pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    if(sequence != -1)
    {
        // cout<<tableName<<" partition page loading with pg: "<<pageIndex<<" seq: "<<sequence<<endl;
        this->pageName = "../data/temp/" + tableName +"_Partition" + "_Page" + to_string(pageIndex) +"_S" + to_string(sequence);
    }
    if(tableCatalogue.isTable(tableName)){
        this->tableName = tableName;
        Table table = *tableCatalogue.getTable(tableName);
        this->columnCount = table.columnCount;
        uint maxRowCount = table.maxRowsPerBlock;
        vector<int> row(columnCount, 0);
        this->rows.assign(maxRowCount, row);
        // cout<<"tableloaded"<<endl;
        ifstream fin(pageName, ios::in);
        if(sequence!=-1){
            this->rowCount = table.maxRowsPerBlock;
            // cout<<"PARTITIONROWCOUNTLOADED"<<this->rowCount<<endl;
        }
        else{
            this->rowCount = table.rowsPerBlockCount[pageIndex];
        }
        int number;
        for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
        {
            for (int columnCounter = 0; columnCounter < columnCount; columnCounter++)
            {
                fin >> number;
                this->rows[rowCounter][columnCounter] = number;
            }
        }
        fin.close();
    } 
    else if(matrixCatalogue.isMatrix(tableName)){
        this->matrixName = tableName;
        Matrix matrix = *matrixCatalogue.getMatrix(this->matrixName);
        if(matrix.sparse)
        {
            ifstream fin(pageName, ios::in);
            this->sparse = true;
            this->rowCount = matrix.rowsPerBlockCount[pageIndex];
            this->columnCount = 3;
            vector<int> row(columnCount, 0);
            // pair<pair<int,int>,int> dummy {{0,0},0};
            // this->sparserows.assign(rowCount,dummy);
            this->rows.assign(rowCount,row);
            int number;
            for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
            {
                fin >> number;
                this->rows[rowCounter][0] = (number);
                // .first.first = number;
                fin >> number;
                this->rows[rowCounter][1] = number;
                fin >> number;
                this->rows[rowCounter][2] = number;
            }
            fin.close();
        }
        else{
            this->columnCount = matrix.colsPerBlockCount[pageIndex];
            this->rowCount = matrix.rowsPerBlockCount[pageIndex];
            vector<int> row(columnCount, 0);
            this->rows.assign(rowCount, row);
            ifstream fin(pageName, ios::in);
            int number;
            for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
            {
                for (int columnCounter = 0; columnCounter < columnCount; columnCounter++)
                {
                    fin >> number;
                    this->rows[rowCounter][columnCounter] = number;
                }
            }
            fin.close();
        }
    }
    // Partition
    else{
        cout<<"table:"<<this->tableName<<endl;
        this->tableName = tableName;
        // this->columnCount = table.columnCount;
        // uint maxRowCount = table.maxRowsPerBlock;
        // vector<int> row(columnCount, 0);
        // this->rows.assign(maxRowCount, row);
        // ifstream fin(pageName, ios::in);
        // this->rowCount = table.rowsPerBlockCount[pageIndex];
        // int number;
        // for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
        // {
        //     for (int columnCounter = 0; columnCounter < columnCount; columnCounter++)
        //     {
        //         fin >> number;
        //         this->rows[rowCounter][columnCounter] = number;
        //     }
        // }
        // fin.close();
        // vector<int> row(this->columnCount, 0);
        // this->rows.assign(this->rowCount, row);
        // ifstream fin(pageName, ios::in);
        // int number;
        // for (uint rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
        // {
        //     for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        //     {
        //         fin >> number;
        //         cout<<"parti num"<<number<<endl;
        //         this->rows[rowCounter][columnCounter] = number;
        //     }
        // }
        // fin.close();
    }
}

/**
 * @brief Get row from page indexed by rowIndex
 * 
 * @param rowIndex 
 * @return vector<int> 
 */
vector<int> Page::getRow(int rowIndex)
{
    logger.log("Page::getRow");
    vector<int> result;
    result.clear();
    // cout<<"GETROW:"<<rowIndex<<endl;
    if (rowIndex >= this->rowCount)
        return result;
    return this->rows[rowIndex];
}

Page::Page(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount)
{
    logger.log("Page::Page");
    this->matrixName = tableName;
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->rows = rows;
    this->rowCount = rowCount;
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/"+this->tableName + "_Page" + to_string(pageIndex);
}
// MATRIX
Page::Page(string tableName, int pageIndex, vector<vector<int>> rows)
{
    logger.log("Page::Page");
    this->matrixName = tableName;
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->rows = rows;
    this->rowCount = rows.size();
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/"+this->matrixName + "_Page" + to_string(pageIndex);
}
// SPARSE MATRIX
Page::Page(string tableName, int pageIndex, bool sparse, vector<vector<int>> rows)
{
    logger.log("Page::Page");
    this->matrixName = tableName;
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->sparse = sparse;
    // this->sparserows = rows;
    this->rows = rows;
    this->rowCount = rows.size();
    this->columnCount = 3;
    this->pageName = "../data/temp/"+this->matrixName + "_Page" + to_string(pageIndex);
}
// PARTITION
Page::Page(string tableName, int pageIndex, int sequenceNumber, vector<vector<int>> rows, int rowCount)
{
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex;
    this->rows = rows;
    this->rowCount = rowCount;
    this->columnCount = rows[0].size();
    this->pageName = "../data/temp/"+this->tableName +"_Partition"+ "_Page" + to_string(pageIndex)+"_S" + to_string(sequenceNumber);
}

/**
 * 
 * @brief writes current page contents to file.
 * 
 */
void Page::writePage()
{
    logger.log("Page::writePage");
    ofstream fout(this->pageName, ios::trunc);
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (columnCounter != 0)
                fout << " ";
            fout << this->rows[rowCounter][columnCounter];
        }
        fout << endl;
    }
    fout.close();
}
//MATRIX
void Page::writePageMatrix()
{
    logger.log("Page::writePageMatrix");
    ofstream fout(this->pageName, ios::trunc);
    if(this->sparse)
    {
        for(auto tuple: this->rows)
        {
            // fout<<tuple.first.first<<" "<<tuple.first.second<<" "<<tuple.second<<endl;
            fout<<tuple[0]<<" "<<tuple[1]<<" "<<tuple[2]<<endl;
        }
    }
    else{
        for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
        {
            for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
            {
                if (columnCounter != 0)
                    fout << " ";
                fout << this->rows[rowCounter][columnCounter];
            }
            fout << endl;
        }
    }
    fout.close();
}
