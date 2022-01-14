#include "global.h"

/**
 * @brief Construct a new Table:: Table object
 *
 */
Table::Table()
{
    logger.log("Table::Table");
}

/**
 * @brief Construct a new Table:: Table object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName 
 */
Table::Table(string tableName)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/" + tableName + ".csv";
    this->tableName = tableName;
}

/**
 * @brief Construct a new Table:: Table object used when an assignment command
 * is encountered. To create the table object both the table name and the
 * columns the table holds should be specified.
 *
 * @param tableName 
 * @param columns 
 */
Table::Table(string tableName, vector<string> columns)
{
    logger.log("Table::Table");
    this->sourceFileName = "../data/temp/" + tableName + ".csv";
    this->tableName = tableName;
    this->columns = columns;
    this->columnCount = columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * columnCount));
    this->writeRow<string>(columns);
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded 
 * @return false if an error occurred 
 */
bool Table::load()
{
    logger.log("Table::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        if (this->extractColumnNames(line))
            if (this->blockify())
                return true;
    }
    fin.close();
    return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file. 
 *
 * @param line 
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Table::extractColumnNames(string firstLine)
{
    logger.log("Table::extractColumnNames");
    unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);
    while (getline(s, word, ','))
    {
        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        if (columnNames.count(word))
            return false;
        columnNames.insert(word);
        this->columns.emplace_back(word);
    }
    this->columnCount = this->columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. 
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockify()
{
    logger.log("Table::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    string line, word;
    vector<int> row(this->columnCount, 0);
    vector<vector<int>> rowsInPage(this->maxRowsPerBlock, row);
    int pageCounter = 0;
    unordered_set<int> dummy;
    dummy.clear();
    this->distinctValuesInColumns.assign(this->columnCount, dummy);
    this->distinctValuesPerColumnCount.assign(this->columnCount, 0);
    getline(fin, line);
    while (getline(fin, line))
    {
        stringstream s(line);
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (!getline(s, word, ','))
                return false;
            row[columnCounter] = stoi(word);
            rowsInPage[pageCounter][columnCounter] = row[columnCounter];
        }
        pageCounter++;
        this->updateStatistics(row);
        if (pageCounter == this->maxRowsPerBlock)
        {
            bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
            this->blockCount++;
            this->rowsPerBlockCount.emplace_back(pageCounter);
            pageCounter = 0;
        }
    }
    if (pageCounter)
    {
        bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageCounter);
        this->blockCount++;
        this->rowsPerBlockCount.emplace_back(pageCounter);
        pageCounter = 0;
    }

    if (this->rowCount == 0)
        return false;
    this->distinctValuesInColumns.clear();
    return true;
}

/**
 * @brief Given a row of values, this function will update the statistics it
 * stores i.e. it updates the number of rows that are present in the column and
 * the number of distinct values present in each column. These statistics are to
 * be used during optimisation.
 *
 * @param row 
 */
void Table::updateStatistics(vector<int> row)
{
    this->rowCount++;
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (!this->distinctValuesInColumns[columnCounter].count(row[columnCounter]))
        {
            this->distinctValuesInColumns[columnCounter].insert(row[columnCounter]);
            this->distinctValuesPerColumnCount[columnCounter]++;
        }
    }
}

/**
 * @brief Checks if the given column is present in this table.
 *
 * @param columnName 
 * @return true 
 * @return false 
 */
bool Table::isColumn(string columnName)
{
    logger.log("Table::isColumn");
    for (auto col : this->columns)
    {
        if (col == columnName)
        {
            return true;
        }
    }
    return false;
}

/**
 * @brief Renames the column indicated by fromColumnName to toColumnName. It is
 * assumed that checks such as the existence of fromColumnName and the non prior
 * existence of toColumnName are done.
 *
 * @param fromColumnName 
 * @param toColumnName 
 */
void Table::renameColumn(string fromColumnName, string toColumnName)
{
    logger.log("Table::renameColumn");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (columns[columnCounter] == fromColumnName)
        {
            columns[columnCounter] = toColumnName;
            break;
        }
    }
    return;
}

/**
 * @brief Function prints the first few rows of the table. If the table contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Table::print()
{
    logger.log("Table::print");
    uint count = min((long long)PRINT_COUNT, this->rowCount);

    //print headings
    this->writeRow(this->columns, cout);

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < count; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, cout);
    }
    printRowCount(this->rowCount);
}



/**
 * @brief This function returns one row of the table using the cursor object. It
 * returns an empty row is all rows have been read.
 *
 * @param cursor 
 * @return vector<int> 
 */
void Table::getNextPage(Cursor *cursor)
{
    logger.log("Table::getNext");

        if (cursor->pageIndex < this->blockCount - 1)
        {
            cursor->nextPage(cursor->pageIndex+1);
        }
}



/**
 * @brief called when EXPORT command is invoked to move source file to "data"
 * folder.
 *
 */
void Table::makePermanent()
{
    logger.log("Table::makePermanent");
    if(!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->tableName + ".csv";
    ofstream fout(newSourceFile, ios::out);

    //print headings
    this->writeRow(this->columns, fout);

    Cursor cursor(this->tableName, 0);
    vector<int> row;
    for (int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        row = cursor.getNext();
        this->writeRow(row, fout);
    }
    fout.close();
}

/**
 * @brief Function to check if table is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Table::isPermanent()
{
    logger.log("Table::isPermanent");
    if (this->sourceFileName == "../data/" + this->tableName + ".csv")
    return true;
    return false;
}

/**
 * @brief The unload function removes the table from the database by deleting
 * all temporary files created as part of this table
 *
 */
void Table::unload(){
    logger.log("Table::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
        bufferManager.deleteFile(this->tableName, pageCounter);
    if (!isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
}

/**
 * @brief Function that returns a cursor that reads rows from this table
 * 
 * @return Cursor 
 */
Cursor Table::getCursor()
{
    logger.log("Table::getCursor");
    Cursor cursor(this->tableName, 0);
    return cursor;
}
/**
 * @brief Function that returns the index of column indicated by columnName
 * 
 * @param columnName 
 * @return int 
 */
int Table::getColumnIndex(string columnName)
{
    logger.log("Table::getColumnIndex");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (this->columns[columnCounter] == columnName)
            return columnCounter;
    }
}


void Table::makePartition(int page,int seq,vector<vector<int>> rows)
{
    logger.log("Table::makePartition");
    // Cursor cursor(this->tableName, 0);
    this->rowsPerPartitionCount[page][seq] = (int)rows.size();
    bufferManager.writePage(this->tableName, page, seq, rows, rows.size());
}

void Table::sort()
{
    Table tableA = *tableCatalogue.getTable(parsedQuery.sortRelationName);
    vector<string> resultantColumns(tableA.columns);
    Table *table = new Table(parsedQuery.sortResultRelationName, resultantColumns);
    Table *temptable = new Table(parsedQuery.sortResultRelationName+"temp", resultantColumns);
    int colID = tableA.getColumnIndex(parsedQuery.sortColumnName);
    // Cursor cursorA = tableA.getCursor();
    // vector<int> rowA = cursorA.getNext();
    vector<int> rowB;
    rowB.reserve(table->columnCount);
    // tableCatalogue.insertTable(table);
    // struct compare *cmp;
    // cmp->col = colID;
    // cmp->ord = 0;
    // if(parsedQuery.sortingStrategy == ASC){
    //     cmp->ord = 1;
    // }
    auto compare = [&](vector<int> a, vector<int> b) { 
        if(parsedQuery.sortingStrategy == ASC){
            return a[colID] > b[colID]; 
        }
        return a[colID] < b[colID]; 
    };
    int elementsInPage = 0;
    vector<int> row;
    priority_queue<vector<int>,vector<vector<int>>, decltype(compare)> pqA(compare);
    // priority_queue<vector<int>,vector<vector<int>>, compare> pqA;
    vector<vector<int>>a;
    // sort all pages of table
    for(int pageCounter=0;pageCounter<tableA.blockCount;pageCounter++)
    {
        vector<vector<int>>pageRead;
        Cursor cursor(tableA.tableName,pageCounter);
        elementsInPage=0;
        while(elementsInPage<tableA.rowsPerBlockCount[pageCounter])
        {
            row = cursor.getNext();
            pqA.push(row);
            elementsInPage++;
        }
        while(!pqA.empty())
        {
            row = pqA.top();
            temptable->writeRow<int>(row);
            // pageRead.push_back(row);
            pqA.pop();
        }
        // bufferManager.writePage(temptable->tableName,pageCounter,pageRead);
    }
    temptable->blockify();
    bufferManager.clearPool();
    cout<<temptable->rowsPerBlockCount[0]<<endl;
    for(int pageCounter=0;pageCounter<temptable->blockCount-1;pageCounter++)
    {
        cout<<"PAGEA LOADED"<<endl;
        for(int pageCounter2 = pageCounter+1;pageCounter2<temptable->blockCount;pageCounter2++)
        {
            priority_queue<vector<int>,vector<vector<int>>, decltype(compare)> pq(compare);
            Cursor cursor_1(temptable->tableName,pageCounter);
            Cursor cursor_2(temptable->tableName,pageCounter2);
            vector<vector<int>> page1,page2;
            int page1_count=0,page2_count=0;
            // Read both pages
            // while(page1_count<temptable->rowsPerBlockCount[pageCounter])
            // {
            //     cout<<"reading from page1"<<endl;
            //     pq.push(cursor_1.getNext());
            //     page1_count++;
            // }
            // while(page2_count<temptable->rowsPerBlockCount[pageCounter2])
            // {
            //     cout<<"reading from page2"<<endl;
            //     pq.push(cursor_2.getNext());
            //     page2_count++;
            // }
            // while(!pq.empty() and page1_count--)
            // {
            //     cout<<"writing to page1"<<endl;
            //     row = pq.top();
            //     pq.pop();
            //     page1.push_back(row);
            // }
            // while(!pq.empty() and page2_count--)
            // {
            //     cout<<"writing to page2"<<endl;
            //     row = pq.top();
            //     pq.pop();
            //     page2.push_back(row);
            // }
            // bufferManager.writePage(temptable->tableName,pageCounter,page1);
            // bufferManager.writePage(temptable->tableName,pageCounter2,page2);
        }
    }
    // bufferManager.clearPool();
    // Cursor tmp = temptable->getCursor();
    // vector<int>tmprow = tmp.getNext();
    // // tmprow = tmp.getNext();
    // while(!tmprow.empty()){
    //     table->writeRow(tmprow);
    //     tmprow = tmp.getNext();
    // }
    // if(table->blockify())
    // tableCatalogue.insertTable(table);
    // else{
    //     cout<<"Empty Table"<<endl;
    //     table->unload();
    //     delete table;
    // }
    return;
}