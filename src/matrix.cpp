#include "global.h"

/**
 * @brief Construct a new Matrix:: Matrix object
 *
 */
Matrix::Matrix()
{
    logger.log("Matrix::Matrix");
}

/**
 * @brief Construct a new Matrix:: Matrix object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param matrixName 
 */
Matrix::Matrix(string matrixName)
{
    logger.log("Matrix::Matrix");
    this->sourceFileName = "../data/" + matrixName + ".csv";
    this->matrixName = matrixName;
}

/**
 * @brief Construct a new Matrix:: Matrix object used when an assignment command
 * is encountered. To create the matrix object both the matrix name and the
 * columns the matrix holds should be specified.
 *
 * @param matrixName 
 * @param columns 
 */
Matrix::Matrix(string matrixName, vector<string> columns)
{
    logger.log("Matrix::Matrix");
    this->sourceFileName = "../data/temp/" + matrixName + ".csv";
    this->matrixName = matrixName;
    this->columns = columns;
    this->columnCount = columns.size();
    this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * columnCount));
    this->maxColumnsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * columnCount));
    this->writeRow<string>(columns);
}

/**
 * @brief The load function is used when the LOAD MATRIX command is encountered. It
 * reads data from the source file, splits it into blocks and updates matrix
 * statistics.
 *
 * @return true if the matrix has been successfully loaded 
 * @return false if an error occurred 
 */
bool Matrix::load()
{
    logger.log("Matrix::load");
    fstream fin(this->sourceFileName, ios::in);
    string line;
    if (getline(fin, line))
    {
        fin.close();
        if (this->extractColumnNames(line))
            if(this->isSparse())
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
bool Matrix::extractColumnNames(string firstLine)
{
    logger.log("Matrix::extractColumnNames");
    string word,line;
    fstream f(this->sourceFileName, ios::in);
    int flag=0,count=0;
    stringstream s(firstLine);
    
    while(getline(s, word, ','))
    {
        word.erase(std::remove_if(word.begin(), word.end(), ::isspace), word.end());
        this->columns.emplace_back(word);
    }
    this->columnCount = this->columns.size(); // N
    this->rowCount = this->columns.size(); // N
    this->k = floor(sqrt((float) ((BLOCK_SIZE*1000) / (sizeof(int))))); 
    this->maxColumnsPerBlock = min(this->columnCount,(uint)(this->k)); // n
    this->maxRowsPerBlock = this->maxColumnsPerBlock; // m
    this->submatrixcount = ceil((float)this->columnCount/this->maxColumnsPerBlock);
    // cout<<"maxrows"<<this->maxRowsPerBlock<<endl;
    return true;
}

/**
 * @brief This function checks the number of zeroes in the matrix. If count>=60%
 * it is sparse matrix
 *
 * @return true if found to be sparse
 * @return false otherwise
 */
bool Matrix::isSparse()
{
    logger.log("Matrix::isSparse");
    int sparsecnt=0;
    fstream fin(this->sourceFileName, ios::in);
    string line, word,start_line;
    while (getline(fin, line))
    {
        stringstream s(line);
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (!getline(s, word, ','))
                return false;
            sparsecnt += (stoi(word)==0);
        }
    }
    // cout<<"sparsecount"<<sparsecnt<<endl;
    if ((float)sparsecnt/(this->columnCount*this->columnCount) >= 0.6)
    {
        this->sparse = true;
    }
    this->sparsecount = sparsecnt;
    fin.close();
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. 
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Matrix::blockify()
{
    logger.log("Matrix::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    ifstream ff(this->sourceFileName, ios::in);
    string line, word,start_line;
    int pageCounter = 0;
    int columnCounter = 0;
    int rowCounter = 0;
    if(this->sparse)
    {
        // vector<pair<pair<int,int>,int>>sparseMat;
        vector<vector<int>>sparseMat;
        fstream fsp(this->sourceFileName, ios::in);
        rowCounter=0;
        int elementCounter=0;
        while (getline(fsp, line))
        {
            stringstream s(line);
            for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
            {
                if (!getline(s, word, ','))
                    return false;
                if(elementCounter == (BLOCK_SIZE*1000)/sizeof(int))
                {
                    bufferManager.writePage(this->matrixName,this->blockCount,this->sparse,sparseMat);
                    sparseMat.clear();
                    this->rowsPerBlockCount.emplace_back(elementCounter);
                    elementCounter=0;
                    this->blockCount++;
                }
                if(stoi(word)!=0)
                {
                    vector<int>tuplee {rowCounter,columnCounter,stoi(word)};
                    // sparseMat.push_back({{rowCounter,columnCounter},stoi(word)});
                    sparseMat.push_back(tuplee);
                    elementCounter++;
                }
            }
            rowCounter++;
        }
        if(!sparseMat.empty())
        {
            bufferManager.writePage(this->matrixName,this->blockCount,this->sparse,sparseMat);
            sparseMat.clear();
            this->rowsPerBlockCount.emplace_back(elementCounter);
            elementCounter=0;
            this->blockCount++;
        }
        return true;
    }
    else{
        for(int i=0;i<this->rowCount;i+=this->maxRowsPerBlock)
        {
            getline(fin, start_line);
            stringstream start(start_line);
            for(int j=0;j<this->columnCount;j+=this->maxColumnsPerBlock)
            {
                ifstream ff(this->sourceFileName, ios::in);
                int maxrows = min(this->rowCount-i,this->maxRowsPerBlock);
                int maxcols = min((long long)this->columnCount-j,this->maxColumnsPerBlock);
                vector<int> row(maxcols, 0);
                vector<vector<int>> rowsInPage(maxrows, row);
                string cur_line;
                rowCounter=0;
                while(rowCounter<i)
                {
                    getline(ff,cur_line);
                    rowCounter++;
                }
                for(int sub_row=0;sub_row<maxrows;sub_row++)
                {
                    getline(ff,cur_line);
                    stringstream tmp(cur_line);
                    columnCounter = 0;
                    while(columnCounter<j)
                    {
                        getline(tmp,word,',');
                        columnCounter++;
                    }
                    for(int sub_col = 0;sub_col<maxcols;sub_col++)
                    {
                        if(!getline(tmp, word, ','))
                            return false;
                        row[sub_col] = stoi(word);
                        rowsInPage[sub_row][sub_col] = row[sub_col];
                        // cout<<"row"<<sub_row<<"col"<<sub_col<<"element"<<rowsInPage[sub_row][sub_col]<<endl;
                    }
                }
                pageCounter++;
                bufferManager.writePage(this->matrixName, this->blockCount, rowsInPage);
                this->rowsPerBlockCount.emplace_back(maxrows);
                this->colsPerBlockCount.emplace_back(maxcols);
                this->blockCount++;
            }
        }
        return true;
    }
}


/**
 * @brief Function prints the first few rows of the matrix. If the matrix contains
 * more rows than PRINT_COUNT, exactly PRINT_COUNT rows are printed, else all
 * the rows are printed.
 *
 */
void Matrix::print()
{
    logger.log("Matrix::print");
    uint count = min((long long)PRINT_COUNT, this->rowCount);
    int pageCounter=0;
    if(this->sparse)
    {
        vector<int> row;
        Cursor cursor(this->matrixName,pageCounter,this->sparse);
        row = cursor.getNext(); 
        int elementsInPage = 0;
        for(int i=0;i<count;i++)
        {
            for(int j=0;j<this->columnCount;j++)
            {
                if(row.empty() && pageCounter<this->blockCount)
                {
                    cursor.nextPage(++pageCounter,this->sparse);
                    elementsInPage=0;
                    row = cursor.getNext();
                }
                if(!row.empty() && row[0] == i and row[1] == j)
                {
                    cout<<row[2];
                    row.clear();
                    if(++elementsInPage < this->rowsPerBlockCount[pageCounter])
                        row = cursor.getNext();
                }
                else{
                    cout<<0;
                }
                if(j<this->columnCount-1)
                {
                    cout<<",";
                }
            }
            cout<<endl;
        }
    }
    else{
        vector<int> row;
        for(int i=0;i<count;i++)
        {
            for(int j=0;j<this->submatrixcount;j++)
            {
                int page_id = (i/this->maxRowsPerBlock)*(ceil((float)this->columnCount/this->maxColumnsPerBlock))+j;
                // cout<<endl<<"PAGE INDEX"<<page_id<<endl;
                Cursor cursor(this->matrixName,page_id,this->sparse);
                int l=0;
                while(l<=(i%this->maxRowsPerBlock))
                {
                    // logger.log("Inprint");
                    row = cursor.getNext(); 
                    l++;
                }
                // logger.log("writeprint");
                this->writeRow(row,cout);
                if(j<this->submatrixcount-1)
                {
                    cout<<",";
                }
            }
            cout<<endl;
        }
    }
    printRowCount(this->rowCount);
}



/**
 * @brief This function returns one row of the matrix using the cursor object. It
 * returns an empty row is all rows have been read.
 *
 * @param cursor 
 * @return vector<int> 
 */
void Matrix::getNextPage(Cursor *cursor)
{
    logger.log("Matrix::getNext");
    // cout<<" pgindex "<<cursor->pageIndex<<" blocks "<<this->blockCount<<endl;
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
void Matrix::makePermanent()
{
    logger.log("Matrix::makePermanent");
    if(!this->isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
    string newSourceFile = "../data/" + this->matrixName + ".csv";
    ofstream fout(newSourceFile, ios::out);
    vector<int> row;
    if(this->sparse)
    {
        int pageCounter=0;
        Cursor cursor(this->matrixName,pageCounter,this->sparse);
        row = cursor.getNext(); 
        int elementsInPage = 0;
        for(int i=0;i<this->rowCount;i++)
        {
            for(int j=0;j<this->columnCount;j++)
            {
                if(row.empty() && pageCounter<this->blockCount)
                {
                    cursor.nextPage(++pageCounter,this->sparse);
                    elementsInPage=0;
                    row = cursor.getNext();
                }
                if(!row.empty() && row[0] == i and row[1] == j)
                {
                    fout<<row[2];
                    row.clear();
                    if(++elementsInPage < this->rowsPerBlockCount[pageCounter])
                        row = cursor.getNext();
                }
                else{
                    fout<<0;
                }
                if(j<this->columnCount-1)
                {
                    fout<<",";
                }
            }
            fout<<endl;
        }
    }
    else{
        for(int i=0;i<this->rowCount;i++)
        {
            for(int j=0;j<this->submatrixcount;j++)
            {
                int page_id = (i/maxRowsPerBlock)*(this->submatrixcount)+j;
                Cursor cursor(this->matrixName,page_id,this->sparse);
                int l=0;
                while(l<=(i%this->maxRowsPerBlock))
                {
                    row = cursor.getNext(); 
                    l++;
                }
                this->writeRow(row,fout);
                if(j<this->submatrixcount-1)
                {
                    fout<<",";
                }
            }
            fout<<"\n";
        }
    }
    fout.close();
}

/**
 * @brief Function to check if matrix is already exported
 *
 * @return true if exported
 * @return false otherwise
 */
bool Matrix::isPermanent()
{
    logger.log("Matrix::isPermanent");
    if (this->sourceFileName == "../data/" + this->matrixName + ".csv")
    return true;
    return false;
}

/**
 * @brief The unload function removes the matrix from the database by deleting
 * all temporary files created as part of this matrix
 *
 */
void Matrix::unload(){
    logger.log("Matrix::~unload");
    for (int pageCounter = 0; pageCounter < this->blockCount; pageCounter++)
        bufferManager.deleteFile(this->matrixName, pageCounter);
    if (!isPermanent())
        bufferManager.deleteFile(this->sourceFileName);
}

/**
 * @brief Function that returns a cursor that reads rows from this matrix
 * 
 * @return Cursor 
 */
Cursor Matrix::getCursor()
{
    logger.log("Matrix::getCursor");
    Cursor cursor(this->matrixName, 0);
    return cursor;
}
/**
 * @brief Function that returns the index of column indicated by columnName
 * 
 * @param columnName 
 * @return int 
 */
int Matrix::getColumnIndex(string columnName)
{
    logger.log("Matrix::getColumnIndex");
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (this->columns[columnCounter] == columnName)
            return columnCounter;
    }
}
int Matrix::get_pageid(int row, int col)
{
    return (row)*(ceil((float)this->columnCount/this->maxColumnsPerBlock))+col;
}

/**
 * @brief Function tranposes the matrix
 *
 */
void Matrix::transpose()
{
    logger.log("Matrix::transpose");
    if(this->sparse)
    {
        int elementsInPage = 0;
        vector<int> row;
        for(int pageCounter=0;pageCounter<this->blockCount;pageCounter++)
        {
            vector<vector<int>>pageRead;
            Cursor cursor(this->matrixName,pageCounter,this->sparse);
            elementsInPage=0;
            while(elementsInPage<this->rowsPerBlockCount[pageCounter])
            {
                row = cursor.getNext();
                swap(row[0],row[1]);
                pageRead.push_back(row);
                elementsInPage++;
            }
            bufferManager.writePage(this->matrixName,pageCounter,this->sparse,pageRead);
        }

        if(this->blockCount==1)
        {
            for(int pageCounter = 0; pageCounter<this->blockCount; pageCounter++)
            {
                priority_queue<vector<int>,vector<vector<int>>, greater<vector<int>>> pq;
                Cursor cursor_1(this->matrixName,pageCounter,this->sparse);
                int page1_count=0;
                vector<vector<int>> page1;
                while(page1_count<this->rowsPerBlockCount[pageCounter])
                {
                    pq.push(cursor_1.getNext());
                    page1_count++;
                }
                while(!pq.empty() and page1_count--)
                {
                    row = pq.top();
                    pq.pop();
                    page1.push_back(row);
                }
                bufferManager.writePage(this->matrixName,pageCounter,this->sparse,page1);
            }
        }

        for(int pageCounter=0;pageCounter<this->blockCount-1;pageCounter++)
        {
            for(int pageCounter2 = pageCounter+1;pageCounter2<this->blockCount;pageCounter2++)
            {
                priority_queue<vector<int>,vector<vector<int>>, greater<vector<int>>> pq;
                Cursor cursor_1(this->matrixName,pageCounter,this->sparse);
                Cursor cursor_2(this->matrixName,pageCounter2,this->sparse);
                vector<vector<int>> page1,page2;
                int page1_count=0,page2_count=0;
                // Read both pages
                while(page1_count<this->rowsPerBlockCount[pageCounter])
                {
                    pq.push(cursor_1.getNext());
                    page1_count++;
                }
                while(page2_count<this->rowsPerBlockCount[pageCounter2])
                {
                    pq.push(cursor_2.getNext());
                    page2_count++;
                }
                while(!pq.empty() and page1_count--)
                {
                    row = pq.top();
                    pq.pop();
                    page1.push_back(row);
                }
                while(!pq.empty() and page2_count--)
                {
                    row = pq.top();
                    pq.pop();
                    page2.push_back(row);
                }
                bufferManager.writePage(this->matrixName,pageCounter,this->sparse,page1);
                bufferManager.writePage(this->matrixName,pageCounter2,this->sparse,page2);
            }
        }
        bufferManager.clearPool();
    }
    else{
        vector<int> row;
        for(int i=0;i<this->submatrixcount;i++)
        {
            for(int j=0;j<this->submatrixcount;j++)
            {
                vector<vector<int>>grid,grid1;
                int page_id = this->get_pageid(i,j);
                int swap_page_id = this->get_pageid(j,i);
                if(i > j)
                    continue;
                else if(i==j)
                {
                    Cursor cursor(this->matrixName,page_id,this->sparse);
                    int l=i*this->maxRowsPerBlock;
                    while(l<this->rowCount && (l-i*this->maxRowsPerBlock)<this->maxRowsPerBlock)
                    {
                        grid.push_back(cursor.getNext());
                        l++;
                    }
                    vector<vector<int>> temp (grid[0].size(), vector<int> (grid.size()));
                    for(int x=0;x<grid.size();x++)
                    {
                        for(int y=0;y<grid[0].size();y++)
                        {
                            temp[y][x] = grid[x][y];
                        }
                    }
                    bufferManager.writePage(this->matrixName,page_id,temp);
                }
                else{
                    Cursor cursor(this->matrixName,page_id,this->sparse);
                    int l=i*this->maxRowsPerBlock,r=j*this->maxRowsPerBlock;
                    // logger.log("First page");
                    while(l<this->rowCount && (l-i*this->maxRowsPerBlock)<this->maxRowsPerBlock)
                    {
                        grid.push_back(cursor.getNext());
                        l++;
                    }
                    // logger.log("Second page");
                    Cursor cursor_swap(this->matrixName,swap_page_id,this->sparse);
                    while(r<this->rowCount && (r-j*this->maxRowsPerBlock)<this->maxRowsPerBlock)
                    {
                        grid1.push_back(cursor_swap.getNext());
                        r++;
                    }
                    // logger.log("Loaded both pages");
                    vector<vector<int>> temp (grid1[0].size(), vector<int> (grid1.size()));
                    // tranpose grids of both pages
                    for(int x=0;x<grid.size();x++)
                    {
                        for(int y=0;y<grid[0].size();y++)
                        {
                            swap(grid1[y][x],grid[x][y]);
                        }
                    }
                    bufferManager.writePage(this->matrixName,page_id,grid);
                    bufferManager.writePage(this->matrixName,swap_page_id,grid1);
                }
            }
        }
    }
    bufferManager.clearPool();
    printRowCount(this->rowCount);
}