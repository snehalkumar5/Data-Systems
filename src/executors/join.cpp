#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- JOIN USING algorithm relation_name1, relation_name2 ON column_name1 bin_op column_name2 BUFFER buffer_size
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 bin_op column_name2
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if(tokenizedQuery.size() == 13 && tokenizedQuery[3] == "USING" && tokenizedQuery[7] == "ON" && tokenizedQuery[11] == "BUFFER")
    {
        parsedQuery.queryType = JOIN;
        parsedQuery.joinResultRelationName = tokenizedQuery[0];
        parsedQuery.joinFirstRelationName = tokenizedQuery[5];
        parsedQuery.joinSecondRelationName = tokenizedQuery[6];
        parsedQuery.joinFirstColumnName = tokenizedQuery[8];
        parsedQuery.joinSecondColumnName = tokenizedQuery[10];
        parsedQuery.joinBufferSize = stoi(tokenizedQuery[12]);
        
        string Algorithm = tokenizedQuery[4];
        if (Algorithm == "NESTED")
            parsedQuery.joinAlgorithm = NESTED;
        else if (Algorithm == "PARTHASH")
            parsedQuery.joinAlgorithm = PARTHASH;
        else
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }

        string binaryOperator = tokenizedQuery[9];
        if (binaryOperator == "<")
            parsedQuery.joinBinaryOperator = LESS_THAN;
        else if (binaryOperator == ">")
            parsedQuery.joinBinaryOperator = GREATER_THAN;
        else if (binaryOperator == ">=" || binaryOperator == "=>")
            parsedQuery.joinBinaryOperator = GEQ;
        else if (binaryOperator == "<=" || binaryOperator == "=<")
            parsedQuery.joinBinaryOperator = LEQ;
        else if (binaryOperator == "==")
            parsedQuery.joinBinaryOperator = EQUAL;
        else if (binaryOperator == "!=")
            parsedQuery.joinBinaryOperator = NOT_EQUAL;
        else
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        return true;
    }
    else if (tokenizedQuery.size() == 9 && tokenizedQuery[5] == "ON")
    {
        parsedQuery.queryType = JOIN;
        parsedQuery.joinResultRelationName = tokenizedQuery[0];
        parsedQuery.joinFirstRelationName = tokenizedQuery[3];
        parsedQuery.joinSecondRelationName = tokenizedQuery[4];
        parsedQuery.joinFirstColumnName = tokenizedQuery[6];
        parsedQuery.joinSecondColumnName = tokenizedQuery[8];

        string binaryOperator = tokenizedQuery[7];
        if (binaryOperator == "<")
            parsedQuery.joinBinaryOperator = LESS_THAN;
        else if (binaryOperator == ">")
            parsedQuery.joinBinaryOperator = GREATER_THAN;
        else if (binaryOperator == ">=" || binaryOperator == "=>")
            parsedQuery.joinBinaryOperator = GEQ;
        else if (binaryOperator == "<=" || binaryOperator == "=<")
            parsedQuery.joinBinaryOperator = LEQ;
        else if (binaryOperator == "==")
            parsedQuery.joinBinaryOperator = EQUAL;
        else if (binaryOperator == "!=")
            parsedQuery.joinBinaryOperator = NOT_EQUAL;
        else
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
        return true;
    }
    else {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    return true;
}

// int hash(int value)
// {
//     int hashval;
//     int BUCKETS = 100000000;
//     // int t = (value*value + value + 1)%BUCKETS;
//     return value%BUCKETS;
// }

void executeJOIN()
{
    logger.log("executeJOIN");
    Table tableA = *tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table tableB = *tableCatalogue.getTable(parsedQuery.joinSecondRelationName);
    vector<string> resultantColumns(tableA.columns);
    resultantColumns.insert(resultantColumns.end(), tableB.columns.begin(), tableB.columns.end());
    vector<vector<int>> resultantRows;
    Table *table = new Table(parsedQuery.joinResultRelationName, resultantColumns);
    int resultantRowCount = 0, resultantPageIndex = 0;
    int tableAcolID = tableA.getColumnIndex(parsedQuery.joinFirstColumnName);
    int tableBcolID = tableB.getColumnIndex(parsedQuery.joinSecondColumnName);
    int TOTALPARTITIONWRITES = 0;

    if (parsedQuery.joinAlgorithm == NESTED)
    {
        int br = 0;
        int buffersA = min(parsedQuery.joinBufferSize-2,(int) tableA.blockCount-br);
        for(br; br<tableA.blockCount; br+=buffersA)
        {
            vector<Cursor> crsA;
            for(int ba = 0; ba<buffersA; ba++){
                crsA.emplace_back(Cursor(parsedQuery.joinFirstRelationName, br + ba));
            }
            int bs = 0;
            int buffersB = min(2,(int) tableB.blockCount);
            // int buffersB = 1;
            for(bs; bs<tableB.blockCount; bs+=buffersB)
            {
                vector<Cursor> crsB;
                for(int bb = 0; bb<buffersB; bb++){
                    crsB.emplace_back(Cursor(parsedQuery.joinSecondRelationName, bs + bb));
                }
                // Cursor cursorA = Cursor(parsedQuery.joinFirstRelationName,br);
                vector<int> rowA;
                // rowA = cursorA.getNext();
                int cursoraint=0;
                for(auto cursorA:crsA)
                {
                    rowA = cursorA.getNext();
                    // for each row in br
                    int rowcountA=0;
                    while(rowcountA < tableA.rowsPerBlockCount[br+cursoraint])
                    // while(!rowA.empty())
                    {
                        // Cursor cursorB = Cursor(parsedQuery.joinSecondRelationName,bs);
                        int cursorbint =0;
                        for(auto cursorB:crsB)
                        {
                            // for each row in bs
                            int rowcountB=0;
                            vector<int> rowB;
                            rowB = cursorB.getNext();
                            while(rowcountB < tableB.rowsPerBlockCount[bs+cursorbint])
                            // while(!rowB.empty())
                            {
                                if (rowA[tableAcolID] == rowB[tableBcolID])
                                {
                                    vector<int> addRow(rowA);
                                    addRow.insert(addRow.end(),rowB.begin(),rowB.end());
                                    table->writeRow<int>(addRow);
                                    // resultantRows.emplace_back(addRow);
                                    // resultantRowCount++;
                                    // if(resultantRowCount == table->maxRowsPerBlock){
                                    //     resultantPageIndex++;
                                    //     bufferManager.writePage(parsedQuery.joinResultRelationName, resultantPageIndex, resultantRows, resultantRowCount);
                                    //     resultantRowCount = 0;
                                    //     resultantRows.clear();
                                    // }
                                    // table->writeRow(rowA);
                                }
                                rowB = cursorB.getNext();
                                rowcountB++;
                            }
                            cursorbint++;
                        }
                        rowA = cursorA.getNext();
                        rowcountA++;
                    }
                    cursoraint++;

                }
                buffersB = min(2,(int) tableB.blockCount-bs);

            }
            buffersA = min(parsedQuery.joinBufferSize-2,(int) tableA.blockCount-br);
        }
        if(table->blockify())
        tableCatalogue.insertTable(table);
        else{
            cout<<"Empty Table"<<endl;
            table->unload();
            delete table;
        }
        // bufferManager.writePage(parsedQuery.joinResultRelationName, resultantPageIndex, resultantRows, resultantRowCount);
    }
    else{
        // PARTHASH 
        int BUCKETS = 100000000;
        // partition R
        int M = parsedQuery.joinBufferSize-1;
        // vector<Cursor>crs;
        // for(int i=0;i<M;i++)
        // {
        //     crs.emplace_back(Cursor(parsedQuery.joinFirstRelationName,i));
        // }
        int maxRowsPerPage = table->maxRowsPerBlock;
        map<int,vector<vector<int>>>mpA;
        vector<vector<int>> v;
        v.clear();
        // vector<vector<vector<int>>>mpA(M, vector<vector<int>>(0,vector<int>(0)));
        // vector<vector<vector<int>>>mpA(M, v);
        // mpA.assign(M, vector<vector<int>>);
        vector<int>sequenceNumMapA(M,0);
        vector<int>visA(M,0);
        // cout<<tableA.rowsPerBlockCount[0]<<" "<<tableA.blockCount<<endl;
        vector<int>rowA;
        memset(tableA.rowsPerPartitionCount,0,sizeof(tableA.rowsPerPartitionCount));
        memset(tableB.rowsPerPartitionCount,0,sizeof(tableB.rowsPerPartitionCount));
        // tableB.rowsPerPartitionCount.clear();
        // tableA.rowsPerPartitionCount.assign(M,vector<int>(0));
        // tableB.rowsPerPartitionCount.assign(M,vector<int>(0));
        for(int br=0;br<tableA.blockCount;br++)
        {
            // cout<<"blockA"<<endl;
            Cursor cursorA = Cursor(parsedQuery.joinFirstRelationName, br);
            rowA = cursorA.getNext();
            for(int i=0;i<tableA.rowsPerBlockCount[br];i++)
            // while(!(rowA.empty()))
            {
                // cout<<"rowA"<<endl;
                int hashval = rowA[tableAcolID]%M;
                hashval = min(M-1, hashval);
                // overflow of buffer
                if(mpA[hashval].size() >= tableA.maxRowsPerBlock)
                {
                    // if(sequenceNumMapA[hashval] < 0)
                    // {
                    //     sequenceNumMapA[hashval] = 1;
                    // }
                    // cout<<"overflow"<<endl;
                    bufferManager.writePage(parsedQuery.joinFirstRelationName, hashval, sequenceNumMapA[hashval], mpA[hashval], (int)mpA[hashval].size());
                    BLOCK_ACCESSES++;
                    TOTALPARTITIONWRITES++;
                    // tableA.rowsPerPartitionCount[hashval].push_back((int)mpA[hashval].size());
                    tableA.rowsPerPartitionCount[hashval][sequenceNumMapA[hashval]] = (int)mpA[hashval].size();
                    mpA[hashval].clear();
                    sequenceNumMapA[hashval]++;
                }
                // Page page = bufferManager.getPage(parsedQuery.joinFirstRelationName, hashval);
                mpA[hashval].push_back(rowA);
                // if(!visA[hashval]){
                //     sequenceNumMapA[hashval]=0;
                //     visA[hashval] = 1;
                // }
                rowA = cursorA.getNext();
                // if(rowA.empty()) cout<<"empty row"<<endl;
            }
        }
        // partition S
        // map<int,vector<vector<int>>>mpB;
        // vector<vector<vector<int>>>mpB(M, vector<vector<int>>(0,vector<int>(0)));
        vector<vector<vector<int>>>mpB(M, v);
        vector<int>sequenceNumMapB(M,0);
        // vector<vector<vector<int>>> mpB(parsedQuery.joinBufferSize-1,vector<vector<int>>());
        vector<int>rowB;
        for(int bs=0;bs<tableB.blockCount;bs++)
        {
            Cursor cursorB = Cursor(parsedQuery.joinSecondRelationName, bs);
            rowB = cursorB.getNext();
            int rowBcount = 0;
            while(rowBcount < tableB.rowsPerBlockCount[bs])
            // while(!rowB.empty())
            {
                int hashval = rowB[tableBcolID]%M;
                hashval = min(M-1, hashval);
                if(mpB[hashval].size() >= tableB.maxRowsPerBlock)
                {
                    // if(sequenceNumMapB[hashval] < 0)
                    // {
                    //     sequenceNumMapB[hashval] = 0;
                    // }
                    tableB.makePartition(hashval,sequenceNumMapA[hashval],mpB[hashval]);
                    BLOCK_ACCESSES++;
                    TOTALPARTITIONWRITES++;
                    // bufferManager.writePage(parsedQuery.joinSecondRelationName, hashval, sequenceNumMapB[hashval], mpB[hashval], (int)mpB[hashval].size());
                    // tableB.rowsPerPartitionCount[hashval].push_back((int)mpB[hashval].size());
                    // tableB.rowsPerPartitionCount[hashval][sequenceNumMapB[hashval]] = (int)mpB[hashval].size();
                    mpB[hashval].clear();
                    sequenceNumMapB[hashval]++;
                }
                mpB[hashval].push_back(rowB);
                rowB = cursorB.getNext();
                rowBcount++;
            }
        }
        
        // join
        // for each hashvalue
        for(int hashA=0;hashA<M;hashA++)
        {
            // for all sequence pgsA in hashA
            for(int seqA = 0; seqA<sequenceNumMapA[hashA]; seqA++)
            {
                Cursor curpgA = Cursor(parsedQuery.joinFirstRelationName, hashA, seqA);
                // cout<<"seqA"<<endl;
                rowA = curpgA.getNext();
                // for each rowA
                for(int i=0;i<tableA.maxRowsPerBlock;i++)
                // while(!rowA.empty())
                {
                    // cout<<"rowApg"<<endl;
                    // for all sequence pgsB in hashA
                    for(int seqB=0;seqB<sequenceNumMapB[hashA];seqB++)
                    {
                        Cursor curpgB = Cursor(parsedQuery.joinSecondRelationName, hashA, seqB);
                        rowB = curpgB.getNext();
                        for(int j=0;j<tableB.maxRowsPerBlock;j++)
                        // while (!rowB.empty())
                        {
                            // match found
                            if(rowA[tableAcolID] == rowB[tableBcolID])
                            {
                                vector<int>addRow(rowA);
                                addRow.insert(addRow.end(),rowB.begin(),rowB.end());
                                table->writeRow<int>(addRow);
                            }
                            rowB = curpgB.getNext();
                        }
                    }
                    // check pageA with mapB
                    for(auto rB: mpB[hashA])
                    {
                        // match found
                        if(rowA[tableAcolID] == rB[tableBcolID])
                        {
                            vector<int>addRow(rowA);
                            addRow.insert(addRow.end(),rB.begin(),rB.end());
                            table->writeRow<int>(addRow);
                        }
                    }
                    rowA = curpgA.getNext();
                }
            }

            if(mpA.find(hashA)!=mpA.end())
            {
                // cout<<"IN HASHMAP A"<<endl;
                for(auto rowA:mpA[hashA])
                {
                    for(int seqB=0;seqB<sequenceNumMapB[hashA];seqB++)
                    {
                        Cursor curpgB = Cursor(parsedQuery.joinSecondRelationName, hashA, seqB);
                        rowB = curpgB.getNext();
                        for(int i=0;i<tableB.maxRowsPerBlock;i++)
                        // while (!rowB.empty())
                        {
                            // match found
                            if(rowA[tableAcolID] == rowB[tableBcolID])
                            {
                                vector<int>addRow(rowA);
                                addRow.insert(addRow.end(),rowB.begin(),rowB.end());
                                table->writeRow<int>(addRow);
                            }
                            rowB = curpgB.getNext();
                        }
                    }
                    for(auto rB: mpB[hashA])
                    {
                        // match found
                        if(rowA[tableAcolID] == rB[tableBcolID])
                        {
                            vector<int>addRow(rowA);
                            addRow.insert(addRow.end(),rB.begin(),rB.end());
                            table->writeRow<int>(addRow);
                        }
                    }
                }
            }
        }
        if(table->blockify())
        tableCatalogue.insertTable(table);
        else{
            cout<<"Empty Table"<<endl;
            table->unload();
            delete table;
        }

        cout<<"BLOCK ACCESSES FOR PARTITION WRITES: "<<TOTALPARTITIONWRITES<<endl;
    }
    cout<<"TOTAL BLOCK ACCESSES: "<<BLOCK_ACCESSES<<endl;
    
    return;
}