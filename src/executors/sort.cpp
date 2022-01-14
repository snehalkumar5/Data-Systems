#include"global.h"
/**
 * @brief File contains method to process SORT commands.
 * 
 * syntax:
 * R <- SORT relation_name BY column_name IN sorting_order BUFFER buffer_size
 * R <- SORT relation_name BY column_name IN sorting_order
 * 
 * sorting_order = ASC | DESC 
 */
bool syntacticParseSORT(){
    logger.log("syntacticParseSORT");
    if(tokenizedQuery.size() == 10 && tokenizedQuery[4] == "BY" && tokenizedQuery[6] == "IN" && tokenizedQuery[8] == "BUFFER"){
        parsedQuery.queryType = SORT;
        parsedQuery.sortResultRelationName = tokenizedQuery[0];
        parsedQuery.sortRelationName = tokenizedQuery[3];
        parsedQuery.sortColumnName = tokenizedQuery[5];
        parsedQuery.sortBuffer = stoi(tokenizedQuery[9]);
        string sortingStrategy = tokenizedQuery[7];
        if(sortingStrategy == "ASC")
            parsedQuery.sortingStrategy = ASC;
        else if(sortingStrategy == "DESC")
            parsedQuery.sortingStrategy = DESC;
        else{
            cout<<"SYNTAX ERROR"<<endl;
            return false;
        }
        return true;
    }
    else if(tokenizedQuery.size() == 8 && tokenizedQuery[4] == "BY" && tokenizedQuery[6] == "IN"){
        parsedQuery.queryType = SORT;
        parsedQuery.sortResultRelationName = tokenizedQuery[0];
        parsedQuery.sortRelationName = tokenizedQuery[3];
        parsedQuery.sortColumnName = tokenizedQuery[5];
        string sortingStrategy = tokenizedQuery[7];
        if(sortingStrategy == "ASC")
            parsedQuery.sortingStrategy = ASC;
        else if(sortingStrategy == "DESC")
            parsedQuery.sortingStrategy = DESC;
        else{
            cout<<"SYNTAX ERROR"<<endl;
            return false;
        }
        return true;

    }
    else{
        cout<<"SYNTAX ERROR"<<endl;
        return false;
    }
}

bool semanticParseSORT(){
    logger.log("semanticParseSORT");

    if(tableCatalogue.isTable(parsedQuery.sortResultRelationName)){
        cout<<"SEMANTIC ERROR: Resultant relation already exists"<<endl;
        return false;
    }

    if(!tableCatalogue.isTable(parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Relation doesn't exist"<<endl;
        return false;
    }

    if(!tableCatalogue.isColumnFromTable(parsedQuery.sortColumnName, parsedQuery.sortRelationName)){
        cout<<"SEMANTIC ERROR: Column doesn't exist in relation"<<endl;
        return false;
    }

    return true;
}

void executeSORT(){
    logger.log("executeSORT");
    Table tableA = *tableCatalogue.getTable(parsedQuery.sortRelationName);
    vector<string> resultantColumns(tableA.columns);
    Table *table = new Table(parsedQuery.sortResultRelationName, resultantColumns);
    int colID = tableA.getColumnIndex(parsedQuery.sortColumnName);

    auto compare = [&](vector<int> a, vector<int> b) { 
        if(parsedQuery.sortingStrategy == ASC){
            return a[colID] > b[colID]; 
        }
        return a[colID] < b[colID]; 
    };
    auto compare1 = [&](pair<int,vector<int>> a, pair<int,vector<int>> b) { 
        auto f = a.second;
        auto s = b.second;
        if(parsedQuery.sortingStrategy == ASC){
            return f[colID] > s[colID]; 
        }
        return f[colID] < s[colID]; 
    };
    int M = parsedQuery.sortBuffer;
    int numchunks = ceil((float)(tableA.blockCount)/M);
    int chunksize = M;
    vector<int> row;
    // read chunk at a time
    for(int cknum = 0; cknum<numchunks;cknum++)
    {
        Table *temptable = new Table(parsedQuery.sortResultRelationName+"Chunk"+to_string(cknum), resultantColumns);
        // sort m blocks in each chunk
        priority_queue<vector<int>,vector<vector<int>>, decltype(compare)> pqA(compare);
        for(int bk = 0;bk<min(chunksize,(int)tableA.blockCount-(chunksize*cknum));bk++)
        {
            int pgid = (chunksize*cknum)+bk;
            int eles = 0;
            Cursor curs(tableA.tableName, pgid);
            while(eles<tableA.rowsPerBlockCount[pgid]){
                row = curs.getNext();
                pqA.push(row);
                eles++;
            }
        }
        while(!pqA.empty())
        {
            row = pqA.top();
            temptable->writeRow<int>(row);
            pqA.pop();
        }
        temptable->blockify();
        tableCatalogue.insertTable(temptable);
    }

    bufferManager.clearPool();
    // mergesort k chunks
    int inputbuf = M-1;
    vector<vector<int>>outputrows;
    vector<Cursor>crs;
    vector<vector<int>>buffers(numchunks, vector<int>(0));
    priority_queue<pair<int,vector<int>>,vector<pair<int,vector<int>>>, decltype(compare1)> pq(compare1);
    map<Page,pair<int,int>>pageread;
    map<int,Page>mp;
    for(int i=0;i<numchunks;i++)
    {   
        // auto curpage = Page (parsedQuery.sortResultRelationName+"Chunk"+to_string(i), 0);
        crs.emplace_back(Cursor(parsedQuery.sortResultRelationName+"Chunk"+to_string(i),0));
        // pageread[curpage] = {0,0};
        // buffers[i] = curpage.getRow(pageread[curpage].second++);
        buffers[i] = crs[i].getNext();
        // mp[i] = curpage;
        pq.push({i,buffers[i]});
    }
    int lastb = 0;
    int flag=1;
    while(!pq.empty()){
        auto tp = pq.top();
        lastb = tp.first;
        row = tp.second;
        pq.pop();
        if(row.empty()){
            break;
        }
        // auto tptble = tableCatalogue.getTable(parsedQuery.sortResultRelationName+"Chunk"+to_string(lastb));
        // if(pageread[mp[lastb]].second == tptble->rowsPerBlockCount[pageread[mp[lastb]].first]){
        //     Page curpage(parsedQuery.sortResultRelationName+"Chunk"+to_string(lastb), pageread[mp[lastb]].first++);
        //     pageread[curpage].second=0;
        //     buffers[lastb] = curpage.getRow(pageread[curpage].second++);
        // }
        buffers[lastb] = crs[lastb].getNext();
        if(!buffers[lastb].empty())
            pq.push({lastb,buffers[lastb]});
        outputrows.push_back(row);

        //  write to disk if overflow
        if(outputrows.size() == tableA.maxRowsPerBlock){
            table->writeRows<int>(outputrows);
            outputrows.clear();
        }
    }
    if(!outputrows.empty())
        table->writeRows<int>(outputrows);
    if(table->blockify())
    tableCatalogue.insertTable(table);
    else{
        cout<<"Empty Table"<<endl;
        table->unload();
        delete table;
    }
    return;
}
