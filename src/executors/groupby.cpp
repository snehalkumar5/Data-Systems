#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- GROUP BY grouping_attr FROM relation_name RETURN aggregate(<attribute>)
 */
bool syntacticParseGROUPBY()
{
    logger.log("syntacticParseGROUPBY");
    if(tokenizedQuery.size() != 9 || tokenizedQuery[5] != "FROM" || tokenizedQuery[7] != "RETURN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = GROUPBY;
    parsedQuery.groupbyResultRelationName = tokenizedQuery[0];
    parsedQuery.groupbyGroupingColumnName = tokenizedQuery[4];
    parsedQuery.groupbyRelationName = tokenizedQuery[6];
    
    string groupbyAggregate = tokenizedQuery[8];
    string aggrOperator = "";
    int pos = 0;
    for(int i = 0; i < groupbyAggregate.size(); i++)
    {
        if(i == groupbyAggregate.size()-1 && groupbyAggregate[i] == ')')
        {
            parsedQuery.groupbyAggregateColumnName = groupbyAggregate.substr(pos+1);
            parsedQuery.groupbyAggregateColumnName.pop_back();
        }
        else if(groupbyAggregate[i]=='(')
        {
            aggrOperator = groupbyAggregate.substr(0,i);
            pos = i;
        }
    }
    parsedQuery.groupbyResultColumnName = aggrOperator+parsedQuery.groupbyAggregateColumnName;
    
    if (aggrOperator == "MAX")
        parsedQuery.groupbyAggregateOperator = MAX;
    else if (aggrOperator == "MIN")
        parsedQuery.groupbyAggregateOperator = MIN;
    else if (aggrOperator == "SUM")
        parsedQuery.groupbyAggregateOperator = SUM;
    else if (aggrOperator == "AVG")
        parsedQuery.groupbyAggregateOperator = AVG;
    else
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseGROUPBY()
{
    logger.log("semanticParseGROUPBY");

    if (tableCatalogue.isTable(parsedQuery.groupbyResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupbyGroupingColumnName, parsedQuery.groupbyRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.groupbyAggregateColumnName, parsedQuery.groupbyRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    return true;
}
pair<int,int> evaluateGroupOp(pair<int,int> value1, int value2, AggregateOperator op)
{
    switch (op)
    {
    case MAX:
        return {max(value1.first, value2),value1.second};
    case MIN:
        return {min(value1.first, value2),value1.second};
    case SUM:
        return {(value1.first + value2),value1.second};
    case AVG:
        value1.second++;
        return {(value1.first + value2),value1.second};
    default:
        return {-1,value1.second};
    }
}
void executeGROUPBY()
{
    logger.log("executeGROUPBY");
    Table table = *tableCatalogue.getTable(parsedQuery.groupbyRelationName);
    vector<string> resultantColumns = {parsedQuery.groupbyGroupingColumnName, parsedQuery.groupbyResultColumnName};
    vector<vector<int>> resultantRows;
    Table *resultTable = new Table(parsedQuery.groupbyResultRelationName, resultantColumns);
    int resultantRowCount = 0, resultantPageIndex = 0;
    int groupID = table.getColumnIndex(parsedQuery.groupbyGroupingColumnName);
    int aggID = table.getColumnIndex(parsedQuery.groupbyAggregateColumnName);
    map<int,pair<int,int>> mp;
    for(int block = 0; block<table.blockCount; block++){
        Cursor cursor = Cursor(parsedQuery.groupbyRelationName,block);
        vector<int> row;
        row = cursor.getNext();
        while(!row.empty())
        {
            if(mp.find(row[groupID])==mp.end())
            {
                mp[row[groupID]] = {row[aggID],1};
            }
            else{
                mp[row[groupID]] = evaluateGroupOp(mp[row[groupID]], row[aggID], parsedQuery.groupbyAggregateOperator);
            }
            row = cursor.getNext();
        }
        resultantPageIndex = block;
    }
    for(auto val : mp)
    {
        if(parsedQuery.groupbyAggregateOperator == AVG)
        {
            vector<int>addRow = {val.first, val.second.first/val.second.second};
            resultTable->writeRow<int>(addRow);
        }
        else{
            vector<int>addRow = {val.first, val.second.first};
            resultTable->writeRow<int>(addRow);
        }
    }
    if(resultTable->blockify())
    tableCatalogue.insertTable(resultTable);
    else{
        cout<<"Empty Table"<<endl;
        resultTable->unload();
        delete resultTable;
    }
    cout<<"BLOCK ACCESSES: "<<BLOCK_ACCESSES<<endl;
    return;
}