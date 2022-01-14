#include"semanticParser.h"

void executeCommand();

void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeEXPORT();
void executeEXPORTMATRIX();
void executeINDEX();
void executeJOIN();
void executeLIST();
void executeLOAD();
void executeLOADMATRIX();
void executePRINT();
void executePRINTMATRIX();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeSOURCE();
void executeTRANSPOSE();
void executeGROUPBY();

bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
pair<int,int> evaluateGroupOp(pair<int,int> value1, int value2, AggregateOperator aggregateOperator);
void printRowCount(int rowCount);