#include"global.h"

bool semanticParse(){
    logger.log("semanticParse");
    switch(parsedQuery.queryType){
        case CLEAR: return semanticParseCLEAR();
        case CROSS: return semanticParseCROSS();
        case DISTINCT: return semanticParseDISTINCT();
        case EXPORT: return semanticParseEXPORT();
        case EXPORTMATRIX: return semanticParseEXPORTMATRIX();
        case INDEX: return semanticParseINDEX();
        case JOIN: return semanticParseJOIN();
        case LIST: return semanticParseLIST();
        case LOAD: return semanticParseLOAD();
        case LOADMATRIX: return semanticParseLOADMATRIX();
        case PRINT: return semanticParsePRINT();
        case PRINTMATRIX: return semanticParsePRINTMATRIX();
        case PROJECTION: return semanticParsePROJECTION();
        case RENAME: return semanticParseRENAME();
        case SELECTION: return semanticParseSELECTION();
        case SORT: return semanticParseSORT();
        case SOURCE: return semanticParseSOURCE();
        case TRANSPOSE: return semanticParseTRANSPOSE();
        case GROUPBY: return semanticParseGROUPBY();
        default: cout<<"SEMANTIC ERROR"<<endl;
    }

    return false;
}