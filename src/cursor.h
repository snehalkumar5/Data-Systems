#include"bufferManager.h"
/**
 * @brief The cursor is an important component of the system. To read from a
 * table, you need to initialize a cursor. The cursor reads rows from a page one
 * at a time.
 *
 */
class Cursor{
    public:
    Page page;
    int pageIndex;
    string tableName;
    string matrixName;
    int pagePointer;
    bool isSparse = false;

    public:
    Cursor(string tableName, int pageIndex);
    Cursor(string matrixName, int pageIndex, bool isSparse);
    Cursor(string matrixName, int pageIndex, int sequence);
    vector<int> getNext();
    void nextPage(int pageIndex);
    void nextPage(int pageIndex, bool sparse);
};