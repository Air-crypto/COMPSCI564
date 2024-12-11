#include "heapfile.h"
#include "error.h"

const Status createHeapFile(const string fileName)
{
    File* file;
    Status status;
    FileHdrPage* hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page* newPage;

    // Try to open file first
    status = db.openFile(fileName, file);
    if (status == OK)
    {
        // File exists
        return FILEEXISTS;
    }

    // Create new file
    status = db.createFile(fileName);
    if (status != OK)
        return status;

    // Open the newly created file
    status = db.openFile(fileName, file);
    if (status != OK)
        return status;

    // Allocate header page
    status = bufMgr->allocPage(file, hdrPageNo, (Page*&)hdrPage);
    if (status != OK)
    {
        db.closeFile(file);
        return status;
    }

    // Initialize header page
    memset(hdrPage->fileName, 0, MAXNAMESIZE);
    strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE - 1);
    hdrPage->firstPage = -1;
    hdrPage->lastPage = -1;
    hdrPage->pageCnt = 1;    // Header page itself
    hdrPage->recCnt = 0;     // No records yet

    // Allocate first data page
    status = bufMgr->allocPage(file, newPageNo, newPage);
    if (status != OK)
    {
        bufMgr->unPinPage(file, hdrPageNo, true);
        db.closeFile(file);
        return status;
    }

    // Initialize the data page
    newPage->init(newPageNo);

    // Update header page to point to first data page
    hdrPage->firstPage = newPageNo;
    hdrPage->lastPage = newPageNo;
    hdrPage->pageCnt++;

    // Unpin both pages
    status = bufMgr->unPinPage(file, hdrPageNo, true);
    if (status != OK)
    {
        bufMgr->unPinPage(file, newPageNo, true);
        db.closeFile(file);
        return status;
    }

    status = bufMgr->unPinPage(file, newPageNo, true);
    if (status != OK)
    {
        db.closeFile(file);
        return status;
    }

    return db.closeFile(file);
}

const Status destroyHeapFile(const string fileName)
{
    return db.destroyFile(fileName);
}

HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status status;
    
    // Open the file
    status = db.openFile(fileName, filePtr);
    if (status != OK)
    {
        returnStatus = status;
        return;
    }

    // Get header page number
    status = filePtr->getFirstPage(headerPageNo);
    if (status != OK)
    {
        db.closeFile(filePtr);
        returnStatus = status;
        return;
    }

    // Read header page
    status = bufMgr->readPage(filePtr, headerPageNo, (Page*&)headerPage);
    if (status != OK)
    {
        db.closeFile(filePtr);
        returnStatus = status;
        return;
    }

    hdrDirtyFlag = false;

    // If there's a first page, read it
    if (headerPage->firstPage != -1)
    {
        status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
        if (status != OK)
        {
            bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
            db.closeFile(filePtr);
            returnStatus = status;
            return;
        }
        curPageNo = headerPage->firstPage;
        curDirtyFlag = false;
    }
    else
    {
        curPage = NULL;
        curPageNo = -1;
        curDirtyFlag = false;
    }
    
    curRec = NULLRID;
    returnStatus = OK;
}

HeapFile::~HeapFile()
{
    Status status;

    // Unpin current data page if any
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
            cerr << "Error in HeapFile destructor: unpin of data page failed" << endl;
    }

    // Unpin header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK)
        cerr << "Error in HeapFile destructor: unpin of header page failed" << endl;

    // Close the file
    status = db.closeFile(filePtr);
    if (status != OK)
        cerr << "Error in HeapFile destructor: close of file failed" << endl;
}

const int HeapFile::getRecCnt() const
{
    return headerPage->recCnt;
}

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    if (rid.pageNo < 0 || rid.slotNo < 0)
        return BADRID;

    // Check if record is on current page
    if (curPage != NULL && rid.pageNo == curPageNo)
    {
        status = curPage->getRecord(rid, rec);
        if (status == OK)
            curRec = rid;
        return status;
    }

    // Need to read a different page
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
            return status;
    }

    // Read requested page
    status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
    if (status != OK)
    {
        curPage = NULL;
        return status;
    }

    curPageNo = rid.pageNo;
    curDirtyFlag = false;

    status = curPage->getRecord(rid, rec);
    if (status == OK)
        curRec = rid;

    return status;
}

HeapFileScan::HeapFileScan(const string & name, Status & status) 
    : HeapFile(name, status)
{
    filter = NULL;
}

HeapFileScan::~HeapFileScan()
{
    Status status;
    
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
        {
            cerr << "Error in HeapFileScan destructor: failed to unpin page" << endl;
        }
        curPage = NULL;
        curPageNo = -1;
        curDirtyFlag = false;
    }
}

const Status HeapFileScan::startScan(const int offset_,
                                   const int length_,
                                   const Datatype type_,
                                   const char* filter_,
                                   const Operator op_)
{
    if (!filter_)
    {
        filter = NULL;
        return OK;
    }

    if (offset_ < 0 || length_ < 1 ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)) ||
        (type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

const Status HeapFileScan::endScan()
{
    Status status;
    
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
            return status;
        curPage = NULL;
        curPageNo = -1;
        curDirtyFlag = false;
        curRec = NULLRID;
    }
    
    return OK;
}

const Status HeapFileScan::markScan()
{
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;

    if (markedPageNo != curPageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
        }

        status = bufMgr->readPage(filePtr, markedPageNo, curPage);
        if (status != OK)
            return status;

        curPageNo = markedPageNo;
        curDirtyFlag = false;
    }

    curRec = markedRec;
    return OK;
}

const Status HeapFileScan::scanNext(RID& outRid)
{
    Status status;
    Record rec;
    RID nextRid;
    
    while (true)
    {
        // If no current page, start from first page
        if (curPage == NULL)
        {
            if (headerPage->firstPage == -1)
                return FILEEOF;
                
            status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
            if (status != OK)
                return status;
                
            curPageNo = headerPage->firstPage;
            curDirtyFlag = false;
            curRec = NULLRID;
        }

        // Get next valid record
        if (curRec.pageNo == -1 && curRec.slotNo == -1)
        {
            // First record on page
            status = curPage->firstRecord(curRec);
        }
        else
        {
            // Next record on current page
            status = curPage->nextRecord(curRec, nextRid);
            if (status == OK)
                curRec = nextRid;
        }

        // If error or no more records on current page
        if (status != OK)
        {
            int nextPageNo;
            status = curPage->getNextPage(nextPageNo);
            
            if (status != OK || nextPageNo == -1)
                return FILEEOF;

            // Unpin current page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;

            // Read next page
            status = bufMgr->readPage(filePtr, nextPageNo, curPage);
            if (status != OK)
            {
                curPage = NULL;
                return status;
            }

            curPageNo = nextPageNo;
            curDirtyFlag = false;
            curRec = NULLRID;
            continue;
        }

        // Try to get the record
        status = curPage->getRecord(curRec, rec);
        if (status != OK)
        {
            // Skip invalid records
            RID nextRid;
            status = curPage->nextRecord(curRec, nextRid);
            if (status == OK)
                curRec = nextRid;
            continue;
        }

        // Check if record matches filter criteria
        if (!filter || matchRec(rec))
        {
            outRid = curRec;
            return OK;
        }
    }
}

const Status HeapFileScan::getRecord(Record & rec)
{
    if (curPage == NULL)
        return BADPAGENO;
    return curPage->getRecord(curRec, rec);
}

const Status HeapFileScan::deleteRecord()
{
    Status status;

    if (curPage == NULL)
        return BADPAGENO;

    // Delete the record
    status = curPage->deleteRecord(curRec);
    if (status != OK)
        return status;

    // Update counts and flags
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    curDirtyFlag = true;

    // Don't advance the cursor - let scanNext handle that
    // This ensures we don't skip records during a delete scan
    
    return OK;
}

const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    if (!filter)
        return true;

    if ((offset + length - 1) >= rec.length)
        return false;

    switch (type)
    {
        case INTEGER:
        {
            int iattr, ifltr;
            memcpy(&iattr, (char*)rec.data + offset, length);
            memcpy(&ifltr, filter, length);
            switch (op)
            {
                case LT:  return iattr < ifltr;
                case LTE: return iattr <= ifltr;
                case EQ:  return iattr == ifltr;
                case GTE: return iattr >= ifltr;
                case GT:  return iattr > ifltr;
                case NE:  return iattr != ifltr;
                default:  return false;
            }
        }

        case FLOAT:
        {
            float fattr, ffltr;
            memcpy(&fattr, (char*)rec.data + offset, length);
            memcpy(&ffltr, filter, length);
            switch (op)
            {
                case LT:  return fattr < ffltr;
                case LTE: return fattr <= ffltr;
                case EQ:  return fattr == ffltr;
                case GTE: return fattr >= ffltr;
                case GT:  return fattr > ffltr;
                case NE:  return fattr != ffltr;
                default:  return false;
            }
        }

        case STRING:
        {
            int cmp = strncmp((char*)rec.data + offset, filter, length);
            switch (op)
            {
                case LT:  return cmp < 0;
                case LTE: return cmp <= 0;
                case EQ:  return cmp == 0;
                case GTE: return cmp >= 0;
                case GT:  return cmp > 0;
                case NE:  return cmp != 0;
                default:  return false;
            }
        }

        default:
            return false;
    }
}

InsertFileScan::InsertFileScan(const string & name, Status & status)
    : HeapFile(name, status)
{
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK)
            cerr << "Error in InsertFileScan destructor" << endl;
        curPage = NULL;
        curPageNo = -1;
        curDirtyFlag = false;
    }
}

const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page* newPage;
    int newPageNo;
    Status status;
    RID rid;

    // Check record size
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED)
        return INVALIDRECLEN;

    // If no current page, set to last page
    if (curPage == NULL)
    {
        curPageNo = headerPage->lastPage;
        if (curPageNo == -1)
        {
            // Allocate first page
            status = bufMgr->allocPage(filePtr, curPageNo, curPage);
            if (status != OK)
                return status;

            curPage->init(curPageNo);
            
            headerPage->firstPage = curPageNo;
            headerPage->lastPage = curPageNo;
            headerPage->pageCnt++;
            hdrDirtyFlag = true;
            curDirtyFlag = false;
        }
        else
        {
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK)
                return status;
            curDirtyFlag = false;
        }
    }

    // Try to insert into current page
    status = curPage->insertRecord(rec, rid);
    if (status == OK)
    {
        outRid = rid;
        headerPage->recCnt++;
        hdrDirtyFlag = true;
        curDirtyFlag = true;
        return OK;
    }

    if (status != NOSPACE)
        return status;

    // Need new page
    status = bufMgr->allocPage(filePtr, newPageNo, newPage);
    if (status != OK)
        return status;

    newPage->init(newPageNo);

    status = curPage->setNextPage(newPageNo);
    if (status != OK)
    {
        bufMgr->unPinPage(filePtr, newPageNo, true);
        return status;
    }

    headerPage->lastPage = newPageNo;
    headerPage->pageCnt++;
    hdrDirtyFlag = true;

    status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
    if (status != OK)
    {
        bufMgr->unPinPage(filePtr, newPageNo, true);
        return status;
    }

    curPage = newPage;
    curPageNo = newPageNo;
    curDirtyFlag = false;

    status = curPage->insertRecord(rec, rid);
    if (status != OK)
        return status;

    outRid = rid;
    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curDirtyFlag = true;

    return OK;
}