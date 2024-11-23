/**
 * @file buf.C
 * 
 * Team Members:
 *  - Anna Huang (9084475889)
 *  - Arihan Yadav (9085801976)
 *  - Jimmy He (9083706573)
 * 
 * Description: Manages the buffer pool through functions such as page retrieval, allocation, flushing, and page replacement using the clock algorithm. *
 */
#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
	cerr << "At line " << __LINE__ << ":" << endl << "  "; \
	cerr << "This condition should hold: " #c << endl; \
	exit(1); \
} \
}

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
	numBufs = bufs;

	bufTable = new BufDesc[bufs];
	memset(bufTable, 0, bufs * sizeof(BufDesc));
	for (int i = 0; i < bufs; i++) 
	{
		bufTable[i].frameNo = i;
		bufTable[i].valid = false;
	}

	bufPool = new Page[bufs];
	memset(bufPool, 0, bufs * sizeof(Page));

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
	hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

	clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

	// flush out all unwritten pages
	for (int i = 0; i < numBufs; i++) 
	{
		BufDesc* tmpbuf = &bufTable[i];
		if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
			cout << "flushing page " << tmpbuf->pageNo
				<< " from frame " << i << endl;
#endif

			tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
		}
	}

// Allocates a free frame using the clock replacement algorithm.
// The function attempts to find an unpinned frame in the buffer pool that can be reused. If a dirty
// page is found, it is written back to disk before the frame is reused.
// Input:
//   frame - A reference to an integer where the allocated frame number will be stored
// Output:
//   Returns a status:
//      - OK: A buffer frame was successfully allocated and is now available for use
//      - BUFFEREXCEEDED: All buffers are currently pinned (in use or cannot be allocated)
const Status BufMgr::allocBuf(int &frame) {
    // Iterate over all buffer frames
    for (int i = 0; i < numBufs; ++i) {
        // Advance the clock hand to the next frame
        clockHand = (clockHand + 1) % numBufs;
        // Get the buffer descriptor for the current frame
        BufDesc &desc = bufTable[clockHand];

        // Check if the current frame is unpinned and available
        if (desc.pinCnt == 0) {
            if (desc.valid && desc.dirty) {
                // If the page in the frame is dirty, write it back to disk
                Status status = desc.file->writePage(desc.pageNo, &bufPool[clockHand]);
                if (status != OK) 
                    return UNIXERR;
                
                // Clear the dirty flag
                desc.dirty = false;  
            }

            // If the page was valid, remove from the hash table
            if (desc.valid) {
                Status status = hashTable->remove(desc.file, desc.pageNo);
                if (status != OK) 
                    return HASHTBLERROR;
            }

            // Clear the buffer descriptor to mark the frame as free
            desc.Clear();

            // Set the allocated frame number to the current clock hand position
            frame = clockHand;  
            return OK;
        }
    }

    // If no unpinned frames are available, return an error
    return BUFFEREXCEEDED;
}

// Reads a page into the buffer pool if it is not already in the pool.
// If the page is in the pool, returns a pointer and marks referenced.
// Input: 
//   file - The file from which the page is to be read.
//   PageNo - The page number to read.
//   page - A reference to a pointer where the page will be stored upon successful read.
// Output:
//   Returns a status code:
//      - OK: The page was successfully read or is already in the pool.
//      - HASHNOTFOUND: The page was not found in the hash table.
//      - HASHTBLERROR: Error occurred when inserting into or looking up the hash table.
//      - UNIXERR: An error occurred while reading the page from the disk.
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page) {
    int frameNo;

    // Lookup the page in hash table to check if page is in buffer pool
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status == HASHNOTFOUND) {
        // allocate a new buffer frame if page is not in the buffer pool
        status = allocBuf(frameNo);
        if (status != OK) 
            return status;

        // Read the page from disk into the allocated buffer
        status = file->readPage(PageNo, &bufPool[frameNo]);
        if (status != OK) 
            return UNIXERR; 
        // Update the buffer descriptor with file and page
        bufTable[frameNo].Set(file, PageNo);

        // Insert the page into the hash table
        status = hashTable->insert(file, PageNo, frameNo);
        if (status != OK) 
            return HASHTBLERROR; 

        // Return a pointer to the page 
        page = &bufPool[frameNo];
        return OK;
    } 
    else if (status == OK) {
        // Mark pages already in buffer pool as referenced, increase pin count
        BufDesc &desc = bufTable[frameNo];
        desc.refbit = true;
        desc.pinCnt++;
        page = &bufPool[frameNo]; 
        return OK;
    }
    return HASHTBLERROR;
}

// Unpins a page from the buffer pool, decreasing its pin count.
// If the page is dirty, it will be marked as dirty before unpinning.
// Input:
//   file - The file containing the page to unpin.
//   PageNo - The page number to unpin.
//   dirty - A boolean indicating whether the page should be marked dirty upon unpinning.
// Output:
//   Returns a status code:
//   - OK: Page was successfully unpinned and the pin count was decremented.
//   - HASHNOTFOUND: Page is not found in the buffer pool.
//   - PAGENOTPINNED: Page is not currently pinned (pin count is 0 or negative).
const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) {
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    
    // Page not found in the buffer pool
    if (status != OK) 
        return HASHNOTFOUND;  

    BufDesc &desc = bufTable[frameNo];
    if (desc.pinCnt <= 0) 
        return PAGENOTPINNED;

    // Decrease the pin count and mark the page as dirty if requested.
    desc.pinCnt--;

    // If the page should be marked as dirty, set the dirty flag
    if (dirty) 
        desc.dirty = true;

    return OK;
}

// Allocates a new page on disk, inserts it into the buffer pool, and returns a pointer to the page in the pool.
// Input:
//   file   - The file in which the page is to be allocated.
//   PageNo - A reference to an integer where the newly allocated page number will be stored.
//   page   - A reference to a pointer where the allocated page in the buffer pool will be stored.
// Output:
//   Returns a status code:
//   - OK: Page was successfully allocated on disk and inserted into the buffer pool.
//   - UNIXERR: Error allocating the page on disk.
//   - HASHTBLERROR: Error inserting the page into the hash table.
const Status BufMgr::allocPage(File* file, int& PageNo, Page*& page) {
    // Allocate a new page on disk
    Status status = file->allocatePage(PageNo);
    if (status != OK) return UNIXERR;  

    // Allocate a buffer frame to store the page
    int frameNo;
    status = allocBuf(frameNo);
    if (status != OK) 
        return status; 

    // Initialize the buffer frame for the new page
    bufTable[frameNo].Set(file, PageNo);
    bufPool[frameNo].init(PageNo);

    // Insert the page into the hash table
    status = hashTable->insert(file, PageNo, frameNo);
    if (status != OK) 
        return HASHTBLERROR;

    // Return a pointer to the newly allocated page in the buffer pool
    page = &bufPool[frameNo];
    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
	// see if it is in the buffer pool
	Status status = OK;
	int frameNo = 0;
	status = hashTable->lookup(file, pageNo, frameNo);
	if (status == OK)
	{
		// clear the page
		bufTable[frameNo].Clear();
	}
	status = hashTable->remove(file, pageNo);

	// deallocate it in the file
	return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
	Status status;

	for (int i = 0; i < numBufs; i++) {
		BufDesc* tmpbuf = &(bufTable[i]);
		if (tmpbuf->valid == true && tmpbuf->file == file) {

			if (tmpbuf->pinCnt > 0)
				return PAGEPINNED;

			if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
				cout << "flushing page " << tmpbuf->pageNo
					<< " from frame " << i << endl;
#endif
				if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
								&(bufPool[i]))) != OK)
					return status;

				tmpbuf->dirty = false;
			}

			hashTable->remove(file,tmpbuf->pageNo);

			tmpbuf->file = NULL;
			tmpbuf->pageNo = -1;
			tmpbuf->valid = false;
		}

		else if (tmpbuf->valid == false && tmpbuf->file == file)
			return BADBUFFER;
	}

	return OK;
}

void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
