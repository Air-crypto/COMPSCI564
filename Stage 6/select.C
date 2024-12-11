#include "catalog.h"
#include "query.h"

// forward declaration
const Status ScanSelect(const string & result, 
                        const int projCnt, 
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc, 
                        const Operator op, 
                        const char *filter,
                        const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
const Status QU_Select(const string & result, 
                       const int projCnt, 
                       const attrInfo projNames[],
                       const attrInfo *attr, 
                       const Operator op, 
                       const char *attrValue)
{
    cout << "Doing QU_Select " << endl;

    Status status;
    AttrDesc attrDescArray[projCnt];
    AttrDesc *attrDesc = nullptr;  // We will only allocate this if needed

    // Step 1: Fetch metadata for the projection attributes
    for (int i = 0; i < projCnt; ++i) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrDescArray[i]);
        if (status != OK) {
            cerr << "Error: Unable to fetch metadata for attribute " 
                 << projNames[i].attrName << " in relation " 
                 << projNames[i].relName << endl;
            return status;
        }
    }

    // Step 2: Calculate the record length for the result relation
    int resultRecLen = 0;
    for (int i = 0; i < projCnt; ++i) {
        resultRecLen += attrDescArray[i].attrLen;
    }

    // Step 3: If no condition is specified, perform an unconditional scan
    if (attr == NULL) {
        return ScanSelect(result, projCnt, attrDescArray, nullptr, op, nullptr, resultRecLen);
    } else {
        // Step 4: If condition is specified, fetch metadata for the condition attribute
        attrDesc = new AttrDesc;

        status = attrCat->getInfo(attr->relName, attr->attrName, *attrDesc);
        if (status != OK) {
            delete attrDesc;
            return status;
        }
    }

    // Step 5: Perform the scan with the filter condition
    return ScanSelect(result, projCnt, attrDescArray, attrDesc, op, attrValue, resultRecLen);
}

/*
 * Performs the scan and selection on the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
const Status ScanSelect(const string &result,
                        const int projCnt,
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc,
                        const Operator op,
                        const char *filter,
                        const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;

    // Step 1: Validate record length
    if (reclen <= 0) {
        return INVALIDRECLEN;
    }

    // Step 2: Validate attribute metadata
    for (int i = 0; i < projCnt; ++i) {
        if (projNames[i].attrLen <= 0) {
            cerr << "Error: Invalid attribute length for attribute " 
                 << projNames[i].attrName << ": " << projNames[i].attrLen << endl;
            return ATTRTYPEMISMATCH;
        }
    }

    // Step 3: Initialize HeapFileScan for the input relation
    HeapFileScan scan(projNames[0].relName, status);
    if (status != OK) {
        cerr << "Error: Unable to initialize scan on relation " << projNames[0].relName << endl;
        return status;
    }

    // Step 4: If a filter (WHERE clause) is specified, set the filter condition
    if (attrDesc != nullptr) {
        cout << "Applying filter on " << attrDesc->attrName << endl;

        int intFilter;
        float floatFilter;

        if (attrDesc->attrType == STRING) {
            // For STRING, use as-is
            status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, 
                                    STRING, filter, op);
        } 
        else if (attrDesc->attrType == FLOAT) {
            // Convert filter to FLOAT
            floatFilter = atof(filter);
            status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, 
                                    FLOAT, (char *)&floatFilter, op);
        } 
        else if (attrDesc->attrType == INTEGER) {
            // Convert filter to INTEGER
            intFilter = atoi(filter);  // Converts "12" to 12
            status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, 
                                    INTEGER, (char *)&intFilter, op);
        } 
        else {
            cerr << "Error: Unsupported attribute type in filter." << endl;
            return ATTRTYPEMISMATCH;
        }

        if (status != OK) {
            cerr << "Error: Unable to start scan on relation " << projNames[0].relName << endl;
            return status;
        }
    }

    // Step 5: Open the result file for inserting the projected tuples
    InsertFileScan resultRel(result, status);
    if (status != OK) {
        cerr << "Error: Unable to open result file for inserting records." << endl;
        return status;
    }

    // Step 6: Allocate a reusable buffer for projected data
    char *projData = new (nothrow) char[reclen];
    if (!projData) {
        cerr << "Error: Insufficient memory to allocate projection buffer." << endl;
        return INSUFMEM;
    }

    // Step 7: Iterate through the records in the relation
    RID rid;
    Record rec;
    while (scan.scanNext(rid) == OK) {
        status = scan.getRecord(rec);
        if (status != OK) {
            cerr << "Error: Unable to fetch record with RID: (" << rid.pageNo << ", " << rid.slotNo << ")" << endl;
            delete[] projData; // Cleanup before returning
            return status;
        }

        // Step 8: Project attributes into the reusable buffer
        int offset = 0;
        for (int i = 0; i < projCnt; ++i) {
            if (projNames[i].attrType == STRING) {
                memcpy(projData + offset, 
                       reinterpret_cast<char*>(rec.data) + projNames[i].attrOffset,
                       projNames[i].attrLen);
            }
            else if (projNames[i].attrType == FLOAT) {
                memcpy(projData + offset,
                       reinterpret_cast<char*>(rec.data) + projNames[i].attrOffset,
                       projNames[i].attrLen);  // Use proper casting
            }
            else if (projNames[i].attrType == INTEGER) {
                memcpy(projData + offset,
                       reinterpret_cast<char*>(rec.data) + projNames[i].attrOffset,
                       projNames[i].attrLen);  // Use proper casting
            }
            offset += projNames[i].attrLen;
        }

        // Step 9: Create a record structure for the projected tuple
        Record projRec;
        projRec.data = projData;
        projRec.length = reclen;

        // Step 10: Insert the projected record into the result file
        status = resultRel.insertRecord(projRec, rid);
        if (status != OK) {
            cerr << "Error: Unable to insert projected record." << endl;
            delete[] projData; // Cleanup before returning
            return status;
        }
    }

    // Step 11: End the scan
    status = scan.endScan();
    if (status != OK) {
        cerr << "Error: Unable to end scan on relation " << projNames[0].relName << endl;
        delete[] projData; // Cleanup before returning
        return status;
    }

    // Step 12: Free the reusable buffer
    delete[] projData;
    return OK;
}
