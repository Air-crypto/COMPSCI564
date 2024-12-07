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
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

    Status status;

    // Validate attribute metadata and fetch schema details
    AttrDesc projDesc[projCnt];
    for (int i = 0; i < projCnt; ++i) {
        status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, projDesc[i]);
        if (status != OK) {
            cerr << "Error: Unable to fetch metadata for attribute " 
                 << projNames[i].attrName << " in relation " 
                 << projNames[i].relName << endl;
            return status;
        }
    }

    // Determine record length for the result relation
    int resultRecLen = 0;
    for (int i = 0; i < projCnt; ++i) {
        if (projDesc[i].attrLen <= 0) {
            cerr << "Error: Invalid attribute length for attribute " 
                 << projDesc[i].attrName << ": " << projDesc[i].attrLen << endl;
            return ATTRTYPEMISMATCH;
        }
        resultRecLen += projDesc[i].attrLen;
    }

    // If attr is NULL, perform an unconditional scan
    if (attr == NULL) {
        return ScanSelect(result, projCnt, projDesc, NULL, op, NULL, resultRecLen);
    }

    // Convert attrValue to the appropriate type
    char *convertedValue = NULL;
    switch (attr->attrType) {
        case INTEGER: {
            int intValue = atoi(attrValue);
            convertedValue = (char*)&intValue;
            break;
        }
        case FLOAT: {
            float floatValue = atof(attrValue);
            convertedValue = (char*)&floatValue;
            break;
        }
        case STRING:
            convertedValue = (char*)attrValue; // No conversion needed
            break;
        default:
            cerr << "Error: Unsupported attribute type in QU_Select." << endl;
            return ATTRTYPEMISMATCH;
    }

    // Call ScanSelect to perform the operation
    return ScanSelect(result, projCnt, projDesc, 
                      (AttrDesc*)attr, op, convertedValue, resultRecLen);
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

Status status;

    // Validate record length
    if (reclen <= 0) {
        cerr << "Error: Invalid record length for projection: " << reclen << endl;
        return INVALIDRECLEN;
    }

    // Validate attribute metadata
    for (int i = 0; i < projCnt; ++i) {
        if (projNames[i].attrLen <= 0) {
            cerr << "Error: Invalid attribute length for attribute " 
                 << projNames[i].attrName << ": " << projNames[i].attrLen << endl;
            return ATTRTYPEMISMATCH;
        }
    }

    // Initialize HeapFileScan for the input relation
    HeapFileScan scan(projNames[0].relName, status);
    if (status != OK) {
        cerr << "Error: Unable to initialize scan on relation " << projNames[0].relName << endl;
        return status;
    }

    // Set the filter condition if specified
    if (attrDesc != NULL) {
        status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, 
                                static_cast<Datatype>(attrDesc->attrType), filter, op);
        if (status != OK) {
            cerr << "Error: Unable to start scan on relation " << projNames[0].relName << endl;
            return status;
        }
    }

    // Open the result file for inserting projected tuples
    InsertFileScan resultFile(result, status);
    if (status != OK) {
        cerr << "Error: Unable to open result file for insertion." << endl;
        return status;
    }

    // Allocate a reusable buffer for projection
    char *projData = new (nothrow) char[reclen];
    if (!projData) {
        cerr << "Error: Memory allocation failed for projection buffer." << endl;
        return INSUFMEM;
    }

    // Iterate through the records
    RID rid;
    Record rec;
    while (scan.scanNext(rid) == OK) {
        // Fetch the record using the current RID
        status = scan.getRecord(rec);
        if (status != OK) {
            cerr << "Error: Unable to fetch record during scan." << endl;
            delete[] projData; // Cleanup before returning
            return status;
        }

        // Project attributes into the reusable buffer
        int offset = 0;
        for (int i = 0; i < projCnt; ++i) {
            memcpy(projData + offset,
                   reinterpret_cast<char*>(rec.data) + projNames[i].attrOffset,
                   projNames[i].attrLen);
            offset += projNames[i].attrLen;
        }

        // Create a record structure for the projected tuple
        Record projRec;
        projRec.data = projData;
        projRec.length = reclen;

        // Insert the projected record into the result file
        status = resultFile.insertRecord(projRec, rid);
        if (status != OK) {
            cerr << "Error: Unable to insert record into result file." << endl;
            delete[] projData; // Cleanup before returning
            return status;
        }
    }

    // End the scan
    status = scan.endScan();
    if (status != OK) {
        cerr << "Error: Unable to end scan on relation " << projNames[0].relName << endl;
        delete[] projData; // Cleanup before returning
        return status;
    }

    // Free the reusable buffer
    delete[] projData;
    return OK;
}