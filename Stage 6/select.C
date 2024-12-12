#include "catalog.h"
#include "query.h"
#include <cmath>
#include <cstring>
#include <iostream>
#include <new>

using namespace std;

static bool applyFilter(const AttrDesc &attrDesc, const Operator op, const char *filterVal, const char *tupleVal) {
    switch (attrDesc.attrType) {
        case INTEGER: {
            int tupleInt;
            memcpy(&tupleInt, tupleVal, sizeof(int));
            int filterInt = atoi(filterVal);
            switch (op) {
                case EQ: return tupleInt == filterInt;
                case NE: return tupleInt != filterInt;
                case GT: return tupleInt > filterInt;
                case LT: return tupleInt < filterInt;
                case GTE: return tupleInt >= filterInt;  // Changed to GTE
                case LTE: return tupleInt <= filterInt;  // Changed to LTE
                default: return false;
            }
        }
        case FLOAT: {
            float tupleFloat;
            memcpy(&tupleFloat, tupleVal, sizeof(float));
            float filterFloat = atof(filterVal);
            switch (op) {
                case EQ: return fabs(tupleFloat - filterFloat) < 1e-6;
                case NE: return fabs(tupleFloat - filterFloat) > 1e-6;
                case GT: return tupleFloat > filterFloat;
                case LT: return tupleFloat < filterFloat;
                case GTE: return tupleFloat >= filterFloat;  // Changed to GTE
                case LTE: return tupleFloat <= filterFloat;  // Changed to LTE
                default: return false;
            }
        }
        case STRING: {
            char buffer[attrDesc.attrLen + 1];
            memcpy(buffer, tupleVal, attrDesc.attrLen);
            buffer[attrDesc.attrLen] = '\0';
            int cmp = strcmp(buffer, filterVal);
            switch (op) {
                case EQ: return (cmp == 0);
                case NE: return (cmp != 0);
                case GT: return (cmp > 0);
                case LT: return (cmp < 0);
                case GTE: return (cmp >= 0);  // Changed to GTE
                case LTE: return (cmp <= 0);  // Changed to LTE
                default: return false;
            }
        }
        default:
            return false;
    }
}

// Forward declaration
const Status ScanSelect(const string & result, 
                        const int projCnt, 
                        const AttrDesc projNames[],
                        const AttrDesc *attrDesc, 
                        const Operator op, 
                        const char *attrValue,
                        const int reclen);

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
    AttrDesc *attrDesc = nullptr;

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

    // Validate record length
    if (reclen <= 0) {
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

    // Determine the relation to scan (based on the first projected attribute)
    const string &scanRel = projNames[0].relName;

    // Initialize HeapFileScan for the input relation
    HeapFileScan scan(scanRel, status);
    if (status != OK) {
        cerr << "Error: Unable to initialize scan on relation " << scanRel << endl;
        return status;
    }

    bool manualFiltering = false;

    // If no attribute condition, do unconditional scan
    if (attrDesc == nullptr) {
        status = scan.startScan(0, 0, STRING, NULL, EQ);
        if (status != OK) {
            cerr << "Error: Unable to start unconditional scan on relation " << scanRel << endl;
            return status;
        }
    } else {
        cout << "Applying filter on " << attrDesc->attrName << endl;

        // We only directly apply EQ filters via startScan
        if (op == EQ) {
            // Convert filter based on type and startScan
            if (attrDesc->attrType == STRING) {
                // STRING filter
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, STRING, (char *)filter, EQ);
            } else if (attrDesc->attrType == FLOAT) {
                float floatFilter = atof(filter);
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, FLOAT, (char *)&floatFilter, EQ);
            } else if (attrDesc->attrType == INTEGER) {
                int intFilter = atoi(filter);
                status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, INTEGER, (char *)&intFilter, EQ);
            } else {
                cerr << "Error: Unsupported attribute type in filter." << endl;
                return ATTRTYPEMISMATCH;
            }
            if (status != OK) {
                cerr << "Error: Unable to start scan on relation " << scanRel << " with equality filter." << endl;
                return status;
            }
        } else {
            // For non-EQ operators, start an unconditional scan and do manual filtering
            status = scan.startScan(0, 0, STRING, NULL, EQ);
            if (status != OK) {
                cerr << "Error: Unable to start unconditional scan on relation " << scanRel << endl;
                return status;
            }
            manualFiltering = true;
        }
    }

    // Open the result file for inserting the projected tuples
    InsertFileScan resultRel(result, status);
    if (status != OK) {
        cerr << "Error: Unable to open result file for inserting records." << endl;
        return status;
    }

    // Allocate a reusable buffer for projected data
    char *projData = new (nothrow) char[reclen];
    if (!projData) {
        cerr << "Error: Insufficient memory to allocate projection buffer." << endl;
        return INSUFMEM;
    }

    RID rid;
    Record rec;
    while ((status = scan.scanNext(rid)) == OK) {
        status = scan.getRecord(rec);
        if (status != OK) {
            cerr << "Error: Unable to fetch record with RID: (" << rid.pageNo << ", " << rid.slotNo << ")" << endl;
            delete[] projData;
            return status;
        }

        bool match = true;
        if (manualFiltering && attrDesc != nullptr && filter != nullptr) {
            // Manually apply the filter
            const char* tupleVal = (char*)rec.data + attrDesc->attrOffset;
            match = applyFilter(*attrDesc, op, filter, tupleVal);
        }

        if (match) {
            // Project attributes into the buffer
            int offset = 0;
            for (int i = 0; i < projCnt; ++i) {
                memcpy(projData + offset,
                       (char*)rec.data + projNames[i].attrOffset,
                       projNames[i].attrLen);
                offset += projNames[i].attrLen;
            }

            Record projRec;
            projRec.data = projData;
            projRec.length = reclen;

            status = resultRel.insertRecord(projRec, rid);
            if (status != OK) {
                cerr << "Error: Unable to insert projected record." << endl;
                delete[] projData;
                return status;
            }
        }
    }

    if (status != FILEEOF) {
        cerr << "Error during scan: " << status << endl;
        delete[] projData;
        return status;
    }

    status = scan.endScan();
    if (status != OK) {
        cerr << "Error: Unable to end scan on relation " << scanRel << endl;
        delete[] projData;
        return status;
    }

    delete[] projData;
    return OK;
}
