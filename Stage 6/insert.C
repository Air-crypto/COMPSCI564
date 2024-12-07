#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// part 6
    Status status;

    // Fetch relation metadata
    RelDesc relDesc;
    status = relCat->getInfo(relation.c_str(), relDesc);
    if (status != OK) {
        cout << "Error: Relation " << relation << " not found in catalog." << endl;
        return status;
    }

    // Fetch attributes for the relation
    AttrDesc *attrs;
    int relAttrCnt;
    status = attrCat->getRelInfo(relation.c_str(), relAttrCnt, attrs);
    if (status != OK) {
        cout << "Error: Unable to fetch attributes for relation " << relation << endl;
        return status;
    }

    // Validate attribute count
    if (attrCnt != relAttrCnt) {
        cout << "Error: Attribute count mismatch for relation " << relation << endl;
        return ATTRNOTFOUND;
    }

    // Compute record length dynamically
    int recordLen = 0;
    for (int i = 0; i < relAttrCnt; ++i) {
        recordLen += attrs[i].attrLen;
    }

    // Allocate buffer for the new record
    char *record = new char[recordLen]();

    // Map input attributes to their correct offsets in the record
    for (int i = 0; i < attrCnt; ++i) {
        bool matched = false;
        for (int j = 0; j < relAttrCnt; ++j) {
            if (strcmp(attrList[i].attrName, attrs[j].attrName) == 0) {
                // Verify attribute types match
                if (attrList[i].attrType != attrs[j].attrType) {
                    cout << "Error: Type mismatch for attribute " << attrList[i].attrName << endl;
                    delete[] record;
                    return ATTRTYPEMISMATCH;
                }

                // Copy the attribute value to the correct offset
                memcpy(record + attrs[j].attrOffset, attrList[i].attrValue, attrs[j].attrLen);
                matched = true;
                break;
            }
        }
        if (!matched) {
            cout << "Error: Attribute " << attrList[i].attrName << " not found in relation schema." << endl;
            delete[] record;
            return ATTRNOTFOUND;
        }
    }

    // Insert the record using InsertFileScan
    InsertFileScan insertScan(relation, status);
    if (status != OK) {
        cout << "Error: Unable to open relation " << relation << " for insertion." << endl;
        delete[] record;
        return status;
    }

    Record rec;
    rec.data = record;
    rec.length = recordLen;

    RID rid;
    status = insertScan.insertRecord(rec, rid);
    if (status != OK) {
        cout << "Error: Unable to insert record into relation " << relation << endl;
    }

    // Clean up
    delete[] record;
    return status;
}