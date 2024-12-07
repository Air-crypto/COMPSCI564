#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
// part 6

    Status status;

    // Open the catalog to fetch attribute description
    AttrDesc attrDesc;
    status = attrCat->getInfo(relation.c_str(), attrName.c_str(), attrDesc);
    if (status != OK) {
        cout << "Error: Attribute " << attrName << " not found in relation " << relation << endl;
        return status;
    }

    // Convert the attrValue to the correct type
    char *convertedValue = nullptr;
    switch (type) {
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
            cout << "Error: Unsupported attribute type" << endl;
            return ATTRTYPEMISMATCH;
    }

    // Initialize the HeapFileScan for the relation
    HeapFileScan scan(relation, status);
    if (status != OK) {
        cout << "Error: Unable to open relation " << relation << " for scanning." << endl;
        return status;
    }

    // Set up the filter for scanning
    status = scan.startScan(attrDesc.attrOffset, attrDesc.attrLen, 
                            static_cast<Datatype>(attrDesc.attrType), 
                            convertedValue, op);

    if (status != OK) {
        cout << "Error: Unable to start scan on relation " << relation << endl;
        return status;
    }

    // Delete matching records
    RID rid;
    while (scan.scanNext(rid) == OK) {
        status = scan.deleteRecord();
        if (status != OK) {
            cout << "Error: Unable to delete record in relation " << relation << endl;
            return status;
        }
    }

    // End the scan
    status = scan.endScan();
    if (status != OK) {
        cout << "Error: Unable to end scan on relation " << relation << endl;
        return status;
    }

    return OK;
}


