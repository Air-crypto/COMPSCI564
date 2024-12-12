#include "catalog.h"
#include "query.h"
#include <cstdlib>  // For atoi and atof
#include <cstring>  // For strlen

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */
const Status QU_Delete(const string &relation,
                       const string &attrName,
                       const Operator op,
                       const Datatype type,
                       const char *attrValue)
{
    Status status;

    // If no attribute name is provided, delete all records
    if (attrName.empty()) {
        std::cout << "Doing QU_Delete" << std::endl;

        // Initialize HeapFileScan for the relation
        HeapFileScan scan(relation, status);
        if (status != OK) {
            std::cout << "Error: Unable to open relation '" << relation << "' for scanning." << std::endl;
            return status;
        }

        // Scan through all records and delete them
        RID rid;
        while (scan.scanNext(rid) == OK) {
            status = scan.deleteRecord();
            if (status != OK) {
                std::cout << "Error: Unable to delete record in relation '" << relation << "'" << std::endl;
                return status;
            }
        }

        // End the scan
        status = scan.endScan();
        if (status != OK) {
            std::cout << "Error: Unable to end scan on relation '" << relation << "'" << std::endl;
            return status;
        }

        return OK;
    }

    // If an attribute name is provided, delete based on the condition
    std::cout << "Doing QU_Delete" << std::endl;

    // Fetch attribute description from catalog
    AttrDesc attrDesc;
    status = attrCat->getInfo(relation.c_str(), attrName.c_str(), attrDesc);
    if (status != OK) {
        std::cout << "Error: Attribute '" << attrName << "' not found in relation '" << relation << "'" << std::endl;
        return status;
    }

    // Convert the attrValue to the correct type
    char *convertedValue = nullptr;
    switch (type) {
        case INTEGER: {
            int intValue = atoi(attrValue);
            convertedValue = new char[sizeof(int)];
            memcpy(convertedValue, &intValue, sizeof(int));
            break;
        }
        case FLOAT: {
            float floatValue = atof(attrValue);
            convertedValue = new char[sizeof(float)];
            memcpy(convertedValue, &floatValue, sizeof(float));
            break;
        }
        case STRING: {
            size_t len = strlen(attrValue) + 1; // +1 for null terminator
            convertedValue = new char[len];
            memcpy(convertedValue, attrValue, len);
            break;
        }
        default:
            std::cout << "Error: Unsupported attribute type" << std::endl;
            return ATTRTYPEMISMATCH;
    }

    // Initialize HeapFileScan for the relation
    HeapFileScan scan(relation, status);
    if (status != OK) {
        std::cout << "Error: Unable to open relation '" << relation << "' for scanning." << std::endl;
        delete[] convertedValue;
        return status;
    }

    // Start the scan using the filter on the attribute
    status = scan.startScan(attrDesc.attrOffset, attrDesc.attrLen,
                            static_cast<Datatype>(attrDesc.attrType),
                            convertedValue, op);
    if (status != OK) {
        std::cout << "Error: Unable to start scan on relation '" << relation << "'" << std::endl;
        delete[] convertedValue;
        return status;
    }

    // Delete matching records
    RID rid;
    while (scan.scanNext(rid) == OK) {
        status = scan.deleteRecord();
        if (status != OK) {
            std::cout << "Error: Unable to delete record in relation '" << relation << "'" << std::endl;
            delete[] convertedValue;
            return status;
        }
    }

    // Clean up and end the scan
    status = scan.endScan();
    if (status != OK) {
        std::cout << "Error: Unable to end scan on relation '" << relation << "'" << std::endl;
        delete[] convertedValue;
        return status;
    }

    // Clean up the allocated memory for converted value
    delete[] convertedValue;

    return OK;
}