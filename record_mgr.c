#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "record_mgr.h"
#include "tables.h"
#include "expr.h"

/***************************************************************
 * Function Name: initRecordManager
 *
 * Description: initial record manager
 *
 * Parameters: void *mgmtData
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      2016/03/12      Xiaoliang Wu                Complete
 *
***************************************************************/

RC initRecordManager (void *mgmtData) {
    printf("----------------------------- Initial Record Manager ---------------------------------\n");
    return RC_OK;
}

/***************************************************************
 * Function Name: shutdownRecordManager
 *
 * Description: shutdown record manager
 *
 * Parameters: NULL
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      2016/03/12      Xiaoliang Wu                Complete
 *
***************************************************************/

RC shutdownRecordManager () {
    printf("-------------------------------- shutdown record manager ---------------------------------\n");
    return RC_OK;
}

/***************************************************************
 * Function Name: createTable
 *
 * Description: Creating a table should create the underlying page file and store information about the schema, free-space, ... and so on in the Table Information pages
 *
 * Parameters: char *name, Schema *schema
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      03/19/16        Xiaoliang Wu                Complete.
 *      03/22/16        Xiaoliang Wu                Change int convert to string method.
 *
***************************************************************/

RC createTable (char *name, Schema *schema) {
    RC RC_flag;
    SM_FileHandle fh;
    SM_PageHandle ph;
    int fileMetadataSize;
    int recordSize;
    int slotSize;
    int recordNum;
    char *input;
    char *charSchema;

    int i, j, k;

    // create file
    RC_flag = createPageFile(name);

    if (RC_flag != RC_OK) {
        return RC_flag;
    }

    // open file
    RC_flag = openPageFile(name, &fh);

    if (RC_flag != RC_OK) {
        return RC_flag;
    }

    // get file metadata size
    fileMetadataSize = strlen(serializeSchema(schema)) + 4 * sizeof(int);

    if (fileMetadataSize % PAGE_SIZE == 0) {
        fileMetadataSize = fileMetadataSize / PAGE_SIZE;
    }
    else {
        fileMetadataSize = fileMetadataSize / PAGE_SIZE + 1;
    }

    // get metadata and store in file
    recordSize = (getRecordSize(schema) / (slotSize));
    slotSize = 256;
    recordNum = 0;

    input = (char *)calloc(PAGE_SIZE, sizeof(char));

    memcpy(input, &fileMetadataSize, sizeof(int));
    memcpy(input + sizeof(int), &recordSize, sizeof(int));
    memcpy(input + 2 * sizeof(int), &slotSize, sizeof(int));
    memcpy(input + 3 * sizeof(int), &recordNum, sizeof(int));

    charSchema = serializeSchema(schema);

    RC_flag = ensureCapacity(fileMetadataSize, &fh);

    if (RC_flag != RC_OK) {
        RC_flag = closePageFile(&fh);
        return RC_flag;
    }

    if (strlen(charSchema) < PAGE_SIZE - 4 * sizeof(int)) {
        memcpy(input + 4 * sizeof(int), charSchema, strlen(charSchema));
        RC_flag = writeBlock(0, &fh, input);
        free(input);
    } else {
        memcpy(input + 4 * sizeof(int), charSchema, PAGE_SIZE - 4 * sizeof(int));
        RC_flag = writeBlock(0, &fh, input);
        free(input);
        for (i = 1; i < fileMetadataSize; ++i) {
            input = (char *)calloc(PAGE_SIZE, sizeof(char));
            if (i == fileMetadataSize - 1) {
                memcpy(input, charSchema + i * PAGE_SIZE, strlen(charSchema + i * PAGE_SIZE));
            } else {
                memcpy(input, charSchema + i * PAGE_SIZE, PAGE_SIZE);
            }
            RC_flag = writeBlock(i, &fh, input);
            free(input);
        }
    }

    if (RC_flag != RC_OK) {
        RC_flag = closePageFile(&fh);
        return RC_flag;
    }

    RC_flag = addPageMetadataBlock(&fh);

    if (RC_flag != RC_OK) {
        RC_flag = closePageFile(&fh);
        return RC_flag;
    }

    RC_flag = closePageFile(&fh);
    return RC_flag;
}

/***************************************************************
 * Function Name: openTable
 *
 * Description: open a table
 *
 * Parameters: RM_TableData *rel, char *name
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      03/23/16        Xiaoliang Wu                Complete.
 *
***************************************************************/

RC openTable (RM_TableData *rel, char *name) {
    RC RC_flag;
    SM_FileHandle fh;
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *h = MAKE_PAGE_HANDLE();

    Schema *schema;
    int numAttr;
    int *typeLength, *keyAttrs;
    int keySize;
    DataType * dataType;
    char **attrNames;

    int fileMetadataSize;
    char * charSchema;
    int i, j, k;
    char * flag;
    char * flag2;
    char * temp;

    // open file and initial buffer pool
    RC_flag = openPageFile(name, &fh);
    if (RC_flag != RC_OK) {
        return RC_flag;
    }

    RC_flag = initBufferPool(bm, name, 10, RS_LRU, NULL);
    if (RC_flag != RC_OK) {
        return RC_flag;
    }

    // read first page, get how many page are used to store file metadata

    fileMetadataSize = getFileMetaDataSize(bm);

    // get schema as char
    charSchema = (char *)calloc(fileMetadataSize, PAGE_SIZE);

    for (i = 0; i < fileMetadataSize; ++i) {
        RC_flag = pinPage(bm, h, i);
        if (RC_flag != RC_OK) {
            return RC_flag;
        }

        memcpy(charSchema + i * PAGE_SIZE, h, sizeof(PAGE_SIZE));
        RC_flag = unpinPage(bm, h);
        if (RC_flag != RC_OK) {
            return RC_flag;
        }
    }

    charSchema = charSchema + 4 * sizeof(int);

    // process char and convert to specific type

    // assign numAttr
    flag = strchr(charSchema, '<');
    flag++;

    temp = (char *)calloc(40, sizeof(char));
    for (i = 0; 1; ++i) {
        if (*(flag + i) == '>') {
            break;
        }

        temp[i] = flag[i];
    }
    numAttr = atoi(temp);
    free(temp);

    // assign attrNames, dataType, typeLength
    flag = strchr(flag, '(');
    flag++;

    attrNames = (char **)calloc(numAttr, sizeof(char *));
    dataType = (DataType *)calloc(numAttr, sizeof(DataType));
    typeLength = (int *)calloc(numAttr, sizeof(int));

    for (i = 0; i < numAttr; ++i) {
        for (j = 0; 1; ++j) {

            if (*(flag + j) == ':') {
                attrNames[i] = (char *)calloc(j, sizeof(char));
                memcpy(attrNames[i], flag, j);

                if (*(flag + j + 2) == 'I') {
                    dataType[i] = DT_INT;
                    typeLength[i] = 0;
                } else if (*(flag + j + 2) == 'F') {
                    dataType[i] = DT_FLOAT;
                    typeLength[i] = 0;
                } else if (*(flag + j + 2) == 'S') {
                    dataType[i] = DT_STRING;

                    temp = (char *)calloc(40, sizeof(char));
                    for (k = 0; 1; ++k) {
                        if (*(flag + k + j + 9) == ']') {
                            break;
                        }
                        temp[k] = flag[k + j + 9];
                    }

                    typeLength[i] = atoi(temp);
                    free(temp);
                } else if (*(flag + j + 2) == 'B') {
                    dataType[i] = DT_BOOL;
                    typeLength[i] = 0;
                } else {
                    return RC_RM_UNKOWN_DATATYPE;
                }
                // after assign final attribute, flag didn't find next ,
                if (i == numAttr - 1) {
                    break;
                }
                flag = strchr(flag, ',');
                flag = flag + 2;
                break;
            }
        }
    }

    // assign keyAttrs, keySize
    flag = strchr(flag, '(');
    flag++;
    flag2 = flag;

    for (i = 0; 1; ++i) {
        flag2 = strchr(flag2, ',');
        if (flag2 == NULL) break;
        flag2++;
    }

    keySize = i + 1; // assign keySize

    keyAttrs = (int *)calloc(keySize, sizeof(int));

    for (i = 0; i < keySize; ++i) {
        for (j = 0; 1; ++j) {
            if ((*(flag + j) == ',') || (*(flag + j) == ')')) {
                temp = (char *)calloc(100, sizeof(char));
                memcpy(temp, flag, j);
                for (k = 0; k < numAttr; ++k) {
                    if (strcmp(temp, attrNames[k]) == 0) {
                        keyAttrs[i] = k; // assign keyAttrs
                        free(temp);
                        break;
                    }
                }
                if (*(flag + j) == ',') {
                    flag = flag + j + 2;
                }
                else {
                    flag = flag + j;
                }
                break;
            }
        }
    }
    // assign to rel
    schema = createSchema(numAttr, attrNames, dataType, typeLength, keySize, keyAttrs);
    rel->name = name;
    rel->schema = schema;
    rel->bm = bm;
    rel->fh = &fh;

    free(h);
    free(charSchema);
    return RC_OK;
}

/***************************************************************
 * Function Name: closeTable
 *
 * Description: close a table
 *
 * Parameters: RM_TableData *rel
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      03/22/16        Xiaoliang Wu                Complete;
 *
***************************************************************/

RC closeTable (RM_TableData *rel) {
    freeSchema(rel->schema);
    shutdownBufferPool(rel->bm);
    closePageFile(rel->fh);
    return RC_OK;
}

/***************************************************************
 * Function Name: deleteTable
 *
 * Description: delete a table
 *
 * Parameters: char *name
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      03/19/16        Xiaoliang Wu                Complete.
 *
***************************************************************/

RC deleteTable (char *name) {
    return destroyPageFile(name);
}

/***************************************************************
 * Function Name: getNumTuples
 *
 * Description: get the number of record
 *
 * Parameters: RM_TableData *rel
 *
 * Return: int
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      03/23/16        Xiaoliang Wu                Complete.
 *
***************************************************************/

int getNumTuples (RM_TableData *rel) {
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    int numTuples;
    pinPage(rel->bm, h, 0);
    memcpy(&numTuples, h->data + 3 * sizeof(int), sizeof(int));
    unpinPage(rel->bm, h);
    free(h);
    return numTuples;
}

/***************************************************************
 * Function Name:
 *
 * Description:
 *
 * Parameters:
 *
 * Return:
 *
 * Author:
 *
 * History:
 *      Date            Name                        Content
 *
***************************************************************/

RC insertRecord (RM_TableData *rel, Record *record) {
}

/***************************************************************
 * Function Name:
 *
 * Description:
 *
 * Parameters:
 *
 * Return:
 *
 * Author:
 *
 * History:
 *      Date            Name                        Content
 *
***************************************************************/

RC deleteRecord (RM_TableData *rel, RID id) {
}

/***************************************************************
 * Function Name:
 *
 * Description:
 *
 * Parameters:
 *
 * Return:
 *
 * Author:
 *
 * History:
 *      Date            Name                        Content
 *
***************************************************************/

RC updateRecord (RM_TableData *rel, Record *record) {
}

/***************************************************************
 * Function Name:
 *
 * Description:
 *
 * Parameters:
 *
 * Return:
 *
 * Author:
 *
 * History:
 *      Date            Name                        Content
 *
***************************************************************/

RC getRecord (RM_TableData *rel, RID id, Record *record) {
}

/***************************************************************
 * Function Name:startScan
 *
 * Description:initialize a scan by the parameters
 *
 * Parameters:RM_TableData *rel, RM_ScanHandle *scan, Expr *cond
 *
 * Return:RC
 *
 * Author:liu zhipeng
 *
 * History:
 *      Date            Name                        Content
 *03/26/2016    liu zhipeng             first time to implement the function
***************************************************************/

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    scan->rel=rel;
    scan->currentPage=0;
    scan->currentSlot=0;
    scan->expr=cond;
    return RC_OK;
}

/***************************************************************
 * Function Name:next
 *
 * Description:do the search in the scanhanlde and return the next tuple that fulfills the scan condition in parameter "record"
 *
 * Parameters:RM_ScanHandle *scan, Record *record
 *
 * Return:RC
 *
 * Author:liu zhipeng
 *
 * History:
 *      Date            Name                        Content
 *03/26/2016    liu zhipeng             design the outline of the function
***************************************************************/

RC next (RM_ScanHandle *scan, Record *record) 
{
    int index,page,maxslot;
    page=0;
    BM_BufferPool *tmpbm;
    tmpbm=scan->rel->bm;
    BM_PageHandle *ph;
    RID rid;

    Value *result=(Value *)calloc(1,sizeof(Value));
    Record *tmp=(Record *)calloc(1,sizeof(Record));

    index=getFileMetaDataSize(tmpbm)+1;
    pinPage(tmpbm,ph,index);
    while(page!=index)
    {
        memcpy(&page, ph->data + (scan->currentPage) *2* sizeof(int), sizeof(int));
        memcpy(&maxslot, ph->data + ((scan->currentPage) *2+1)* sizeof(int), sizeof(int));
        int i;
        if(scan->currentSlot!=0)
        {
            for(i=scan->currentSlot;i<maxslot;i++)
            {
                rid.page=scan->currentPage;
                rid.slot=i;
                getRecord(scan->rel,rid,tmp);
                evalExpr (tmp, scan->rel->schema, scan->expr,&result);
                if(result->v.boolV)
                {
                    record=tmp;
                    if(i==maxslot)
                    {
                        scan->currentPage++;
                        scan->currentSlot=0;
                    }
                    else
                        scan->currentSlot=i+1;
                    free(result);
                    free(tmp);
                    return RC_OK;
                }
            }
        }
        else
        {
            for(i=0;i<maxslot;i++)
            {
                rid.page=scan->currentPage;
                rid.slot=i;
                getRecord(scan->rel,rid,tmp);
                evalExpr (tmp, scan->rel->schema, scan->expr,&result);
                if(result->v.boolV)
                {
                    record=tmp;
                    if(i==maxslot)
                    {
                        scan->currentPage++;
                        scan->currentSlot=0;
                    }
                    else
                        scan->currentSlot=i+1;
                    free(result);
                    free(tmp);
                    return RC_OK;
                }
            }
        }
        scan->currentPage++;
    }
    return RC_RM_NO_MORE_TUPLES;
}

/***************************************************************
 * Function Name: closeScan
 *
 * Description: free the memo space of this scan
 *
 * Parameters: RM_ScanHandle *scan
 *
 * Return: RC
 *
 * Author: liu zhipeng
 *
 * History:
 *      Date            Name                        Content
 * 03/19/2016    liuzhipeng first time to implement the function
***************************************************************/

RC closeScan (RM_ScanHandle *scan)
{
    free(scan->rel);
    free(scan->mgmtData);
    free(scan);

    return RC_OK;
}

/***************************************************************
 * Function Name: getRecordSize
 *
 * Description: get the size of record described by "schema"
 *
 * Parameters: Schema *schema
 *
 * Return: int
 *
 * Author: liu zhipeng
 *
 * History:
 *      Date            Name                        Content
 * 03/19/2016    liuzhipeng first time to implement the function
***************************************************************/

int getRecordSize (Schema *schema)
{
    int i;
    int result = 0;

    for (i = 0; i < schema->numAttr; i++)
    {
        switch (schema->dataTypes[i])
        {
        case DT_INT:
            result += sizeof(int);
            break;
        case DT_FLOAT:
            result += sizeof(float);
            break;
        case DT_BOOL:
            result += sizeof(bool);
            break;
        case DT_STRING:
            result += schema->typeLength[i];
            break;
        }
    }
    return result;
}

/***************************************************************
 * Function Name: createSchema
 *
 * Description: create a new schema described by the parameters
 *
 * Parameters: int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys
 *
 * Return: Schema
 *
 * Author: liu zhipeng
 *
 * History:
 *      Date            Name                        Content
 * 03/19/2016    liuzhipeng first time to implement the function
***************************************************************/

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *newschema = (Schema*)malloc(sizeof(Schema));

    newschema->numAttr = numAttr;
    newschema->attrNames = attrNames;
    newschema->dataTypes = dataTypes;
    newschema->typeLength = typeLength;
    newschema->keySize = keySize;
    newschema->keyAttrs = keys;

    return newschema;
}

/***************************************************************
 * Function Name: freeSchema
 *
 * Description: free the memo space of this schema
 *
 * Parameters: Schema *schema
 *
 * Return: RC
 *
 * Author: liu zhipeng
 *
 * History:
 *      Date            Name                        Content
 * 03/19/2016    liuzhipeng first time to implement the function
***************************************************************/

RC freeSchema (Schema *schema)
{
    int i;

    free(schema->keyAttrs);
    free(schema->typeLength);
    free(schema->dataTypes);
    for (i = 0; i < schema->numAttr; i++)
        free(schema->attrNames[i]);
    free(schema->attrNames);
    free(schema);

    return RC_OK;
}

/***************************************************************
 * Function Name: createRecord
 *
 * Description: Create a record by the schema
 *
 * Parameters: Record *record, Schema *schema
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/3/18      Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC createRecord (Record **record, Schema *schema) {
    *record = (struct Record *)calloc(1, sizeof(struct Record));
    (*record)->data = (char*)calloc(getRecordSize(schema), sizeof(char));

    return RC_OK;
}

/***************************************************************
 * Function Name: freeRecord
 *
 * Description: Free the space of a record
 *
 * Parameters: Record *record
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/3/18      Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC freeRecord (Record *record) {
    free(record->data);
    free(record);

    return RC_OK;
}

/***************************************************************
 * Function Name: getAttr
 *
 * Description: Get the value of a record
 *
 * Parameters: Record *record, Schema *schema, int attrNum, Value **value
 *
 * Return: RC, value
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/3/18      Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {
    int offset = 0;

    // Calculate offset
    for (int i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
        case DT_INT:
            offset += sizeof(int);
            break;
        case DT_FLOAT:
            offset += sizeof(float);
            break;
        case DT_BOOL:
            offset += sizeof(bool);
            break;
        case DT_STRING:
            offset += schema->typeLength[i];
            break;
        }
    }

    // Get value from record.
    *value = (Value *)malloc(sizeof(Value));
    (*value)->dt = schema->dataTypes[attrNum];
    switch (schema->dataTypes[attrNum])
    {
    case DT_INT:
        memcpy(&((*value)->v.intV), record->data + offset, sizeof(int));
        break;
    case DT_FLOAT:
        memcpy(&((*value)->v.floatV), record->data + offset, sizeof(float));
        break;
    case DT_BOOL:
        memcpy(&((*value)->v.boolV), record->data + offset, sizeof(int));
        break;
    case DT_STRING:
        //We need append \0 in the end of string.
        char end = '\0';
        (*value)->v.stringV = (char *)malloc(schema->typeLength[attrNum] + 1);
        memcpy((*value)->v.stringV, record->data + offset, schema->typeLength[attrNum]);
        memcpy((*value)->v.stringV + schema->typeLength[attrNum], &end, 1);
        break;
    }

    return RC_OK;
}

/***************************************************************
 * Function Name: setAttr
 *
 * Description: Set the value of a record
 *
 * Parameters: Record *record, Schema *schema, int attrNum, Value *value
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/3/18      Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {
    int offset = 0;

    // Calculate offset
    for (int i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
        case DT_INT:
            offset += sizeof(int);
            break;
        case DT_FLOAT:
            offset += sizeof(float);
            break;
        case DT_BOOL:
            offset += sizeof(bool);
            break;
        case DT_STRING:
            offset += schema->typeLength[i];
            break;
        }
    }

    // Set value into record.
    switch (schema->dataTypes[attrNum])
    {
    case DT_INT:
        memcpy(record->data + offset, &(value->v.intV), sizeof(int));
        break;
    case DT_FLOAT:
        memcpy(record->data + offset, &(value->v.floatV), sizeof(float));
        break;
    case DT_BOOL:
        memcpy(record->data + offset, &(value->v.boolV), sizeof(int));
        break;
    case DT_STRING:
        // We need to calculate the strlen of the input string.
        if (strlen(value->v.stringV) >= schema->typeLength[attrNum]) {
            memcpy(record->data + offset, value->v.stringV, schema->typeLength[attrNum]);
        } else {
            strcpy(record->data + offset, value->v.stringV);
        }
        break;
    }

    return RC_OK;
}

/***************************************************************
 * Function Name: addPageMetadataBlock
 *
 * Description: add block that contain pages metadata
 *
 * Parameters: SM_FileHandle *fh
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      03/22/16        Xiaoliang Wu                Complete.
 *
***************************************************************/
RC addPageMetadataBlock(SM_FileHandle *fh) {
    RC RC_flag;
    Value *pageNum, *capacityFlag;
    char * pageMetadataInput;
    int pageMetadataNum;

    int i;
    RC_flag = appendEmptyBlock(fh);
    if (RC_flag != RC_OK) {
        RC_flag = closePageFile(fh);
        return RC_flag;
    }

    pageNum->dt = DT_INT;
    pageNum->v.intV = fh->totalNumPages;
    capacityFlag->dt = DT_INT;
    capacityFlag->v.intV = -1;

    pageMetadataNum = PAGE_SIZE / (strlen(serializeValue(pageNum)) + strlen(serializeValue(capacityFlag)));

    pageMetadataInput = (char *)calloc(PAGE_SIZE, sizeof(char));

    for (i = 0; i < pageMetadataNum; ++i) {
        strcat(pageMetadataInput, serializeValue(pageNum));
        strcat(pageMetadataInput, serializeValue(capacityFlag));
        pageNum->v.intV = i + 1;
        if (i == pageMetadataNum - 1) {
            pageNum->v.intV = fh->totalNumPages - 1;
        }
    }
    RC_flag = writeBlock(fh->totalNumPages - 1, fh, pageMetadataInput);
    if (RC_flag != RC_OK) {
        return RC_flag;
    }

    free(pageMetadataInput);
    return RC_OK;
}

/***************************************************************
 * Function Name: getFileMetaDataSize
 *
 * Description: get file metadata size from file
 *
 * Parameters: BM_BufferPool *bm
 *
 * Return: int
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      03/23           Xiaoliang Wu                Complete.
 *
***************************************************************/

int getFileMetaDataSize(BM_BufferPool *bm) {
    int fileMetadataSize;

    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    pinPage(bm, h, 0);
    memcpy(&fileMetadataSize, h->data, sizeof(int));
    unpinPage(bm, h);
    free(h);
    return fileMetadataSize;
}

/***************************************************************
 * Function Name: recordCostSlot
 *
 * Description: get record size(slot) from file
 *
 * Parameters: BM_BufferPool *bm
 *
 * Return: int
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      03/23/16        Xiaoliang Wu                Complete.
 *
***************************************************************/

int recordCostSlot(BM_BufferPool *bm) {
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    int recordSize;

    pinPage(bm, h, 0);
    memcpy(&recordSize, h->data + sizeof(int), sizeof(int));
    unpinPage(bm, h);
    free(h);
    return recordSize;
}

/***************************************************************
 * Function Name: getSlotSize
 *
 * Description: get slot size from file
 *
 * Parameters: BM_BufferPool *bm
 *
 * Return: int
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      03/23/16        Xiaoliang Wu                Complete.
 *
***************************************************************/

int getSlotSize(BM_BufferPool *bm) {
    int slotSize;
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    pinPage(bm, h, 0);
    memcpy(&slotSize, h->data + 2 * sizeof(int), sizeof(int));
    unpinPage(bm, h);
    free(h);
    return slotSize;
}
