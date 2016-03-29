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
    slotSize = 256;
    recordSize = (getRecordSize(schema) / (slotSize));
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
    SM_FileHandle *fh = (SM_FileHandle*)calloc(1, sizeof(SM_FileHandle));
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
    RC_flag = openPageFile(name, fh);

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

    for (i = 0; i < fileMetadataSize; ++i) {
        RC_flag = pinPage(bm, h, i);
        if (RC_flag != RC_OK) {
            return RC_flag;
        }

        RC_flag = unpinPage(bm, h);
        if (RC_flag != RC_OK) {
            return RC_flag;
        }
    }

    charSchema = h->data + 4 * sizeof(int);

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
    rel->fh = fh;

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
    free(rel->bm);
    free(rel->fh);
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
 * Function Name: insertRecord
 *
 * Description: insert a record.
 *
 * Parameters: RM_TableData *rel, Record *record
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/3/27      Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC insertRecord (RM_TableData *rel, Record *record) {
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    int r_size = getRecordSize(rel->schema);
    int p_mata_index = getFileMetaDataSize(rel->bm);
    int r_slotnum = (r_size + sizeof(bool)) / 256 + 1;
    int offset = 0;
    int r_current_num, numTuples;
    bool r_stat = true;
       
    // Find out the target page and slot at the end.
    do {
        pinPage(rel->bm, h, p_mata_index);
        memcpy(&p_mata_index, h->data + PAGE_SIZE - sizeof(int), sizeof(int));
        if(p_mata_index != -1){
            unpinPage(rel->bm, h);
        } else {
            break;
        }
    } while(true);

    // Find out the target meta index and record number of the page.
    do {
        memcpy(&r_current_num, h->data + offset + sizeof(int), sizeof(int));
        offset += 2*sizeof(int);
    } while (r_current_num == PAGE_SIZE / 256);

    // If no page exist, add new page.
    if(r_current_num == -1){       
        // If page mata is full, add new matadata block.
        if(offset == PAGE_SIZE){       
            memcpy(h->data + PAGE_SIZE - sizeof(int), &rel->fh->totalNumPages, sizeof(int));   // Link into new meta data page. 
            addPageMetadataBlock(rel->fh);
            markDirty(rel->bm, h);
            unpinPage(rel->bm, h);      // Unpin the last meta page.
            pinPage(rel->bm, h, rel->fh->totalNumPages-1);  // Pin the new page.
            offset = 2*sizeof(int);
        }
        memcpy(h->data + offset - 2*sizeof(int), &rel->fh->totalNumPages, sizeof(int));  // set page number.
        appendEmptyBlock(rel->fh);
        r_current_num = 0;                                  
    } 

    // Read record->id and set record number add 1 in meta data.
    memcpy(&record->id.page, h->data + offset - 2*sizeof(int), sizeof(int));   // Set record->id page number.
    record->id.slot = r_current_num * r_slotnum;                                // Set record->id slot.
    r_current_num++;                                
    memcpy(h->data + offset - sizeof(int), &r_current_num, sizeof(int));   // Set record number++ into meta data.
    markDirty(rel->bm, h);
    unpinPage(rel->bm, h);              // unpin meta page.

    // Insert record header and record data into page.
    pinPage(rel->bm, h, record->id.page);
    memcpy(h->data + 256*record->id.slot, &r_stat, sizeof(bool));   // Record header is a del_flag 
    memcpy(h->data + 256*record->id.slot + sizeof(bool), record->data, r_size); // Record body is values.
    markDirty(rel->bm, h);
    unpinPage(rel->bm, h);
    // Tuple number add 1.
    pinPage(rel->bm, h, 0);
    memcpy(&numTuples, h->data + 3 * sizeof(int), sizeof(int));
    numTuples++;       
    memcpy(h->data + 3 * sizeof(int), &numTuples, sizeof(int));
    markDirty(rel->bm, h);
    unpinPage(rel->bm, h);

    free(h);
    return RC_OK;
}

/***************************************************************
 * Function Name: deleteRecord
 *
 * Description: delete a record by id
 *
 * Parameters: RM_TableData *rel, RID id
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/3/27      Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC deleteRecord (RM_TableData *rel, RID id) {
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    int r_size = getRecordSize(rel->schema);
    char* r_deleted = (char *)calloc(sizeof(bool) + r_size, sizeof(char));
    int numTuples;
    
    // Delete record and mark record status as false(calloc 0).
    pinPage(rel->bm, h, id.page);
    memcpy(h->data + 256*id.slot, r_deleted, sizeof(bool) + r_size);
    markDirty(rel->bm, h);
    unpinPage(rel->bm, h);
    
    // Tuple number minus 1.
    pinPage(rel->bm, h, 0);
    memcpy(&numTuples, h->data + 3 * sizeof(int), sizeof(int));
    numTuples--;       
    memcpy(h->data + 3 * sizeof(int), &numTuples, sizeof(int));
    markDirty(rel->bm, h);
    unpinPage(rel->bm, h);
    
    free(r_deleted);
    free(h);
    return RC_OK;
}

/***************************************************************
 * Function Name: updateRecord
 *
 * Description: update a record by its id
 *
 * Parameters: RM_TableData *rel, Record *record
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/3/27      Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC updateRecord (RM_TableData *rel, Record *record) {
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    int r_size = getRecordSize(rel->schema);
    
    pinPage(rel->bm, h, record->id.page);
    memcpy(h->data + 256*record->id.slot + sizeof(bool), record->data, r_size);
    markDirty(rel->bm, h);
    unpinPage(rel->bm, h);
    
    free(h);
    return RC_OK;
}

/***************************************************************
 * Function Name: getRecord
 *
 * Description: get a record by id
 *
 * Parameters: RM_TableData *rel, RID id, Record *record
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/3/27      Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC getRecord (RM_TableData *rel, RID id, Record *record) {
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    int r_size = getRecordSize(rel->schema);
    bool r_stat;
    
    record->id = id;
    pinPage(rel->bm, h, id.page);

    // If the record status not valid.(not exist or wrong status or deleted)
    memcpy(&r_stat, h->data + 256*id.slot, sizeof(bool));
    if(r_stat != true){
        free(h);
        return RC_RM_RECORD_NOT_EXIST;
    } else {
        record->data = (char*) malloc(r_size);
        memcpy(record->data, h->data + 256*id.slot + sizeof(bool), r_size);
        unpinPage(rel->bm, h);
        free(h);
        return RC_OK;
    }
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
    int index,maxslot,rpage,trs,rc;
    BM_BufferPool *tmpbm;
    tmpbm=scan->rel->bm;
    BM_PageHandle *ph=(BM_PageHandle*)calloc(1,sizeof(BM_PageHandle));
    RID rid;

    Value *result=(Value *)calloc(1,sizeof(Value));
    Record *tmp=(Record *)calloc(1,sizeof(Record));


//printf("ceshi : %s\n",scan->rel->name);
    trs=(getRecordSize (scan->rel->schema)+sizeof(bool))/256+1;
    index=getFileMetaDataSize(tmpbm);
    
    pinPage(tmpbm,ph,index);

    while(scan->currentPage!=index)
    {
        memcpy(&rpage,ph->data+(scan->currentPage)*2*sizeof(int),sizeof(int));
        memcpy(&maxslot, ph->data + ((scan->currentPage) *2+1)* sizeof(int), sizeof(int));
        int i;
        if(maxslot!=-1)
        {
            for(i=scan->currentSlot;i<maxslot;i++)
            {
                rid.page=rpage;
                rid.slot=i*trs;
                if((rc=getRecord(scan->rel,rid,tmp))==RC_OK)
                {   
                    evalExpr (tmp, scan->rel->schema, scan->expr,&result);;
                    if(result->v.boolV)
                    {
                        record->id.page=rid.page;
                        record->id.slot=rid.slot;
                        record->data=tmp->data;
                        if(i==maxslot-1)
                        {
                            scan->currentPage++;
                            scan->currentSlot=0;
                        }
                        else{
                            scan->currentSlot=i+1;
                        }
                        free(result);
                        free(tmp);
                        unpinPage (tmpbm, ph);
                        free(ph);
                        return RC_OK;
                    }
                }
            }
        }
        else
        {
            unpinPage(tmpbm,ph);
            free(ph);
            return RC_RM_NO_MORE_TUPLES;
        }
        scan->currentPage++;
    }
    unpinPage (tmpbm, ph);
    free(ph);
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
    //free(scan->rel);
    //free(scan->mgmtData);
    //free(scan);

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
    int i,offset = 0;
    char end = '\0';

    // Calculate offset
    for (i = 0; i < attrNum; i++) {
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
        // We need append end:\0 in the end of string.
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
    int i, offset = 0;

    // Calculate offset
    for (i = 0; i < attrNum; i++) {
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
    int pageNum, capacity;
    char * pageMetadataInput;
    int pageMetadataNum;

    int i;
    RC_flag = appendEmptyBlock(fh);
    if (RC_flag != RC_OK) {
        RC_flag = closePageFile(fh);
        return RC_flag;
    }


    pageMetadataNum = PAGE_SIZE / (2*sizeof(int));

    pageMetadataInput = (char *)calloc(PAGE_SIZE, sizeof(char));
    pageNum = fh->totalNumPages;
    capacity = -1;

    for (i = 0; i < pageMetadataNum; ++i){ 
        memcpy(pageMetadataInput+i*2*sizeof(int), &pageNum, sizeof(int));
        memcpy(pageMetadataInput+i*2*sizeof(int)+sizeof(int), &capacity, sizeof(int));
        pageNum++;
        if (i == pageMetadataNum - 1) {
            pageNum = fh->totalNumPages - 1;
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
