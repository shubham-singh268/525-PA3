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

RC initRecordManager (void *mgmtData){
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

RC shutdownRecordManager (){
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

RC createTable (char *name, Schema *schema){
    RC RC_flag;
    SM_FileHandle fh;
    SM_PageHandle ph;
    int fileMetadataSize;
    int recordSize;
    int slotSize;
    int recordNum;
    char *input;
    char *charSchema;

    int i,j,k;

    // create file
    RC_flag = createPageFile(name);

    if(RC_flag != RC_OK){
        return RC_flag;
    }

    // open file
    RC_flag = openPageFile(name, &fh);

    if(RC_flag != RC_OK){
        return RC_flag;
    }

    // get file metadata size
    fileMetadataSize = strlen(serializeSchema(schema)) + 4*sizeof(int);

    if(fileMetadataSize%PAGE_SIZE == 0){
        fileMetadataSize = fileMetadataSize/PAGE_SIZE;
    }
    else{
        fileMetadataSize = fileMetadataSize/PAGE_SIZE + 1;
    }

    // get metadata and store in file
    recordSize = (getRecordSize(schema)/(slotSize));
    slotSize = 256;
    recordNum = 0;

    input = (char *)calloc(PAGE_SIZE, sizeof(char));

    memcpy(input, &fileMetadataSize, sizeof(int));
    memcpy(input+sizeof(int), &recordSize, sizeof(int));
    memcpy(input+2*sizeof(int), &slotSize, sizeof(int));
    memcpy(input+3*sizeof(int), &recordNum, sizeof(int));

    charSchema = serializeSchema(schema);

    RC_flag = ensureCapacity(fileMetadataSize, &fh);

    if(RC_flag != RC_OK){
        RC_flag = closePageFile(&fh);
        return RC_flag;
    }

    if(strlen(charSchema)<PAGE_SIZE-4*sizeof(int)){
        memcpy(input+4*sizeof(int), charSchema, strlen(charSchema));
        RC_flag = writeBlock(0, &fh, input);
        free(input);
    }else{
        memcpy(input+4*sizeof(int), charSchema, PAGE_SIZE-4*sizeof(int));
        RC_flag = writeBlock(0, &fh, input);
        free(input);
        for(i = 1; i<fileMetadataSize; ++i){
            input = (char *)calloc(PAGE_SIZE, sizeof(char));
            if(i==fileMetadataSize-1){
                memcpy(input, charSchema+i*PAGE_SIZE, strlen(charSchema+i*PAGE_SIZE));
            }else{
                memcpy(input, charSchema+i*PAGE_SIZE, PAGE_SIZE);
            }
            RC_flag = writeBlock(i, &fh, input);
            free(input);
        }
    }
    
    if(RC_flag != RC_OK){
       RC_flag = closePageFile(&fh);
       return RC_flag;
    }

    RC_flag = addPageMetadataBlock(&fh);
    
    if(RC_flag != RC_OK){
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
 *
***************************************************************/

RC openTable (RM_TableData *rel, char *name){
    RC RC_flag;
    SM_FileHandle fh;
    BM_BufferPool *bm = MAKE_POOL();
    BM_PageHandle *h = MAKE_PAGE_HANDLE();

    Schema *schema;
    Value numAttr;
    int *typeLength, keyAttrs;
    int keySize;
    DataType * dataType;
    char **attrNames;

    char * fileMetadataSize;
    char * charSchema;
    Value *fMetadataSize;
    int i;
    char * flag;

    // open file and initial buffer pool
    RC_flag = openPageFile(name, &fh);
    if(RC_flag != RC_OK){
        return RC_flag;
    }

    RC_flag = initBufferPool(bm, name, 10, RS_LRU, NULL);
    if(RC_flag != RC_OK){
        return RC_flag;
    }

    // read first page, get how many page are used to store file metadata
    RC_flag = pinPage(bm, h, 0);

    fileMetadataSize = (char *)calloc(sizeof(int), sizeof(char));
    memcpy(fileMetadataSize, h->data, sizeof(int));
    fMetadataSize = stringToValue(fileMetadataSize);

    // get schema as char
    charSchema = (char *)calloc(100, sizeof(char));
    memcpy(charSchema, h->data+4*sizeof(int), PAGE_SIZE-4*sizeof(int));

    if(fMetadataSize->v.intV > 0){
        for (i = 1; i < fMetadataSize->v.intV; ++i) {
            pinPage(bm, h,1);
            strcat(charSchema, h->data);
        }
    }

    // process char and convert to specific type
    
    flag = strchr(charSchema, '<');

    // assign to rel
    //schema = createSchema();

    rel->name = name;
    rel->schema = schema;
    rel->bm = bm;
    rel->fh = &fh;

    free(h);
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

RC closeTable (RM_TableData *rel){
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

RC deleteTable (char *name){
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
 *
***************************************************************/

int getNumTuples (RM_TableData *rel){
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

RC insertRecord (RM_TableData *rel, Record *record){
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

RC deleteRecord (RM_TableData *rel, RID id){
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

RC updateRecord (RM_TableData *rel, Record *record){
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

RC getRecord (RM_TableData *rel, RID id, Record *record){
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

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{

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

RC next (RM_ScanHandle *scan, Record *record){
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
 * 03/19/2016    liuzhipeng	first time to implement the function
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
 * 03/19/2016    liuzhipeng	first time to implement the function
***************************************************************/

int getRecordSize (Schema *schema)
{
	int i;
	int result=0;

	for(i=0;i<schema->numAttr;i++)
	{
		switch (schema->dataTypes[i])
		{
			case DT_INT:
				result+=sizeof(int);
				break;
			case DT_FLOAT:
				result+=sizeof(float);
				break;
			case DT_BOOL:
				result+=sizeof(bool);
				break;
			case DT_STRING:
				result+=schema->typeLength[i];
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
 * 03/19/2016    liuzhipeng	first time to implement the function
***************************************************************/

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	Schema *newschema=(Schema*)malloc(sizeof(Schema));

	newschema->numAttr=numAttr;
	newschema->attrNames=attrNames;
	newschema->dataTypes=dataTypes;
	newschema->typeLength=typeLength;
	newschema->keySize=keySize;
	newschema->keyAttrs=keys;

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
 * 03/19/2016    liuzhipeng	first time to implement the function
***************************************************************/

RC freeSchema (Schema *schema)
{
	int i;

	free(schema->keyAttrs);
	free(schema->typeLength);
	free(schema->dataTypes);
	for(i=0;i<schema->numAttr;i++)
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
RC createRecord (Record **record, Schema *schema){
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
RC freeRecord (Record *record){
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
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    int offset = 0;
    
    // Calculate offset
    for(int i=0; i< attrNum; i++){
        switch (schema->dataTypes[i]){
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
    (*value)->dt=schema->dataTypes[attrNum];
    switch (schema->dataTypes[attrNum]) 
    {
        case DT_INT:
            memcpy(&((*value)->v.intV), record->data+offset, sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(&((*value)->v.floatV), record->data+offset, sizeof(float));
            break;
        case DT_BOOL:		
            memcpy(&((*value)->v.boolV), record->data+offset, sizeof(int));
            break;
        case DT_STRING:
            //We need append \0 in the end of string.
            char end = '\0';
            (*value)->v.stringV = (char *)malloc(schema->typeLength[attrNum] + 1);
            memcpy((*value)->v.stringV, record->data+offset, schema->typeLength[attrNum]);
            memcpy((*value)->v.stringV+schema->typeLength[attrNum], &end, 1);
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
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    int offset = 0;
    
    // Calculate offset 
    for(int i=0; i< attrNum; i++){
        switch (schema->dataTypes[i]){
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
            memcpy(record->data+offset, &(value->v.intV), sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(record->data+offset, &(value->v.floatV), sizeof(float));
            break;
        case DT_BOOL:
            memcpy(record->data+offset, &(value->v.boolV), sizeof(int));
            break;
        case DT_STRING:
            // We need to calculate the strlen of the input string.
            if(strlen(value->v.stringV) >= schema->typeLength[attrNum]){
                memcpy(record->data + offset, value->v.stringV, schema->typeLength[attrNum]);
            } else {
                strcpy(record->data+offset, value->v.stringV);
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
RC addPageMetadataBlock(SM_FileHandle *fh){
    RC RC_flag;
    Value *pageNum, *capacityFlag;
    char * pageMetadataInput;
    int pageMetadataNum;

    int i;
    RC_flag = appendEmptyBlock(fh);
    if(RC_flag != RC_OK){
        RC_flag = closePageFile(fh);
        return RC_flag;
    }

    pageNum->dt = DT_INT;
    pageNum->v.intV = fh->totalNumPages;
    capacityFlag->dt = DT_INT;
    capacityFlag->v.intV = -1;

    pageMetadataNum = PAGE_SIZE/(strlen(serializeValue(pageNum)) + strlen(serializeValue(capacityFlag)));

    pageMetadataInput = (char *)calloc(PAGE_SIZE, sizeof(char));

    for (i = 0; i < pageMetadataNum; ++i) {
        strcat(pageMetadataInput, serializeValue(pageNum)); 
        strcat(pageMetadataInput, serializeValue(capacityFlag)); 
        pageNum->v.intV = i+1;
        if(i == pageMetadataNum-1){
            pageNum->v.intV = fh->totalNumPages-1;
        }
    }
    RC_flag = writeBlock(fh->totalNumPages-1, fh, pageMetadataInput);
    if(RC_flag != RC_OK){
        return RC_flag;
    }

    free(pageMetadataInput);
    return RC_OK;
}

/***************************************************************
 * Function Name: getFileMetaDataSize
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

int getFileMetaDataSize(BM_BufferPool *bm){
}

/***************************************************************
 * Function Name: recordCostSlot
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

int recordCostSlot(BM_BufferPool *bm){
}

/***************************************************************
 * Function Name: getSlotSize
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

int getSlotSize(BM_BufferPool *bm){
}
