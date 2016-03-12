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
 *
***************************************************************/

RC createTable (char *name, Schema *schema){
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

RC openTable (RM_TableData *rel, char *name){
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

RC closeTable (RM_TableData *rel){
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

RC deleteTable (char *name){
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

RC next (RM_ScanHandle *scan, Record *record){
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

RC closeScan (RM_ScanHandle *scan){
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

int getRecordSize (Schema *schema){
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

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
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

RC freeSchema (Schema *schema){
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

RC createRecord (Record **record, Schema *schema){
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

RC freeRecord (Record *record){
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

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
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

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
}

