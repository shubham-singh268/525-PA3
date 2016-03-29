#include <stdio.h> 
#include <stdlib.h> 
#include <fcntl.h> 
#include <sys/types.h> 
#include <sys/stat.h> 
#include <errno.h> 
#include <string.h> 
#include <limits.h> 
#include "storage_mgr.h" 

/************************************************************
 *                    handle data structures                *
 ************************************************************/
/*
typedef struct SM_FileHandle {
  char *fileName;
  int totalNumPages;
  int curPagePos;
  void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;
*/

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
/*
extern void initStorageManager (void);
extern RC createPageFile (char *fileName);
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);
extern RC closePageFile (SM_FileHandle *fHandle);
extern RC destroyPageFile (char *fileName);
*/

/* reading blocks from disc */
/*
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern int getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
*/

/* writing blocks to a page file */
/*
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock (SM_FileHandle *fHandle);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle);
*/

/* manipulating page files */


/***************************************************************
 * Function Name: initStorageManager
 * 
 * Description:to let user know enter the StorageManager
 *
 * Parameters:void
 *
 * Return:void
 *
 * Author:Chuanwei Tu
 *
 * History:
 *      Date            Name                        Content
 *      2016/2/1    	 Chuanwei Tu				  initializing the Storage Manager 
 *
***************************************************************/


void initStorageManager(void){
	
   
	printf("--------Initialzing the StorageManager----------");   // to let user know enter the StorageManager
	printf("--------the StorageManager version is 1.00---------- \n");
	//return;
}



/***************************************************************
 * Function Name: createPageFile
 * 
 * Description: Create a new page file fileName. The initial file size should be one page. This method should fill this single page with '\0' bytes.
 *
 * Parameters: char *fileName
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      --------------  --------------------------  ----------------
 *      2016/02/07      Xiaoliang Wu                implement function
 *
***************************************************************/


RC createPageFile(char *fileName){
    FILE *fp;
    char fill[PAGE_SIZE];
    int write_result;

    fp = fopen(fileName,"w+");

    if(fp == NULL){
        fclose(fp);
        return RC_CREATE_FILE_FAIL;
    }

    memset(fill, '\0', sizeof(fill));
    write_result = fwrite(fill, 1, PAGE_SIZE, fp);

    if(write_result != PAGE_SIZE){
        fclose(fp);
        destroyPageFile(fileName);
        return RC_CREATE_FILE_FAIL;
    }

    fclose(fp);
    return RC_OK;
}

/***************************************************************
 * Function Name: openPageFile
 * 
 * Description: Opens an existing page file.
 *
 * Parameters:char *fileName ,SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      2016/02/07      Xiaoliang Wu                implement function
 *
***************************************************************/


RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    FILE *fp;
    int size;
    int seek_result;

    fp = fopen(fileName, "r");

    if(fp == NULL){
        return RC_FILE_NOT_FOUND;
    }

    seek_result = fseek(fp, 0, SEEK_END);

    if(seek_result != 0){
        fclose(fp);
        return RC_GET_NUMBER_OF_BYTES_FAILED;
    }

    size = ftell(fp);
    if(size == -1){
        fclose(fp);
        return RC_GET_NUMBER_OF_BYTES_FAILED;
    }

    fHandle->fileName = fileName;
    if(size%PAGE_SIZE == 0){
    	fHandle->totalNumPages = (size / PAGE_SIZE);
    }else{
    	fHandle->totalNumPages = (size/PAGE_SIZE +1);
    }
    fHandle->curPagePos = 0;
    
    fclose(fp);
    return RC_OK;

}

/***************************************************************
 * Function Name: closePageFile
 * 
 * Description: Close an open page file
 *
 * Parameters: SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu
 *
 * History:
 *      Date            Name                        Content
 *      2016/02/07      Xiaoliang Wu                implement function
 *
***************************************************************/


RC closePageFile (SM_FileHandle *fHandle){
    fHandle->fileName = "";
    fHandle->curPagePos = 0;
    fHandle->totalNumPages = 0;
    return RC_OK;
}

/***************************************************************
 * Function Name: destroyPageFile
 * 
 * Description: destroy (delete) a page file
 *
 * Parameters: SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xiaoliang Wu 
 *
 * History:
 *      Date            Name                        Content
 *      2016/02/07      Xiaoliang Wu                implement function
 *
***************************************************************/


RC destroyPageFile (char *fileName){
    int remove_result;
    remove_result = remove(fileName);
    if(remove_result == 0){
        return RC_OK;
    }else{
        return RC_FILE_NOT_FOUND;
    }
}

/* reading blocks from disc */


/***************************************************************
 * Function Name: readBlock
 * 
 * Description: read the pageNum block from the file defined by fHandle into address memPage
 *
 * Parameters:int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage
 *
 * Return:RC
 *
 * Author:Zhipeng Liu
 *
 * History:
 *      Date            Name                        Content
 *      2016/1/30      Zhipeng Liu            first time to implement the function
 *
***************************************************************/


RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(pageNum>fHandle->totalNumPages-1||pageNum<0)
		return RC_READ_NON_EXISTING_PAGE;
	else
	{
		FILE *fp;
		fp=fopen(fHandle->fileName,"r");
		int offset;
		offset=fHandle->curPagePos*PAGE_SIZE;
		fseek(fp,offset,SEEK_SET);
		fread(memPage,sizeof(char),PAGE_SIZE,fp);
		fHandle->curPagePos=pageNum;
		fclose(fp);
		return RC_OK;
	}
}

/***************************************************************
 * Function Name: getBlockPos
 * 
 * Description:return the current block position in the file
 *
 * Parameters:SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author:Zhipeng Liu
 *
 * History:
 *      Date            Name                        Content
 *      2016/1/30      Zhipeng Liu            first time to implement the function
 *
***************************************************************/


int getBlockPos (SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}

/***************************************************************
 * Function Name:  readFirstBlock
 * 
 * Description:read the first block of the file described in fHandle
 *
 * Parameters:SM_FileHandle *fHandle, SM_PageHandle memPage
 *
 * Return:RC
 *
 * Author:Zhipeng Liu
 *
 * History:
 *      Date                     Name                        Content
 *      2016/1/30      Zhipeng Liu                    first time to implement the function
 *
***************************************************************/


RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	FILE *fp;
	fp=fopen(fHandle->fileName,"r");
	fseek(fp,0,SEEK_SET);
	fread(memPage,sizeof(char),PAGE_SIZE,fp);
	fHandle->curPagePos=0;
	fclose(fp);
	return RC_OK;
}

/***************************************************************
 * Function Name:readPreviousBlock
 * 
 * Description:read the previousblock in this file into the address witch memPage pointed
 *
 * Parameters:SM_FileHandle *fHandle, SM_PageHandle memPage
 *
 * Return:RC
 *
 * Author:Zhipeng Liu
 *
 * History:
 *      Date                     Name                             Content
 *      2016/1/27       Zhipeng Liu                    first time to implement the function
 *
***************************************************************/


RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(fHandle->curPagePos<=0||fHandle->curPagePos>fHandle->totalNumPages-1)
		return RC_READ_NON_EXISTING_PAGE;
	else
	{
		FILE *fp;
		fp=fopen(fHandle->fileName,"r");
		int offset=(fHandle->curPagePos-1)*PAGE_SIZE;
		fseek(fp,offset,SEEK_SET);
		fread(memPage,sizeof(char),PAGE_SIZE,fp);
		fHandle->curPagePos=fHandle->curPagePos-1;
		fclose(fp);
		return RC_OK;
	}
}

/***************************************************************
 * Function Name: readCurrentBlock
 * 
 * Description: read the current block (pointed by the fHandle->curPagePos) into memo address memPage
 *
 * Parameters: SM_FileHandle *fHandle, SM_PageHandle memPage
 *
 * Return: RC
 *
 * Author: Zhipeng Liu
 *
 * History:
 *      Date            Name                        Content
 *   2016/1/27     Zhipeng Liu             first time to implement the function
 *
***************************************************************/


RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(fHandle->curPagePos<0||fHandle->curPagePos>fHandle->totalNumPages-1)
		return RC_READ_NON_EXISTING_PAGE;
	else
	{
		FILE *fp;
		fp=fopen(fHandle->fileName,"r");
		int offset=fHandle->curPagePos*PAGE_SIZE;
		fseek(fp,offset,SEEK_SET);
		fread(memPage,sizeof(char),PAGE_SIZE,fp);
		fclose(fp);
		return RC_OK;
	}
}

/***************************************************************
 * Function Name: readNextBlock
 * 
 * Description: read the next block (the current block defined in the fHandle->curPagePos) in to memo address memPage
 *
 * Parameters: SM_FileHandle *fHandle, SM_PageHandle memPage
 *
 * Return: RC
 *
 * Author: Zhipeng Liu
 *
 * History:
 *      Date            Name                        Content
 *   2016/1/27     Zhipeng Liu             first time to implement the function
 *
***************************************************************/


RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(fHandle->curPagePos<0||fHandle->curPagePos>fHandle->totalNumPages-2)
		return RC_READ_NON_EXISTING_PAGE;
	else
	{
		FILE *fp;
		fp=fopen(fHandle->fileName,"r");
		int offset=(fHandle->curPagePos+1)*PAGE_SIZE;
		fseek(fp,offset,SEEK_SET);
		fread(memPage,sizeof(char),PAGE_SIZE,fp);
		fHandle->curPagePos=fHandle->curPagePos+1;
		fclose(fp);
		return RC_OK;
	}
}

/***************************************************************
 * Function Name: readLastBlock
 * 
 * Description: read the last block in this file into memo address memPage
 *
 * Parameters: SM_FileHandle *fHandle, SM_PageHandle memPage
 *
 * Return: RC
 *
 * Author: Zhipeng Liu
 *
 * History:
 *      Date            Name                        Content
 *   2016/1/27     Zhipeng Liu             first time to implement the function
 *
***************************************************************/


RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	FILE *fp;
	fp=fopen(fHandle->fileName,"r");
	int offset=-PAGE_SIZE;
	fseek(fp,offset,SEEK_END);
	fread(memPage,sizeof(char),PAGE_SIZE,fp);
	fHandle->curPagePos=fHandle->totalNumPages-1;
	fclose(fp);
	return RC_OK;
}

/* writing blocks to a page file */

/***************************************************************
 * Function Name: writeBlock 
 * 
 * Description: Write a page to disk using either the current position or an absolute position.
 *
 * Parameters: int numberOfPages, SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/2		Xincheng Yang             first time to implement the function
 *   2016/2/2		Xincheng Yang			  modified some codes
 *
***************************************************************/
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	ensureCapacity (pageNum, fHandle);		//Make sure the program have enough capacity to write block.
	
	FILE *fp;
	RC rv;

	fp=fopen(fHandle->fileName,"rb+");
	if(fseek(fp,pageNum * PAGE_SIZE, SEEK_SET) != 0){
		rv = RC_READ_NON_EXISTING_PAGE;	
	} else if (fwrite(memPage, PAGE_SIZE, 1, fp) != 1){
		rv = RC_WRITE_FAILED; 
	} else {
		fHandle->curPagePos=pageNum;		//Success write block, then curPagePos should be changed.
		rv = RC_OK;
	}

	fclose(fp);
	return rv;
}


/***************************************************************
 * Function Name: writeCurrentBlock 
 * 
 * Description: Write a page to disk using either the current position or an absolute position.
 *
 * Parameters: int numberOfPages, SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/2		Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	} 
	if(fHandle->curPagePos < 0){
		return RC_WRITE_FAILED;
	}

	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

/***************************************************************
 * Function Name: appendEmptyBlock 
 * 
 * Description: Increase the number of pages in the file by one. The new last page should be filled with zero bytes.
 *
 * Parameters: int numberOfPages, SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/1		Xincheng Yang             first time to implement the function
 *   2016/2/2		Xincheng Yang			  modified some codes
 *
***************************************************************/
RC appendEmptyBlock (SM_FileHandle *fHandle){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	} 

	FILE *fp;
	char *allocData;
	RC rv;

	allocData = (char *)calloc(1, PAGE_SIZE);
	fp=fopen(fHandle->fileName,"ab+");

	if(fwrite(allocData, PAGE_SIZE, 1, fp) != 1)   
	{
		rv = RC_WRITE_FAILED;
	} else {
		fHandle -> totalNumPages += 1;
		rv = RC_OK;		
	}

	free(allocData);
	fclose(fp);

	return  rv;
}

/***************************************************************
 * Function Name: ensureCapacity 
 * 
 * Description: If the file has less than numberOfPages pages then increase the size to numberOfPages.
 *
 * Parameters: int numberOfPages, SM_FileHandle *fHandle
 *
 * Return: RC
 *
 * Author: Xincheng Yang
 *
 * History:
 *      Date            Name                        Content
 *   2016/2/1		Xincheng Yang             first time to implement the function
 *
***************************************************************/
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
	if(fHandle == NULL){
		return RC_FILE_HANDLE_NOT_INIT;
	} 
	if(fHandle -> totalNumPages >= numberOfPages){
		return RC_OK;
	}
	
	FILE *fp;
	long allocCapacity;
	char *allocData;
	RC rv;

	allocCapacity= (numberOfPages - fHandle -> totalNumPages) * PAGE_SIZE;
	allocData = (char *)calloc(1,allocCapacity);
	
	fp=fopen(fHandle->fileName,"ab+");
   
	if(fwrite(allocData, allocCapacity, 1, fp) == 0)   
	{
		rv = RC_WRITE_FAILED;
	} else {
		fHandle -> totalNumPages = numberOfPages;		//When write success, totalNumPages should be changed to numberOfPages.	
		rv = RC_OK;
	}

	free(allocData);
	fclose(fp);

	return rv;
}
