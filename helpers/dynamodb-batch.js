
const { DynamoDB } = require("@aws-sdk/client-dynamodb");
const { marshall } = require("@aws-sdk/util-dynamodb");
const { table } = require("console");
const fs = require('fs');

const sleep = (timeoutMs) =>  { 
    return new Promise(resolve => {
        setTimeout(() => { resolve(); }, timeoutMs)
    });
}

const convertDataToDynamoDbBatch = (tableName, items) => {
    const batchData = {
      "RequestItems": {
        [tableName]: items.map(item => {
          return {
            "PutRequest": {
              "Item": marshall(item),
              // May need to use batchGetItem in order to filter out the items that already exist or possibly let the writes fail.
              //"ConditionExpression": `attribute_not_exists(${primaryKey})`
            }
          }
        })
      }
    };
  
    return batchData;
  
  }

const filterExistingItems = async (client, tableName, keyField, keyFieldType, items) => {
  
  const chunkSize = 100;
  const filteredItems = [];

  for (let i = 0; i < items.length; i += chunkSize) {
      const chunkedItems = items.slice(i, i + chunkSize);
      const batchGetCommand = {
        "RequestItems": {
          [tableName]: {
            "AttributesToGet": [keyField],
            "Keys": chunkedItems.map(item => { 
                return {
                  [keyField]: { [keyFieldType]: item[keyField] }
                }
              })
          }
        }
      }
    
      const results = await client.batchGetItem(batchGetCommand);
      const resultItems = results.Responses[tableName];
      const filteredChunk = chunkedItems.filter(item => {
        const itemExists = resultItems.some(resultItem => item[keyField] === resultItem?.[keyField]?.[keyFieldType]);
        return !itemExists;
      }) || [];
      filteredItems.push(...filteredChunk);
    }
    return filteredItems;
}
const writeBatch = async (clientConfig, tableName, keyField, keyFieldType, items) =>{
    
    const unprocessedItems = [];
    const client = new DynamoDB(clientConfig);
    // const documentClient = DynamoDBDocumentClient.from(client);
    // documentClient.batchWriteItem()

    const chunkSize = 25;
    const nonExistingItems = await filterExistingItems(client, tableName, keyField, keyFieldType, items )

    for (let i = 0; i < nonExistingItems.length; i += chunkSize) {
        const chunkedItems = nonExistingItems.slice(i, i + chunkSize);
        const chunkedBatch = convertDataToDynamoDbBatch(tableName, chunkedItems);
        try {
            const chunkInsertResults = await client.batchWriteItem(chunkedBatch); // TBD
            unprocessedItems.push(...(chunkInsertResults?.UnprocessedItems?.[tableName] || []))
        } catch (ex) {
            const e = ex;
            console.log('batchWriteItem operation failed');
            console.log(ex);
            console.log('-----------');

            chunkedBatch[tableName] && unprocessedItems.push(...chunkedBatch[tableName]);
        }
        // await sleep(1000); //Implement retry policy instead        
    }


    const totalItems = items.length;
    const totalNonExistingItems = nonExistingItems.length;
    const totalImportedItems = totalNonExistingItems - unprocessedItems.length;

    console.log(`Total Records: ${totalItems}`);
    console.log(`Total Non-existing Records: ${totalNonExistingItems}`);
    console.log(`Total imported items: ${totalImportedItems}`);
    console.log(`Total unprocessed items: ${unprocessedItems.length}`)
    const unprocessedItemsOutputFile = './data/testdata-unprocessed.json'
    fs.writeFileSync(unprocessedItemsOutputFile, JSON.stringify(unprocessedItems));
}

module.exports = { writeBatch }