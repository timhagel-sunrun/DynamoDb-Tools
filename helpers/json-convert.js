const fs = require('fs');


const parseJSONFile = (jsonFile) => {

  const jsonDataStr = fs.readFileSync(jsonFile);
  const items = JSON.parse(jsonDataStr);
  if (!Array.isArray(items)) {
    console.log('json file should contain array of items');
  }
  return items;
}


//export convertJsonToDynamoDbBatch;
module.exports = { parseJSONFile }